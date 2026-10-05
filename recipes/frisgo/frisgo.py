"""OpenRecon image-to-image Fuzzy Ripple correction for BOLD time series.

All reconstructed images of a run are collected, assembled into a 4D NIfTI
time series, corrected with LayNii ``LN2_FRISGO -tshift`` and returned to the
scanner as two source-geometry 2D streams: the original time series and the
FRISGO-corrected time series. Scanner identity and IceMiniHead handling reuse
the helpers of the ``openreconi2iexample`` reference module.
"""
import itertools
import logging
import os
import subprocess
import tempfile
import traceback

import constants
import ismrmrd
import nibabel as nib
import numpy as np

import openreconi2iexample as i2i


VERSION_ENV_VAR = "FRISGO_VERSION"
EXECUTABLE_ENV_VAR = "FRISGO_EXECUTABLE"
ORIGINAL_SERIES_INDEX = 100
SERIES_INDEX_STRIDE = 2
ORIGINAL_SERIES_NAME = "openrecon_original"
FRISGO_SERIES_NAME = "openrecon_frisgo"
FRISGO_IMAGE_TYPE_TOKEN = "FRISGO"
FRISGO_HISTORY = ["PYTHON", "LAYNII", "LN2_FRISGO_TSHIFT"]
MIN_REPETITIONS = 4
SLICE_POSITION_DECIMALS = 3
DEFAULT_REPETITION_TIME_SECONDS = 1.0
MAX_IMAGES_PER_SERIES = 65535
MAX_SERIES_INDEX = 65535
SEND_BATCH_SIZE = 128


class TimeSeries:
    """Images of one source series arranged on a dense [time][slice] grid."""

    def __init__(self, grid, repetition_time):
        self.grid = grid
        self.repetition_time = repetition_time

    @property
    def n_repetitions(self):
        return len(self.grid)

    @property
    def n_slices(self):
        return len(self.grid[0])

    def volume(self):
        """Return the time series as a float32 [x, y, slice, time] array."""
        first = np.asarray(self.grid[0][0].data)
        n_y, n_x = first.shape[-2:]
        data = np.zeros((n_x, n_y, self.n_slices, self.n_repetitions), np.float32)
        for t, volume in enumerate(self.grid):
            for z, image in enumerate(volume):
                data[:, :, z, t] = _image_plane(image).T
        return data

    def affine(self):
        return nifti_affine(self.grid[0])


def process(connection, config, metadata):
    logging.info("frisgo runtime version=%s", os.environ.get(VERSION_ENV_VAR, "unknown"))
    logging.info("Config: %s", config)
    input_images = []

    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                input_images.append(item)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        send_original = i2i._config_bool(config, "sendoriginal", default=True)
        send_frisgo = i2i._config_bool(config, "sendfrisgo", default=True)
        logging.info(
            "Configured outputs: original=%s frisgo=%s (%d received image(s))",
            send_original,
            send_frisgo,
            len(input_images),
        )

        groups = _source_series_groups(input_images)
        original_index = ORIGINAL_SERIES_INDEX
        output_indexes = []
        for group_images in groups:
            chunk_count = (len(group_images) + MAX_IMAGES_PER_SERIES - 1) // MAX_IMAGES_PER_SERIES
            next_index = original_index + SERIES_INDEX_STRIDE * chunk_count
            if next_index - 1 > MAX_SERIES_INDEX:
                raise ValueError("too many output series for the MRD series index")
            output_indexes.append(original_index)
            original_index = next_index

        for group_index, (group_images, original_index) in enumerate(zip(groups, output_indexes)):
            if send_original:
                _send_outputs(connection, _original_outputs(group_images, original_index))
            if not send_frisgo:
                continue
            if not _is_magnitude(group_images[0]):
                logging.info("Source group %d is not magnitude; FRISGO skipped", group_index)
                continue
            try:
                repetition_time = _repetition_time_seconds(metadata, group_images[0])
                series = assemble_time_series(group_images, repetition_time)
                logging.info(
                    "Source group %d: %d repetition(s) x %d slice(s)",
                    group_index, series.n_repetitions, series.n_slices,
                )
                if series.n_repetitions < MIN_REPETITIONS:
                    logging.warning(
                        "Source group %d has %d repetition(s); LN2_FRISGO -tshift "
                        "needs at least %d, FRISGO skipped",
                        group_index, series.n_repetitions, MIN_REPETITIONS,
                    )
                    continue
                corrected = run_frisgo(series)
            except Exception:
                logging.error(traceback.format_exc())
                connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
                continue
            _send_outputs(connection, _frisgo_outputs(series, corrected, original_index + 1))
            del corrected

    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
    finally:
        connection.send_close()


def run_frisgo(series):
    """Run ``LN2_FRISGO -tshift`` on the series and return the corrected volume."""
    executable = os.environ.get(EXECUTABLE_ENV_VAR, "LN2_FRISGO")
    with tempfile.TemporaryDirectory(prefix="frisgo_") as work_dir:
        input_path = os.path.join(work_dir, "timeseries.nii")
        output_path = os.path.join(work_dir, "timeseries_Tshift.nii")
        volume = series.volume()
        image = nib.Nifti1Image(volume, series.affine())
        image.header.set_xyzt_units(xyz="mm", t="sec")
        zooms = list(image.header.get_zooms())
        zooms[3] = series.repetition_time
        image.header.set_zooms(zooms)
        nib.save(image, input_path)
        volume_shape = volume.shape
        del image, volume

        command = [executable, "-input", input_path, "-tshift"]
        logging.info("Running %s", " ".join(command))
        result = subprocess.run(
            command,
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        logging.info("LN2_FRISGO output:\n%s", result.stdout)
        if result.returncode != 0 or not os.path.exists(output_path):
            raise RuntimeError(
                f"LN2_FRISGO failed with exit code {result.returncode}; "
                f"expected output {output_path}"
            )
        corrected = np.asarray(nib.load(output_path).dataobj, dtype=np.float32)

    if corrected.shape != volume_shape:
        raise RuntimeError(
            f"LN2_FRISGO output shape {corrected.shape} does not match the input {volume_shape}"
        )
    return corrected


def assemble_time_series(images, repetition_time=DEFAULT_REPETITION_TIME_SECONDS):
    """Arrange one source series on a dense [time][slice] grid.

    Slices are ordered by physical position along the slice normal, so
    interleaved slice counters cannot scramble the volume. Time points use the
    MRD repetition counter, or arrival order per slice when the scanner leaves
    every repetition counter equal.
    """
    shapes = {np.asarray(image.data).shape for image in images}
    if len(shapes) != 1:
        raise ValueError(f"source series mixes image shapes {sorted(shapes)}")
    shape = shapes.pop()
    if int(np.prod(shape[:-2])) != 1:
        raise ValueError(
            f"expected single-channel 2D images, received data shape {shape}"
        )

    slice_axis = i2i._infer_slice_axis([image.getHead() for image in images])
    positions = [
        round(i2i._projected_position(image, slice_axis), SLICE_POSITION_DECIMALS)
        for image in images
    ]
    slice_positions = sorted(set(positions))
    slice_of = {position: z for z, position in enumerate(slice_positions)}

    repetitions = [int(image.repetition) for image in images]
    if len(set(repetitions)) > 1:
        repetition_ids = sorted(set(repetitions))
        if any(b != a + 1 for a, b in zip(repetition_ids, repetition_ids[1:])):
            raise ValueError("repetition counter gap; temporal interpolation requires consecutive samples")
        rank = {repetition: t for t, repetition in enumerate(repetition_ids)}
        time_of = [rank[repetition] for repetition in repetitions]
    else:
        seen = {}
        time_of = []
        for position in positions:
            time_of.append(seen.get(position, 0))
            seen[position] = time_of[-1] + 1

    n_repetitions = max(time_of) + 1
    grid = [[None] * len(slice_positions) for _ in range(n_repetitions)]
    for image, position, t in zip(images, positions, time_of):
        z = slice_of[position]
        if grid[t][z] is not None:
            raise ValueError(f"duplicate image for repetition {t}, slice {z}")
        grid[t][z] = image
    missing = [
        (t, z)
        for t, volume in enumerate(grid)
        for z, image in enumerate(volume)
        if image is None
    ]
    if missing:
        raise ValueError(
            f"incomplete time series: {len(missing)} of "
            f"{n_repetitions * len(slice_positions)} image(s) missing, "
            f"first missing (repetition, slice) {missing[0]}"
        )
    return TimeSeries(grid, repetition_time)


def nifti_affine(volume_images):
    """RAS affine for slices ordered along the slice normal (MRD positions are LPS centres)."""
    first = volume_images[0].getHead()
    n_x, n_y = int(first.matrix_size[0]), int(first.matrix_size[1])
    read_dir = i2i._header_vector(first, "read_dir")
    phase_dir = i2i._header_vector(first, "phase_dir")
    d_x = float(first.field_of_view[0]) / n_x
    d_y = float(first.field_of_view[1]) / n_y

    first_position = i2i._header_position(first)
    if len(volume_images) > 1:
        last_position = i2i._header_position(volume_images[-1].getHead())
        slice_step = (last_position - first_position) / (len(volume_images) - 1)
    else:
        slice_step = i2i._header_vector(first, "slice_dir") * float(first.field_of_view[2])

    corner = first_position - read_dir * d_x * (n_x - 1) / 2 - phase_dir * d_y * (n_y - 1) / 2
    lps_to_ras = np.diag([-1.0, -1.0, 1.0])
    affine = np.eye(4)
    affine[:3, 0] = lps_to_ras @ (read_dir * d_x)
    affine[:3, 1] = lps_to_ras @ (phase_dir * d_y)
    affine[:3, 2] = lps_to_ras @ slice_step
    affine[:3, 3] = lps_to_ras @ corner
    return affine


def _original_outputs(images, series_index):
    return _outputs(((image, image.data) for image in images), series_index)


def _frisgo_outputs(series, corrected, series_index):
    planes = (
        (source, i2i._cast_like(corrected[:, :, z, t].T.reshape(source.data.shape), source.data))
        for t, volume in enumerate(series.grid)
        for z, source in enumerate(volume)
    )
    return _outputs(planes, series_index, derived=True)


def _outputs(source_data_pairs, series_index, derived=False):
    suffix = "frisgo" if derived else "original"
    fallback_name = FRISGO_SERIES_NAME if derived else ORIGINAL_SERIES_NAME
    for offset, (source_image, data) in enumerate(source_data_pairs):
        chunk, output_index = divmod(offset, MAX_IMAGES_PER_SERIES)
        output_series_index = series_index + SERIES_INDEX_STRIDE * chunk
        if output_index == 0:
            identity = i2i._build_output_series_identity(
                source_image, output_series_index, suffix, fallback_name
            )
        output = _copy_with_data(source_image, data)
        _stamp_time_series_image(
            output, source_image, output_series_index, output_index, identity, derived=derived
        )
        yield output


def _send_outputs(connection, outputs):
    outputs = iter(outputs)
    while batch := list(itertools.islice(outputs, SEND_BATCH_SIZE)):
        _validate_outputs(batch)
        connection.send_image(batch)


def _copy_with_data(source_image, data):
    output = ismrmrd.Image.from_array(data, transpose=False)
    header = source_image.getHead()
    header.data_type = output.data_type
    output.setHead(header)
    return output


def _stamp_time_series_image(
    output,
    source_image,
    series_index,
    output_index,
    series_identity,
    derived=False,
):
    """Give a returned image new series identity while keeping source geometry.

    The source slice and repetition counters stay intact so the scanner sees a
    regular time series; only series identity, image numbering and the
    scanner-unsafe fields handled by the i2i reference are changed.
    """
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = output_index + 1
    output.setHead(header)
    output.image_series_index = series_index

    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = i2i._derived_instance_uid(
        source_image, series_index, series_name, output_index, series_uid
    )
    storage_fields = i2i._original_storage_fields(source_image, output_index, output)
    source_chron_slice = i2i._source_storage_int(source_image, "ChronSliceNo")
    storage_fields["ChronSliceNo"] = (
        source_chron_slice if source_chron_slice is not None else storage_fields["SliceNo"]
    )

    meta = i2i._meta_from_image(source_image)
    i2i._strip_source_parent_refs(meta)
    i2i._strip_scanner_write_unsafe_meta(meta)
    if derived:
        image_type = f"DERIVED\\PRIMARY\\M\\{FRISGO_IMAGE_TYPE_TOKEN}"
        for key in ("SeriesDescription", "SequenceDescription", "ProtocolName", "ImageComments"):
            meta[key] = series_name
        meta["ImageType"] = image_type
        meta["DicomImageType"] = image_type
        meta["ImageTypeValue4"] = FRISGO_IMAGE_TYPE_TOKEN
        meta["ImageProcessingHistory"] = FRISGO_HISTORY
        meta["DataRole"] = "Image"
        meta["ComplexImageComponent"] = "MAGNITUDE"
        meta["SequenceDescriptionAdditional"] = "frisgo"
    else:
        for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
            if not i2i._meta_text(meta, key):
                meta[key] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    i2i._set_meta_scalar(meta, "Keep_image_geometry", 1)
    i2i._ensure_original_storage_meta(meta, source_image, output_index, storage_fields)
    meta.update(i2i._header_geometry_meta(output.getHead()))

    minihead = i2i._decode_ice_minihead(i2i._meta_text(meta, "IceMiniHead"))
    if minihead:
        minihead, changed = i2i._patch_original_ice_minihead(
            minihead, series_grouping, series_uid, sop_uid, storage_fields
        )
        if derived:
            for name in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
                minihead, _ = i2i._replace_or_append_minihead_string_param(
                    minihead, name, series_name
                )
            minihead, _ = i2i._replace_or_append_minihead_string_param(
                minihead, "ImageType", image_type
            )
            minihead, _ = i2i._replace_or_append_minihead_array_token(
                minihead, "ImageTypeValue4", FRISGO_IMAGE_TYPE_TOKEN
            )
            changed = True
        if changed:
            meta["IceMiniHead"] = i2i._encode_ice_minihead(minihead)
    output.attribute_string = meta.serialize()
    return output


def _validate_outputs(images):
    errors = []
    sop_uids = set()
    for index, image in enumerate(images):
        if int(image.image_index) < 1:
            errors.append(f"image {index} has invalid image_index {image.image_index}")
        meta = i2i._meta_from_image(image)
        errors.extend(
            i2i._scanner_write_unsafe_field_errors(meta, i2i._image_minihead(image), index)
        )
        sop_uid = i2i._meta_text(meta, "SOPInstanceUID")
        if sop_uid in sop_uids:
            errors.append(f"image {index} repeats SOPInstanceUID {sop_uid}")
        sop_uids.add(sop_uid)
    if errors:
        raise ValueError("invalid FRISGO outputs: " + "; ".join(errors[:10]))


def _source_series_groups(images):
    groups = {}
    for image in images:
        key = (
            int(image.image_series_index), int(image.image_type), i2i._source_group_key(image),
            int(image.contrast), int(image.phase), int(image.set), int(image.average),
        )
        groups.setdefault(key, []).append(image)
    return list(groups.values())


def _is_magnitude(image):
    return image.image_type in (ismrmrd.IMTYPE_MAGNITUDE, 0)


def _image_plane(image):
    data = np.asarray(image.data)
    return data.reshape(data.shape[-2:])


def _repetition_time_seconds(metadata, image):
    candidates = []
    try:
        candidates.append(float(metadata.sequenceParameters.TR[0]))
    except Exception:
        pass
    candidates.append(i2i._meta_text(i2i._meta_from_image(image), "RepetitionTime"))
    for value in candidates:
        try:
            repetition_time_ms = float(value)
        except (TypeError, ValueError):
            continue
        if repetition_time_ms > 0:
            return repetition_time_ms / 1000.0
    logging.warning(
        "RepetitionTime missing; writing NIfTI with TR %.3f s",
        DEFAULT_REPETITION_TIME_SECONDS,
    )
    return DEFAULT_REPETITION_TIME_SECONDS
