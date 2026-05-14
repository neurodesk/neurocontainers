"""OpenRecon Bloch-Siegert B1 mapping from reconstructed MRD images."""

import base64
import json
import logging
import math
import re
import traceback
import uuid

import ismrmrd
import numpy as np

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


RECIPE_NAME = "blochsiegertb1mapping"
BSS_PULSE_WIDTH_MS = 10.0
PHASE_WRAP = 4096.0
KBS_SCALE = 0.044 / 6.0

B1_SERIES_INDEX_START = 101
BSP_SERIES_INDEX_START = 120
PHSC_SERIES_INDEX_START = 140
B0_SERIES_INDEX = 160
MASK_SERIES_INDEX = 161

SOURCE_PARENT_REFERENCE_META_KEYS = {
    "DicomEngineDimString",
    "MFInstanceNumber",
    "MultiFrameSOPInstanceUID",
    "PSMultiFrameSOPInstanceUID",
    "PSSeriesInstanceUID",
    "SOPInstanceUID",
}
SOURCE_PARENT_REFERENCE_META_PREFIXES = (
    "ReferencedGSPS",
    "ReferencedImageSequence",
)
SCANNER_WRITE_UNSAFE_META_KEYS = {
    "ImageTypeValue3",
}


def process(connection, config, metadata):
    logging.info("Config: %s", config)
    input_images = []

    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                input_images.append(item)
            else:
                logging.info("Ignoring unsupported MRD message %s", type(item).__name__)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        settings = _settings_from_config(config)
        result = compute_bloch_siegert_maps(input_images, settings)
        output_images = build_output_images(result, settings, input_images)

        logging.info(
            "Bloch-Siegert OpenRecon: slices=%d nframe=%d ntx=%d outputs=%d",
            result["slice_count"],
            result["nframe"],
            result["ntx"],
            len(output_images),
        )
        if not output_images:
            logging.info("No output maps enabled; closing without output")
            return

        _validate_output_images(output_images, input_images)
        _send_images_by_series(connection, output_images)

    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def compute_bloch_siegert_maps(input_images, settings=None):
    settings = dict(settings or {})
    magnitude_images, phase_images = _split_magnitude_phase_images(input_images)
    if not magnitude_images:
        raise ValueError("Bloch-Siegert mapping requires magnitude image messages")
    if not phase_images:
        raise ValueError("Bloch-Siegert mapping requires phase image messages")

    magnitude_groups = _group_frames_by_slice(magnitude_images)
    phase_groups = _group_frames_by_slice(phase_images)
    magnitude_groups, phase_groups = _pair_slice_groups(magnitude_groups, phase_groups)

    slice_results = []
    nframe = None
    ntx = None
    for slice_index, (magnitude_group, phase_group) in enumerate(
        zip(magnitude_groups, phase_groups)
    ):
        sequence_nframe, sequence_ntx = _sequence_shape(
            min(len(magnitude_group), len(phase_group))
        )
        if nframe is None:
            nframe = sequence_nframe
            ntx = sequence_ntx
        elif nframe != sequence_nframe or ntx != sequence_ntx:
            raise ValueError(
                "All Bloch-Siegert slices must use the same sequence shape; "
                f"slice 0 has nframe={nframe}, ntx={ntx}, slice {slice_index} "
                f"has nframe={sequence_nframe}, ntx={sequence_ntx}"
            )

        slice_results.append(
            _compute_slice_maps(
                magnitude_group[:nframe],
                phase_group[:nframe],
                ntx,
                settings,
            )
        )

    b1 = np.concatenate([item["b1"] for item in slice_results], axis=1)
    bsp = np.concatenate([item["bsp"] for item in slice_results], axis=1)
    phsc = np.concatenate([item["phsc"] for item in slice_results], axis=1)
    b0 = np.concatenate([item["b0"] for item in slice_results], axis=0)
    mask = np.concatenate([item["mask"] for item in slice_results], axis=0)

    return {
        "nframe": nframe,
        "ntx": ntx,
        "slice_count": len(slice_results),
        "anchor_images": [group[0] for group in magnitude_groups],
        "magnitude_images": magnitude_images,
        "phase_images": phase_images,
        "b1": b1.astype(np.float32),
        "bsp": bsp.astype(np.float32),
        "phsc": phsc.astype(np.float32),
        "b0": b0.astype(np.float32),
        "mask": mask.astype(np.uint16),
    }


def build_output_images(result, settings=None, input_images=None):
    settings = dict(settings or {})
    input_images = input_images or result["magnitude_images"] + result["phase_images"]
    series_indices = _allocate_output_series_indices(input_images, result["ntx"])
    anchor = result["anchor_images"][0]
    slice_anchors = result["anchor_images"]
    source_name = _source_series_name(anchor) or RECIPE_NAME

    outputs = []
    if _setting_bool(settings, "sendb1", default=True):
        for tx_index in range(result["ntx"]):
            outputs.append(
                _map_to_mrd_image(
                    result["b1"][tx_index],
                    anchor,
                    slice_anchors,
                    series_indices["b1"][tx_index],
                    _map_series_name(source_name, "b1", tx_index, result["ntx"]),
                    "BSSB1",
                    "BlochSiegertB1Map",
                    "uT",
                    tx_index,
                )
            )

    if _setting_bool(settings, "sendbsp", default=True):
        for tx_index in range(result["ntx"]):
            outputs.append(
                _map_to_mrd_image(
                    result["bsp"][tx_index],
                    anchor,
                    slice_anchors,
                    series_indices["bsp"][tx_index],
                    _map_series_name(source_name, "bsp", tx_index, result["ntx"]),
                    "BSSBSP",
                    "BlochSiegertPhase",
                    "radians",
                    tx_index,
                )
            )

    if _setting_bool(settings, "sendphsc", default=True):
        for phase_index in range(result["phsc"].shape[0]):
            outputs.append(
                _map_to_mrd_image(
                    result["phsc"][phase_index],
                    anchor,
                    slice_anchors,
                    series_indices["phsc"][phase_index],
                    _map_series_name(
                        source_name,
                        "phsc",
                        phase_index,
                        result["phsc"].shape[0],
                    ),
                    "BSSPHSC",
                    "BlochSiegertCorrectedPhase",
                    "radians",
                    phase_index,
                )
            )

    if _setting_bool(settings, "sendb0", default=True):
        outputs.append(
            _map_to_mrd_image(
                result["b0"],
                anchor,
                slice_anchors,
                series_indices["b0"],
                f"{source_name}-b0",
                "BSSB0",
                "BlochSiegertB0Map",
                "Hz",
            )
        )

    if _setting_bool(settings, "sendmask", default=False):
        outputs.append(
            _map_to_mrd_image(
                result["mask"],
                anchor,
                slice_anchors,
                series_indices["mask"],
                f"{source_name}-mask",
                "BSSMASK",
                "BlochSiegertMask",
                "binary",
                window_center=0.5,
                window_width=1.0,
            )
        )

    return outputs


def _compute_slice_maps(magnitude_images, phase_images, ntx, settings):
    magnitude_stack = np.stack([_image_volume_data(image) for image in magnitude_images])
    phase_stack = np.stack([_image_volume_data(image) for image in phase_images])

    magnitude_stack = np.nan_to_num(magnitude_stack.astype(np.float32), copy=False)
    phase_stack = np.nan_to_num(phase_stack.astype(np.float32), copy=False)
    phase_stack = _phase_to_radians(
        phase_stack,
        _setting_float(settings, "phasewrap", default=PHASE_WRAP),
    )

    mask = _bloch_siegert_mask(magnitude_stack, ntx)
    complex_phase = np.exp(1j * phase_stack)
    pair_start = complex_phase[1 : 2 * ntx + 1 : 2]
    pair_end = complex_phase[2 : 2 * ntx + 2 : 2]
    bsp = np.angle(pair_start * np.conj(pair_end)).astype(np.float32)
    bsp[bsp < -math.pi / 2] += 2 * math.pi
    bsp[bsp < 0] = 0

    pulse_width = _setting_float(settings, "bspulsewidthms", default=BSS_PULSE_WIDTH_MS)
    kbs = KBS_SCALE * pulse_width
    if kbs <= 0:
        raise ValueError(f"bspulsewidthms must be positive, got {pulse_width}")
    b1 = np.sqrt(bsp / kbs)

    phsc = np.angle(complex_phase[2 * ntx + 2 :]).astype(np.float32)
    if phsc.shape[0] == 0:
        raise ValueError("Phase-corrected output frames are missing")
    b0 = (
        np.angle(complex_phase[2 * ntx + 1] * np.conj(complex_phase[0]))
        * 1000.0
        / (2.0 * math.pi)
    ).astype(np.float32)

    return {
        "bsp": bsp,
        "b1": b1.astype(np.float32),
        "phsc": phsc,
        "b0": b0,
        "mask": mask.astype(np.uint16),
    }


def _bloch_siegert_mask(magnitude_stack, ntx):
    mask_source = np.mean(magnitude_stack[: ntx + 1], axis=0)
    finite_values = mask_source[np.isfinite(mask_source)]
    if finite_values.size == 0:
        return np.zeros(mask_source.shape, dtype=bool)

    source_mean = float(np.mean(finite_values))
    low_values = finite_values[finite_values < source_mean]
    if low_values.size == 0:
        threshold = source_mean
    else:
        threshold = float(np.mean(low_values) + 2.0 * np.std(low_values))

    return _fill_holes_per_slice(mask_source > threshold)


def _phase_to_radians(phase_stack, phase_wrap):
    if phase_wrap <= 0:
        return phase_stack.astype(np.float32)
    return phase_stack.astype(np.float32) * (2.0 * math.pi / phase_wrap) - math.pi


def _split_magnitude_phase_images(images):
    magnitude_images = []
    phase_images = []
    for image in images:
        if _is_phase_image(image):
            phase_images.append(image)
        elif _is_magnitude_image(image):
            magnitude_images.append(image)
        else:
            logging.info("Ignoring non-magnitude/non-phase image_type=%s", image.image_type)
    if not phase_images:
        fallback_magnitude, fallback_phase = _fallback_split_magnitude_phase_series(
            magnitude_images
        )
        if fallback_phase:
            logging.warning(
                "No explicit phase MRD image_type was present; treating the first "
                "of two equal-length image series as magnitude and the second as phase"
            )
            return fallback_magnitude, fallback_phase
    return magnitude_images, phase_images


def _fallback_split_magnitude_phase_series(images):
    series_groups = []
    group_index_by_key = {}
    for image in images:
        key = _source_series_key(image)
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(series_groups)
            group_index_by_key[key] = group_index
            series_groups.append([])
        series_groups[group_index].append(image)

    if len(series_groups) != 2 or len(series_groups[0]) != len(series_groups[1]):
        return images, []

    series_groups = sorted(series_groups, key=_series_group_sort_key)
    frame_count = len(series_groups[0])
    if frame_count < 5:
        return images, []
    return series_groups[0], series_groups[1]


def _source_series_key(image):
    meta = _meta_from_image(image)
    return (
        int(image.getHead().image_series_index),
        _meta_text(meta, "SeriesInstanceUID"),
        _meta_text(meta, "SeriesNumberRangeNameUID"),
        _meta_text(meta, "SequenceDescription"),
        _meta_text(meta, "ProtocolName"),
    )


def _series_group_sort_key(group):
    first_image = group[0]
    meta = _meta_from_image(first_image)
    series_number = _dicom_json_numeric_value(meta, "00200011")
    if series_number is None:
        series_number = int(first_image.getHead().image_series_index)
    return (series_number, int(first_image.getHead().image_series_index))


def _is_magnitude_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "MAGNITUDE" in text or re.search(r"(^|\\|\s)M($|\\|\s)", text) is not None


def _is_phase_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type == int(getattr(ismrmrd, "IMTYPE_PHASE", 2)):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "PHASE" in text or re.search(r"(^|\\|\s)P($|\\|\s)", text) is not None


def _group_frames_by_slice(images):
    indexed = list(enumerate(images))
    if indexed and all(_is_volume_frame_image(image) for _order, image in indexed):
        logging.info(
            "Grouped %d volume-frame image(s) into one Bloch-Siegert frame group",
            len(indexed),
        )
        return [[image for _order, image in sorted(indexed, key=_frame_sort_key)]]

    candidates = []
    for name, key_func in (
        ("position", _position_group_key),
        ("slice", _slice_group_key),
    ):
        groups = _groups_from_key(indexed, key_func)
        if groups and all(len(group) >= 5 for group in groups):
            candidates.append((name, groups))

    chunked = _chunked_frame_groups(indexed)
    if chunked:
        candidates.append(("chunk", chunked))

    if not candidates:
        raise ValueError(
            "Bloch-Siegert mapping requires at least 5 frames per slice "
            "(1Tx) or 26 frames per slice (8Tx)"
        )

    def score(candidate):
        _name, groups = candidate
        exact = sum(1 for group in groups if len(group) in (5, 26))
        return (exact, len(groups), -max(len(group) for group in groups))

    selected_name, selected_groups = max(candidates, key=score)
    logging.info(
        "Grouped %d image(s) into %d Bloch-Siegert slice group(s) using %s",
        len(images),
        len(selected_groups),
        selected_name,
    )
    return [
        [image for _order, image in sorted(group, key=_frame_sort_key)]
        for group in sorted(selected_groups, key=_slice_sort_key)
    ]


def _groups_from_key(indexed_images, key_func):
    groups = []
    group_index_by_key = {}
    for item in indexed_images:
        key = key_func(item[1])
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(groups)
            group_index_by_key[key] = group_index
            groups.append([])
        groups[group_index].append(item)
    return groups


def _position_group_key(image):
    header = image.getHead()
    position = tuple(round(float(value), 3) for value in header.position)
    slice_dir = tuple(round(float(value), 3) for value in header.slice_dir)
    return position, slice_dir


def _slice_group_key(image):
    return int(image.getHead().slice)


def _chunked_frame_groups(indexed_images):
    total = len(indexed_images)
    if total >= 26 and total % 26 == 0:
        chunk_size = 26
    elif total >= 5 and total % 5 == 0:
        chunk_size = 5
    else:
        return []
    return [
        indexed_images[index : index + chunk_size]
        for index in range(0, total, chunk_size)
    ]


def _frame_sort_key(indexed_image):
    order, image = indexed_image
    header = image.getHead()
    image_index = int(getattr(header, "image_index", 0))
    return (
        image_index if image_index > 0 else order + 1,
        int(getattr(header, "contrast", 0)),
        int(getattr(header, "phase", 0)),
        int(getattr(header, "repetition", 0)),
        int(getattr(header, "set", 0)),
        order,
    )


def _slice_sort_key(group):
    first_image = group[0][1]
    header = first_image.getHead()
    axis = _normalize_vector(header.slice_dir)
    if axis is None:
        axis = np.asarray((0.0, 0.0, 1.0))
    position = np.asarray(header.position, dtype=float)
    return (float(np.dot(position, axis)), group[0][0])


def _pair_slice_groups(magnitude_groups, phase_groups):
    if len(magnitude_groups) != len(phase_groups):
        raise ValueError(
            "Magnitude and phase image groups do not describe the same number of "
            f"slices: {len(magnitude_groups)} magnitude, {len(phase_groups)} phase"
        )

    paired_magnitude = []
    paired_phase = []
    for index, (magnitude_group, phase_group) in enumerate(
        zip(magnitude_groups, phase_groups)
    ):
        if len(magnitude_group) < 5 or len(phase_group) < 5:
            raise ValueError(
                f"Slice {index} has too few frames: {len(magnitude_group)} "
                f"magnitude, {len(phase_group)} phase"
            )
        paired_magnitude.append(magnitude_group)
        paired_phase.append(phase_group)
    return paired_magnitude, paired_phase


def _sequence_shape(frame_count):
    if frame_count > 25:
        return 26, 8
    if frame_count >= 5:
        return 5, 1
    raise ValueError(
        f"Bloch-Siegert mapping requires 5 or 26 frames, got {frame_count}"
    )


def _is_volume_frame_image(image):
    data = np.asarray(image.data)
    return data.ndim == 4 and data.shape[0] == 1 and data.shape[1] > 1


def _image_volume_data(image):
    data = np.asarray(image.data)
    if data.ndim == 4 and data.shape[0] == 1:
        return data[0]
    data = np.squeeze(data)
    if data.ndim == 2:
        return data[np.newaxis, :, :]
    if data.ndim != 3:
        raise ValueError(
            "Bloch-Siegert input images must be single-channel 2D frames or "
            "single-channel 3D volume frames; "
            f"got data shape {np.asarray(image.data).shape}"
        )
    return data


def _map_to_mrd_image(
    volume,
    anchor_image,
    slice_anchor_images,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    tx_index=None,
    window_center=None,
    window_width=None,
):
    volume = np.asarray(volume)
    if volume.ndim != 3:
        raise ValueError(f"Output map volume must be 3D, got shape {volume.shape}")

    output_data = volume.astype(np.float32, copy=False)
    if image_type_token == "BSSMASK":
        output_data = volume.astype(np.uint16, copy=False)
    output = ismrmrd.Image.from_array(output_data, transpose=False)

    header = anchor_image.getHead()
    header.data_type = output.data_type
    header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
    header.image_series_index = int(series_index)
    header.image_index = 1
    header.slice = 0
    header.contrast = 0
    output_header = output.getHead()
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in output_header.matrix_size],
    )

    slice_axis = _infer_slice_axis(slice_anchor_images)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in slice_anchor_images[0].getHead().position],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )
    fov = [float(value) for value in header.field_of_view]
    fov[2] = _output_fov_z(slice_anchor_images, slice_axis, volume.shape[0])
    _set_header_sequence_field(header, "field_of_view", fov)

    output.setHead(header)
    output.image_series_index = int(series_index)

    center, width = _window_center_width(output_data)
    if window_center is not None:
        center = window_center
    if window_width is not None:
        width = window_width
    output.attribute_string = _output_meta(
        anchor_image,
        header,
        series_index,
        series_name,
        image_type_token,
        map_role,
        units,
        volume.shape[0],
        center,
        width,
        tx_index,
    ).serialize()
    return output


def _output_meta(
    source_image,
    header,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    slice_count,
    window_center,
    window_width,
    tx_index=None,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    _strip_scanner_write_unsafe_meta(meta)
    if "IceMiniHead" in meta:
        del meta["IceMiniHead"]

    series_uid = _derived_series_uid(source_image, series_index, series_name)
    sop_uid = _derived_instance_uid(source_image, series_uid, series_index, series_name)
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "BLOCHSIEGERTB1MAPPING"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesNumberRangeNameUID"] = _derived_series_grouping(
        series_name,
        series_index,
    )
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SequenceDescriptionAdditional"] = "openrecon"
    meta["Keep_image_geometry"] = "0"
    meta["partition_count"] = "1"
    meta["slice_count"] = str(int(slice_count))
    meta["NumberOfSlices"] = str(int(slice_count))
    meta["ImagesInAcquisition"] = str(int(slice_count))
    meta["NumberInSeries"] = "1"
    meta["SliceNo"] = "0"
    meta["AnatomicalSliceNo"] = "0"
    meta["ChronSliceNo"] = "0"
    meta["ProtocolSliceNumber"] = "0"
    meta["Actual3DImagePartNumber"] = "0"
    meta["AnatomicalPartitionNo"] = "0"
    meta["BlochSiegertOutput"] = map_role
    meta["BlochSiegertUnits"] = units
    if tx_index is not None:
        meta["BlochSiegertTxIndex"] = str(int(tx_index + 1))
    meta["WindowCenter"] = f"{float(window_center):.6g}"
    meta["WindowWidth"] = f"{float(window_width):.6g}"
    meta.update(_header_geometry_meta(header))
    _strip_scanner_write_unsafe_meta(meta)
    return meta


def _header_geometry_meta(header):
    return {
        "ImageRowDir": [f"{float(value):.18f}" for value in header.read_dir],
        "ImageColumnDir": [f"{float(value):.18f}" for value in header.phase_dir],
        "ImageSliceNormDir": [f"{float(value):.18f}" for value in header.slice_dir],
        "SlicePosLightMarker": [f"{float(value):.18f}" for value in header.position],
    }


def _allocate_output_series_indices(input_images, ntx):
    used = {int(image.getHead().image_series_index) for image in input_images}

    def reserve(preferred):
        series_index = int(preferred)
        while series_index in used:
            series_index += 1
        used.add(series_index)
        return series_index

    return {
        "b1": [reserve(B1_SERIES_INDEX_START + index) for index in range(ntx)],
        "bsp": [reserve(BSP_SERIES_INDEX_START + index) for index in range(ntx)],
        "phsc": [reserve(PHSC_SERIES_INDEX_START + index) for index in range(ntx)],
        "b0": reserve(B0_SERIES_INDEX),
        "mask": reserve(MASK_SERIES_INDEX),
    }


def _map_series_name(source_name, map_name, index, count):
    if count == 1:
        return f"{source_name}-{map_name}"
    return f"{source_name}-{map_name}-tx{index + 1:02d}"


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_identity_text(series_name)}_{int(series_index)}"


def _derived_series_uid(source_image, series_index, series_name):
    seed = "|".join(
        [
            RECIPE_NAME,
            _source_series_uid(source_image) or _source_series_name(source_image) or "source",
            str(int(series_index)),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_instance_uid(source_image, series_uid, series_index, series_name):
    seed = "|".join(
        [
            f"{RECIPE_NAME}-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _source_series_name(source_image):
    meta = _meta_from_image(source_image)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _meta_text(meta, key)
        if value:
            return value

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _minihead_string_value(minihead, key)
        if value:
            return value
    return ""


def _source_series_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SeriesInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SeriesInstanceUID",
    )


def _source_sop_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SOPInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SOPInstanceUID",
    )


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta[key]


def _strip_scanner_write_unsafe_meta(meta):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta:
            del meta[key]


def _validate_output_images(output_images, input_images):
    errors = []
    input_series_indices = {
        int(image.getHead().image_series_index)
        for image in input_images
    }
    input_series_uids = {
        _source_series_uid(image)
        for image in input_images
        if _source_series_uid(image)
    }
    seen_series_uids = {}
    seen_sop_uids = {}

    for index, image in enumerate(output_images):
        header = image.getHead()
        series_index = int(header.image_series_index)
        meta = _meta_from_image(image)
        series_uid = _meta_text(meta, "SeriesInstanceUID")
        sop_uid = _meta_text(meta, "SOPInstanceUID")
        keep_geometry = _meta_int(meta, "Keep_image_geometry")

        if series_index in input_series_indices:
            errors.append(f"image {index} reuses input image_series_index {series_index}")
        if keep_geometry != 0:
            errors.append(f"image {index} has Keep_image_geometry={keep_geometry}, expected 0")
        if _meta_text(meta, "IceMiniHead"):
            errors.append(f"image {index} keeps source IceMiniHead on derived output")
        if _meta_text(meta, "ImageTypeValue3"):
            errors.append(f"image {index} keeps unsafe ImageTypeValue3")
        if not series_uid:
            errors.append(f"image {index} is missing SeriesInstanceUID")
        if series_uid in input_series_uids:
            errors.append(f"image {index} reuses input SeriesInstanceUID {series_uid}")
        if not sop_uid:
            errors.append(f"image {index} is missing SOPInstanceUID")
        if series_uid:
            previous = seen_series_uids.setdefault(series_uid, series_index)
            if previous != series_index:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by series "
                    f"{previous} and {series_index}"
                )
        if sop_uid:
            previous = seen_sop_uids.setdefault(sop_uid, index)
            if previous != index:
                errors.append(f"image {index} duplicates SOPInstanceUID from image {previous}")
        if int(header.image_index) < 1:
            errors.append(f"image {index} has image_index {header.image_index}, expected >= 1")
        if int(header.slice) != 0:
            errors.append(f"image {index} has slice {header.slice}, expected 0")

    if errors:
        raise ValueError(
            "Invalid blochsiegertb1mapping output series contract before send: "
            + "; ".join(errors)
        )


def _send_images_by_series(connection, images):
    batches = []
    batch_by_series = {}
    for image in images:
        series_index = int(image.getHead().image_series_index)
        if series_index not in batch_by_series:
            batch_by_series[series_index] = []
            batches.append(batch_by_series[series_index])
        batch_by_series[series_index].append(image)
    for batch in batches:
        connection.send_image(batch)


def _settings_from_config(config):
    return {
        "sendb1": _config_bool(config, "sendb1", default=True),
        "sendbsp": _config_bool(config, "sendbsp", default=True),
        "sendphsc": _config_bool(config, "sendphsc", default=True),
        "sendb0": _config_bool(config, "sendb0", default=True),
        "sendmask": _config_bool(config, "sendmask", default=False),
        "bspulsewidthms": _config_float(
            config,
            "bspulsewidthms",
            default=BSS_PULSE_WIDTH_MS,
        ),
        "phasewrap": _config_float(config, "phasewrap", default=PHASE_WRAP),
    }


def _config_parameters(config):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    if not isinstance(config, dict):
        return {}
    parameters = config.get("parameters", config)
    return parameters if isinstance(parameters, dict) else {}


def _config_bool(config, key, default=False):
    return _coerce_bool(_config_parameters(config).get(key, default), default)


def _config_float(config, key, default=0.0):
    value = _config_parameters(config).get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _setting_bool(settings, key, default=False):
    return _coerce_bool(settings.get(key, default), default)


def _setting_float(settings, key, default=0.0):
    try:
        return float(settings.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _fill_holes_per_slice(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim == 2:
        return _fill_holes_2d(mask)
    if mask.ndim != 3:
        raise ValueError(f"_fill_holes_per_slice expects 2D or 3D mask, got {mask.shape}")
    return np.stack([_fill_holes_2d(slice_mask) for slice_mask in mask], axis=0)


def _fill_holes_2d(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim != 2:
        raise ValueError(f"_fill_holes_2d expects a 2D mask, got {mask.shape}")

    inverse = ~mask
    visited = np.zeros(mask.shape, dtype=bool)
    stack = []
    height, width = mask.shape
    for row in range(height):
        for col in (0, width - 1):
            if inverse[row, col] and not visited[row, col]:
                visited[row, col] = True
                stack.append((row, col))
    for col in range(width):
        for row in (0, height - 1):
            if inverse[row, col] and not visited[row, col]:
                visited[row, col] = True
                stack.append((row, col))

    while stack:
        row, col = stack.pop()
        for next_row, next_col in (
            (row - 1, col),
            (row + 1, col),
            (row, col - 1),
            (row, col + 1),
        ):
            if (
                0 <= next_row < height
                and 0 <= next_col < width
                and inverse[next_row, next_col]
                and not visited[next_row, next_col]
            ):
                visited[next_row, next_col] = True
                stack.append((next_row, next_col))

    holes = inverse & ~visited
    return mask | holes


def _infer_slice_axis(images):
    if images:
        axis = _normalize_vector(images[0].getHead().slice_dir)
        if axis is not None:
            return axis
    if len(images) > 1:
        positions = [np.asarray(image.getHead().position, dtype=float) for image in images]
        delta = positions[-1] - positions[0]
        axis = _normalize_vector(delta)
        if axis is not None:
            return axis
    return np.asarray((0.0, 0.0, 1.0), dtype=float)


def _normalize_vector(values):
    vector = np.asarray(values, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        return None
    return vector / norm


def _output_fov_z(slice_anchor_images, slice_axis, output_slice_count):
    source_fov_z = float(slice_anchor_images[0].getHead().field_of_view[2])
    if len(slice_anchor_images) <= 1:
        return source_fov_z

    projections = [
        float(np.dot(np.asarray(image.getHead().position, dtype=float), slice_axis))
        for image in slice_anchor_images
    ]
    projections = sorted(projections)
    spacings = np.diff(projections)
    spacings = spacings[np.abs(spacings) > 1e-6]
    if spacings.size:
        return float(np.median(np.abs(spacings)) * output_slice_count)
    return float(source_fov_z * output_slice_count)


def _window_center_width(data):
    values = np.asarray(data, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 1.0
    data_min = float(np.min(finite))
    data_max = float(np.max(finite))
    width = data_max - data_min
    if width <= 0:
        width = 1.0
    return data_min + width / 2.0, width


def _set_header_sequence_field(header, field_name, values):
    target = getattr(header, field_name)
    for index, value in enumerate(values):
        target[index] = value


def _meta_from_image(image):
    if not getattr(image, "attribute_string", ""):
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _meta_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    text = str(value or "").strip()
    return "" if text.upper() == "N/A" else text


def _meta_int(meta, key):
    value = _meta_text(meta, key)
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _dicom_json_numeric_value(meta, tag):
    text = _meta_text(meta, "DicomJson")
    if not text:
        return None
    try:
        dicom_json = json.loads(base64.b64decode(text).decode("utf-8"))
    except Exception:
        return None
    values = dicom_json.get(tag, {}).get("Value", [])
    if not values:
        return None
    try:
        return float(values[0])
    except (TypeError, ValueError):
        return None


def _decode_ice_minihead(value):
    if not value:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _minihead_string_value(minihead, key):
    match = re.search(
        rf'<ParamString\."{re.escape(key)}">\s*{{\s*"([^"]*)"',
        minihead,
    )
    return match.group(1).strip() if match else ""


def _sanitize_identity_text(value):
    return (
        str(value or "")
        .strip()
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )
