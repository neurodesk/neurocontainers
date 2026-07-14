"""OpenRecon image-to-image adapter for ME-ICA v4.

The adapter converts reconstructed multi-echo magnitude MRD images into one
four-dimensional NIfTI file per echo, runs ME-ICA, and converts selected output
time series back into scanner-displayable MRD image messages.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import tempfile
import traceback

import ismrmrd
import nibabel as nib
import numpy as np

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


DEFAULT_MEICA_BINARY = "/usr/local/bin/meica.py"
OPENRECON_WORK_ROOT = Path("/tmp/share/meica_openrecon")
OUTPUT_SERIES_START = 200
OUTPUT_SPECS = {
    "medn": ("MEICA_MEDN", "ME-ICA conservative denoised"),
    "tsoc": ("MEICA_TSOC", "ME-ICA optimally combined"),
    "hikts": ("MEICA_HIKTS", "ME-ICA high-kappa denoised"),
}


def process(connection, config, metadata):
    """Run ME-ICA for all reconstructed magnitude images in one MRD stream."""
    logging.info("ME-ICA OpenRecon config: %s", config)
    images = []
    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                images.append(item)
            else:
                logging.info("Ignoring unsupported MRD message %s", type(item).__name__)

        magnitude_images = [image for image in images if _is_magnitude(image)]
        if not magnitude_images:
            raise ValueError("ME-ICA requires reconstructed magnitude image messages")

        settings = _settings(config)
        OPENRECON_WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="run_", dir=str(OPENRECON_WORK_ROOT)
        ) as work_name:
            work_dir = Path(work_name)
            conversion = write_meica_inputs(
                magnitude_images, metadata, work_dir / "input", settings
            )
            _run_meica(conversion, work_dir / "output", settings)
            selected = _find_outputs(work_dir / "output", settings["output"])
            output_images = _outputs_to_mrd(selected, conversion, images)

        if settings["send_original"]:
            connection.send_image([_original_passthrough(image) for image in images])
        for series in output_images:
            connection.send_image(series)
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def write_meica_inputs(images, metadata, input_dir, settings):
    """Write one four-dimensional NIfTI dataset for each detected echo."""
    input_dir.mkdir(parents=True, exist_ok=True)
    echo_groups = _group_echo_images(images, settings["echo_times_ms"])
    if len(echo_groups) < 3:
        raise ValueError(
            f"ME-ICA requires at least three echoes; detected {len(echo_groups)}"
        )

    echo_times_ms = _resolve_echo_times_ms(
        echo_groups, metadata, settings["echo_times_ms"]
    )
    datasets = []
    reference_shape = None
    reference_timepoints = None
    source_geometry = None

    for echo_index, group in enumerate(echo_groups):
        time_groups = _group_echo_into_timepoints(group)
        volumes = []
        for time_group in time_groups:
            volume, affine = _images_to_volume(time_group)
            volumes.append(volume)
        data = np.stack(volumes, axis=3).astype(np.float32)

        if reference_shape is None:
            reference_shape = data.shape[:3]
            reference_timepoints = data.shape[3]
            source_geometry = {
                "anchor": _ordered_slices(time_groups[0])[0],
                "first_timepoint": _ordered_slices(time_groups[0]),
                "shape": data.shape[:3],
            }
        elif data.shape[:3] != reference_shape or data.shape[3] != reference_timepoints:
            raise ValueError(
                "All ME-ICA echoes must have matching spatial dimensions and "
                f"time points; echo 1 is {reference_shape + (reference_timepoints,)} "
                f"but echo {echo_index + 1} is {data.shape}"
            )

        path = input_dir / f"openrecon_echo-{echo_index + 1}_bold.nii.gz"
        image = nib.Nifti1Image(data, affine)
        image.header.set_xyzt_units("mm", "sec")
        tr_seconds = _metadata_tr_seconds(metadata)
        if tr_seconds is not None:
            image.header["pixdim"][4] = tr_seconds
        nib.save(image, str(path))
        _write_json(
            _sidecar_path(path),
            {
                "EchoTime": echo_times_ms[echo_index] / 1000.0,
                "RepetitionTime": tr_seconds,
                "Modality": "MR",
                "TaskName": "openrecon",
            },
        )
        datasets.append(path)

    logging.info(
        "Prepared %d ME-ICA echoes with %d time points: TE=%s ms",
        len(datasets),
        reference_timepoints,
        [round(value, 6) for value in echo_times_ms],
    )
    return {
        "datasets": datasets,
        "echo_times_ms": echo_times_ms,
        "source_geometry": source_geometry,
    }


def _run_meica(conversion, output_dir, settings):
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        settings["binary"],
        "-d",
        *[str(path) for path in conversion["datasets"]],
        "-e",
        *[f"{value:g}" for value in conversion["echo_times_ms"]],
        "-j",
        str(settings["cpus"]),
        "--prefix",
        "openrecon",
        "--OVERWRITE_NOWAIT",
    ]
    logging.info("Running ME-ICA: %s", " ".join(command))
    result = subprocess.run(
        command,
        cwd=str(output_dir),
        text=True,
        capture_output=True,
        check=False,
        env=os.environ.copy(),
    )
    if result.stdout:
        logging.info("ME-ICA stdout:\n%s", result.stdout)
    if result.stderr:
        logging.info("ME-ICA stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"ME-ICA failed with exit code {result.returncode}")


def _find_outputs(output_dir, requested):
    output_ids = list(OUTPUT_SPECS) if requested == "all" else [requested]
    found = []
    for output_id in output_ids:
        candidates = sorted(output_dir.glob(f"openrecon_{output_id}_*.nii*"))
        if not candidates:
            if requested != "all":
                raise FileNotFoundError(
                    f"ME-ICA did not create the requested {output_id} time series"
                )
            logging.warning("ME-ICA output %s was not produced", output_id)
            continue
        found.append((output_id, candidates[0]))
    if not found:
        raise FileNotFoundError("ME-ICA completed without a selected NIfTI output")
    return found


def _outputs_to_mrd(selected, conversion, input_images):
    used_series = {
        int(image.getHead().image_series_index) for image in input_images
    }
    all_series = []
    for offset, (output_id, path) in enumerate(selected):
        series_index = _reserve_series(used_series, OUTPUT_SERIES_START + offset)
        role, description = OUTPUT_SPECS[output_id]
        all_series.append(
            _nifti_to_mrd_series(
                path,
                conversion["source_geometry"],
                series_index,
                role,
                description,
            )
        )
    return all_series


def _nifti_to_mrd_series(path, source_geometry, series_index, role, description):
    nifti = nib.load(str(path))
    data = np.asarray(nifti.get_fdata(dtype=np.float32), dtype=np.float32)
    if data.ndim == 3:
        data = data[:, :, :, np.newaxis]
    if data.ndim != 4:
        raise ValueError(f"ME-ICA output must be 3D or 4D, got {path}: {data.shape}")
    if tuple(data.shape[:3]) != tuple(source_geometry["shape"]):
        raise ValueError(
            f"ME-ICA output geometry changed from {source_geometry['shape']} "
            f"to {data.shape[:3]}"
        )

    display, scale, offset = _scanner_display_data(data)
    data_zyxt = np.transpose(display, (2, 1, 0, 3))
    source_slices = source_geometry["first_timepoint"]
    anchor = source_geometry["anchor"]
    output = []
    image_index = 1

    for time_index in range(data_zyxt.shape[3]):
        for slice_index in range(data_zyxt.shape[0]):
            slice_data = data_zyxt[slice_index:slice_index + 1, :, :, time_index]
            result = ismrmrd.Image.from_array(slice_data.astype(np.uint16), transpose=False)
            geometry_image = _geometry_image_for_slice(
                source_slices, anchor, slice_index, data_zyxt.shape[0]
            )
            header = copy.deepcopy(geometry_image.getHead())
            generated = result.getHead()
            header.data_type = result.data_type
            header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
            header.image_series_index = series_index
            header.image_index = image_index
            header.slice = slice_index
            header.repetition = time_index
            header.contrast = 0
            _set_vector(header.matrix_size, generated.matrix_size)
            if len(source_slices) == 1 and data_zyxt.shape[0] > 1:
                _set_vector(
                    header.position,
                    _packed_slice_position(
                        anchor.getHead(), slice_index, data_zyxt.shape[0]
                    ),
                )
            result.setHead(header)
            result.image_series_index = series_index
            result.attribute_string = _derived_meta(
                geometry_image,
                series_index,
                role,
                description,
                image_index,
                data_zyxt.shape[0],
                data_zyxt.shape[3],
                scale,
                offset,
            ).serialize()
            output.append(result)
            image_index += 1
    logging.info("Prepared %d MRD images for %s", len(output), role)
    return output


def _group_echo_images(images, configured_echo_times):
    numbered = [_echo_number(image) for image in images]
    times = [_image_echo_time_ms(image) for image in images]
    contrasts = [int(getattr(image.getHead(), "contrast", 0)) for image in images]
    series = [int(image.getHead().image_series_index) for image in images]

    if len({value for value in numbered if value is not None}) > 1:
        keys = [("number", value) for value in numbered]
    elif len({round(value, 6) for value in times if value is not None}) > 1:
        keys = [("time", round(value, 6) if value is not None else None) for value in times]
    elif len(set(contrasts)) > 1:
        keys = [("contrast", value) for value in contrasts]
    elif len(set(series)) > 1 and (
        not configured_echo_times or len(set(series)) == len(configured_echo_times)
    ):
        keys = [("series", value) for value in series]
    elif configured_echo_times and all(_is_packed_volume(image) for image in images):
        count = len(configured_echo_times)
        keys = [("ordered", index % count) for index in range(len(images))]
        logging.warning(
            "No echo identity metadata found; assigning packed volumes to %d "
            "echoes in stream order",
            count,
        )
    else:
        raise ValueError(
            "Could not separate ME-ICA echoes. Preserve EchoNumber, EchoTime, "
            "contrast, or distinct series metadata in the MRD image stream."
        )

    groups = {}
    order = []
    for image, key in zip(images, keys):
        if key[1] is None:
            raise ValueError("Some images are missing the selected echo identity field")
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(image)

    if all(key[0] in {"number", "time", "contrast", "series", "ordered"} for key in order):
        order.sort(key=lambda key: key[1])
    logging.info(
        "Detected ME-ICA echo groups %s with image counts %s",
        order,
        [len(groups[key]) for key in order],
    )
    return [groups[key] for key in order]


def _group_echo_into_timepoints(images):
    if all(_is_packed_volume(image) for image in images):
        return [[image] for image in sorted(images, key=_image_time_sort_key)]

    candidate_keys = [_time_identity(image) for image in images]
    if len(set(candidate_keys)) > 1:
        groups = {}
        for image, key in zip(images, candidate_keys):
            groups.setdefault(key, []).append(image)
        return [groups[key] for key in sorted(groups)]

    ordered = sorted(images, key=_image_index_sort_key)
    slice_ids = [_slice_identity(image) for image in ordered]
    slice_count = len(set(slice_ids))
    if slice_count <= 1:
        return [[image] for image in ordered]
    if len(ordered) % slice_count != 0:
        raise ValueError(
            f"Cannot divide {len(ordered)} images into {slice_count}-slice time points"
        )
    return [
        ordered[index:index + slice_count]
        for index in range(0, len(ordered), slice_count)
    ]


def _images_to_volume(images):
    ordered = _ordered_slices(images)
    volumes = [_image_to_volume(image) for image in ordered]
    if len(volumes) == 1:
        volume = volumes[0]
        return volume, _affine_from_image(ordered[0], volume.shape)
    if any(volume.shape[2] != 1 for volume in volumes):
        raise ValueError("A time point mixes packed volumes and separate slices")
    volume = np.concatenate(volumes, axis=2)
    return volume, _affine_from_stack(ordered, volume.shape)


def _image_to_volume(image):
    data = np.asarray(image.data)
    if data.ndim == 4:
        if np.iscomplexobj(data):
            volume_zyx = np.sqrt(np.sum(np.abs(data) ** 2, axis=0))
        elif data.shape[0] > 1:
            volume_zyx = np.sqrt(np.sum(data.astype(np.float32) ** 2, axis=0))
        else:
            volume_zyx = data[0]
    else:
        volume_zyx = np.squeeze(data)
        if volume_zyx.ndim == 2:
            volume_zyx = volume_zyx[np.newaxis, :, :]
    if volume_zyx.ndim != 3:
        raise ValueError(f"Unsupported MRD image shape {data.shape}")
    return np.transpose(np.asarray(volume_zyx, dtype=np.float32), (2, 1, 0))


def _resolve_echo_times_ms(echo_groups, metadata, configured):
    if configured:
        values = configured
        source = "config"
    else:
        values = _metadata_echo_times_ms(metadata)
        source = "MRD header"
        if not values:
            values = [_image_echo_time_ms(group[0]) for group in echo_groups]
            source = "image metadata"
    if not values or any(value is None for value in values):
        raise ValueError(
            "ME-ICA echo times are missing; set echotimesms to a comma-separated "
            "list in milliseconds"
        )
    if len(values) != len(echo_groups):
        raise ValueError(
            f"Detected {len(echo_groups)} echoes but resolved {len(values)} echo times"
        )
    values = [float(value) for value in values]
    if any(value <= 0 for value in values) or len(set(values)) != len(values):
        raise ValueError(f"ME-ICA echo times must be positive and unique: {values}")
    logging.info("Resolved echo times from %s", source)
    return values


def _settings(config):
    params = _config_parameters(config)
    echo_times = _parse_float_list(params.get("echotimesms", ""))
    output = str(params.get("output", "medn") or "medn").lower()
    if output not in {*OUTPUT_SPECS, "all"}:
        raise ValueError(f"Unsupported ME-ICA output selection: {output}")
    try:
        cpus = int(params.get("cpus", 4))
    except (TypeError, ValueError):
        cpus = 4
    return {
        "echo_times_ms": echo_times,
        "output": output,
        "cpus": max(1, cpus),
        "send_original": _as_bool(params.get("sendoriginal", False)),
        "binary": str(
            params.get("meicabinary")
            or os.environ.get("MEICA_BINARY")
            or DEFAULT_MEICA_BINARY
        ),
    }


def _config_parameters(config):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    if not isinstance(config, dict):
        return {}
    params = config.get("parameters", config)
    return params if isinstance(params, dict) else {}


def _parse_float_list(value):
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple)):
        parts = value
    else:
        parts = re.split(r"[,;\s]+", str(value).strip())
    return [float(part) for part in parts if str(part).strip()]


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _is_magnitude(image):
    image_type = int(getattr(image, "image_type", image.getHead().image_type))
    return image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1)))


def _is_packed_volume(image):
    return _image_to_volume(image).shape[2] > 1


def _echo_number(image):
    meta = _meta(image)
    for key in ("EchoNumber", "EchoNo", "EchoIndex"):
        value = _meta_first(meta, key)
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    return None


def _image_echo_time_ms(image):
    meta = _meta(image)
    for key in ("EchoTime", "TE"):
        value = _meta_first(meta, key)
        try:
            value = float(value)
            return value * 1000.0 if 0 < value < 1 else value
        except (TypeError, ValueError):
            pass
    dicom_json = _meta_first(meta, "DicomJson")
    if dicom_json:
        try:
            import base64

            parsed = json.loads(base64.b64decode(str(dicom_json)).decode("utf-8"))
            value = parsed.get("00180081", {}).get("Value", [None])[0]
            return float(value) if value is not None else None
        except (ValueError, TypeError, json.JSONDecodeError):
            pass
    return None


def _metadata_echo_times_ms(metadata):
    sequence = getattr(metadata, "sequenceParameters", None)
    values = getattr(sequence, "TE", None)
    if values is None:
        return []
    if not isinstance(values, (list, tuple)):
        values = [values]
    values = [float(value) for value in values]
    return [value * 1000.0 if 0 < value < 1 else value for value in values]


def _metadata_tr_seconds(metadata):
    sequence = getattr(metadata, "sequenceParameters", None)
    values = getattr(sequence, "TR", None)
    if values is None:
        return None
    if isinstance(values, (list, tuple)):
        if not values:
            return None
        values = values[0]
    value = float(values)
    return value / 1000.0 if value > 100 else value


def _time_identity(image):
    header = image.getHead()
    return tuple(
        int(getattr(header, field, 0))
        for field in ("repetition", "phase", "set", "average")
    )


def _image_time_sort_key(image):
    return _time_identity(image) + (int(image.getHead().image_index),)


def _image_index_sort_key(image):
    return (int(image.getHead().image_index), _slice_identity(image))


def _slice_identity(image):
    header = image.getHead()
    slice_index = int(getattr(header, "slice", 0))
    if slice_index:
        return (0, slice_index)
    position = tuple(round(float(value), 5) for value in header.position)
    return (1, position)


def _ordered_slices(images):
    return sorted(images, key=lambda image: (_slice_identity(image), int(image.getHead().image_index)))


def _affine_from_image(image, shape_xyz):
    header = image.getHead()
    read = _unit_vector(header.read_dir, (1.0, 0.0, 0.0))
    phase = _unit_vector(header.phase_dir, (0.0, 1.0, 0.0))
    slice_dir = _unit_vector(header.slice_dir, (0.0, 0.0, 1.0))
    fov = np.asarray(header.field_of_view, dtype=float)
    dims = np.asarray(shape_xyz, dtype=float)
    voxel = np.divide(fov, dims, out=np.ones(3), where=dims > 0)
    if shape_xyz[2] > 1 and voxel[2] < min(voxel[:2]) * 0.1:
        voxel[2] = fov[2]
    position = np.asarray(header.position, dtype=float)
    origin = (
        position
        - read * voxel[0] * (shape_xyz[0] - 1) / 2.0
        - phase * voxel[1] * (shape_xyz[1] - 1) / 2.0
        - slice_dir * voxel[2] * (shape_xyz[2] - 1) / 2.0
    )
    affine = np.eye(4)
    affine[:3, 0] = read * voxel[0]
    affine[:3, 1] = phase * voxel[1]
    affine[:3, 2] = slice_dir * voxel[2]
    affine[:3, 3] = origin
    return affine


def _affine_from_stack(images, shape_xyz):
    affine = _affine_from_image(images[0], shape_xyz)
    positions = [np.asarray(image.getHead().position, dtype=float) for image in images]
    if len(positions) > 1:
        step = (positions[-1] - positions[0]) / (len(positions) - 1)
        if np.linalg.norm(step) > 0:
            affine[:3, 2] = step
    affine[:3, 3] = (
        positions[0]
        - affine[:3, 0] * (shape_xyz[0] - 1) / 2.0
        - affine[:3, 1] * (shape_xyz[1] - 1) / 2.0
    )
    return affine


def _unit_vector(value, default):
    vector = np.asarray(value, dtype=float)
    norm = np.linalg.norm(vector)
    return vector / norm if np.isfinite(norm) and norm > 0 else np.asarray(default)


def _scanner_display_data(data):
    finite = data[np.isfinite(data)]
    if not finite.size:
        return np.zeros(data.shape, dtype=np.uint16), 1.0, 0.0
    low, high = np.percentile(finite, [0.5, 99.5])
    if high <= low:
        low = float(np.min(finite))
        high = float(np.max(finite))
    if high <= low:
        return np.zeros(data.shape, dtype=np.uint16), 1.0, float(low)
    scale = 4095.0 / float(high - low)
    display = np.clip((data - low) * scale, 0, 4095).astype(np.uint16)
    return display, scale, float(low)


def _geometry_image_for_slice(source_slices, anchor, slice_index, slice_count):
    if len(source_slices) == slice_count:
        return source_slices[slice_index]
    return anchor


def _packed_slice_position(header, slice_index, slice_count):
    position = np.asarray(header.position, dtype=float)
    slice_dir = _unit_vector(header.slice_dir, (0.0, 0.0, 1.0))
    spacing = float(header.field_of_view[2])
    if spacing <= 0:
        spacing = 1.0
    return position + slice_dir * spacing * (slice_index - (slice_count - 1) / 2.0)


def _derived_meta(
    source, series_index, role, description, image_index,
    slices, timepoints, scale, offset,
):
    meta = _meta(source)
    for key in ("IceMiniHead", "SOPInstanceUID", "SeriesInstanceUID"):
        if key in meta:
            del meta[key]
    meta["Keep_image_geometry"] = "1"
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["MEICA", "OPENRECON"]
    meta["SequenceDescription"] = description
    meta["SeriesDescription"] = description
    meta["SeriesNumber"] = str(series_index)
    meta["ImageType"] = ["DERIVED", "PRIMARY", "M", role]
    meta["ImageTypeValue4"] = role
    meta["MEICAScale"] = f"{scale:.12g}"
    meta["MEICAOffset"] = f"{offset:.12g}"
    meta["MEICAInverseScaleFormula"] = "value = display / MEICAScale + MEICAOffset"
    meta["NumberOfSlices"] = str(slices)
    meta["ImagesInAcquisition"] = str(slices * timepoints)
    meta["NumberInSeries"] = str(image_index)
    return meta


def _original_passthrough(image):
    original = copy.deepcopy(image)
    meta = _meta(original)
    meta["Keep_image_geometry"] = "1"
    original.attribute_string = meta.serialize()
    return original


def _meta(image):
    try:
        return ismrmrd.Meta.deserialize(image.attribute_string or "")
    except Exception:
        return ismrmrd.Meta()


def _meta_first(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _set_vector(target, values):
    for index, value in enumerate(values):
        target[index] = value


def _reserve_series(used, preferred):
    value = preferred
    while value in used:
        value += 1
    used.add(value)
    return value


def _sidecar_path(path):
    name = path.name
    if name.endswith(".nii.gz"):
        return path.with_name(name[:-7] + ".json")
    return path.with_suffix(".json")


def _write_json(path, contents):
    path.write_text(
        json.dumps({key: value for key, value in contents.items() if value is not None}, indent=2)
        + "\n",
        encoding="utf-8",
    )
