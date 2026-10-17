"""OpenRecon image-to-image adapter for ME-ICA v4.

The adapter converts reconstructed multi-echo magnitude MRD images into one
four-dimensional NIfTI file per echo, runs ME-ICA, and returns the denoised
BOLD-like apparent dR2* series and T2* map as scanner-displayable MRD images.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import tempfile
import traceback
import uuid

import ismrmrd
import nibabel as nib
from nibabel.processing import resample_from_to
import numpy as np

from openrecon_metadata import new_uid, processing_metadata, stamp_derived

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


DEFAULT_MEICA_BINARY = "/usr/local/bin/meica.py"
DEFAULT_CPUS = 24
OPENRECON_WORK_ROOT = Path("/tmp/share/meica_openrecon")
OUTPUT_SERIES_START = 200
OUTPUT_SPECS = {
    "dr2s": ("OR_MEICA_DR2S", "MEICA denoised BOLD dR2*"),
    "t2s": ("OR_MEICA_T2S", "MEICA T2* map"),
}
OUTPUT_UNITS = {"dr2s": "1/s", "t2s": "ms"}
SIGNED_OUTPUTS = {"dr2s"}
SCANNER_DISPLAY_MAX = 4095
SCANNER_DISPLAY_CENTER = 2048
SCANNER_DISPLAY_PADDING_VALUE = 0
# Stop at 1 so stored steps never exceed 1 ms or 1/s; larger values saturate.
SCANNER_DISPLAY_SCALE_FACTORS = (1000000.0, 100000.0, 10000.0, 1000.0, 100.0, 10.0, 1.0)
SCANNER_DISPLAY_SCALE_PERCENTILE = 99.9
SCANNER_WINDOW_PERCENTILE = {"dr2s": 99.0, "t2s": 95.0}
# Multi-frame parent references that tie an image to its source DICOM object.
SOURCE_PARENT_REFERENCE_META_KEYS = {
    "DicomEngineDimString",
    "MFInstanceNumber",
    "MultiFrameSOPInstanceUID",
    "PSMultiFrameSOPInstanceUID",
    "PSSeriesInstanceUID",
}
SOURCE_PARENT_REFERENCE_META_PREFIXES = (
    "ReferencedGSPS",
    "ReferencedImageSequence",
)
SOURCE_SCALING_META_KEYS = (
    "PixelPaddingRangeLimit",
    "PixelPaddingValue",
    "RescaleIntercept",
    "RescaleSlope",
    "RescaleType",
    "VOILUTFunction",
    "WindowCenter",
    "WindowWidth",
)


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

        settings = _settings(config)
        if settings["send_original"] and images:
            connection.send_image([
                _original_passthrough(image, metadata) for image in images
            ])
        OPENRECON_WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="run_", dir=str(OPENRECON_WORK_ROOT)
        ) as work_name:
            work_dir = Path(work_name)
            conversion = write_meica_inputs(
                images, metadata, work_dir / "input", settings
            )
            _run_meica(conversion, work_dir / "output", settings)
            selected = _find_outputs(work_dir / "output")
            output_images = _outputs_to_mrd(selected, conversion, images)

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
    images = _select_bold_images(images)
    if not images:
        raise ValueError("ME-ICA requires reconstructed magnitude fMRI image messages")
    echo_groups = _group_echo_images(images)
    if len(echo_groups) < 3:
        raise ValueError(
            f"ME-ICA requires at least three echoes; detected {len(echo_groups)}"
        )

    echo_times_ms = _resolve_echo_times_ms(echo_groups, metadata)
    datasets = []
    reference_shape = None
    reference_timepoints = None
    source_geometry = None

    for echo_index, group in enumerate(echo_groups):
        time_groups = _group_echo_into_timepoints(group)
        volumes = []
        for time_index, time_group in enumerate(time_groups):
            volume, affine = _images_to_volume(time_group)
            if volumes and volume.shape != volumes[0].shape:
                raise ValueError(
                    f"ME-ICA echo {echo_index + 1}, time point {time_index + 1} "
                    f"has volume shape {volume.shape}; expected {volumes[0].shape} "
                    f"from {len(time_groups[0])} slice images"
                )
            volumes.append(volume)
        data = np.stack(volumes, axis=3).astype(np.float32)

        if reference_shape is None:
            reference_shape = data.shape[:3]
            reference_timepoints = data.shape[3]
            source_geometry = {
                "anchor": _ordered_slices(time_groups[0])[0],
                "first_timepoint": _ordered_slices(time_groups[0]),
                "shape": data.shape[:3],
                "affine": affine.copy(),
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
        "processing": processing_metadata("ME-ICA", __file__, {
            **settings, "echo_times_ms": echo_times_ms,
            "repetition_time_s": _metadata_tr_seconds(metadata),
        }),
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


def _find_outputs(output_dir):
    found = []
    for output_id in OUTPUT_SPECS:
        candidates = [
            candidate
            for candidate in (
                output_dir / f"openrecon_{output_id}_epi.nii.gz",
                output_dir / f"openrecon_{output_id}_epi.nii",
            )
            if candidate.is_file()
        ]
        if not candidates:
            raise FileNotFoundError(
                f"ME-ICA did not create the required {output_id}_epi output"
            )
        found.append((output_id, candidates[0]))
    return found


def _outputs_to_mrd(selected, conversion, input_images):
    used_series = {
        int(image.getHead().image_series_index) for image in input_images
    }
    all_series = []
    for offset, (output_id, path) in enumerate(selected):
        series_index = _reserve_series(used_series, OUTPUT_SERIES_START + offset)
        all_series.append(
            _nifti_to_mrd_series(
                path,
                conversion["source_geometry"],
                series_index,
                output_id,
                processing=conversion.get("processing"),
            )
        )
    return all_series


def _nifti_to_mrd_series(
    path, source_geometry, series_index, output_id, *, processing=None,
):
    role, description = OUTPUT_SPECS[output_id]
    series_uid = new_uid()
    processing = processing or processing_metadata("ME-ICA", __file__, {})
    nifti = nib.load(str(path))
    data = _output_data_in_source_space(nifti, source_geometry, path)
    if data.ndim == 3:
        data = data[:, :, :, np.newaxis]
    if data.ndim != 4:
        raise ValueError(f"ME-ICA output must be 3D or 4D, got {path}: {data.shape}")

    display, display_meta = _scanner_display_volume(data, output_id)
    logging.info(
        "%s scanner display: %s, %d clipped values",
        role,
        display_meta["formula"],
        display_meta["clipped_voxels"],
    )
    data_zyxt = np.transpose(display, (2, 1, 0, 3))
    source_slices = source_geometry["first_timepoint"]
    anchor = source_geometry["anchor"]
    output = []
    image_index = 1

    for time_index in range(data_zyxt.shape[3]):
        for slice_index in range(data_zyxt.shape[0]):
            slice_data = data_zyxt[slice_index:slice_index + 1, :, :, time_index]
            result = ismrmrd.Image.from_array(slice_data, transpose=False)
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
                display_meta,
                series_uid=series_uid,
                processing=processing,
            ).serialize()
            output.append(result)
            image_index += 1
    logging.info("Prepared %d MRD images for %s", len(output), role)
    return output


def _output_data_in_source_space(nifti, source_geometry, path):
    source_shape = tuple(source_geometry["shape"])
    source_affine = np.asarray(source_geometry["affine"], dtype=float)
    if (
        tuple(nifti.shape[:3]) == source_shape
        and np.allclose(nifti.affine, source_affine, rtol=1e-5, atol=1e-4)
    ):
        return np.asarray(nifti.get_fdata(dtype=np.float32), dtype=np.float32)

    logging.info(
        "Resampling ME-ICA output %s from %s to scanner grid %s",
        path,
        nifti.shape[:3],
        source_shape,
    )
    if len(nifti.shape) == 3:
        resampled = resample_from_to(
            nifti, (source_shape, source_affine), order=1
        )
        return np.asarray(resampled.get_fdata(dtype=np.float32), dtype=np.float32)
    if len(nifti.shape) != 4:
        raise ValueError(f"ME-ICA output must be 3D or 4D, got {path}: {nifti.shape}")

    data = np.empty(source_shape + (nifti.shape[3],), dtype=np.float32)
    for time_index in range(nifti.shape[3]):
        volume = nib.Nifti1Image(
            np.asanyarray(nifti.dataobj[..., time_index]),
            nifti.affine,
        )
        resampled = resample_from_to(
            volume, (source_shape, source_affine), order=1
        )
        data[..., time_index] = resampled.get_fdata(dtype=np.float32)
    return data


def _group_echo_images(images):
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
    elif len(set(series)) > 1:
        keys = [("series", value) for value in series]
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

    if all(key[0] in {"number", "time", "contrast", "series"} for key in order):
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
    use_slice_index = any(int(image.getHead().slice) != 0 for image in images)
    if len(set(candidate_keys)) > 1:
        groups = {}
        for image, key in zip(images, candidate_keys):
            groups.setdefault(key, []).append(image)
        timepoints = [groups[key] for key in sorted(groups)]
        _validate_timepoint_slices(timepoints, use_slice_index=use_slice_index)
        return timepoints

    ordered = sorted(images, key=_image_index_sort_key)
    slice_ids = [
        _slice_identity(image, use_slice_index=use_slice_index) for image in ordered
    ]
    slice_count = len(set(slice_ids))
    if slice_count <= 1:
        return [[image] for image in ordered]
    if len(ordered) % slice_count != 0:
        raise ValueError(
            f"Cannot divide {len(ordered)} images into {slice_count}-slice time points"
        )
    timepoints = [
        ordered[index:index + slice_count]
        for index in range(0, len(ordered), slice_count)
    ]
    _validate_timepoint_slices(timepoints, use_slice_index=use_slice_index)
    return timepoints


def _validate_timepoint_slices(
    timepoints: list[list[ismrmrd.Image]], *, use_slice_index: bool
) -> None:
    expected = {
        _slice_identity(image, use_slice_index=use_slice_index)
        for image in timepoints[0]
    }
    for index, timepoint in enumerate(timepoints):
        identities = [
            _slice_identity(image, use_slice_index=use_slice_index)
            for image in timepoint
        ]
        actual = set(identities)
        if len(identities) != len(actual):
            raise ValueError(
                f"ME-ICA time point {index + 1} contains duplicate slice images; "
                "reference images or multiple source series may share its repetition"
            )
        if actual != expected:
            raise ValueError(
                f"ME-ICA time point {index + 1} has {len(actual)} slices; "
                f"expected {len(expected)} with matching slice identities "
                f"({len(expected - actual)} missing, {len(actual - expected)} unexpected)"
            )


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


def _resolve_echo_times_ms(echo_groups, metadata):
    values = _metadata_echo_times_ms(metadata)
    source = "MRD header"
    if not values or any(value is None for value in values):
        raise ValueError("ME-ICA echo times are missing from the MRD header")
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
    return {
        "cpus": DEFAULT_CPUS,
        "send_original": _as_bool(params.get("sendoriginal", True)),
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


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _is_magnitude(image):
    image_type = int(getattr(image, "image_type", image.getHead().image_type))
    return image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1)))


def _select_bold_images(images: list[ismrmrd.Image]) -> list[ismrmrd.Image]:
    """Keep magnitude fMRI images, excluding reference and noise images."""
    selected = []
    excluded = {}
    for image in images:
        reason = _input_exclusion_reason(image)
        if reason is None:
            selected.append(image)
        else:
            excluded[reason] = excluded.get(reason, 0) + 1
    logging.info(
        "Selected %d ME-ICA fMRI images; excluded image counts: %s",
        len(selected), excluded,
    )
    return selected


def _input_exclusion_reason(image: ismrmrd.Image) -> str | None:
    if not _is_magnitude(image):
        return "non-magnitude"
    return _scanner_image_role(image)


def _scanner_image_role(image: ismrmrd.Image) -> str | None:
    meta = _meta(image)
    description = str(_meta_first(meta, "SequenceDescription") or "")
    if "sbref" in description.lower():
        return "reference"
    value4 = meta.get("ImageTypeValue4", [])
    if not isinstance(value4, (list, tuple)):
        value4 = [value4]
    tokens = {
        token.upper()
        for value in value4
        for token in re.split(r"[\s\\,]+", str(value))
    }
    # Siemens keeps CMRR's NOISE marker in its base64 XProtocol minihead.
    minihead = _decode_minihead(meta)
    if minihead:
        if not description:
            match = re.search(
                r'<ParamString\."SequenceDescription">\s*\{\s*"([^"]*)"',
                minihead,
            )
            if match and "sbref" in match.group(1).lower():
                return "reference"
        match = re.search(r'<ParamArray\."ImageTypeValue4">\s*\{', minihead)
        if match:
            start = match.end()
            depth = 1
            for end in range(start, len(minihead)):
                depth += (minihead[end] == "{") - (minihead[end] == "}")
                if depth == 0:
                    tokens.update(
                        token.upper()
                        for token in re.findall(
                            r'\{\s*"([^"]+)"\s*\}', minihead[start:end]
                        )
                    )
                    break
    if "SBREF" in tokens:
        return "reference"
    if "NOISE" in tokens:
        return "noise"
    return None


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


def _slice_identity(image, *, use_slice_index=True):
    header = image.getHead()
    if use_slice_index:
        return (0, int(getattr(header, "slice", 0)))
    position = tuple(round(float(value), 5) for value in header.position)
    return (1, position)


def _ordered_slices(images):
    use_slice_index = any(int(image.getHead().slice) != 0 for image in images)
    return sorted(
        images,
        key=lambda image: (
            _slice_identity(image, use_slice_index=use_slice_index),
            int(image.getHead().image_index),
        ),
    )


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


def _scanner_display_volume(data, output_id):
    """Encode physical values as 12-bit pixels that DICOM rescaling restores."""
    units = OUTPUT_UNITS[output_id]
    values = np.nan_to_num(
        np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0
    )
    signed = output_id in SIGNED_OUTPUTS
    magnitudes = np.abs(values) if signed else values
    nonzero = magnitudes[magnitudes > 0.0]
    offset = float(SCANNER_DISPLAY_CENTER) if signed else 0.0
    display_min = SCANNER_DISPLAY_PADDING_VALUE + 1 if signed else 0
    headroom = SCANNER_DISPLAY_MAX - offset

    scale_max = (
        float(np.percentile(nonzero, SCANNER_DISPLAY_SCALE_PERCENTILE))
        if nonzero.size
        else 0.0
    )
    scale = SCANNER_DISPLAY_SCALE_FACTORS[0]
    if scale_max > 0.0:
        scale = next(
            (
                factor
                for factor in SCANNER_DISPLAY_SCALE_FACTORS
                if factor * scale_max <= headroom
            ),
            SCANNER_DISPLAY_SCALE_FACTORS[-1],
        )

    scaled = values * scale + offset
    clipped_voxels = int(
        np.count_nonzero((scaled < display_min) | (scaled > SCANNER_DISPLAY_MAX))
    )
    display = np.clip(np.rint(scaled), display_min, SCANNER_DISPLAY_MAX)

    window_high = headroom / scale
    if nonzero.size:
        window_high = min(
            window_high,
            float(np.percentile(nonzero, SCANNER_WINDOW_PERCENTILE[output_id])),
        )
    window_low = -window_high if signed else 0.0

    formula = f"{units} = display / {_format_number(scale)}"
    if offset:
        formula = (
            f"{units} = (display - {_format_number(offset)}) / {_format_number(scale)}"
        )
    return display.astype(np.uint16), {
        "units": units,
        "scale": scale,
        "offset": offset,
        "formula": formula,
        "rescale_slope": 1.0 / scale,
        "rescale_intercept": -offset / scale,
        "padding_value": SCANNER_DISPLAY_PADDING_VALUE if signed else None,
        "clipped_voxels": clipped_voxels,
        # Siemens truncates fractional window values, so publish integers.
        "window_center": int(np.rint((window_low + window_high) / 2.0)),
        "window_width": max(1, int(np.rint(window_high - window_low))),
    }


def _format_number(value):
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6g}"


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
    slices, timepoints, display_meta,
    *, series_uid, processing,
):
    meta = _meta(source)
    for key in (
        "IceMiniHead",
        "SOPInstanceUID",
        "SeriesInstanceUID",
        *SOURCE_SCALING_META_KEYS,
    ):
        if key in meta:
            del meta[key]
    meta["Keep_image_geometry"] = "1"
    meta["DataRole"] = ["Image", "Quantitative"]
    meta["ImageProcessingHistory"] = ["MEICA", "OPENRECON"]
    _set_series_label(meta, description, series_index)
    meta["SeriesNumber"] = str(series_index)
    meta["ImageComments"] = (
        f"{description}; scanner display uint16 0-{SCANNER_DISPLAY_MAX}; "
        f"{display_meta['formula']}"
    )
    meta["MEICAUnits"] = display_meta["units"]
    meta["MEICADisplayScale"] = _format_number(display_meta["scale"])
    meta["MEICADisplayOffset"] = _format_number(display_meta["offset"])
    meta["MEICADisplayFormula"] = display_meta["formula"]
    meta["MEICADisplayClippedVoxels"] = str(display_meta["clipped_voxels"])
    meta["RescaleSlope"] = _format_number(display_meta["rescale_slope"])
    meta["RescaleIntercept"] = _format_number(display_meta["rescale_intercept"])
    meta["RescaleType"] = "US"
    if display_meta["padding_value"] is not None:
        meta["PixelPaddingValue"] = str(display_meta["padding_value"])
        meta["PixelPaddingRangeLimit"] = str(display_meta["padding_value"])
    meta["WindowCenter"] = str(display_meta["window_center"])
    meta["WindowWidth"] = str(display_meta["window_width"])
    meta["NumberOfSlices"] = str(slices)
    meta["ImagesInAcquisition"] = str(slices * timepoints)
    meta["NumberInSeries"] = str(image_index)
    stamp_derived(
        meta, _meta(source), role, series_uid, processing, image_type_as_list=True,
    )
    return meta


def _original_passthrough(image: ismrmrd.Image, metadata=None) -> ismrmrd.Image:
    original = copy.deepcopy(image)
    meta = _meta(original)
    source_name = ""
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        source_name = str(_meta_first(meta, key) or "")
        if source_name:
            break
    if not source_name:
        minihead = _decode_minihead(meta)
        match = re.search(
            r'<ParamString\."SequenceDescription">\s*\{\s*"([^"]*)"', minihead
        )
        if match:
            source_name = match.group(1)
    component = {
        int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1)): "magnitude",
        int(getattr(ismrmrd, "IMTYPE_PHASE", 2)): "phase",
        int(getattr(ismrmrd, "IMTYPE_REAL", 3)): "real",
        int(getattr(ismrmrd, "IMTYPE_IMAG", 4)): "imaginary",
    }.get(int(image.getHead().image_type), "magnitude")
    role = {"reference": "SBRef ", "noise": "noise "}.get(
        _scanner_image_role(image), ""
    )
    echo = _original_echo_number(image, metadata)
    description = f"MEICA original {role}{component} te{echo}"
    if source_name:
        description += f"_{source_name}"
    _set_series_label(meta, description, int(image.getHead().image_series_index))
    _set_original_uids(meta, image)
    meta["Keep_image_geometry"] = "1"
    original.attribute_string = meta.serialize()
    return original


def _set_original_uids(meta: ismrmrd.Meta, image: ismrmrd.Image) -> None:
    """Give a relabelled original its own series and instance UIDs.

    Relabelling splits one source acquisition into a series per echo and role.
    If those series kept the source UIDs, the scanner's DicomWriter would join
    frames from different series to one multi-frame parent and abort the
    measurement after the first frame.
    """
    minihead = _decode_minihead(meta)
    source_series_uid = _source_uid(meta, minihead, "SeriesInstanceUID")
    source_sop_uid = _source_uid(meta, minihead, "SOPInstanceUID")
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS or key.startswith(
            SOURCE_PARENT_REFERENCE_META_PREFIXES
        ):
            del meta[key]
    grouping = str(_meta_first(meta, "SeriesNumberRangeNameUID") or "")
    series_uid = _uid("meica-original-series", source_series_uid, grouping)
    head = image.getHead()
    sop_uid = _uid(
        "meica-original-instance", series_uid, source_sop_uid,
        *(int(getattr(head, field)) for field in (
            "slice", "contrast", "phase", "repetition", "set", "average",
            "image_index",
        )),
    )
    uids = {"SeriesInstanceUID": series_uid, "SOPInstanceUID": sop_uid}
    meta.update(uids)
    if minihead:
        for key, value in uids.items():
            pattern = rf'(<ParamString\."{key}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
            minihead, count = re.subn(
                pattern,
                lambda match: f'{match.group(1)}"{value}"{match.group(2)}',
                minihead,
            )
            if not count:
                minihead += f'\n<ParamString."{key}">{{ "{value}" }}\n'
        meta["IceMiniHead"] = base64.b64encode(minihead.encode()).decode()


def _source_uid(meta: ismrmrd.Meta, minihead: str, key: str) -> str:
    value = str(_meta_first(meta, key) or "")
    if value or not minihead:
        return value
    match = re.search(rf'<ParamString\."{key}">\s*\{{\s*"([^"]*)"', minihead)
    return match.group(1) if match else ""


def _uid(*parts) -> str:
    seed = "|".join(str(part) for part in parts)
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _original_echo_number(image: ismrmrd.Image, metadata) -> int:
    echo_time = _image_echo_time_ms(image)
    for index, time in enumerate(_metadata_echo_times_ms(metadata), 1):
        if echo_time is not None and np.isclose(echo_time, time):
            return index
    number = _echo_number(image)
    if number is not None and number > 0:
        return number
    return int(image.getHead().contrast) + 1


def _decode_minihead(meta: ismrmrd.Meta) -> str:
    encoded = _meta_first(meta, "IceMiniHead")
    if not encoded:
        return ""
    try:
        return base64.b64decode(str(encoded)).decode("utf-8")
    except (ValueError, UnicodeError):
        return ""


def _set_series_label(
    meta: ismrmrd.Meta, description: str, series_index: int
) -> None:
    description = description.replace('"', "'").replace("\r", " ").replace("\n", " ")
    source_group = _meta_first(meta, "SeriesNumberRangeNameUID") or ""
    grouping = f"{source_group}_{description}_{series_index}"
    # Reserve ten of DICOM LO's 64 characters for the scanner's _openrecon suffix.
    description = description[:54]
    labels = {
        "SeriesDescription": description,
        "SequenceDescription": description,
        "ProtocolName": description,
        "SeriesNumberRangeNameUID": grouping,
    }
    meta.update(labels)
    meta["SequenceDescriptionAdditional"] = "openrecon"
    minihead = _decode_minihead(meta)
    if minihead:
        for key, value in labels.items():
            pattern = rf'(<ParamString\."{key}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
            minihead, count = re.subn(
                pattern,
                lambda match: f'{match.group(1)}"{value}"{match.group(2)}',
                minihead,
            )
            if not count:
                minihead += f'\n<ParamString."{key}">{{ "{value}" }}\n'
        meta["IceMiniHead"] = base64.b64encode(minihead.encode()).decode()


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
