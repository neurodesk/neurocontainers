import argparse
import base64
import ctypes
import itertools
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from time import perf_counter
import traceback
import uuid

import ismrmrd
import nibabel as nib
import numpy as np
import numpy.fft as fft
import scipy.ndimage as ndi

import constants
import mrdhelper


# Folder for debug output files
debugFolder = "/tmp/share/debug"
OPENRECON_WORKSPACE_ROOT = "vesselboost_openrecon"
OPENRECON_OUTPUT_NAME_PARAM = "vboutputname"
OPENRECON_MODULE_DEFAULT = "prediction"
OPENRECON_MODULE_VALUES = (OPENRECON_MODULE_DEFAULT,)
OPENRECON_SERIES_SUFFIX = "OR"
VESSELBOOST_OUTPUT_GEOMETRY_2D = "2d"
VESSELBOOST_SEGMENT_SEND_ORDER = "after_originals"
VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY = "SegmentPostProcessing"
VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY = "SegmentPostProcessingChildRole"
VESSELBOOST_SEGMENT_SOURCE_GEOMETRY_META_KEY = "SegmentSourceGeometry"
VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY = "SegmentSourceImageHeader"
VESSELBOOST_SEGMENT_OUTPUT_GEOMETRY_META_KEY = "SegmentOutputGeometry"
VESSELBOOST_SEGMENTATION_LABEL = "vesselboost"
VESSELBOOST_SEGMENTATION_TYPE_TOKEN = VESSELBOOST_SEGMENTATION_LABEL.upper()
VESSELBOOST_SOURCE_IMAGE_HEADER_FALLBACK_TYPE = (
    f"DERIVED\\PRIMARY\\SEGMENTATION\\{VESSELBOOST_SEGMENTATION_LABEL}_source_image_header"
)
VESSELBOOST_SOURCE_IMAGE_HEADER_FALLBACK_VALUE4 = (
    f"{VESSELBOOST_SEGMENTATION_LABEL}_source_image_header"
)
VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE = (
    f"DERIVED\\PRIMARY\\SEGMENTATION\\{VESSELBOOST_SEGMENTATION_LABEL}_source_geometry"
)
VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4 = (
    f"{VESSELBOOST_SEGMENTATION_LABEL}_source_geometry"
)
VESSELBOOST_ORIGINAL_LABEL = "original"
VESSELBOOST_ORIGINAL_TYPE_TOKEN = VESSELBOOST_ORIGINAL_LABEL.upper()
VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3 = "M"
SCANNER_PARTITION_INDEX = 0
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

# Keep this overview aligned with recipes/vesselboost/OpenReconLabel.json.
# Tests can import it to build a minimal OpenRecon config payload.
OPENRECON_DEFAULTS = {
    "config": "vesselboost",
    "sendoriginal": True,
    "vbuseblending": False,
    "vboverlap": 50,
    "vbbiasfieldcorrection": True,
    "vbdenoising": False,
    "vbbrainextraction": True,
    "vbdebugthresholdsegment": False,
    "vbreslicesagittal": False,
    "vbreslicecoronal": False,
    "vbsegmentationmips": False,
}
OPENRECON_TRAINING_DEFAULTS = {
    "vbepochs": "200",
    "vbrate": "0.001",
}
OPENRECON_DEFAULT_TEST_CONFIG = {
    "parameters": OPENRECON_DEFAULTS.copy(),
}

OPENRECON_COMBINATION_PARAMETER_VALUES = {
    "vbuseblending": (False, True),
    "vbbiasfieldcorrection": (False, True),
    "vbdenoising": (False, True),
    "vbbrainextraction": (False, True),
}

PREP_MODE_FROM_FLAGS = {
    (True, False): 1,
    (False, True): 2,
    (True, True): 3,
    (False, False): 4,
}


def _log_array_summary(label: str, data: np.ndarray) -> None:
    logging.info(
        "%s summary: shape=%s ndim=%d dtype=%s is_complex=%s",
        label,
        data.shape,
        data.ndim,
        data.dtype,
        np.iscomplexobj(data),
    )

    if data.size == 0:
        logging.warning("%s is empty", label)
        return

    summary_data = np.abs(data) if np.iscomplexobj(data) else data
    logging.info(
        "%s value range: min=%s max=%s",
        label,
        float(np.min(summary_data)),
        float(np.max(summary_data)),
    )


def _log_directory_contents(label: str, path: Path) -> None:
    if not path.exists():
        logging.info("%s does not exist: %s", label, path)
        return

    entries = sorted(child.name for child in path.iterdir())
    logging.info("%s contents (%s): %s", label, path, entries)


def _simple_threshold_segmentation_volume(input_yxz: np.ndarray, max_val: int) -> np.ndarray:
    data = np.asarray(input_yxz)
    if np.iscomplexobj(data):
        data = np.abs(data)
    data = np.asarray(data, dtype=np.float32)
    if data.ndim == 2:
        data = data[:, :, None]
    if data.ndim != 3:
        raise ValueError(
            "Debug threshold segmentation expects a 2D or 3D input volume, "
            f"got shape {data.shape}"
        )

    threshold = _bright_foreground_threshold(data)
    foreground = data >= threshold
    foreground &= np.isfinite(data)
    segmentation_zyx = _largest_connected_component_per_plane(
        foreground.transpose((2, 0, 1))
    )
    segmentation_yxz = segmentation_zyx.transpose((1, 2, 0))
    logging.warning(
        "Using debug simple threshold segmentation: threshold=%.6g "
        "foreground_voxels=%d kept_voxels=%d",
        float(threshold),
        int(np.count_nonzero(foreground)),
        int(np.count_nonzero(segmentation_yxz)),
    )
    return (segmentation_yxz.astype(np.int16) * int(max_val)).astype(np.int16)


def _bright_foreground_threshold(data: np.ndarray) -> float:
    finite_values = np.asarray(data, dtype=np.float32)
    finite_values = finite_values[np.isfinite(finite_values)]
    if finite_values.size == 0:
        return np.inf

    data_min = float(np.min(finite_values))
    data_max = float(np.max(finite_values))
    if data_max <= data_min:
        return np.inf

    background = float(np.percentile(finite_values, 50))
    upper = float(np.percentile(finite_values, 95))
    if upper <= background:
        upper = data_max

    threshold = background + 0.5 * (upper - background)
    if threshold >= data_max:
        threshold = background + 0.5 * (data_max - background)
    return threshold


def _largest_connected_component_per_plane(mask: np.ndarray) -> np.ndarray:
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim < 2:
        return _largest_connected_component_2d(mask.reshape(1, -1)).reshape(mask.shape)

    result = np.zeros(mask.shape, dtype=bool)
    planes = mask.reshape((-1,) + mask.shape[-2:])
    result_planes = result.reshape((-1,) + mask.shape[-2:])
    for index, plane in enumerate(planes):
        result_planes[index] = _largest_connected_component_2d(plane)
    return result


def _largest_connected_component_2d(mask: np.ndarray) -> np.ndarray:
    mask = np.asarray(mask, dtype=bool)
    height, width = mask.shape
    foreground = mask.ravel()
    foreground_indices = np.flatnonzero(foreground)
    if foreground_indices.size == 0:
        return np.zeros(mask.shape, dtype=bool)

    visited = np.zeros(foreground.size, dtype=bool)
    best_component = []
    for seed in foreground_indices:
        if visited[seed]:
            continue

        component = []
        stack = [int(seed)]
        visited[seed] = True
        while stack:
            current = stack.pop()
            component.append(current)
            row, col = divmod(current, width)
            neighbors = []
            if row > 0:
                neighbors.append(current - width)
            if row + 1 < height:
                neighbors.append(current + width)
            if col > 0:
                neighbors.append(current - 1)
            if col + 1 < width:
                neighbors.append(current + 1)
            for neighbor in neighbors:
                if foreground[neighbor] and not visited[neighbor]:
                    visited[neighbor] = True
                    stack.append(neighbor)

        if len(component) > len(best_component):
            best_component = component

    result = np.zeros(foreground.size, dtype=bool)
    result[best_component] = True
    return result.reshape(mask.shape)


def compute_nifti_affine(image_header, voxel_size):
    # MRD stores geometry in DICOM/LPS. NIfTI uses RAS.
    lps_to_ras = np.array([-1, -1, 1], dtype=float)

    position = np.asarray(image_header.position, dtype=float) * lps_to_ras
    read_dir = np.asarray(image_header.read_dir, dtype=float) * lps_to_ras
    phase_dir = np.asarray(image_header.phase_dir, dtype=float) * lps_to_ras
    slice_dir = np.asarray(image_header.slice_dir, dtype=float) * lps_to_ras

    affine = np.eye(4)
    affine[:3, :3] = np.column_stack(
        [
            voxel_size[0] * read_dir,
            voxel_size[1] * phase_dir,
            voxel_size[2] * slice_dir,
        ]
    )
    affine[:3, 3] = position
    return affine


def _header_vector(image_header, field_name):
    try:
        return np.asarray(getattr(image_header, field_name), dtype=float)
    except Exception:
        return np.zeros(3, dtype=float)


def _normalize_vector(vector):
    vector = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm < 1e-8:
        return None
    return vector / norm


def _format_vector(vector):
    return "[" + ", ".join(f"{float(value):.3f}" for value in vector) + "]"


def _infer_slice_axis(image_headers):
    for image_header in image_headers:
        axis = _normalize_vector(_header_vector(image_header, "slice_dir"))
        if axis is not None:
            return axis

    if len(image_headers) > 1:
        axis = _normalize_vector(
            _header_vector(image_headers[-1], "position")
            - _header_vector(image_headers[0], "position")
        )
        if axis is not None:
            return axis

    return np.array([0.0, 0.0, 1.0], dtype=float)


def _build_slice_geometry_records(image_headers, input_indices=None, slice_axis=None):
    if input_indices is None:
        input_indices = list(range(len(image_headers)))
    if slice_axis is None:
        slice_axis = _infer_slice_axis(image_headers)

    records = []
    for local_index, image_header in enumerate(image_headers):
        position = _header_vector(image_header, "position")
        slice_dir = _normalize_vector(_header_vector(image_header, "slice_dir"))
        slice_dir_dot_axis = float(np.dot(slice_dir, slice_axis)) if slice_dir is not None else 0.0
        records.append(
            {
                "local_index": local_index,
                "input_index": int(input_indices[local_index]),
                "image_index": int(getattr(image_header, "image_index", 0)),
                "slice": int(getattr(image_header, "slice", 0)),
                "phase": int(getattr(image_header, "phase", 0)),
                "position": position,
                "projected_position": float(np.dot(position, slice_axis)),
                "slice_dir_dot_axis": slice_dir_dot_axis,
            }
        )

    return slice_axis, records


def _estimate_slice_spacing(image_headers, slice_axis=None):
    if len(image_headers) < 2:
        return None

    slice_axis, records = _build_slice_geometry_records(image_headers, slice_axis=slice_axis)
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    sorted_diffs = np.diff(np.sort(projected_positions))
    nonzero_diffs = np.abs(sorted_diffs[np.abs(sorted_diffs) > 1e-4])
    if nonzero_diffs.size == 0:
        return None
    return float(np.median(nonzero_diffs))


def _slice_sort_indices(image_headers):
    slice_axis, records = _build_slice_geometry_records(image_headers)
    sorted_records = sorted(
        records,
        key=lambda record: (
            round(record["projected_position"], 4),
            record["slice"],
            record["image_index"],
            record["input_index"],
        ),
    )
    return [record["input_index"] for record in sorted_records], slice_axis, records


def _log_slice_geometry(label, image_headers, input_indices=None, slice_axis=None):
    slice_axis, records = _build_slice_geometry_records(
        image_headers,
        input_indices=input_indices,
        slice_axis=slice_axis,
    )
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    slice_dir_alignment = np.array(
        [record["slice_dir_dot_axis"] for record in records],
        dtype=float,
    )
    min_slice_dir_alignment = float(np.min(slice_dir_alignment)) if slice_dir_alignment.size else 1.0

    if len(projected_positions) > 1:
        current_diffs = np.diff(projected_positions)
        sorted_diffs = np.diff(np.sort(projected_positions))
        nonzero_sorted_diffs = np.abs(sorted_diffs[np.abs(sorted_diffs) > 1e-4])
        median_spacing = float(np.median(nonzero_sorted_diffs)) if nonzero_sorted_diffs.size else 0.0
        duplicate_positions = int(np.sum(np.abs(sorted_diffs) <= 1e-4))
        monotonic_increasing = bool(np.all(current_diffs >= -1e-4))
    else:
        median_spacing = 0.0
        duplicate_positions = 0
        monotonic_increasing = True

    logging.info(
        "%s slice geometry: count=%d axis=%s projected_range=[%.3f, %.3f] "
        "median_spacing=%.3f duplicates=%d order_inc=%s min_slice_dir_dot_axis=%.6f",
        label,
        len(records),
        _format_vector(slice_axis),
        float(np.min(projected_positions)) if projected_positions.size else 0.0,
        float(np.max(projected_positions)) if projected_positions.size else 0.0,
        median_spacing,
        duplicate_positions,
        monotonic_increasing,
        min_slice_dir_alignment,
    )

    if not monotonic_increasing:
        logging.warning(
            "%s slice positions are not increasing along slice_dir in their current order",
            label,
        )
    if duplicate_positions > 0:
        logging.warning(
            "%s has %d duplicate projected slice position(s)",
            label,
            duplicate_positions,
        )
    if min_slice_dir_alignment < 0.99:
        logging.warning(
            "%s has slice_dir vectors that are not aligned with the inferred slice axis",
            label,
        )

    return slice_axis, records


def _log_slice_sort_mapping(sort_indices):
    identity = list(range(len(sort_indices)))
    if sort_indices == identity:
        logging.info("VesselBoost input slice order already matches physical slice order")
        return

    logging.warning(
        "Reordering VesselBoost input slices by physical position: first mappings %s",
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:24])
        ),
    )
    if len(sort_indices) > 24:
        logging.warning(
            "Reordering mapping omitted %d additional slice(s)",
            len(sort_indices) - 24,
        )


def _log_affine_slice_consistency(image_headers, voxel_size):
    if len(image_headers) < 2:
        return

    _, records = _build_slice_geometry_records(image_headers)
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    expected_positions = projected_positions[0] + np.arange(len(projected_positions)) * float(voxel_size[2])
    residuals = projected_positions - expected_positions
    max_abs_residual = float(np.max(np.abs(residuals))) if residuals.size else 0.0
    logging.info(
        "NIfTI affine slice consistency: z_spacing=%.6f residual_range=[%.6f, %.6f] max_abs_residual=%.6f",
        float(voxel_size[2]),
        float(np.min(residuals)),
        float(np.max(residuals)),
        max_abs_residual,
    )
    if max_abs_residual > max(0.25, 0.25 * float(voxel_size[2])):
        logging.warning(
            "Input slice positions are not well described by a single linear NIfTI z-spacing; "
            "VesselBoost output will still reuse original per-slice MRD positions"
        )


def _is_nifti_file(path: Path) -> bool:
    return path.is_file() and path.name.endswith((".nii", ".nii.gz"))


def _find_output_nifti(output_dir: Path, expected_name: str) -> Path:
    if not output_dir.exists():
        raise FileNotFoundError(f"VesselBoost output directory does not exist: {output_dir}")

    preferred_names = (
        expected_name,
        f"{expected_name}.gz" if expected_name.endswith(".nii") else expected_name,
    )
    for name in preferred_names:
        candidate = output_dir / name
        if candidate.exists():
            return candidate

    candidates = [
        path for path in sorted(output_dir.iterdir())
        if _is_nifti_file(path) and not path.name.startswith("SIGMOID_")
    ]
    if len(candidates) == 1:
        return candidates[0]

    output_files = sorted(path.name for path in output_dir.iterdir())
    raise FileNotFoundError(
        f"Could not find a VesselBoost segmentation in {output_dir}. "
        f"Files present: {output_files}"
    )


def _resolve_vesselboost_model(model_name: str) -> Path:
    checked_paths = []
    candidate_dirs = []
    env_home = os.environ.get("VESSELBOOST_HOME")
    if env_home:
        candidate_dirs.append(Path(env_home))

    for command in ("prediction.py", "test_time_adaptation.py", "angiboost.py"):
        command_path = shutil.which(command)
        if command_path:
            candidate_dirs.append(Path(command_path).resolve().parent)

    candidate_dirs.append(Path("/opt/VesselBoost"))

    seen_dirs = set()
    for candidate_dir in candidate_dirs:
        candidate_dir = candidate_dir.resolve()
        candidate_key = str(candidate_dir)
        if candidate_key in seen_dirs:
            continue
        seen_dirs.add(candidate_key)

        model_path = candidate_dir / "saved_models" / model_name
        checked_paths.append(str(model_path))
        if model_path.exists():
            logging.info("Using VesselBoost pretrained model %s", model_path)
            return model_path

    raise FileNotFoundError(
        f"Could not find VesselBoost model '{model_name}'. Checked: {checked_paths}"
    )


def _config_has_param(config, key: str) -> bool:
    return (
        isinstance(config, dict)
        and isinstance(config.get("parameters"), dict)
        and key in config["parameters"]
    )


def _clone_mrd_image(image):
    image_copy = ismrmrd.Image.from_array(
        np.array(image.data, copy=True),
        transpose=False,
    )
    image_copy.setHead(image.getHead())
    image_copy.attribute_string = image.attribute_string
    return image_copy


def _meta_from_image(image):
    if not getattr(image, "attribute_string", ""):
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _copy_meta(meta_obj):
    try:
        return ismrmrd.Meta.deserialize(meta_obj.serialize())
    except Exception:
        meta_copy = ismrmrd.Meta()
        for key in meta_obj.keys():
            try:
                meta_copy[key] = meta_obj[key]
            except Exception:
                pass
        return meta_copy


def _strip_source_parent_refs(meta_obj):
    for key in list(meta_obj.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta_obj[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta_obj[key]


def _strip_scanner_write_unsafe_meta(meta_obj):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta_obj:
            del meta_obj[key]


def _set_meta_scalar(meta_obj, key, value):
    meta_obj[key] = str(int(value))


def _first_non_empty_text(*values):
    for value in values:
        if isinstance(value, (list, tuple)):
            for item in value:
                text = _first_non_empty_text(item)
                if text:
                    return text
            continue

        if value is None:
            continue

        text = str(value).strip()
        if text and text.upper() != "N/A":
            return text

    return ""


def _get_meta_text(meta_obj, key):
    try:
        return _first_non_empty_text(meta_obj.get(key))
    except Exception:
        return ""


def _get_meta_values(meta_obj, key):
    try:
        value = meta_obj.get(key)
    except Exception:
        value = None
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        value = [value]
    values = []
    for item in value:
        text = str(item or "").strip()
        if text and text.upper() != "N/A":
            values.append(text)
    return values


def _get_meta_int(meta_obj, key):
    text = _get_meta_text(meta_obj, key)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _meta_text_values(value):
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        raw_values = value
    else:
        raw_values = str(value).split("\\")

    values = []
    for raw_value in raw_values:
        text = str(raw_value).strip()
        if text:
            values.append(text.upper())
    return values


def _extract_dicom_image_type_values(meta_obj):
    dicom_values = _meta_text_values(meta_obj.get("DicomImageType"))
    if dicom_values:
        return dicom_values

    image_type_values = _meta_text_values(meta_obj.get("ImageType"))
    if not image_type_values:
        return []

    if len(image_type_values) >= 3:
        return image_type_values

    value3 = _meta_text_values(meta_obj.get("ImageTypeValue3"))
    prefix = ["", "", value3[0] if value3 else ""]
    return prefix + image_type_values


def _get_dicom_image_type_value(meta_obj, index):
    values = _extract_dicom_image_type_values(meta_obj)
    if len(values) > index:
        return values[index]
    return ""


def _decode_ice_minihead(meta_obj):
    try:
        encoded = meta_obj.get("IceMiniHead")
        if encoded is None:
            return ""
        if isinstance(encoded, (list, tuple)):
            encoded = encoded[0] if encoded else None
        if not encoded:
            return ""
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _extract_minihead_string_value(minihead_text, name):
    if not minihead_text:
        return ""

    match = re.search(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"([^"]*)"\s*\}}',
        minihead_text,
    )
    if match:
        return match.group(1).strip()
    try:
        return _first_non_empty_text(
            mrdhelper.extract_minihead_string_param(minihead_text, name)
        )
    except Exception:
        pass
    return ""


def _extract_minihead_long_value(minihead_text, name):
    if not minihead_text:
        return None

    match = re.search(
        rf'<ParamLong\."{re.escape(name)}">\s*\{{\s*(-?\d*)\s*\}}',
        minihead_text,
    )
    if not match:
        return None
    value = match.group(1).strip()
    return int(value) if value not in {"", "-"} else None


def _extract_minihead_array_tokens(minihead_text, name):
    if not minihead_text:
        return []

    block_match = re.search(
        rf'<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}',
        minihead_text,
        flags=re.DOTALL | re.MULTILINE,
    )
    if not block_match:
        return []

    return [token.strip() for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_match.group(0))]


def _sanitize_minihead_param_value(value):
    text = _first_non_empty_text(value)
    if not text:
        return ""
    return (
        text.replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_minihead_param_value(value)
    if not minihead_text or not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all(match.group(2) == value for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{value}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    if not minihead_text or value is None:
        return minihead_text, False

    value = int(value)
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})'
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all((match.group(2) or "").strip() == str(value) for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{value}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_minihead_array_token(minihead_text, name, target_token):
    if not minihead_text or not target_token:
        return minihead_text, False

    block_pattern = re.compile(
        rf'<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}',
        flags=re.DOTALL | re.MULTILINE,
    )
    block_match = block_pattern.search(minihead_text)
    if not block_match:
        return minihead_text, False

    block_text = block_match.group(0)
    tokens = [token.strip() for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_text)]
    if not tokens:
        return minihead_text, False

    if tokens == [target_token]:
        return minihead_text, False

    token_pattern = re.compile(r'\{\s*"[^"]*"\s*\}')
    token_matches = list(token_pattern.finditer(block_text))
    if not token_matches:
        replaced_block = block_text.rstrip()[:-1] + f'\n\t{{ "{target_token}" }}\n}}'
    else:
        first_token = token_matches[0]
        last_token = token_matches[-1]
        replaced_block = (
            block_text[:first_token.start()]
            + f'{{ "{target_token}" }}'
            + block_text[last_token.end():]
        )
    return (
        minihead_text[:block_match.start()] + replaced_block + minihead_text[block_match.end():],
        True,
    )


def _replace_or_append_minihead_array_token(minihead_text, name, source_token, target_token):
    return _replace_or_append_minihead_array_tokens(minihead_text, name, [target_token])


def _remove_minihead_string_param(minihead_text, name):
    pattern = re.compile(
        rf'^\s*<ParamString\."{re.escape(name)}">\s*\{{\s*"[^"]*"\s*\}}\s*\n?',
        flags=re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _remove_minihead_array_param(minihead_text, name):
    pattern = re.compile(
        rf'^\s*<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}\s*\n?',
        flags=re.DOTALL | re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _remove_minihead_exam_data_role(minihead_text):
    pattern = re.compile(
        r'^\s*<ParamString\."ExamDataRole">\s*\{\s*".*?</DataRole>"\s*\}\s*\n?',
        flags=re.DOTALL | re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _strip_scanner_write_unsafe_minihead(minihead_text):
    current_text = minihead_text
    changed = False
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        current_text, did_change = _remove_minihead_string_param(current_text, key)
        changed = changed or did_change
        current_text, did_change = _remove_minihead_array_param(current_text, key)
        changed = changed or did_change
    return current_text, changed


def _replace_or_append_minihead_array_tokens(minihead_text, name, target_tokens):
    target_tokens = [
        token
        for token in (
            _sanitize_minihead_param_value(token) for token in target_tokens
        )
        if token
    ]
    if not minihead_text or not target_tokens:
        return minihead_text, False

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    block_pattern = re.compile(
        rf'(<ParamArray\."{re.escape(name)}">\s*\{{)(.*?)(^\s*\}})',
        flags=re.DOTALL | re.MULTILINE,
    )
    block_match = block_pattern.search(minihead_text)
    if not block_match:
        token_lines = "".join(
            f'{line_ending}\t{{ "{token}" }}' for token in target_tokens
        )
        appended_param = (
            f'{line_ending}<ParamArray."{name}">\t{{'
            f"{token_lines}{line_ending}}}{line_ending}"
        )
        return minihead_text.rstrip() + appended_param, True

    block_text = block_match.group(0)
    tokens = _extract_minihead_array_tokens(block_text, name)
    if tokens == target_tokens:
        return minihead_text, False

    token_pattern = re.compile(r'\{\s*"[^"]*"\s*\}')
    token_matches = list(token_pattern.finditer(block_text))
    token_lines = "".join(
        f'{line_ending}\t{{ "{token}" }}' for token in target_tokens
    )
    if not token_matches:
        stripped_block = block_text.rstrip()
        close_index = stripped_block.rfind("}")
        if close_index >= 0:
            replacement_block = (
                stripped_block[:close_index]
                + token_lines
                + line_ending
                + stripped_block[close_index:]
            )
        else:
            replacement_block = stripped_block + token_lines
    else:
        first_token = token_matches[0]
        last_token = token_matches[-1]
        replacement_block = (
            block_text[:first_token.start()]
            + token_lines.lstrip(line_ending)
            + block_text[last_token.end():]
        )
    return (
        minihead_text[:block_match.start()]
        + replacement_block
        + minihead_text[block_match.end():],
        True,
    )


def _format_exam_data_role_sequential_number(value):
    return (
        '<DataRole Version="DR2.0">\n'
        ' <Categories>\n'
        '  <Category Name="SequentialNumber">\n'
        f"   <CategoryEntry>{int(value)}</CategoryEntry>\n"
        "  </Category>\n"
        " </Categories>\n"
        "</DataRole>"
    )


def _minihead_string_literal(value):
    return str(value).replace('"', '""')


def _minihead_param_map_line_span(minihead_text, map_name):
    lines = minihead_text.splitlines(keepends=True)
    map_pattern = re.compile(rf'<ParamMap\."{re.escape(map_name)}">')

    for map_index, line in enumerate(lines):
        if not map_pattern.search(line):
            continue

        open_index = None
        for candidate_index in range(map_index + 1, len(lines)):
            stripped = lines[candidate_index].strip()
            if stripped == "{":
                open_index = candidate_index
                break
            if stripped.startswith('<ParamMap."'):
                break
        if open_index is None:
            continue

        depth = 1
        for close_index in range(open_index + 1, len(lines)):
            stripped = lines[close_index].strip()
            if stripped == "{":
                depth += 1
            elif stripped == "}":
                depth -= 1
                if depth == 0:
                    return lines, map_index, open_index, close_index

    return lines, None, None, None


def _minihead_param_map_text(minihead_text, map_name):
    lines, map_index, _open_index, close_index = _minihead_param_map_line_span(
        minihead_text,
        map_name,
    )
    if map_index is None:
        return ""
    return "".join(lines[map_index:close_index + 1])


def _insert_minihead_string_param_in_map(minihead_text, map_name, name, value):
    literal = _minihead_string_literal(value)
    lines, _map_index, open_index, close_index = _minihead_param_map_line_span(
        minihead_text,
        map_name,
    )
    if close_index is None:
        appended_param = f'\n<ParamString."{name}">\t{{ "{literal}" }}\n'
        return minihead_text.rstrip() + appended_param, True

    indent = ""
    for line in lines[open_index + 1:close_index]:
        match = re.match(r'(\s*)<Param(?:Long|String|Array)\.', line)
        if match:
            indent = match.group(1)
            break
    if not indent:
        indent_match = re.match(r"(\s*)", lines[open_index])
        indent = indent_match.group(1) if indent_match else ""

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    param_line = f'{indent}<ParamString."{name}">\t{{ "{literal}" }}{line_ending}'
    lines.insert(close_index, param_line)
    return "".join(lines), True


def _replace_or_insert_minihead_exam_data_role(minihead_text, exam_data_role):
    if not minihead_text or not exam_data_role:
        return minihead_text, False

    literal = _minihead_string_literal(exam_data_role)
    pattern = re.compile(
        r'(<ParamString\."ExamDataRole">\s*\{\s*")(.*?</DataRole>)("\s*\})',
        flags=re.DOTALL,
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all(match.group(2).replace('""', '"') == exam_data_role for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{literal}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    return _insert_minihead_string_param_in_map(
        minihead_text,
        "DICOM",
        "ExamDataRole",
        exam_data_role,
    )


def _patch_ice_minihead(
    minihead_text,
    series_description,
    series_grouping,
    source_type_token,
    target_type_token,
    target_display_token=None,
    target_image_type_value3="M",
    series_uid=None,
    sop_uid=None,
    output_index=None,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    target_display_token = target_display_token or target_type_token

    for param_name, param_value in (
        ("SeriesDescription", series_description),
        ("SequenceDescription", series_description),
        ("ProtocolName", series_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", f"DERIVED\\PRIMARY\\{target_image_type_value3}\\{target_type_token}"),
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        if not param_value:
            continue
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    current_text, did_change = _replace_or_append_minihead_array_token(
        current_text,
        "ImageTypeValue4",
        source_type_token,
        target_display_token,
    )
    changed = changed or did_change

    if output_index is not None:
        for param_name, param_value in (
            ("Actual3DImagePartNumber", SCANNER_PARTITION_INDEX),
            ("AnatomicalPartitionNo", SCANNER_PARTITION_INDEX),
            ("AnatomicalSliceNo", output_index),
            ("ChronSliceNo", output_index),
            ("NumberInSeries", output_index + 1),
            ("ProtocolSliceNumber", output_index),
            ("SliceNo", output_index),
        ):
            current_text, did_change = _replace_or_append_minihead_long_param(
                current_text,
                param_name,
                param_value,
            )
            changed = changed or did_change

    return current_text, changed


def _source_postprocessing_image_type_identity(source_meta, minihead_text):
    image_type = (
        _get_meta_text(source_meta, "ImageType")
        or _extract_minihead_string_value(minihead_text, "ImageType")
        or VESSELBOOST_SOURCE_IMAGE_HEADER_FALLBACK_TYPE
    )
    dicom_image_type = _get_meta_text(source_meta, "DicomImageType") or image_type
    image_type_value4_tokens = (
        _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4")
        or _get_meta_values(source_meta, "ImageTypeValue4")
        or [VESSELBOOST_SOURCE_IMAGE_HEADER_FALLBACK_VALUE4]
    )
    return image_type, dicom_image_type, image_type_value4_tokens


def _patch_source_image_header_ice_minihead(
    minihead_text,
    series_description,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    image_type_value4_tokens,
    exam_data_role,
    slice_index,
    image_index,
    image_type_value3=None,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    for param_name, param_value in (
        ("SeriesDescription", series_description),
        ("SequenceDescription", series_description),
        ("ProtocolName", series_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", image_type),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        if not param_value:
            continue
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    current_text, did_change = _replace_or_append_minihead_array_tokens(
        current_text,
        "ImageTypeValue4",
        image_type_value4_tokens,
    )
    changed = changed or did_change

    current_text, did_change = _replace_or_insert_minihead_exam_data_role(
        current_text,
        exam_data_role,
    )
    changed = changed or did_change
    if not exam_data_role:
        current_text, did_change = _remove_minihead_exam_data_role(current_text)
        changed = changed or did_change
    current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    changed = changed or did_change
    if image_type_value3:
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            "ImageTypeValue3",
            image_type_value3,
        )
        changed = changed or did_change

    for param_name, param_value in (
        ("Actual3DImagePartNumber", SCANNER_PARTITION_INDEX),
        ("AnatomicalPartitionNo", SCANNER_PARTITION_INDEX),
        ("AnatomicalSliceNo", slice_index),
        ("ChronSliceNo", max(int(image_index), 1) - 1),
        ("NumberInSeries", max(int(image_index), 1)),
        ("ProtocolSliceNumber", slice_index),
        ("SliceNo", slice_index),
    ):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    return current_text, changed


def _patch_original_passthrough_ice_minihead(
    minihead_text,
    series_description,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    image_type_value4_tokens,
    slice_index,
    image_index,
):
    # Native original passthrough: keep the source's inherited ImageType (e.g.
    # ORIGINAL\PRIMARY\M\TOF), drop the standalone ImageTypeValue3, refresh the
    # derived series identity, and do NOT tag an ExamDataRole post-processing
    # child. Mirrors openreconi2iexample._patch_original_ice_minihead so the
    # scanner keeps the originals as the primary acquisition (still inline-MIP'd)
    # while the segment remains the only post-processing child.
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    for param_name, param_value in (
        ("SeriesDescription", series_description),
        ("SequenceDescription", series_description),
        ("ProtocolName", series_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", image_type),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        if not param_value:
            continue
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    if image_type_value4_tokens:
        current_text, did_change = _replace_or_append_minihead_array_tokens(
            current_text,
            "ImageTypeValue4",
            image_type_value4_tokens,
        )
        changed = changed or did_change

    current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    changed = changed or did_change

    for param_name, param_value in (
        ("Actual3DImagePartNumber", SCANNER_PARTITION_INDEX),
        ("AnatomicalPartitionNo", SCANNER_PARTITION_INDEX),
        ("AnatomicalSliceNo", slice_index),
        ("ChronSliceNo", max(int(image_index), 1) - 1),
        ("NumberInSeries", max(int(image_index), 1)),
        ("ProtocolSliceNumber", slice_index),
        ("SliceNo", slice_index),
    ):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    return current_text, changed


def _resolve_source_series_identity(meta_obj):
    minihead_text = _decode_ice_minihead(meta_obj)
    series_description = _get_meta_text(meta_obj, "SeriesDescription")
    parent_sequence = _first_non_empty_text(
        _get_meta_text(meta_obj, "SequenceDescription"),
        _extract_minihead_string_value(minihead_text, "SequenceDescription"),
        series_description,
    )
    parent_grouping = _first_non_empty_text(
        _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID"),
        parent_sequence,
    )
    source_type_token = _first_non_empty_text(
        _get_meta_text(meta_obj, "ImageTypeValue4"),
        _get_dicom_image_type_value(meta_obj, 3),
    )
    series_uid = _first_non_empty_text(
        _get_meta_text(meta_obj, "SeriesInstanceUID"),
        _extract_minihead_string_value(minihead_text, "SeriesInstanceUID"),
    )
    sop_uid = _first_non_empty_text(
        _get_meta_text(meta_obj, "SOPInstanceUID"),
        _extract_minihead_string_value(minihead_text, "SOPInstanceUID"),
    )

    return {
        "series_description": _first_non_empty_text(
            series_description,
            parent_sequence,
        ),
        "parent_sequence": parent_sequence,
        "parent_grouping": parent_grouping,
        "source_type_token": source_type_token,
        "series_uid": series_uid,
        "sop_uid": sop_uid,
    }


def _build_vesselboost_series_name(source_series_description, suffix=""):
    source_series_description = _first_non_empty_text(source_series_description)
    if source_series_description:
        name = f"{source_series_description}_{VESSELBOOST_SEGMENTATION_LABEL}"
    else:
        name = VESSELBOOST_SEGMENTATION_LABEL
    if suffix:
        name = f"{name}_{suffix}"
    return name


def _build_vesselboost_grouping(source_parent_grouping, fallback_series_name, suffix=""):
    source_parent_grouping = _first_non_empty_text(source_parent_grouping)
    if source_parent_grouping:
        grouping = f"{source_parent_grouping}_{VESSELBOOST_SEGMENTATION_LABEL}"
    else:
        grouping = fallback_series_name
    if suffix:
        grouping = f"{grouping}_{suffix}"
    return grouping


def _build_openrecon_output_identity(source_identity, orientation=None, series_index=None):
    orientation = _first_non_empty_text(orientation).lower()
    base_series_description = _build_vesselboost_series_name(
        source_identity.get("series_description", ""),
    )
    base_grouping = _build_vesselboost_grouping(
        source_identity.get("parent_grouping", ""),
        fallback_series_name=base_series_description,
    )
    series_description = (
        f"{base_series_description}_{orientation}"
        if orientation
        else base_series_description
    )
    grouping = f"{base_grouping}_{orientation}" if orientation else base_grouping
    image_comment = (
        f"{VESSELBOOST_SEGMENTATION_LABEL}_{orientation}"
        if orientation
        else VESSELBOOST_SEGMENTATION_LABEL
    )
    identity = {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "display_token": VESSELBOOST_SEGMENTATION_LABEL,
        "type_token": VESSELBOOST_SEGMENTATION_TYPE_TOKEN,
        "image_comment": image_comment,
    }
    if series_index is not None:
        identity["series_index"] = int(series_index)
        identity["series_uid"] = _derived_vesselboost_series_uid(
            source_identity,
            int(series_index),
            series_description,
        )
    return identity


def _build_vesselboost_original_identity(source_identity, series_index=None):
    source_series_description = _first_non_empty_text(
        source_identity.get("series_description", ""),
        "source",
    )
    source_parent_grouping = _first_non_empty_text(
        source_identity.get("parent_grouping", ""),
        source_series_description,
    )
    series_description = f"{source_series_description}_{VESSELBOOST_ORIGINAL_LABEL}"
    grouping = f"{source_parent_grouping}_{VESSELBOOST_ORIGINAL_LABEL}"
    identity = {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "display_token": VESSELBOOST_ORIGINAL_LABEL,
        "type_token": VESSELBOOST_ORIGINAL_TYPE_TOKEN,
        "image_comment": VESSELBOOST_ORIGINAL_LABEL,
    }
    if series_index is not None:
        identity["series_index"] = int(series_index)
        identity["series_uid"] = _derived_vesselboost_series_uid(
            source_identity,
            int(series_index),
            series_description,
        )
    return identity


def _derived_vesselboost_series_uid(source_identity, series_index, series_name):
    seed = "|".join(
        [
            "vesselboost-series",
            _first_non_empty_text(
                source_identity.get("series_uid", ""),
                source_identity.get("series_description", ""),
                "source",
            ),
            str(int(series_index)),
            _first_non_empty_text(series_name, "vesselboost"),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_vesselboost_instance_uid(
    source_image,
    source_identity,
    series_index,
    series_name,
    output_index,
    series_uid=None,
):
    source_instance_uid = ""
    try:
        source_meta = _meta_from_image(source_image)
        minihead_text = _decode_ice_minihead(source_meta)
        source_instance_uid = _first_non_empty_text(
            _get_meta_text(source_meta, "SOPInstanceUID"),
            _extract_minihead_string_value(minihead_text, "SOPInstanceUID"),
        )
    except Exception:
        source_instance_uid = ""
    if not source_instance_uid:
        source_instance_uid = _first_non_empty_text(
            source_identity.get("sop_uid", ""),
            str(output_index),
        )
    series_uid = series_uid or _derived_vesselboost_series_uid(
        source_identity,
        series_index,
        series_name,
    )
    seed = "|".join(
        [
            "vesselboost-instance",
            series_uid,
            source_instance_uid,
            str(int(output_index)),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _stamp_vesselboost_output_image(
    image,
    source_image,
    output_identity,
    source_identity,
    series_index,
    output_index,
    source_type_token,
    processing_history,
    extra_meta=None,
    keep_image_geometry=1,
    patch_minihead=True,
    data_role="Image",
    source_image_header_identity=False,
    segment_source_geometry_identity=False,
    original_passthrough_identity=False,
    segment_scanner_postprocessing=True,
    segment_scanner_mip_processing=False,
):
    source_meta = _copy_meta(_meta_from_image(source_image))
    header = image.getHead()
    header.image_series_index = int(series_index)
    source_geometry_identity = (
        source_image_header_identity or segment_source_geometry_identity
    )
    if source_geometry_identity:
        header.image_index = _source_geometry_header_image_index(
            source_image,
            output_index,
        )
        header.slice = _source_geometry_header_slice(source_image, output_index)
    else:
        header.image_index = int(output_index) + 1
        header.slice = int(output_index)
    header.contrast = 0
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    image.setHead(header)
    image.image_series_index = int(series_index)

    tmp_meta = _copy_meta(source_meta)
    _strip_source_parent_refs(tmp_meta)
    series_name = output_identity["series_description"]
    series_uid = output_identity.get("series_uid") or _derived_vesselboost_series_uid(
        source_identity,
        series_index,
        series_name,
    )
    sop_uid = _derived_vesselboost_instance_uid(
        source_image,
        source_identity,
        series_index,
        series_name,
        output_index,
        series_uid=series_uid,
    )
    minihead_text = _decode_ice_minihead(source_meta)
    # Originals and 2D source-image-header segments inherit the source's native
    # ImageType identity (e.g. ORIGINAL\PRIMARY\M\TOF). The source-geometry
    # segmentation-header path intentionally does not; it mirrors
    # openreconi2iexample's 2d_segment_header_originals mode.
    inherit_source_image_type = (
        source_image_header_identity or original_passthrough_identity
    )
    source_image_type = None
    source_dicom_image_type = None
    source_image_type_value4 = None
    exam_data_role = None
    source_image_header_image_type_value3 = None
    output_image_type = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
    output_dicom_image_type = output_image_type
    output_image_type_value4 = output_identity["display_token"]
    if inherit_source_image_type:
        (
            source_image_type,
            source_dicom_image_type,
            source_image_type_value4,
        ) = _source_postprocessing_image_type_identity(source_meta, minihead_text)
        output_image_type = source_image_type
        output_dicom_image_type = source_dicom_image_type
        output_image_type_value4 = source_image_type_value4
    elif segment_source_geometry_identity:
        output_image_type = VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE
        output_dicom_image_type = VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE
        output_image_type_value4 = VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4
    if source_geometry_identity and segment_scanner_postprocessing:
        exam_data_role = _format_exam_data_role_sequential_number(series_index)
    if source_image_header_identity and segment_scanner_mip_processing:
        source_image_header_image_type_value3 = (
            VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3
        )

    tmp_meta["DataRole"] = "Image" if source_image_header_identity else data_role
    tmp_meta["ImageProcessingHistory"] = processing_history
    tmp_meta["SeriesDescription"] = series_name
    tmp_meta["SequenceDescription"] = output_identity["sequence_description"]
    tmp_meta["ProtocolName"] = series_name
    tmp_meta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
    tmp_meta["SeriesInstanceUID"] = series_uid
    tmp_meta["SOPInstanceUID"] = sop_uid
    tmp_meta["ImageType"] = output_image_type
    if not inherit_source_image_type and not segment_source_geometry_identity:
        tmp_meta["ImageTypeValue3"] = "M"
    tmp_meta["ImageTypeValue4"] = output_image_type_value4
    tmp_meta["DicomImageType"] = output_dicom_image_type
    if source_geometry_identity and segment_scanner_postprocessing:
        tmp_meta["ExamDataRole"] = exam_data_role
    elif source_geometry_identity:
        for key in (
            "ExamDataRole",
            VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY,
            VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY,
        ):
            if key in tmp_meta:
                del tmp_meta[key]
    tmp_meta["ComplexImageComponent"] = "MAGNITUDE"
    tmp_meta["ImageComments"] = (
        series_name if source_geometry_identity else output_identity["image_comment"]
    )
    tmp_meta["ImageComment"] = (
        series_name if source_geometry_identity else output_identity["image_comment"]
    )
    tmp_meta["SequenceDescriptionAdditional"] = OPENRECON_SERIES_SUFFIX
    tmp_meta["Keep_image_geometry"] = str(int(keep_image_geometry))
    _set_output_position_meta(
        tmp_meta,
        int(header.slice),
        image_index=int(header.image_index),
    )
    if source_geometry_identity:
        tmp_meta[VESSELBOOST_SEGMENT_SOURCE_GEOMETRY_META_KEY] = "1"
        if segment_source_geometry_identity and (
            VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY in tmp_meta
        ):
            del tmp_meta[VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY]
    if source_image_header_identity:
        tmp_meta[VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY] = "1"
    if source_geometry_identity and segment_scanner_postprocessing:
        tmp_meta[VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY] = str(
            int(series_index)
        )

    if extra_meta:
        for key, value in extra_meta.items():
            if value is not None:
                tmp_meta[key] = value
    if inherit_source_image_type or segment_source_geometry_identity:
        _strip_scanner_write_unsafe_meta(tmp_meta)
    if source_image_header_image_type_value3:
        tmp_meta["ImageTypeValue3"] = source_image_header_image_type_value3

    minihead_text = _decode_ice_minihead(tmp_meta)
    if not patch_minihead:
        if "IceMiniHead" in tmp_meta:
            try:
                del tmp_meta["IceMiniHead"]
            except Exception:
                tmp_meta["IceMiniHead"] = ""
    elif minihead_text:
        if source_geometry_identity:
            patched_minihead_text, minihead_changed = (
                _patch_source_image_header_ice_minihead(
                    minihead_text,
                    series_name,
                    output_identity["grouping"],
                    series_uid,
                    sop_uid,
                    output_image_type,
                    (
                        output_image_type_value4
                        if isinstance(output_image_type_value4, (list, tuple))
                        else [output_image_type_value4]
                    ),
                    exam_data_role,
                    int(header.slice),
                    int(header.image_index),
                    image_type_value3=source_image_header_image_type_value3,
                )
            )
        elif original_passthrough_identity:
            patched_minihead_text, minihead_changed = (
                _patch_original_passthrough_ice_minihead(
                    minihead_text,
                    series_name,
                    output_identity["grouping"],
                    series_uid,
                    sop_uid,
                    source_image_type,
                    source_image_type_value4,
                    int(header.slice),
                    int(header.image_index),
                )
            )
        else:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                series_name,
                output_identity["grouping"],
                source_type_token,
                output_identity["type_token"],
                target_display_token=output_identity["display_token"],
                series_uid=series_uid,
                sop_uid=sop_uid,
                output_index=output_index,
            )
        if minihead_changed:
            tmp_meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)
        else:
            logging.warning(
                "IceMiniHead was present but not updated for %s",
                output_identity["series_description"],
            )

    image.attribute_string = tmp_meta.serialize()


def _source_geometry_header_image_index(source_image, output_index):
    try:
        source_image_index = int(source_image.getHead().image_index)
    except Exception:
        source_image_index = 0
    if source_image_index >= 1:
        return source_image_index
    return int(output_index) + 1


def _source_geometry_header_slice(source_image, output_index):
    try:
        source_slice = int(source_image.getHead().slice)
    except Exception:
        source_slice = int(output_index)
    return source_slice if source_slice >= 0 else int(output_index)


def _build_vesselboost_original_images(
    ordered_original_images,
    ordered_source_images,
    source_identity,
    series_index,
):
    if len(ordered_original_images) != len(ordered_source_images):
        raise ValueError(
            "Original/source image count mismatch: "
            f"{len(ordered_original_images)} != {len(ordered_source_images)}"
        )

    original_identity = _build_vesselboost_original_identity(
        source_identity,
        series_index=series_index,
    )
    logging.info(
        "Preparing original MRA images as first derived original series "
        "(series_index=%d, name=%s)",
        series_index,
        original_identity["series_description"],
    )

    outputs = []
    for output_index, (original_image, source_image) in enumerate(
        zip(ordered_original_images, ordered_source_images)
    ):
        _stamp_vesselboost_output_image(
            original_image,
            source_image,
            original_identity,
            source_identity,
            series_index,
            output_index,
            source_identity["source_type_token"],
            ["PYTHON", "VESSELBOOST", "ORIGINAL"],
            extra_meta=_explicit_header_geometry_meta(original_image.getHead()),
            keep_image_geometry=1,
            patch_minihead=True,
            original_passthrough_identity=True,
        )
        outputs.append(original_image)

    return outputs


def _set_output_position_meta(meta_obj, slice_index, image_index=None):
    slice_index = int(slice_index)
    image_index = int(image_index) if image_index is not None else slice_index + 1
    for key in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        _set_meta_scalar(meta_obj, key, SCANNER_PARTITION_INDEX)
    for key, value in (
        ("AnatomicalSliceNo", slice_index),
        ("ChronSliceNo", max(image_index, 1) - 1),
        ("NumberInSeries", max(image_index, 1)),
        ("ProtocolSliceNumber", slice_index),
        ("SliceNo", slice_index),
        ("IsmrmrdSliceNo", slice_index),
    ):
        _set_meta_scalar(meta_obj, key, value)


def _explicit_header_geometry_meta(header):
    return {
        "ImageRowDir": _meta_vector(_header_vector(header, "read_dir")),
        "ImageColumnDir": _meta_vector(_header_vector(header, "phase_dir")),
        "ImageSliceNormDir": _meta_vector(_header_vector(header, "slice_dir")),
        "SlicePosLightMarker": _meta_vector(_header_vector(header, "position")),
    }


def _meta_vector(values):
    return [f"{float(value):.18f}" for value in values]


def _validate_vesselboost_output_contract(
    images,
    source_series_indices,
    source_identity,
    context,
):
    # This runs before any send. Raising here makes OpenRecon log the error and
    # close cleanly instead of partially returning images with unsafe identity.
    source_series_indices = {int(index) for index in source_series_indices}
    source_values = {
        _first_non_empty_text(value)
        for value in (
            source_identity.get("series_description", ""),
            source_identity.get("parent_grouping", ""),
            source_identity.get("parent_sequence", ""),
            source_identity.get("series_uid", ""),
            source_identity.get("sop_uid", ""),
        )
        if _first_non_empty_text(value)
    }

    errors = []
    seen_image_keys = {}
    seen_storage_keys = {}
    seen_sop_uids = {}
    series_identity = {}
    series_by_uid = {}
    for index, image in enumerate(_as_image_list(images)):
        header = image.getHead()
        series_index = int(getattr(header, "image_series_index", 0))
        try:
            meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
        except Exception as exc:
            errors.append(f"image {index}: invalid MRD Meta attributes: {exc}")
            continue

        minihead_text = _decode_ice_minihead(meta_obj)
        image_key = (
            series_index,
            int(getattr(header, "slice", 0)),
            int(getattr(header, "image_index", 0)),
        )
        previous_image = seen_image_keys.setdefault(image_key, index)
        if previous_image != index:
            errors.append(
                f"image {index}: duplicates output image key {image_key} "
                f"from image {previous_image}"
            )

        identity = {
            "SeriesDescription": _get_meta_text(meta_obj, "SeriesDescription"),
            "SequenceDescription": _get_meta_text(meta_obj, "SequenceDescription"),
            "ProtocolName": _get_meta_text(meta_obj, "ProtocolName"),
            "SeriesNumberRangeNameUID": _get_meta_text(
                meta_obj,
                "SeriesNumberRangeNameUID",
            ),
            "SeriesInstanceUID": _get_meta_text(meta_obj, "SeriesInstanceUID"),
            "SOPInstanceUID": _get_meta_text(meta_obj, "SOPInstanceUID"),
            "ImageTypeValue4": _get_meta_text(meta_obj, "ImageTypeValue4"),
            "ImageTypeValue4Values": _get_meta_values(meta_obj, "ImageTypeValue4"),
            "ComplexImageComponent": _get_meta_text(meta_obj, "ComplexImageComponent"),
            "SequenceDescriptionAdditional": _get_meta_text(
                meta_obj,
                "SequenceDescriptionAdditional",
            ),
        }
        minihead_identity = {
            "SeriesDescription": _extract_minihead_string_value(
                minihead_text,
                "SeriesDescription",
            ),
            "SequenceDescription": _extract_minihead_string_value(
                minihead_text,
                "SequenceDescription",
            ),
            "ProtocolName": _extract_minihead_string_value(
                minihead_text,
                "ProtocolName",
            ),
            "SeriesNumberRangeNameUID": _extract_minihead_string_value(
                minihead_text,
                "SeriesNumberRangeNameUID",
            ),
            "SeriesInstanceUID": _extract_minihead_string_value(
                minihead_text,
                "SeriesInstanceUID",
            ),
            "SOPInstanceUID": _extract_minihead_string_value(
                minihead_text,
                "SOPInstanceUID",
            ),
        }

        if series_index in source_series_indices:
            errors.append(
                f"image {index}: output still uses source image_series_index={series_index}"
            )
        for field in (
            "SeriesDescription",
            "SequenceDescription",
            "ProtocolName",
            "SeriesNumberRangeNameUID",
            "SeriesInstanceUID",
            "SOPInstanceUID",
        ):
            if not identity[field]:
                errors.append(f"image {index}: output is missing Meta {field}")
            if identity[field] and identity[field] in source_values:
                errors.append(
                    f"image {index}: output Meta reuses source {field}={identity[field]}"
                )
            minihead_value = minihead_identity[field]
            if minihead_value and minihead_value != identity[field]:
                errors.append(
                    f"image {index}: Meta/IceMiniHead {field} mismatch: "
                    f"{identity[field]} != {minihead_value}"
                )
            if minihead_value and minihead_value in source_values:
                errors.append(
                    f"image {index}: output IceMiniHead reuses source "
                    f"{field}={minihead_value}"
                )

        if not identity["ImageTypeValue4Values"]:
            errors.append(f"image {index}: output is missing ImageTypeValue4")
        if identity["ComplexImageComponent"] != "MAGNITUDE":
            errors.append(
                f"image {index}: ComplexImageComponent={identity['ComplexImageComponent']}, "
                "expected MAGNITUDE"
            )
        if identity["SequenceDescriptionAdditional"] != OPENRECON_SERIES_SUFFIX:
            errors.append(
                f"image {index}: SequenceDescriptionAdditional="
                f"{identity['SequenceDescriptionAdditional']!r}, expected "
                f"{OPENRECON_SERIES_SUFFIX!r}"
            )

        keep_image_geometry = _get_meta_int(meta_obj, "Keep_image_geometry")
        if keep_image_geometry is None:
            errors.append(f"image {index}: missing Keep_image_geometry")
        if _is_vesselboost_source_image_header_output(meta_obj):
            data_role = _get_meta_text(meta_obj, "DataRole")
            if data_role != "Image":
                errors.append(
                    f"image {index}: source-image-header output DataRole={data_role}, "
                    "expected Image"
                )
            if keep_image_geometry != 1:
                errors.append(
                    f"image {index}: source-image-header output Keep_image_geometry="
                    f"{keep_image_geometry}, expected 1"
                )
            segment_output_geometry = _get_meta_text(
                meta_obj,
                VESSELBOOST_SEGMENT_OUTPUT_GEOMETRY_META_KEY,
            )
            if segment_output_geometry != VESSELBOOST_OUTPUT_GEOMETRY_2D:
                errors.append(
                    f"image {index}: SegmentOutputGeometry={segment_output_geometry}, "
                    f"expected {VESSELBOOST_OUTPUT_GEOMETRY_2D}"
                )
            if _get_meta_text(meta_obj, VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY):
                errors.append(
                    f"image {index}: source-image-header output still carries "
                    f"{VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY}"
                )
            child_role = _get_meta_int(
                meta_obj,
                VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY,
            )
            exam_data_role = _get_meta_text(meta_obj, "ExamDataRole")
            # Segmentation-MIP mode should still be a post-processing child; the
            # child-role check is conditional only to keep this validator usable
            # for older detached/debug outputs.
            segment_is_postprocessing_child = (
                child_role is not None or bool(exam_data_role)
            )
            image_type_value3 = _get_meta_text(meta_obj, "ImageTypeValue3")
            if segment_is_postprocessing_child:
                if child_role != series_index:
                    errors.append(
                        f"image {index}: SegmentPostProcessingChildRole={child_role}, "
                        f"expected {series_index}"
                    )
                expected_exam_data_role = _format_exam_data_role_sequential_number(
                    series_index
                )
                if exam_data_role != expected_exam_data_role:
                    errors.append(
                        f"image {index}: ExamDataRole does not match image_series_index "
                        f"{series_index}"
                    )
                for field in SCANNER_WRITE_UNSAFE_META_KEYS:
                    field_value = _get_meta_text(meta_obj, field)
                    if (
                        field == "ImageTypeValue3"
                        and field_value == VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3
                    ):
                        continue
                    if field_value:
                        errors.append(
                            f"image {index}: source-image-header output still carries "
                            f"unsafe scanner Meta {field}"
                        )
            elif image_type_value3 != VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3:
                errors.append(
                    f"image {index}: segmentation-MIP output ImageTypeValue3="
                    f"{image_type_value3!r}, expected "
                    f"{VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3!r}"
                )
            if minihead_text:
                dicom_map = _minihead_param_map_text(minihead_text, "DICOM")
                if segment_is_postprocessing_child:
                    if '<ParamString."ExamDataRole">' not in dicom_map:
                        errors.append(
                            f"image {index}: source-image-header IceMiniHead is missing "
                            "ExamDataRole inside the DICOM ParamMap"
                        )
                    expected_entry = f"<CategoryEntry>{series_index}</CategoryEntry>"
                    if expected_entry not in dicom_map:
                        errors.append(
                            f"image {index}: source-image-header IceMiniHead DICOM "
                            f"ExamDataRole is missing {expected_entry}"
                        )
                    for field in SCANNER_WRITE_UNSAFE_META_KEYS:
                        minihead_string_value = _extract_minihead_string_value(
                            minihead_text,
                            field,
                        )
                        minihead_array_tokens = _extract_minihead_array_tokens(
                            minihead_text,
                            field,
                        )
                        if (
                            field == "ImageTypeValue3"
                            and minihead_string_value
                            == VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3
                            and not minihead_array_tokens
                        ):
                            continue
                        if minihead_string_value or minihead_array_tokens:
                            errors.append(
                                f"image {index}: source-image-header IceMiniHead still "
                                f"carries unsafe scanner {field}"
                            )
                else:
                    minihead_image_type_value3 = _extract_minihead_string_value(
                        minihead_text,
                        "ImageTypeValue3",
                    )
                    if (
                        minihead_image_type_value3
                        != VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3
                    ):
                        errors.append(
                            f"image {index}: segmentation-MIP IceMiniHead "
                            f"ImageTypeValue3={minihead_image_type_value3!r}, "
                            f"expected {VESSELBOOST_SCANNER_MIP_IMAGE_TYPE_VALUE3!r}"
                        )
        elif _is_vesselboost_source_geometry_output(meta_obj):
            data_role = _get_meta_text(meta_obj, "DataRole")
            if data_role != "Segmentation":
                errors.append(
                    f"image {index}: source-geometry segment DataRole={data_role}, "
                    "expected Segmentation"
                )
            if keep_image_geometry != 1:
                errors.append(
                    f"image {index}: source-geometry segment Keep_image_geometry="
                    f"{keep_image_geometry}, expected 1"
                )
            segment_output_geometry = _get_meta_text(
                meta_obj,
                VESSELBOOST_SEGMENT_OUTPUT_GEOMETRY_META_KEY,
            )
            if segment_output_geometry != VESSELBOOST_OUTPUT_GEOMETRY_2D:
                errors.append(
                    f"image {index}: source-geometry segment "
                    f"SegmentOutputGeometry={segment_output_geometry}, expected "
                    f"{VESSELBOOST_OUTPUT_GEOMETRY_2D}"
                )
            image_type = _get_meta_text(meta_obj, "ImageType")
            dicom_image_type = _get_meta_text(meta_obj, "DicomImageType")
            image_type_value4 = _get_meta_text(meta_obj, "ImageTypeValue4")
            if image_type != VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE:
                errors.append(
                    f"image {index}: source-geometry segment ImageType={image_type}, "
                    f"expected {VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE}"
                )
            if dicom_image_type != VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE:
                errors.append(
                    f"image {index}: source-geometry segment DicomImageType="
                    f"{dicom_image_type}, expected "
                    f"{VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE}"
                )
            if image_type_value4 != VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4:
                errors.append(
                    f"image {index}: source-geometry segment ImageTypeValue4="
                    f"{image_type_value4}, expected "
                    f"{VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4}"
                )
            for field in (
                "ExamDataRole",
                VESSELBOOST_SEGMENT_POSTPROCESSING_META_KEY,
                VESSELBOOST_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY,
                VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY,
                "ImageTypeValue3",
            ):
                if _get_meta_text(meta_obj, field):
                    errors.append(
                        f"image {index}: source-geometry segment still carries {field}"
                    )
            if minihead_text:
                dicom_map = _minihead_param_map_text(minihead_text, "DICOM")
                if '<ParamString."ExamDataRole">' in dicom_map:
                    errors.append(
                        f"image {index}: source-geometry segment IceMiniHead still "
                        "carries ExamDataRole"
                    )
                minihead_image_type = _extract_minihead_string_value(
                    minihead_text,
                    "ImageType",
                )
                if minihead_image_type and minihead_image_type != image_type:
                    errors.append(
                        f"image {index}: source-geometry segment IceMiniHead "
                        f"ImageType={minihead_image_type}, expected {image_type}"
                    )
                minihead_image_type_value4 = _extract_minihead_array_tokens(
                    minihead_text,
                    "ImageTypeValue4",
                )
                if minihead_image_type_value4 != [
                    VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4
                ]:
                    errors.append(
                        f"image {index}: source-geometry segment IceMiniHead "
                        f"ImageTypeValue4={minihead_image_type_value4}, expected "
                        f"{[VESSELBOOST_SOURCE_GEOMETRY_IMAGE_TYPE_VALUE4]}"
                    )
                for field in SCANNER_WRITE_UNSAFE_META_KEYS:
                    if (
                        _extract_minihead_string_value(minihead_text, field)
                        or _extract_minihead_array_tokens(minihead_text, field)
                    ):
                        errors.append(
                            f"image {index}: source-geometry segment IceMiniHead "
                            f"still carries {field}"
                        )
        if keep_image_geometry == 0:
            header_matrix_z = int(getattr(header, "matrix_size", [0, 0, 0])[2])
            if minihead_text:
                errors.append(
                    f"image {index}: explicit-geometry output still carries IceMiniHead"
                )
            partition_count = _get_meta_int(meta_obj, "partition_count")
            slice_count = _get_meta_int(meta_obj, "slice_count")
            if partition_count is None or partition_count < 1:
                errors.append(
                    f"image {index}: invalid partition_count={partition_count}"
                )
            if slice_count is None or slice_count < 1:
                errors.append(f"image {index}: invalid slice_count={slice_count}")
            elif int(getattr(header, "slice", 0)) >= slice_count:
                errors.append(
                    f"image {index}: slice {int(getattr(header, 'slice', 0))} "
                    f"outside explicit slice_count={slice_count}"
                )
            elif header_matrix_z > 1 and header_matrix_z != slice_count:
                errors.append(
                    f"image {index}: matrix_size[2]={header_matrix_z}, "
                    f"expected explicit slice_count={slice_count}"
                )

            number_of_slices = _get_meta_int(meta_obj, "NumberOfSlices")
            if number_of_slices is None:
                errors.append(f"image {index}: missing NumberOfSlices")
            elif slice_count is not None and number_of_slices != slice_count:
                errors.append(
                    f"image {index}: NumberOfSlices={number_of_slices}, "
                    f"expected {slice_count}"
                )

            images_in_acquisition = _get_meta_int(meta_obj, "ImagesInAcquisition")
            if images_in_acquisition is None:
                errors.append(f"image {index}: missing ImagesInAcquisition")
            elif slice_count is not None and images_in_acquisition != slice_count:
                errors.append(
                    f"image {index}: ImagesInAcquisition={images_in_acquisition}, "
                    f"expected {slice_count}"
                )

        expected_position_fields = {
            "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
            "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
            "AnatomicalSliceNo": int(getattr(header, "slice", 0)),
            "ChronSliceNo": max(int(getattr(header, "image_index", 0)), 1) - 1,
            "NumberInSeries": int(getattr(header, "image_index", 0)),
            "ProtocolSliceNumber": int(getattr(header, "slice", 0)),
            "SliceNo": int(getattr(header, "slice", 0)),
            "IsmrmrdSliceNo": int(getattr(header, "slice", 0)),
        }
        for field, expected in expected_position_fields.items():
            meta_value = _get_meta_int(meta_obj, field)
            if meta_value is None:
                errors.append(f"image {index}: missing Meta {field}")
            elif meta_value != expected:
                errors.append(
                    f"image {index}: Meta {field}={meta_value}, expected {expected}"
                )
            if field == "IsmrmrdSliceNo":
                continue
            minihead_value = _extract_minihead_long_value(minihead_text, field)
            if minihead_text and minihead_value is None:
                errors.append(f"image {index}: missing IceMiniHead {field}")
            elif minihead_value is not None and minihead_value != expected:
                errors.append(
                    f"image {index}: IceMiniHead {field}={minihead_value}, "
                    f"expected {expected}"
                )

        minihead_type_tokens = _extract_minihead_array_tokens(
            minihead_text,
            "ImageTypeValue4",
        )
        if minihead_text and minihead_type_tokens != identity["ImageTypeValue4Values"]:
            errors.append(
                f"image {index}: IceMiniHead ImageTypeValue4={minihead_type_tokens}, "
                f"expected {identity['ImageTypeValue4Values']}"
            )

        storage_key = (
            identity["SeriesInstanceUID"],
            _get_meta_int(meta_obj, "SliceNo"),
            _get_meta_int(meta_obj, "ChronSliceNo"),
            _get_meta_int(meta_obj, "NumberInSeries"),
        )
        previous_storage = seen_storage_keys.setdefault(storage_key, index)
        if previous_storage != index:
            errors.append(
                f"image {index}: duplicates scanner storage key {storage_key} "
                f"from image {previous_storage}"
            )
        uid = identity["SOPInstanceUID"]
        previous_uid = seen_sop_uids.setdefault(uid, index)
        if uid and previous_uid != index:
            errors.append(
                f"image {index}: SOPInstanceUID {uid} is shared with "
                f"image {previous_uid}"
            )

        comparable_identity = (
            identity["SeriesDescription"],
            identity["SequenceDescription"],
            identity["ProtocolName"],
            identity["SeriesNumberRangeNameUID"],
            identity["SeriesInstanceUID"],
        )
        previous_identity = series_identity.setdefault(
            series_index,
            comparable_identity,
        )
        if previous_identity != comparable_identity:
            errors.append(
                f"image {index}: inconsistent identity within series "
                f"{series_index}: {comparable_identity} != {previous_identity}"
            )
        if identity["SeriesInstanceUID"]:
            previous_series = series_by_uid.setdefault(
                identity["SeriesInstanceUID"],
                series_index,
            )
            if previous_series != series_index:
                errors.append(
                    f"SeriesInstanceUID {identity['SeriesInstanceUID']} is shared by "
                    f"image_series_index {previous_series} and {series_index}"
                )

    if errors:
        raise ValueError(
            f"Invalid VesselBoost output identity contract for {context}: "
            + "; ".join(errors[:10])
        )


def _derive_prep_mode(bias_field_correction: bool, denoising: bool) -> int:
    return PREP_MODE_FROM_FLAGS[(bias_field_correction, denoising)]


def _prep_mode_to_flags(prep_mode: int) -> tuple[bool, bool]:
    if prep_mode == 1:
        return True, False
    if prep_mode == 2:
        return False, True
    if prep_mode == 3:
        return True, True
    if prep_mode == 4:
        return False, False
    raise ValueError(
        f"Invalid preprocessing mode: {prep_mode}. Valid modes: 1, 2, 3, 4"
    )


def _safe_path_component(value: str) -> str:
    safe_value = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return safe_value[:120] or "default"


def _openrecon_run_name(
    module: str,
    prep_mode: int,
    brain_extraction: bool,
    use_blending: bool,
    config,
) -> str:
    requested_name = mrdhelper.get_json_config_param(
        config,
        OPENRECON_OUTPUT_NAME_PARAM,
        default="",
        type='str',
    )
    if requested_name:
        return _safe_path_component(requested_name)

    return _safe_path_component(
        f"{module}_prep{prep_mode}_brain{int(brain_extraction)}"
        f"_blend{int(use_blending)}"
    )


def iter_openrecon_parameter_combinations(
    modules: tuple[str, ...] | None = None,
    fast_training: bool = False,
):
    module_values = modules or OPENRECON_MODULE_VALUES
    bool_keys = (
        "vbuseblending",
        "vbbiasfieldcorrection",
        "vbdenoising",
        "vbbrainextraction",
    )
    bool_value_sets = [
        OPENRECON_COMBINATION_PARAMETER_VALUES[key] for key in bool_keys
    ]

    for module, bool_values in itertools.product(
        module_values,
        itertools.product(*bool_value_sets),
    ):
        parameters = OPENRECON_DEFAULTS.copy()
        parameters.update(dict(zip(bool_keys, bool_values)))
        if fast_training and module in ("tta", "booster"):
            parameters["vbepochs"] = "1"

        prep_mode = _derive_prep_mode(
            parameters["vbbiasfieldcorrection"],
            parameters["vbdenoising"],
        )
        name = (
            f"{module}"
            f"_prep{prep_mode}"
            f"_brain{int(parameters['vbbrainextraction'])}"
            f"_blend{int(parameters['vbuseblending'])}"
        )
        parameters[OPENRECON_OUTPUT_NAME_PARAM] = name
        yield name, {"parameters": parameters}


def write_openrecon_parameter_combinations(
    output_dir: Path,
    modules: tuple[str, ...] | None = None,
    fast_training: bool = False,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written_paths = []

    for name, config in iter_openrecon_parameter_combinations(
        modules=modules,
        fast_training=fast_training,
    ):
        output_path = output_dir / f"{name}.json"
        output_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        written_paths.append(output_path)

    return written_paths


def _parse_module_list(value: str) -> tuple[str, ...]:
    modules = tuple(module.strip() for module in value.split(",") if module.strip())
    valid_modules = set(OPENRECON_MODULE_VALUES)
    invalid_modules = sorted(set(modules) - valid_modules)
    if invalid_modules:
        raise ValueError(
            "Invalid module(s): "
            f"{', '.join(invalid_modules)}. "
            f"Valid modules: {', '.join(sorted(valid_modules))}"
        )
    return modules


def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="VesselBoost OpenRecon testing helpers"
    )
    parser.add_argument(
        "--write-openrecon-test-configs",
        type=Path,
        metavar="DIR",
        help="Write JSON configs for all finite OpenRecon choice/boolean combinations.",
    )
    parser.add_argument(
        "--modules",
        default=",".join(OPENRECON_MODULE_VALUES),
        help="Comma-separated module list for generated configs.",
    )
    parser.add_argument(
        "--fast-training",
        action="store_true",
        help="Use one epoch for TTA and booster matrix configs.",
    )
    args = parser.parse_args(argv)

    if args.write_openrecon_test_configs is None:
        parser.print_help()
        return 0

    try:
        modules = _parse_module_list(args.modules)
    except ValueError as exc:
        parser.error(str(exc))
    written_paths = write_openrecon_parameter_combinations(
        args.write_openrecon_test_configs,
        modules=modules,
        fast_training=args.fast_training,
    )
    print(f"Wrote {len(written_paths)} OpenRecon test config(s)")
    for path in written_paths:
        print(path)
    return 0


def _square_pixel_target_shape(
    input_shape: tuple[int, int],
    row_spacing: float,
    col_spacing: float,
) -> tuple[tuple[int, int], float]:
    target_spacing = float(min(row_spacing, col_spacing))
    target_rows = max(
        1,
        int(round(input_shape[0] * float(row_spacing) / target_spacing)),
    )
    target_cols = max(
        1,
        int(round(input_shape[1] * float(col_spacing) / target_spacing)),
    )
    return (target_rows, target_cols), target_spacing


def _ice_compatible_target_shape(
    target_shape: tuple[int, int],
    orientation: str,
) -> tuple[int, int]:
    """Keep reformat image dimensions acceptable to Siemens ICE orientation code."""
    rows, cols = target_shape
    if rows % 2 == cols % 2:
        return target_shape

    adjusted_shape = (
        rows + (rows % 2),
        cols + (cols % 2),
    )
    logging.info(
        "Adjusted %s reformat target_shape from %s to %s so row/column "
        "dimensions have matching parity for ICE orientation handling",
        orientation,
        target_shape,
        adjusted_shape,
    )
    return adjusted_shape


def _diagnostic_reformat_target_shape(
    target_shape: tuple[int, int],
    target_spacing: float,
    orientation: str,
) -> tuple[tuple[int, int], float]:
    downsample_factor = _env_positive_float(OPENRECON_REFORMAT_DOWNSAMPLE_ENV, 1.0)
    if downsample_factor == 1.0:
        return target_shape, target_spacing

    adjusted_shape = (
        max(1, int(round(target_shape[0] / downsample_factor))),
        max(1, int(round(target_shape[1] / downsample_factor))),
    )
    adjusted_shape = _ice_compatible_target_shape(adjusted_shape, orientation)
    adjusted_spacing = float(target_spacing) * downsample_factor
    logging.info(
        "Applying diagnostic %s=%.3f for %s reformat: target_shape %s -> %s, "
        "target_spacing %.4f -> %.4f",
        OPENRECON_REFORMAT_DOWNSAMPLE_ENV,
        downsample_factor,
        orientation,
        target_shape,
        adjusted_shape,
        target_spacing,
        adjusted_spacing,
    )
    return adjusted_shape, adjusted_spacing


def _resize_2d_nearest(
    data: np.ndarray,
    target_shape: tuple[int, int],
) -> np.ndarray:
    if data.shape == target_shape:
        return np.ascontiguousarray(data)

    zoom_factors = (
        target_shape[0] / data.shape[0],
        target_shape[1] / data.shape[1],
    )
    resized = ndi.zoom(data, zoom_factors, order=0, prefilter=False)

    if resized.shape != target_shape:
        corrected = np.zeros(target_shape, dtype=resized.dtype)
        rows = min(target_shape[0], resized.shape[0])
        cols = min(target_shape[1], resized.shape[1])
        corrected[:rows, :cols] = resized[:rows, :cols]
        resized = corrected

    if resized.dtype != data.dtype:
        resized = np.rint(resized).astype(data.dtype, copy=False)

    return np.ascontiguousarray(resized)


def _build_reformatted_images(
    volume_yxz: np.ndarray,
    head_template,
    source_image,
    source_identity: dict,
    output_identity: dict,
    voxel_size: np.ndarray,
    fov: np.ndarray,
    orientation: str,
    series_index: int,
    max_val: int,
):
    """Build one explicit 3D MRD volume for a sagittal or coronal reformat.

    volume_yxz is an int16 array with shape (N_y, N_x, N_z), where the axes map
    to the original axial phase_dir, read_dir and slice_dir respectively.
    """
    if orientation not in ("sagittal", "coronal"):
        raise ValueError(f"Unsupported reformat orientation: {orientation}")

    N_y, N_x, N_z = volume_yxz.shape
    read_dir = np.asarray(head_template.read_dir, dtype=float)
    phase_dir = np.asarray(head_template.phase_dir, dtype=float)
    slice_dir = np.asarray(head_template.slice_dir, dtype=float)

    first_position = np.asarray(head_template.position, dtype=float)
    # Center of the 3D volume in world space (slice 0 position is the slice
    # center, so the stack center is offset by half of the remaining slices).
    volume_center = first_position + 0.5 * (N_z - 1) * slice_dir * float(voxel_size[2])

    if orientation == "sagittal":
        n_slices = N_x
        new_read_dir = phase_dir
        new_phase_dir = slice_dir
        new_slice_dir = read_dir
        slice_spacing = float(voxel_size[0])
        inplane_row_spacing = float(voxel_size[2])
        inplane_col_spacing = float(voxel_size[1])
        inplane_input_shape = (N_z, N_y)
        orthogonal_fov = float(fov[0])
    else:  # coronal
        n_slices = N_y
        new_read_dir = read_dir
        new_phase_dir = slice_dir
        new_slice_dir = phase_dir
        slice_spacing = float(voxel_size[1])
        inplane_row_spacing = float(voxel_size[2])
        inplane_col_spacing = float(voxel_size[0])
        inplane_input_shape = (N_z, N_x)
        orthogonal_fov = float(fov[1])

    target_inplane_shape, target_inplane_spacing = _square_pixel_target_shape(
        inplane_input_shape,
        row_spacing=inplane_row_spacing,
        col_spacing=inplane_col_spacing,
    )
    target_inplane_shape = _ice_compatible_target_shape(
        target_inplane_shape,
        orientation,
    )
    target_inplane_shape, target_inplane_spacing = _diagnostic_reformat_target_shape(
        target_inplane_shape,
        target_inplane_spacing,
        orientation,
    )
    new_fov = (
        float(target_inplane_shape[1] * target_inplane_spacing),
        float(target_inplane_shape[0] * target_inplane_spacing),
        orthogonal_fov,
    )

    logging.info(
        "Building %s reformat: n_slices=%d slice_policy=packed_3d_volume "
        "slice_spacing=%.4f new_fov=%s series_index=%d target_shape=%s "
        "target_spacing=%.4f",
        orientation,
        n_slices,
        slice_spacing,
        new_fov,
        series_index,
        target_inplane_shape,
        target_inplane_spacing,
    )

    reformat_volume_zyx = np.empty(
        (n_slices, target_inplane_shape[0], target_inplane_shape[1]),
        dtype=volume_yxz.dtype,
    )
    for j in range(n_slices):
        if orientation == "sagittal":
            # volume_yxz[:, j, :] has shape (N_y, N_z) -> transpose to (N_z, N_y)
            # so rows = Z (phase_dir), cols = Y (read_dir).
            slice2d = np.ascontiguousarray(volume_yxz[:, j, :].T)
        else:
            # volume_yxz[j, :, :] has shape (N_x, N_z) -> transpose to (N_z, N_x)
            # so rows = Z (phase_dir), cols = X (read_dir).
            slice2d = np.ascontiguousarray(volume_yxz[j, :, :].T)

        slice2d = _resize_2d_nearest(slice2d, target_inplane_shape)
        reformat_volume_zyx[j, :, :] = slice2d

    mrd_image = ismrmrd.Image.from_array(
        np.ascontiguousarray(reformat_volume_zyx),
        transpose=False,
    )

    first_slice_position = (
        volume_center - 0.5 * (n_slices - 1) * slice_spacing * new_slice_dir
    )
    new_header = mrd_image.getHead()
    new_header.data_type = mrd_image.data_type
    new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    new_header.position = tuple(float(v) for v in first_slice_position)
    new_header.read_dir = tuple(float(v) for v in new_read_dir)
    new_header.phase_dir = tuple(float(v) for v in new_phase_dir)
    new_header.slice_dir = tuple(float(v) for v in new_slice_dir)
    new_header.field_of_view = (
        ctypes.c_float(new_fov[0]),
        ctypes.c_float(new_fov[1]),
        ctypes.c_float(new_fov[2]),
    )
    new_header.image_index = 1
    new_header.image_series_index = series_index
    new_header.slice = 0
    new_header.contrast = 0

    for attr in (
        "measurement_uid",
        "patient_table_position",
        "acquisition_time_stamp",
        "physiology_time_stamp",
        "user_int",
        "user_float",
    ):
        try:
            setattr(new_header, attr, getattr(head_template, attr))
        except Exception:
            pass
    mrd_image.setHead(new_header)

    extra_meta = {
        "WindowCenter": str((max_val + 1) / 2),
        "WindowWidth": str(max_val + 1),
        "partition_count": "1",
        "slice_count": str(n_slices),
        "NumberOfSlices": str(n_slices),
        "ImagesInAcquisition": str(n_slices),
    }
    extra_meta.update(_explicit_header_geometry_meta(new_header))
    _stamp_vesselboost_output_image(
        mrd_image,
        source_image,
        output_identity,
        source_identity,
        series_index,
        0,
        source_identity.get("source_type_token", ""),
        ["PYTHON", "VESSELBOOST", f"RESLICE_{orientation.upper()}"],
        extra_meta=extra_meta,
        keep_image_geometry=0,
        patch_minihead=False,
        data_role="Segmentation",
    )

    logging.info(
        "Packed VesselBoost %s reformat into one explicit volume: "
        "series_index=%d matrix_size=%s field_of_view=%s slice_count=%d "
        "position=%s",
        orientation,
        series_index,
        _format_vector(mrd_image.getHead().matrix_size),
        _format_vector(mrd_image.getHead().field_of_view),
        n_slices,
        [round(float(v), 6) for v in first_slice_position],
    )
    return [mrd_image]


def _build_vesselboost_segmentation_images(
    volume_yxz: np.ndarray,
    ordered_source_images,
    source_identity: dict,
    output_identity: dict,
    series_index: int,
    max_val: int,
    scanner_postprocessing: bool = True,
    segmentation_mips: bool = False,
):
    """Build one 2D MRD segmentation image per source image.

    ``segmentation_mips`` switches to the openreconi2iexample
    ``2d_segment_header_originals`` contract: source-geometry segmentation
    headers, no source-image-header marker, and no scanner post-processing
    child metadata on the segment stream.
    """
    if volume_yxz.ndim != 3:
        raise ValueError(
            "VesselBoost segmentation image stack must be 3D, "
            f"got shape {volume_yxz.shape}"
        )

    ordered_source_images = list(ordered_source_images)
    if volume_yxz.shape[2] != len(ordered_source_images):
        raise ValueError(
            "VesselBoost 2D segmentation slice count does not match sources: "
            f"output_z={volume_yxz.shape[2]} input_images={len(ordered_source_images)}"
        )

    use_source_geometry_identity = bool(segmentation_mips)
    segment_scanner_postprocessing = (
        bool(scanner_postprocessing) and not use_source_geometry_identity
    )
    processing_history = (
        ["PYTHON", "VESSELBOOST", "SEGMENT_SOURCE_GEOMETRY_2D"]
        if use_source_geometry_identity
        else ["PYTHON", "VESSELBOOST", "SOURCE_IMAGE_HEADER_2D"]
    )

    outputs = []
    for output_index, source_image in enumerate(ordered_source_images):
        source_shape = np.asarray(source_image.data).shape
        expected_yx = tuple(int(value) for value in source_shape[-2:])
        slice_yx = np.ascontiguousarray(volume_yxz[:, :, output_index])
        if expected_yx and slice_yx.shape != expected_yx:
            raise ValueError(
                "VesselBoost 2D segmentation slice shape does not match source: "
                f"slice={output_index} output_yx={slice_yx.shape} "
                f"source_yx={expected_yx}"
            )

        mrd_image = ismrmrd.Image.from_array(
            slice_yx[np.newaxis, np.newaxis, :, :],
            transpose=False,
        )
        new_header = source_image.getHead()
        new_header.data_type = mrd_image.data_type
        new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        mrd_image.setHead(new_header)

        extra_meta = {
            "WindowCenter": str((max_val + 1) / 2),
            "WindowWidth": str(max_val + 1),
            "VesselBoostOutputGeometry": VESSELBOOST_OUTPUT_GEOMETRY_2D,
            VESSELBOOST_SEGMENT_OUTPUT_GEOMETRY_META_KEY: VESSELBOOST_OUTPUT_GEOMETRY_2D,
        }
        _stamp_vesselboost_output_image(
            mrd_image,
            source_image,
            output_identity,
            source_identity,
            series_index,
            output_index,
            source_identity.get("source_type_token", ""),
            processing_history,
            extra_meta=extra_meta,
            keep_image_geometry=1,
            patch_minihead=True,
            data_role="Segmentation" if use_source_geometry_identity else "Image",
            source_image_header_identity=not use_source_geometry_identity,
            segment_source_geometry_identity=use_source_geometry_identity,
            segment_scanner_postprocessing=segment_scanner_postprocessing,
            segment_scanner_mip_processing=False,
        )
        outputs.append(mrd_image)

    segment_contract = (
        "source-geometry segmentation-header"
        if segmentation_mips
        else "source-image-header"
    )
    logging.info(
        "Built VesselBoost axial segmentation as %d %s 2D image(s): "
        "series_index=%d matrix_yx=%s outputgeometry=%s scanner_postprocessing=%s "
        "segmentation_mips=%s",
        len(outputs),
        segment_contract,
        series_index,
        tuple(int(value) for value in volume_yxz.shape[:2]),
        VESSELBOOST_OUTPUT_GEOMETRY_2D,
        segment_scanner_postprocessing,
        segmentation_mips,
    )
    return outputs


OPENRECON_SEND_IMAGE_CHUNK_SIZE = 96
OPENRECON_SEND_CHUNK_SIZE_ENV = "VESSELBOOST_OPENRECON_SEND_IMAGE_CHUNK_SIZE"
OPENRECON_REFORMAT_DOWNSAMPLE_ENV = "VESSELBOOST_OPENRECON_REFORMAT_DOWNSAMPLE_FACTOR"
OPENRECON_DELAY_BEFORE_SERIES_ENV = "VESSELBOOST_OPENRECON_DELAY_BEFORE_SERIES"


def _env_positive_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default

    try:
        value = int(raw_value)
    except ValueError:
        logging.warning(
            "Ignoring invalid %s=%r; expected a positive integer",
            name,
            raw_value,
        )
        return default

    if value <= 0:
        logging.warning(
            "Ignoring invalid %s=%r; expected a positive integer",
            name,
            raw_value,
        )
        return default

    return value


def _env_positive_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default

    try:
        value = float(raw_value)
    except ValueError:
        logging.warning(
            "Ignoring invalid %s=%r; expected a positive number",
            name,
            raw_value,
        )
        return default

    if value <= 0:
        logging.warning(
            "Ignoring invalid %s=%r; expected a positive number",
            name,
            raw_value,
        )
        return default

    return value


def _series_send_delay_seconds(series_index: int) -> float:
    """Return an optional diagnostic delay before sending a series.

    Format: VESSELBOOST_OPENRECON_DELAY_BEFORE_SERIES="3:5,4:5".
    This is intentionally opt-in so normal OpenRecon behavior is unchanged.
    """
    raw_value = os.environ.get(OPENRECON_DELAY_BEFORE_SERIES_ENV, "")
    if not raw_value:
        return 0.0

    for entry in raw_value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            raw_series, raw_delay = entry.split(":", 1)
            candidate_series = int(raw_series.strip())
            delay_seconds = float(raw_delay.strip())
        except ValueError:
            logging.warning(
                "Ignoring invalid %s entry %r; expected '<series>:<seconds>'",
                OPENRECON_DELAY_BEFORE_SERIES_ENV,
                entry,
            )
            continue
        if candidate_series == series_index:
            return max(0.0, delay_seconds)

    return 0.0


def _image_payload_bytes(image) -> int:
    try:
        return int(np.asarray(image.data).nbytes)
    except Exception:
        return 0


def _image_header_field(image, name: str, default=None):
    try:
        header = image.getHead()
        return getattr(header, name, default)
    except Exception:
        return getattr(image, name, default)


def _format_chunk_image_summary(chunk) -> dict:
    if not chunk:
        return {}

    first = chunk[0]
    last = chunk[-1]
    try:
        matrix = tuple(int(v) for v in _image_header_field(first, "matrix_size", [])[:])
    except Exception:
        matrix = None
    try:
        dtype = str(np.asarray(first.data).dtype)
    except Exception:
        dtype = "unknown"

    return {
        "matrix": matrix,
        "dtype": dtype,
        "payload_bytes": sum(_image_payload_bytes(image) for image in chunk),
        "first_image_index": int(_image_header_field(first, "image_index", -1)),
        "last_image_index": int(_image_header_field(last, "image_index", -1)),
        "first_slice": int(_image_header_field(first, "slice", -1)),
        "last_slice": int(_image_header_field(last, "slice", -1)),
    }


def _ordered_unique_strings(values):
    ordered_values = []
    seen = set()
    for value in values:
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        ordered_values.append(text)
    return ordered_values


def _is_vesselboost_source_image_header_output(meta_obj):
    return (
        _get_meta_int(meta_obj, VESSELBOOST_SEGMENT_SOURCE_GEOMETRY_META_KEY) == 1
        and _get_meta_int(meta_obj, VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY) == 1
    )


def _is_vesselboost_source_geometry_output(meta_obj):
    return (
        _get_meta_int(meta_obj, VESSELBOOST_SEGMENT_SOURCE_GEOMETRY_META_KEY) == 1
        and _get_meta_int(meta_obj, VESSELBOOST_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY) != 1
    )


def _output_role(image):
    meta_obj = _meta_from_image(image)
    if _is_vesselboost_source_image_header_output(meta_obj):
        return "segment_source_image_header"
    if _is_vesselboost_source_geometry_output(meta_obj):
        return "segment_source_geometry"
    if _get_meta_text(meta_obj, "ImageTypeValue4") == VESSELBOOST_ORIGINAL_LABEL:
        return "original_passthrough"
    if _get_meta_int(meta_obj, "Keep_image_geometry") == 0:
        return "segment_reformat"
    return "image"


def _send_batch_summary(context, images):
    images = list(images)
    series = _ordered_unique_strings(
        int(image.image_series_index) for image in images
    )
    keep_geometry = _ordered_unique_strings(
        _get_meta_text(_meta_from_image(image), "Keep_image_geometry") or "missing"
        for image in images
    )
    names = _ordered_unique_strings(
        _get_meta_text(_meta_from_image(image), "SeriesDescription") or "missing"
        for image in images
    )
    targets = _ordered_unique_strings(_output_role(image) for image in images)
    return (
        "VESSELBOOST_OPENRECON_BATCH context=%s images=%d target=%s "
        "series=%s keep_geometry=%s names=%s"
        % (
            context,
            len(images),
            "+".join(targets) if targets else "empty",
            ",".join(series) if series else "empty",
            ",".join(keep_geometry) if keep_geometry else "empty",
            "|".join(names) if names else "empty",
        )
    )


def _send_images_by_series(connection, images, context: str = "images") -> None:
    """Send MRD images in series-preserving chunks."""
    if images is None:
        return
    if isinstance(images, ismrmrd.Image):
        images = [images]
    images = list(images)
    if not images:
        logging.info("Skipping send for %s because there are no images", context)
        return

    batch = []
    batch_series = None
    delayed_series = set()
    chunk_size = _env_positive_int(
        OPENRECON_SEND_CHUNK_SIZE_ENV,
        OPENRECON_SEND_IMAGE_CHUNK_SIZE,
    )
    if chunk_size != OPENRECON_SEND_IMAGE_CHUNK_SIZE:
        logging.info(
            "Using diagnostic OpenRecon send chunk size %d from %s",
            chunk_size,
            OPENRECON_SEND_CHUNK_SIZE_ENV,
        )

    def flush_batch():
        nonlocal batch, batch_series
        if not batch:
            return
        if batch_series not in delayed_series:
            delay_seconds = _series_send_delay_seconds(batch_series)
            if delay_seconds > 0:
                logging.info(
                    "Applying diagnostic delay before sending series_index=%s: %.3f seconds",
                    batch_series,
                    delay_seconds,
                )
                time.sleep(delay_seconds)
            delayed_series.add(batch_series)

        logging.info(_send_batch_summary(context, batch))
        for chunk_start in range(0, len(batch), chunk_size):
            chunk = batch[chunk_start:chunk_start + chunk_size]
            chunk_summary = _format_chunk_image_summary(chunk)
            logging.info(
                "Sending %s batch: series_index=%s chunk=%d-%d/%d "
                "image_count=%d matrix=%s dtype=%s payload_bytes=%d "
                "first_image_index=%d last_image_index=%d first_slice=%d last_slice=%d",
                context,
                batch_series,
                chunk_start + 1,
                chunk_start + len(chunk),
                len(batch),
                len(chunk),
                chunk_summary.get("matrix"),
                chunk_summary.get("dtype", "unknown"),
                chunk_summary.get("payload_bytes", 0),
                chunk_summary.get("first_image_index", -1),
                chunk_summary.get("last_image_index", -1),
                chunk_summary.get("first_slice", -1),
                chunk_summary.get("last_slice", -1),
            )
            send_start = perf_counter()
            try:
                connection.send_image(chunk)
            except Exception:
                logging.error(
                    "Failed sending %s batch: series_index=%s chunk=%d-%d/%d "
                    "image_count=%d matrix=%s payload_bytes=%d elapsed_seconds=%.3f",
                    context,
                    batch_series,
                    chunk_start + 1,
                    chunk_start + len(chunk),
                    len(batch),
                    len(chunk),
                    chunk_summary.get("matrix"),
                    chunk_summary.get("payload_bytes", 0),
                    perf_counter() - send_start,
                )
                raise
            logging.info(
                "Finished sending %s batch: series_index=%s chunk=%d-%d/%d "
                "elapsed_seconds=%.3f",
                context,
                batch_series,
                chunk_start + 1,
                chunk_start + len(chunk),
                len(batch),
                perf_counter() - send_start,
            )
        batch = []
        batch_series = None

    for image in images:
        series_index = int(getattr(image, "image_series_index", 0))
        if batch and series_index != batch_series:
            flush_batch()
        batch.append(image)
        batch_series = series_index

    flush_batch()


def _as_image_list(images):
    if images is None:
        return []
    if isinstance(images, ismrmrd.Image):
        return [images]
    return list(images)


def process(connection, config, metadata):
    logging.info("Config: \n%s", config)

    # Metadata should be MRD formatted header, but may be a string
    # if it failed conversion earlier
    try:
        # Disabled due to incompatibility between PyXB and Python 3.8:
        # https://github.com/pabigot/pyxb/issues/123
        # # logging.info("Metadata: \n%s", metadata.toxml('utf-8'))

        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info("First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3", 
            metadata.encoding[0].trajectory, 
            metadata.encoding[0].encodedSpace.matrixSize.x, 
            metadata.encoding[0].encodedSpace.matrixSize.y, 
            metadata.encoding[0].encodedSpace.matrixSize.z, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y, 
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z)

    except:
        logging.info("Improperly formatted metadata: \n%s", metadata)

    # Continuously parse incoming data parsed from MRD messages
    acqGroup = []
    imageGroups = {}
    skipped_passthrough_images = 0
    waveformGroup = []
    try:
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                # Accumulate all imaging readouts in a group
                if (not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA) and
                    not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)):
                    acqGroup.append(item)

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing a group of k-space data")
                    image = process_raw(acqGroup, connection, config, metadata)
                    _send_images_by_series(connection, image, "processed raw output")
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imageGroups.setdefault(int(item.image_series_index), []).append(item)
                else:
                    skipped_passthrough_images += 1
                    continue

            # ----------------------------------------------------------
            # Waveform data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Waveform):
                waveformGroup.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        logging.info(
            "Input stream drained before VesselBoost image processing: "
            "processable_series=%d processable_images=%d skipped_non_magnitude_images=%d",
            len(imageGroups),
            sum(len(group) for group in imageGroups.values()),
            skipped_passthrough_images,
        )

        if skipped_passthrough_images:
            logging.warning(
                "Skipped %d non-magnitude input image(s). VesselBoost only returns "
                "derived outputs and optional restamped originals.",
                skipped_passthrough_images,
            )

        for series_index, images in imageGroups.items():
            logging.info(
                "Processing buffered VesselBoost image series after input drain: "
                "series_index=%d images=%d",
                series_index,
                len(images),
            )
            image = process_image(images, connection, config, metadata)
            _send_images_by_series(
                connection,
                image,
                "processed image output after input drain",
            )

        imageGroups.clear()

        # Extract raw ECG waveform data. Basic sorting to make sure that data 
        # is time-ordered, but no additional checking for missing data.
        # ecgData has shape (5 x timepoints)
        if len(waveformGroup) > 0:
            waveformGroup.sort(key = lambda item: item.time_stamp)
            ecgData = [item.data for item in waveformGroup if item.waveform_id == 0]
            ecgData = np.concatenate(ecgData,1)

        # Process any remaining groups of raw data.  This can
        # happen if the trigger condition for these groups are not met.
        if len(acqGroup) > 0:
            logging.info("Processing a group of k-space data (untriggered)")
            image = process_raw(acqGroup, connection, config, metadata)
            _send_images_by_series(connection, image, "processed raw output")
            acqGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()


def process_raw(group, connection, config, metadata):
        
    if len(group) == 0:
        return []

    # Start timer
    tic = perf_counter()

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    # Format data into single [cha PE RO phs] array
    lin = [acquisition.idx.kspace_encode_step_1 for acquisition in group]
    phs = [acquisition.idx.phase                for acquisition in group]

    # Use the zero-padded matrix size
    data = np.zeros((group[0].data.shape[0], 
                     metadata.encoding[0].encodedSpace.matrixSize.y, 
                     metadata.encoding[0].encodedSpace.matrixSize.x, 
                     max(phs)+1), 
                    group[0].data.dtype)

    rawHead = [None]*(max(phs)+1)

    for acq, lin, phs in zip(group, lin, phs):
        if (lin < data.shape[1]) and (phs < data.shape[3]):
            # TODO: Account for asymmetric echo in a better way
            data[:,lin,-acq.data.shape[1]:,phs] = acq.data

            # center line of k-space is encoded in user[5]
            if (rawHead[phs] is None) or (np.abs(acq.getHead().idx.kspace_encode_step_1 - acq.getHead().idx.user[5]) < np.abs(rawHead[phs].idx.kspace_encode_step_1 - rawHead[phs].idx.user[5])):
                rawHead[phs] = acq.getHead()

    # Flip matrix in RO/PE to be consistent with ICE
    data = np.flip(data, (1, 2))

    logging.debug("Raw data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "raw.npy", data)

    # Fourier Transform
    data = fft.fftshift( data, axes=(1, 2))
    data = fft.ifft2(    data, axes=(1, 2))
    data = fft.ifftshift(data, axes=(1, 2))
    data *= np.prod(data.shape) # FFT scaling for consistency with ICE

    # Sum of squares coil combination
    # Data will be [PE RO phs]
    data = np.abs(data)
    data = np.square(data)
    data = np.sum(data, axis=0)
    data = np.sqrt(data)

    logging.debug("Image data is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "img.npy", data)

    # Remove readout oversampling
    offset = int((data.shape[1] - metadata.encoding[0].reconSpace.matrixSize.x)/2)
    data = data[:,offset:offset+metadata.encoding[0].reconSpace.matrixSize.x]

    # Remove phase oversampling
    offset = int((data.shape[0] - metadata.encoding[0].reconSpace.matrixSize.y)/2)
    data = data[offset:offset+metadata.encoding[0].reconSpace.matrixSize.y,:]

    logging.debug("Image without oversampling is size %s" % (data.shape,))
    np.save(debugFolder + "/" + "imgCrop.npy", data)

    # Measure processing time
    toc = perf_counter()
    strProcessTime = "Total processing time: %.2f ms" % ((toc-tic)*1000.0)
    logging.info(strProcessTime)

    # Send this as a text message back to the client
    connection.send_logging(constants.MRD_LOGGING_INFO, strProcessTime)

    # Format as ISMRMRD image data
    imagesOut = []
    for phs in range(data.shape[2]):
        # Create new MRD instance for the processed image
        # data has shape [PE RO phs], i.e. [y x].
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        tmpImg = ismrmrd.Image.from_array(data[...,phs], transpose=False)

        # Set the header information
        tmpImg.setHead(mrdhelper.update_img_header_from_raw(tmpImg.getHead(), rawHead[phs]))
        tmpImg.field_of_view = (ctypes.c_float(metadata.encoding[0].reconSpace.fieldOfView_mm.x), 
                                ctypes.c_float(metadata.encoding[0].reconSpace.fieldOfView_mm.y), 
                                ctypes.c_float(metadata.encoding[0].reconSpace.fieldOfView_mm.z))
        tmpImg.image_index = phs

        # Set ISMRMRD Meta Attributes
        tmpMeta = ismrmrd.Meta()
        tmpMeta['DataRole']               = 'Image'
        tmpMeta['ImageProcessingHistory'] = ['FIRE', 'PYTHON']
        tmpMeta['Keep_image_geometry']    = 1

        xml = tmpMeta.serialize()
        # logging.debug("Image MetaAttributes: %s", xml)
        tmpImg.attribute_string = xml
        imagesOut.append(tmpImg)

    # Call process_image() to run vesselboost on the images
    imagesOut = process_image(imagesOut, connection, config, metadata)

    return imagesOut


def process_image(images, connection, config, metadata):
    if len(images) == 0:
        return []

    def boolean_checker(id:str, default_val:bool=False):
        option = mrdhelper.get_json_config_param(config, id, default_val, type='bool')
        if isinstance(option, str):
            return option.strip().lower() in ("1", "true", "yes", "on")
        else:
            return bool(option)

    send_original = boolean_checker(
        "sendoriginal",
        default_val=OPENRECON_DEFAULTS["sendoriginal"],
    )
    called_from_raw = traceback.extract_stack()[-2].name == "process_raw"
    original_images = []
    if send_original and not called_from_raw:
        original_images = [_clone_mrd_image(image) for image in images]
    logging.info("sendoriginal resolved to %s", send_original)

    source_identity = _resolve_source_series_identity(_meta_from_image(images[0]))

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")

    if hasattr(ismrmrd, "get_dtype_from_data_type"):
        data_type = ismrmrd.get_dtype_from_data_type(images[0].data_type)
    else:
        # Fallback for pyismrmrd versions that removed get_dtype_from_data_type
        data_type = images[0].data.dtype

    logging.debug("Processing data with %d images of type %s", len(images), data_type)

    # Note: The MRD Image class stores data as [cha z y x]
    unsorted_head = [img.getHead() for img in images]
    slice_sort_indices, slice_axis, _ = _slice_sort_indices(unsorted_head)
    _log_slice_geometry(
        "Incoming VesselBoost source",
        unsorted_head,
        slice_axis=slice_axis,
    )
    _log_slice_sort_mapping(slice_sort_indices)

    ordered_images = [images[index] for index in slice_sort_indices]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data for img in ordered_images])
    head = [unsorted_head[index] for index in slice_sort_indices]
    source_series_indices = [
        int(getattr(image_header, "image_series_index", 0))
        for image_header in head
    ]
    first_output_series_index = max(source_series_indices, default=0) + 1
    if send_original and not called_from_raw:
        original_series_index = first_output_series_index
        segmentation_series_index = first_output_series_index + 1
    else:
        original_series_index = None
        segmentation_series_index = first_output_series_index
        if send_original and called_from_raw:
            logging.warning(
                "sendoriginal is true, but input was raw data, so no original images to return."
            )
    segmentation_identity = _build_openrecon_output_identity(
        source_identity,
        series_index=segmentation_series_index,
    )
    logging.info(
        "VesselBoost output identity: source_series=%s source_sequence=%s "
        "source_grouping=%s source_series_uid=%s derived_series=%s "
        "derived_grouping=%s derived_series_uid=%s source_type=%s "
        "output_type=%s display_token=%s image_series_index=%d "
        "(source series indices=%s)",
        source_identity["series_description"] or "N/A",
        source_identity["parent_sequence"] or "N/A",
        source_identity["parent_grouping"] or "N/A",
        source_identity["series_uid"] or "N/A",
        segmentation_identity["series_description"],
        segmentation_identity["grouping"],
        segmentation_identity["series_uid"],
        source_identity["source_type_token"] or "N/A",
        segmentation_identity["type_token"],
        segmentation_identity["display_token"],
        segmentation_series_index,
        sorted(set(source_series_indices)),
    )
    imagesOut = []
    original_output_images = []
    if original_series_index is not None:
        ordered_original_images = [
            original_images[index] for index in slice_sort_indices
        ]
        original_output_images = _build_vesselboost_original_images(
            ordered_original_images,
            ordered_images,
            source_identity,
            original_series_index,
        )
        imagesOut.extend(original_output_images)

    legacy_option = None
    if isinstance(config, dict):
        parameters = config.get("parameters")
        if isinstance(parameters, dict):
            legacy_option = parameters.get("options")

    _log_slice_geometry(
        "Sorted VesselBoost source",
        head,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
    )

    matrix = np.asarray(head[0].matrix_size[:], dtype=float)
    fov = np.asarray(head[0].field_of_view[:], dtype=float)
    if matrix.size < 3 or fov.size < 3:
        raise ValueError(
            "MRD image geometry is incomplete: "
            f"matrix_size={matrix.tolist()} field_of_view={fov.tolist()}"
        )

    matrix = matrix[:3].copy()
    fov = fov[:3].copy()
    if matrix[2] != len(ordered_images):
        matrix[2] = len(ordered_images)

    slice_thickness = float(fov[2])
    measured_slice_spacing = _estimate_slice_spacing(head, slice_axis=slice_axis)
    if measured_slice_spacing is not None:
        if abs(float(measured_slice_spacing) - slice_thickness) > 0.05:
            logging.warning(
                "MRD slice thickness %.6f differs from measured slice spacing %.6f; "
                "using measured spacing for NIfTI affine",
                slice_thickness,
                float(measured_slice_spacing),
            )
        fov[2] = measured_slice_spacing * len(ordered_images)
    else:
        fov[2] = slice_thickness * len(ordered_images)

    if np.any(matrix <= 0) or np.any(fov <= 0):
        raise ValueError(
            "MRD image geometry has non-positive matrix or FOV values: "
            f"matrix_size={matrix.tolist()} field_of_view={fov.tolist()}"
        )

    voxel_size = fov / matrix
    _log_affine_slice_consistency(head, voxel_size)

    # Reformat data to [y x img cha z], i.e. [row ~col] for the first two dimensions
    data = data.transpose((3, 4, 0, 1, 2))

    logging.debug("Original image data is size %s" % (data.shape,))
    # e.g. gre with 128x128x10 with phase and magnitude results in [128 128 1 1 1]
    # np.save(debugFolder + "/" + "imgOrig.npy", data)

    # convert data to nifti using nibabel
    # vesselboost needs 3D data:
    _log_array_summary("OpenRecon input before squeeze", data)
    data = np.squeeze(data)
    logging.debug("Cropped to 3D from Original image data is size %s" % (data.shape,))
    if data.ndim != 3:
        logging.warning(
            "OpenRecon input shape after squeeze is %s (%dD). "
            "VesselBoost expects 3D input and preprocessing or inference may fail.",
            data.shape,
            data.ndim,
        )
    if np.iscomplexobj(data):
        logging.warning(
            "Complex-valued input received. VesselBoost expects real-valued data, "
            "so the input will be converted to magnitude before inference."
        )
        data = np.abs(data)
    _log_array_summary("OpenRecon input after squeeze", data)

    # Read user's choice of vesselboost modules
    module = mrdhelper.get_json_config_param(
        config,
        "vbmodules",
        default=OPENRECON_MODULE_DEFAULT,
        type='str',
    )
    bias_field_correction = boolean_checker(
        "vbbiasfieldcorrection",
        default_val=OPENRECON_DEFAULTS["vbbiasfieldcorrection"],
    )
    denoising = boolean_checker(
        "vbdenoising",
        default_val=OPENRECON_DEFAULTS["vbdenoising"],
    )
    if not (
        _config_has_param(config, "vbbiasfieldcorrection")
        or _config_has_param(config, "vbdenoising")
    ):
        legacy_prep_mode = None
        if _config_has_param(config, "vbprepmode"):
            legacy_prep_mode = int(
                mrdhelper.get_json_config_param(config, "vbprepmode", type='int')
            )
        elif _config_has_param(config, "prep_mode"):
            legacy_prep_mode = int(
                mrdhelper.get_json_config_param(config, "prep_mode", type='int')
            )
        if legacy_prep_mode is not None:
            bias_field_correction, denoising = _prep_mode_to_flags(legacy_prep_mode)

    prep_mode = _derive_prep_mode(bias_field_correction, denoising)
    brain_extraction = boolean_checker(
        "vbbrainextraction",
        default_val=OPENRECON_DEFAULTS["vbbrainextraction"],
    )
    if prep_mode == 4 and brain_extraction:
        logging.warning(
            "Brain masking was requested without preprocessing. VesselBoost only "
            "runs brain extraction during preprocessing, so brain masking will be "
            "ignored."
        )
        brain_extraction = False
    epochs = mrdhelper.get_json_config_param(
        config,
        "vbepochs",
        default=OPENRECON_TRAINING_DEFAULTS["vbepochs"],
        type='str',
    )
    l_rate = mrdhelper.get_json_config_param(
        config,
        "vbrate",
        default=OPENRECON_TRAINING_DEFAULTS["vbrate"],
        type='str',
    )
    use_blending = boolean_checker(
        "vbuseblending",
        default_val=OPENRECON_DEFAULTS["vbuseblending"],
    )
    overlap_percent = int(
        mrdhelper.get_json_config_param(
            config,
            "vboverlap",
            default=OPENRECON_DEFAULTS["vboverlap"],
            type='int',
        )
    )
    if not 0 <= overlap_percent < 100:
        logging.warning(
            "Invalid blending overlap percentage %s requested. Falling back to %s.",
            overlap_percent,
            OPENRECON_DEFAULTS["vboverlap"],
        )
        overlap_percent = OPENRECON_DEFAULTS["vboverlap"]
    overlap_ratio = overlap_percent / 100.0
    reslice_sagittal = boolean_checker(
        "vbreslicesagittal",
        default_val=OPENRECON_DEFAULTS["vbreslicesagittal"],
    )
    reslice_coronal = boolean_checker(
        "vbreslicecoronal",
        default_val=OPENRECON_DEFAULTS["vbreslicecoronal"],
    )
    output_geometry = VESSELBOOST_OUTPUT_GEOMETRY_2D
    debug_threshold_segment = boolean_checker(
        "vbdebugthresholdsegment",
        default_val=OPENRECON_DEFAULTS["vbdebugthresholdsegment"],
    )
    segmentation_mips = boolean_checker(
        "vbsegmentationmips",
        default_val=OPENRECON_DEFAULTS["vbsegmentationmips"],
    )
    logging.info("vbsegmentationmips resolved to %s", segmentation_mips)
    run_name = _openrecon_run_name(
        module,
        prep_mode,
        brain_extraction,
        use_blending,
        config,
    )

    workspace_root = Path(debugFolder) / OPENRECON_WORKSPACE_ROOT
    work_dir = workspace_root / run_name
    if work_dir.exists():
        shutil.rmtree(work_dir)

    input_dir = work_dir / "tof_input"
    output_dir = work_dir / "tof_output"
    preproc_dir = work_dir / "tof_preproc"
    init_label_dir = work_dir / "init_label"
    input_name = "tof.nii"
    input_path = input_dir / input_name

    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    affine = compute_nifti_affine(head[0], voxel_size)
    logging.info("Computed VesselBoost input NIfTI affine:\n%s", affine)

    if data.ndim == 2:
        data_nifti = np.asarray(data.T[:, :, None])
    elif data.ndim == 3:
        data_nifti = np.asarray(data.transpose((1, 0, 2)))
    else:
        data_nifti = np.asarray(data)

    new_img = nib.nifti1.Nifti1Image(data_nifti, affine)
    new_img.header.set_xyzt_units(xyz="mm", t="sec")
    new_img.header.set_dim_info(freq=1, phase=0, slice=2)
    new_img.set_qform(affine, code=1)
    new_img.set_sform(affine, code=1)
    nib.save(new_img, str(input_path))
    logging.info(
        "Saved OpenRecon input image to %s with shape=%s dtype=%s zooms=%s",
        input_path,
        new_img.shape,
        new_img.get_data_dtype(),
        new_img.header.get_zooms(),
    )
    logging.info("Using VesselBoost workspace %s", work_dir)

    # Determine max value (12 or 16 bit)
    BitsStored = 12
    # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
    #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
    maxVal = 2**BitsStored - 1

    pretrained_model = None
    if not debug_threshold_segment:
        pretrained_model = _resolve_vesselboost_model("manual_0429")
    logging.info(
        "OpenRecon VesselBoost options: run_name=%s module=%s bias_field_correction=%s "
        "denoising=%s prep_mode=%s brain_extraction=%s epochs=%s "
        "learning_rate=%s use_blending=%s overlap_ratio=%s "
        "outputgeometry=%s segmentation_send_order=%s "
        "debug_threshold_segment=%s "
        "reslice_sagittal=%s reslice_coronal=%s",
        run_name,
        module,
        bias_field_correction,
        denoising,
        prep_mode,
        brain_extraction,
        epochs,
        l_rate,
        use_blending,
        overlap_ratio,
        output_geometry,
        VESSELBOOST_SEGMENT_SEND_ORDER,
        debug_threshold_segment,
        reslice_sagittal,
        reslice_coronal,
    )

    def maybe_add_preprocessing_args(vb_cmd):
        if prep_mode != 4:
            preproc_dir.mkdir(parents=True, exist_ok=True)
            vb_cmd.extend(["--preprocessed_path", str(preproc_dir)])
            if brain_extraction:
                vb_cmd.append("--enable_brain_extraction")

    def maybe_add_blending_args(vb_cmd):
        if use_blending:
            vb_cmd.extend(["--use_blending", "--overlap_ratio", str(overlap_ratio)])

    def run_vesselboost_command(vb_cmd):
        logging.info("Running VesselBoost command: %s", " ".join(vb_cmd))
        result = subprocess.run(vb_cmd, check=False, capture_output=True, text=True)

        if result.stdout and result.stdout.strip():
            logging.info("VesselBoost stdout:\n%s", result.stdout.rstrip())
        if result.stderr and result.stderr.strip():
            logging.info("VesselBoost stderr:\n%s", result.stderr.rstrip())

        _log_directory_contents("VesselBoost input directory", input_dir)
        _log_directory_contents("VesselBoost preprocessing directory", preproc_dir)
        _log_directory_contents("VesselBoost output directory", output_dir)

        if result.returncode != 0:
            logging.error("VesselBoost command failed with exit code %s", result.returncode)
            raise subprocess.CalledProcessError(
                result.returncode,
                vb_cmd,
                output=result.stdout,
                stderr=result.stderr,
            )

    if debug_threshold_segment:
        logging.warning(
            "vbdebugthresholdsegment is enabled; skipping VesselBoost model "
            "execution and using the openreconi2iexample-style simple threshold "
            "segmentation instead."
        )
        volume_yxz = _simple_threshold_segmentation_volume(data, maxVal)
        debug_output_path = output_dir / input_name
        debug_output_data = np.asarray(volume_yxz).transpose((1, 0, 2))
        debug_img = nib.nifti1.Nifti1Image(debug_output_data, affine)
        debug_img.header.set_xyzt_units(xyz="mm", t="sec")
        debug_img.header.set_dim_info(freq=1, phase=0, slice=2)
        debug_img.set_qform(affine, code=1)
        debug_img.set_sform(affine, code=1)
        nib.save(debug_img, str(debug_output_path))
        logging.info(
            "Saved debug threshold segmentation to %s with shape=%s dtype=%s",
            debug_output_path,
            debug_img.shape,
            debug_img.get_data_dtype(),
        )
        print('Debug threshold processing done')

    elif module == 'prediction':
        logging.info("Running prediction module")

        vb_cmd = [
            "prediction.py",
            "--image_path", str(input_dir),
            "--output_path", str(output_dir),
            "--pretrained", str(pretrained_model),
            "--prep_mode", str(prep_mode),
        ]
        maybe_add_preprocessing_args(vb_cmd)
        maybe_add_blending_args(vb_cmd)
        run_vesselboost_command(vb_cmd)
        logging.info("prediction module completed successfully")


    elif module == 'tta':
        logging.info("Running tta module")

        vb_cmd = [
            "test_time_adaptation.py",
            "--image_path", str(input_dir),
            "--output_path", str(output_dir),
            "--pretrained", str(pretrained_model),
            "--epochs", str(epochs),
            "--learning_rate", str(l_rate),
            "--prep_mode", str(prep_mode),
        ]
        maybe_add_preprocessing_args(vb_cmd)
        maybe_add_blending_args(vb_cmd)
        run_vesselboost_command(vb_cmd)

    elif module == 'booster':
        logging.info("Running booster module")
        init_label_dir.mkdir(parents=True, exist_ok=True)

        vb_cmd = [
            "angiboost.py",
            "--image_path", str(input_dir),
            "--pretrained", str(pretrained_model),
            "--label_path", str(init_label_dir),
            "--output_path", str(output_dir),
            "--output_model", str(output_dir / "output_model"),
            "--prep_mode", str(prep_mode),
            "--epochs", str(epochs),
            "--learning_rate", str(l_rate),
        ]
        maybe_add_preprocessing_args(vb_cmd)
        run_vesselboost_command(vb_cmd)

    else:
        raise ValueError(f"Unsupported VesselBoost module requested: {module}")

    if not debug_threshold_segment:
        print('Processing done')
        output_image = _find_output_nifti(output_dir, input_name)
        logging.info("Loading VesselBoost output image %s", output_image)
        img = nib.load(str(output_image))
        data = img.get_fdata(dtype=np.float32)

        # Reformat data
        print("shape after loading with nibabel")
        print(data.shape)
        if data.ndim == 2:
            data = data[:, :, None]
        if data.ndim == 3:
            # Bring NIfTI [x, y, z] back to OpenRecon/MRD convenience [y, x, z].
            data = data.transpose((1, 0, 2))
        if data.ndim != 3:
            raise ValueError(
                f"VesselBoost output must be 3D after squeezing, got shape {data.shape}"
            )
        if data.shape[-1] != len(head):
            raise ValueError(
                "VesselBoost output slice count does not match MRD input: "
                f"output_z={data.shape[-1]} input_images={len(head)}"
            )

        if legacy_option == "complex":
            logging.warning(
                "Ignoring legacy complex output request because VesselBoost returns "
                "real-valued segmentations."
            )

        # Normalize real-valued output and convert to int16 for MRD export.
        data = np.nan_to_num(data, nan=0.0, posinf=0.0, neginf=0.0)
        if np.min(data) < 0:
            logging.warning("Negative values detected in VesselBoost output; clipping to zero.")
            data = np.clip(data, 0, None)

        data_max = float(np.max(data))
        if data_max > 0:
            volume_yxz = np.rint(data * (maxVal / data_max)).astype(np.int16)
        else:
            logging.warning("VesselBoost output is all zeros; returning a zero-valued image.")
            volume_yxz = np.zeros(data.shape, dtype=np.int16)

    if volume_yxz.shape[-1] != len(head):
        raise ValueError(
            "VesselBoost output slice count does not match MRD input: "
            f"output_z={volume_yxz.shape[-1]} input_images={len(head)}"
        )

    segmentation_images = _build_vesselboost_segmentation_images(
        volume_yxz=volume_yxz,
        ordered_source_images=ordered_images,
        source_identity=source_identity,
        output_identity=segmentation_identity,
        series_index=segmentation_series_index,
        max_val=maxVal,
        scanner_postprocessing=not segmentation_mips,
        segmentation_mips=segmentation_mips,
    )
    imagesOut.extend(segmentation_images)

    segmentation_contract = (
        "source-geometry segmentation-header 2D"
        if segmentation_mips
        else "source-image-header 2D"
    )
    if original_series_index is not None:
        logging.info(
            "VesselBoost send order is source-geometry originals first, then "
            "%s segmentation images",
            segmentation_contract,
        )
    else:
        logging.info(
            "VesselBoost send order is %s segmentation images",
            segmentation_contract,
        )

    if reslice_sagittal or reslice_coronal:
        base_series = segmentation_series_index + 1
        reformat_images = []

        if reslice_sagittal:
            sagittal_identity = _build_openrecon_output_identity(
                source_identity,
                orientation="sagittal",
                series_index=base_series,
            )
            logging.info(
                "Appending sagittal reformat output series (series_index=%d, name=%s)",
                base_series,
                sagittal_identity["series_description"],
            )
            sagittal_images = _build_reformatted_images(
                volume_yxz=volume_yxz,
                head_template=head[0],
                source_image=ordered_images[0],
                source_identity=source_identity,
                output_identity=sagittal_identity,
                voxel_size=voxel_size,
                fov=fov,
                orientation="sagittal",
                series_index=base_series,
                max_val=maxVal,
            )
            imagesOut.extend(sagittal_images)
            reformat_images.extend(sagittal_images)
            base_series += 1

        if reslice_coronal:
            coronal_identity = _build_openrecon_output_identity(
                source_identity,
                orientation="coronal",
                series_index=base_series,
            )
            logging.info(
                "Appending coronal reformat output series (series_index=%d, name=%s)",
                base_series,
                coronal_identity["series_description"],
            )
            coronal_images = _build_reformatted_images(
                volume_yxz=volume_yxz,
                head_template=head[0],
                source_image=ordered_images[0],
                source_identity=source_identity,
                output_identity=coronal_identity,
                voxel_size=voxel_size,
                fov=fov,
                orientation="coronal",
                series_index=base_series,
                max_val=maxVal,
            )
            imagesOut.extend(coronal_images)
            reformat_images.extend(coronal_images)
    else:
        reformat_images = []

    if segmentation_mips:
        segment_header_geometry = "2d_segment_header_originals"
        postprocessing_target = (
            "originals"
            if original_series_index is not None
            else "none_segment_not_postprocessed"
        )
    else:
        segment_header_geometry = "2d_source_image_header"
        postprocessing_target = (
            "originals+segment_2d_source_image_header"
            if original_series_index is not None
            else "segment_2d_source_image_header"
        )
    logging.info(
        "Configured outputs: original=%s segment=True "
        "segmentheadergeometry=%s postprocessing_target=%s "
        "reformat_sagittal=%s reformat_coronal=%s",
        original_series_index is not None,
        segment_header_geometry,
        postprocessing_target,
        reslice_sagittal,
        reslice_coronal,
    )
    logging.info("VESSELBOOST_OPENRECON_POSTPROCESSING target=%s", postprocessing_target)
    logging.info(
        "Sending %d original image(s), %d segmentation image(s), and %d reformat image(s)",
        len(original_output_images),
        len(segmentation_images),
        len(reformat_images),
    )

    _validate_vesselboost_output_contract(
        imagesOut,
        source_series_indices,
        source_identity,
        "processed image output",
    )

    return imagesOut


if __name__ == "__main__":
    raise SystemExit(_main())
