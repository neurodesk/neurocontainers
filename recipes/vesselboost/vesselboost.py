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
import xml.dom.minidom

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
VESSELBOOST_SEGMENTATION_LABEL = "vesselboost_segmentation"
VESSELBOOST_SEGMENTATION_TYPE_TOKEN = VESSELBOOST_SEGMENTATION_LABEL.upper()
VESSELBOOST_SOURCE_COPY_LABEL = "vesselboost_source_copy"
VESSELBOOST_SOURCE_COPY_TYPE_TOKEN = VESSELBOOST_SOURCE_COPY_LABEL.upper()

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
    "vbreslicesagittal": False,
    "vbreslicecoronal": False,
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

    try:
        return _first_non_empty_text(mrdhelper.extract_minihead_string_param(minihead_text, name))
    except Exception:
        pass

    match = re.search(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"([^"]*)"\s*\}}',
        minihead_text,
    )
    if match:
        return match.group(1).strip()
    return ""


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


def _replace_minihead_string_param(minihead_text, name, value):
    if not minihead_text or not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    match = pattern.search(minihead_text)
    if not match:
        return minihead_text, False

    replacement = f"{match.group(1)}{value}{match.group(3)}"
    return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True


def _sanitize_minihead_param_value(value):
    text = _first_non_empty_text(value)
    if not text:
        return ""
    return (
        text.replace("\\", "/")
        .replace('"', "'")
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
    match = pattern.search(minihead_text)
    if match:
        if match.group(2) == value:
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_minihead_array_token(minihead_text, name, source_token, target_token):
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

    if any(token.upper() == target_token.upper() for token in tokens):
        return minihead_text, False

    replacement_source = ""
    source_token = (source_token or "").strip().upper()
    for token in tokens:
        if token.upper() == source_token:
            replacement_source = token
            break

    if not replacement_source:
        reserved_tokens = {"NORM", "DIS2D", "DIS3D"}
        for token in tokens:
            if token.upper() not in reserved_tokens:
                replacement_source = token
                break

    if not replacement_source:
        return minihead_text, False

    token_pattern = re.compile(rf'(\{{\s*"){re.escape(replacement_source)}("\s*\}})')
    token_match = token_pattern.search(block_text)
    if not token_match:
        return minihead_text, False

    replaced_block = (
        block_text[:token_match.start()]
        + f'{token_match.group(1)}{target_token}{token_match.group(2)}'
        + block_text[token_match.end():]
    )
    return (
        minihead_text[:block_match.start()] + replaced_block + minihead_text[block_match.end():],
        True,
    )


def _replace_or_append_minihead_array_token(minihead_text, name, source_token, target_token):
    current_text, did_change = _replace_minihead_array_token(
        minihead_text,
        name,
        source_token,
        target_token,
    )
    if did_change or not current_text or not target_token:
        return current_text, did_change
    if _extract_minihead_array_tokens(current_text, name):
        return current_text, False

    target_token = _sanitize_minihead_param_value(target_token)
    appended_param = f'\n<ParamArray."{name}">\t{{\n\t{{ "{target_token}" }}\n}}\n'
    return current_text.rstrip() + appended_param, True


def _patch_ice_minihead(
    minihead_text,
    parent_sequence,
    parent_grouping,
    source_type_token,
    target_type_token,
    target_display_token=None,
    target_image_type_value3="M",
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    target_display_token = target_display_token or target_type_token

    for param_name, param_value in (
        ("SequenceDescription", parent_sequence),
        ("SeriesNumberRangeNameUID", parent_grouping),
        ("ImageType", f"DERIVED\\PRIMARY\\{target_image_type_value3}\\{target_type_token}"),
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
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

    return {
        "series_description": _first_non_empty_text(
            series_description,
            parent_sequence,
        ),
        "parent_sequence": parent_sequence,
        "parent_grouping": parent_grouping,
        "source_type_token": source_type_token,
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


def _build_openrecon_output_identity(source_identity, orientation=None):
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
    return {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "display_token": VESSELBOOST_SEGMENTATION_LABEL,
        "type_token": VESSELBOOST_SEGMENTATION_TYPE_TOKEN,
        "image_comment": image_comment,
    }


def _build_vesselboost_source_copy_identity(source_identity):
    source_series_description = _first_non_empty_text(
        source_identity.get("series_description", ""),
        "source",
    )
    source_parent_grouping = _first_non_empty_text(
        source_identity.get("parent_grouping", ""),
        source_series_description,
    )
    series_description = f"{source_series_description}_{VESSELBOOST_SOURCE_COPY_LABEL}"
    grouping = f"{source_parent_grouping}_{VESSELBOOST_SOURCE_COPY_LABEL}"
    return {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "display_token": VESSELBOOST_SOURCE_COPY_LABEL,
        "type_token": VESSELBOOST_SOURCE_COPY_TYPE_TOKEN,
        "image_comment": VESSELBOOST_SOURCE_COPY_LABEL,
    }


def _apply_vesselboost_output_identity(
    image,
    output_identity,
    source_type_token,
    processing_history,
):
    header = image.getHead()
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    image.setHead(header)

    tmp_meta = ismrmrd.Meta.deserialize(image.attribute_string)
    tmp_meta["DataRole"] = "Image"
    tmp_meta["ImageProcessingHistory"] = processing_history
    tmp_meta["SeriesDescription"] = output_identity["series_description"]
    tmp_meta["SequenceDescription"] = output_identity["sequence_description"]
    tmp_meta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
    tmp_meta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
    tmp_meta["ImageTypeValue3"] = "M"
    tmp_meta["ImageTypeValue4"] = output_identity["display_token"]
    tmp_meta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
    tmp_meta["ComplexImageComponent"] = "MAGNITUDE"
    tmp_meta["ImageComments"] = output_identity["image_comment"]
    tmp_meta["ImageComment"] = output_identity["image_comment"]
    if "SequenceDescriptionAdditional" in tmp_meta:
        try:
            del tmp_meta["SequenceDescriptionAdditional"]
        except Exception:
            tmp_meta["SequenceDescriptionAdditional"] = ""

    minihead_text = _decode_ice_minihead(tmp_meta)
    if minihead_text:
        patched_minihead_text, minihead_changed = _patch_ice_minihead(
            minihead_text,
            output_identity["sequence_description"],
            output_identity["grouping"],
            source_type_token,
            output_identity["type_token"],
            target_display_token=output_identity["display_token"],
        )
        if minihead_changed:
            tmp_meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)
        else:
            logging.warning(
                "IceMiniHead was present but not updated for %s",
                output_identity["series_description"],
            )

    tmp_meta["Keep_image_geometry"] = 1
    image.attribute_string = tmp_meta.serialize()


def _validate_vesselboost_output_contract(
    images,
    source_series_indices,
    source_identity,
    context,
):
    # This runs before any send. Raising here makes OpenRecon log the error and
    # close cleanly instead of partially returning images with unsafe identity.
    source_series_indices = {int(index) for index in source_series_indices}
    source_series_description = _first_non_empty_text(
        source_identity.get("series_description", "")
    )
    source_grouping = _first_non_empty_text(source_identity.get("parent_grouping", ""))
    source_sequence = _first_non_empty_text(source_identity.get("parent_sequence", ""))

    errors = []
    for index, image in enumerate(_as_image_list(images)):
        series_index = int(getattr(image, "image_series_index", 0))
        try:
            meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
        except Exception as exc:
            errors.append(f"image {index}: invalid MRD Meta attributes: {exc}")
            continue

        minihead_text = _decode_ice_minihead(meta_obj)
        meta_series_description = _get_meta_text(meta_obj, "SeriesDescription")
        minihead_series_description = _extract_minihead_string_value(
            minihead_text,
            "SeriesDescription",
        )
        meta_grouping = _get_meta_text(meta_obj, "SeriesNumberRangeNameUID")
        minihead_grouping = _extract_minihead_string_value(
            minihead_text,
            "SeriesNumberRangeNameUID",
        )
        grouping = _first_non_empty_text(meta_grouping, minihead_grouping)
        meta_sequence = _get_meta_text(meta_obj, "SequenceDescription")
        minihead_sequence = _extract_minihead_string_value(
            minihead_text,
            "SequenceDescription",
        )
        sequence = _first_non_empty_text(meta_sequence, minihead_sequence)
        image_type_value4 = _first_non_empty_text(
            _get_meta_text(meta_obj, "ImageTypeValue4"),
            _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4"),
        )

        if series_index in source_series_indices:
            errors.append(
                f"image {index}: output still uses source image_series_index={series_index}"
            )
        if (
            source_series_description
            and meta_series_description == source_series_description
        ):
            errors.append(
                f"image {index}: output Meta still uses source "
                f"SeriesDescription={meta_series_description}"
            )
        if (
            source_series_description
            and minihead_series_description == source_series_description
        ):
            errors.append(
                f"image {index}: output IceMiniHead still uses source "
                f"SeriesDescription={minihead_series_description}"
            )
        if source_grouping and meta_grouping == source_grouping:
            errors.append(
                f"image {index}: output Meta still uses source "
                f"SeriesNumberRangeNameUID={meta_grouping}"
            )
        if source_grouping and minihead_grouping == source_grouping:
            errors.append(
                f"image {index}: output IceMiniHead still uses source "
                f"SeriesNumberRangeNameUID={minihead_grouping}"
            )
        if source_sequence and meta_sequence == source_sequence:
            errors.append(
                f"image {index}: output Meta still uses source "
                f"SequenceDescription={meta_sequence}"
            )
        if source_sequence and minihead_sequence == source_sequence:
            errors.append(
                f"image {index}: output IceMiniHead still uses source "
                f"SequenceDescription={minihead_sequence}"
            )
        if not grouping:
            errors.append(f"image {index}: output is missing SeriesNumberRangeNameUID")
        if not image_type_value4:
            errors.append(f"image {index}: output is missing ImageTypeValue4")

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
    meta_template,
    source_type_token: str,
    output_identity: dict,
    voxel_size: np.ndarray,
    fov: np.ndarray,
    orientation: str,
    series_index: int,
    max_val: int,
):
    """Build MRD images for a sagittal or coronal reformat of the segmentation.

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

    source_slice_count = max(1, int(N_z))
    logging.info(
        "Building %s reformat: n_slices=%d source_slice_count=%d "
        "slice_policy=modulo_source_slice_count slice_spacing=%.4f new_fov=%s "
        "series_index=%d target_shape=%s target_spacing=%.4f",
        orientation,
        n_slices,
        source_slice_count,
        slice_spacing,
        new_fov,
        series_index,
        target_inplane_shape,
        target_inplane_spacing,
    )

    images_out = []
    for j in range(n_slices):
        offset = (j - 0.5 * (n_slices - 1)) * slice_spacing
        slice_position = volume_center + offset * new_slice_dir

        if orientation == "sagittal":
            # volume_yxz[:, j, :] has shape (N_y, N_z) -> transpose to (N_z, N_y)
            # so rows = Z (phase_dir), cols = Y (read_dir).
            slice2d = np.ascontiguousarray(volume_yxz[:, j, :].T)
        else:
            # volume_yxz[j, :, :] has shape (N_x, N_z) -> transpose to (N_z, N_x)
            # so rows = Z (phase_dir), cols = X (read_dir).
            slice2d = np.ascontiguousarray(volume_yxz[j, :, :].T)

        slice2d = _resize_2d_nearest(slice2d, target_inplane_shape)
        mrd_image = ismrmrd.Image.from_array(slice2d, transpose=False)

        new_header = mrd_image.getHead()
        new_header.data_type = mrd_image.data_type
        new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        new_header.position = tuple(float(v) for v in slice_position)
        new_header.read_dir = tuple(float(v) for v in new_read_dir)
        new_header.phase_dir = tuple(float(v) for v in new_phase_dir)
        new_header.slice_dir = tuple(float(v) for v in new_slice_dir)
        new_header.field_of_view = (
            ctypes.c_float(new_fov[0]),
            ctypes.c_float(new_fov[1]),
            ctypes.c_float(new_fov[2]),
        )
        reformat_slice_counter = j % source_slice_count
        new_header.image_index = j
        new_header.image_series_index = series_index
        new_header.slice = reformat_slice_counter

        if j < 5 or j >= n_slices - 5:
            logging.info(
                "%s reformat image header sample: image_index=%d slice=%d "
                "series_index=%d position=%s read_dir0=%.6f phase_dir0=%.6f "
                "slice_dir0=%.6f",
                orientation,
                j,
                reformat_slice_counter,
                series_index,
                [round(float(v), 6) for v in slice_position],
                float(new_read_dir[0]),
                float(new_phase_dir[0]),
                float(new_slice_dir[0]),
            )

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

        tmp_meta = ismrmrd.Meta()
        if meta_template is not None:
            for key in meta_template.keys():
                try:
                    tmp_meta[key] = meta_template[key]
                except Exception:
                    pass
        tmp_meta["DataRole"] = "Image"
        tmp_meta["ImageProcessingHistory"] = [
            "PYTHON",
            "VESSELBOOST",
            f"RESLICE_{orientation.upper()}",
        ]
        tmp_meta["WindowCenter"] = str((max_val + 1) / 2)
        tmp_meta["WindowWidth"] = str((max_val + 1))
        tmp_meta["SeriesDescription"] = output_identity["series_description"]
        tmp_meta["SequenceDescription"] = output_identity["sequence_description"]
        tmp_meta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
        tmp_meta["ImageType"] = (
            f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        )
        tmp_meta["ImageTypeValue3"] = "M"
        tmp_meta["ImageTypeValue4"] = output_identity["display_token"]
        tmp_meta["DicomImageType"] = (
            f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        )
        tmp_meta["ComplexImageComponent"] = "MAGNITUDE"
        tmp_meta["ImageComments"] = output_identity["image_comment"]
        tmp_meta["ImageComment"] = output_identity["image_comment"]
        if "SequenceDescriptionAdditional" in tmp_meta:
            try:
                del tmp_meta["SequenceDescriptionAdditional"]
            except Exception:
                tmp_meta["SequenceDescriptionAdditional"] = ""
        # The host requires this attribute to be present, but reformatted
        # outputs must use the explicit geometry set on the MRD header.
        tmp_meta["Keep_image_geometry"] = "0"
        tmp_meta["ImageRowDir"] = [
            "{:.18f}".format(new_read_dir[0]),
            "{:.18f}".format(new_read_dir[1]),
            "{:.18f}".format(new_read_dir[2]),
        ]
        tmp_meta["ImageColumnDir"] = [
            "{:.18f}".format(new_phase_dir[0]),
            "{:.18f}".format(new_phase_dir[1]),
            "{:.18f}".format(new_phase_dir[2]),
        ]
        if "IceMiniHead" in tmp_meta:
            # The source miniheader contains original stack geometry and slice
            # counts. Reusing it on reformats makes ICE treat sagittal/coronal
            # outputs like the original axial series.
            try:
                del tmp_meta["IceMiniHead"]
            except Exception:
                tmp_meta["IceMiniHead"] = ""
        mrd_image.attribute_string = tmp_meta.serialize()
        images_out.append(mrd_image)

    return images_out


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
    passthroughImages = []
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
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry']    = 1
                    item.attribute_string = tmpMeta.serialize()

                    passthroughImages.append(item)
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
            "processable_series=%d processable_images=%d passthrough_images=%d",
            len(imageGroups),
            sum(len(group) for group in imageGroups.values()),
            len(passthroughImages),
        )

        if passthroughImages:
            _send_images_by_series(
                connection,
                passthroughImages,
                "passthrough image after input drain",
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

    source_identity = _resolve_source_series_identity(
        ismrmrd.Meta.deserialize(images[0].attribute_string)
    )
    segmentation_identity = _build_openrecon_output_identity(source_identity)
    logging.info(
        "VesselBoost output identity: source_series=%s source_sequence=%s "
        "source_grouping=%s derived_series=%s derived_grouping=%s "
        "source_type=%s output_type=%s display_token=%s",
        source_identity["series_description"] or "N/A",
        source_identity["parent_sequence"] or "N/A",
        source_identity["parent_grouping"] or "N/A",
        segmentation_identity["series_description"],
        segmentation_identity["grouping"],
        source_identity["source_type_token"] or "N/A",
        segmentation_identity["type_token"],
        segmentation_identity["display_token"],
    )

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
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in ordered_images]
    source_series_indices = [
        int(getattr(image_header, "image_series_index", 0))
        for image_header in head
    ]
    segmentation_series_index = max(source_series_indices, default=0) + 1
    logging.info(
        "Using image_series_index=%d for VesselBoost segmentation "
        "(source series indices=%s)",
        segmentation_series_index,
        sorted(set(source_series_indices)),
    )
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

    # Display MetaAttributes for first image
    # logging.debug("MetaAttributes[0]: %s", ismrmrd.Meta.serialize(meta[0]))

    # Optional serialization of ICE MiniHeader
    # if 'IceMiniHead' in meta[0]:
    #     logging.debug("IceMiniHead[0]: %s", base64.b64decode(meta[0]['IceMiniHead']).decode('utf-8'))

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

    pretrained_model = _resolve_vesselboost_model("manual_0429")
    logging.info(
        "OpenRecon VesselBoost options: run_name=%s module=%s bias_field_correction=%s "
        "denoising=%s prep_mode=%s brain_extraction=%s epochs=%s "
        "learning_rate=%s use_blending=%s overlap_ratio=%s "
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

    if module == 'prediction':
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

    # Determine max value (12 or 16 bit)
    BitsStored = 12
    # if (mrdhelper.get_userParameterLong_value(metadata, "BitsStored") is not None):
    #     BitsStored = mrdhelper.get_userParameterLong_value(metadata, "BitsStored")
    maxVal = 2**BitsStored - 1

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

    # Reshape the axial segmentation volume for the per-slice MRD loop.
    data = volume_yxz[:, :, :, None, None]
    data = data.transpose((0, 1, 4, 3, 2))

    currentSeries = 0

    # Re-slice back into 2D images
    imagesOut = [None] * data.shape[-1]
    for iImg in range(data.shape[-1]):
        # Create new MRD instance for the final image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
        imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)

        # Create a copy of the original fixed header and update the data_type
        # (we changed it to int16 from all other types)
        oldHeader = head[iImg]
        oldHeader.data_type = imagesOut[iImg].data_type

        # Set the image_type to match the data_type for complex data
        if (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXFLOAT) or (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXDOUBLE):
            oldHeader.image_type = ismrmrd.IMTYPE_COMPLEX
        else:
            oldHeader.image_type = ismrmrd.IMTYPE_MAGNITUDE

        oldHeader.image_series_index = segmentation_series_index
        oldHeader.image_index = iImg
        oldHeader.slice = iImg

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], 'IceMiniHead') is not None:
            if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[iImg]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = _copy_meta(meta[iImg])
        tmpMeta['DataRole']                       = 'Image'
        tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'VESSELBOOST']
        tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
        tmpMeta['WindowWidth']                    = str((maxVal+1))
        tmpMeta['SeriesDescription']              = segmentation_identity["series_description"]
        tmpMeta['SequenceDescription']            = segmentation_identity["sequence_description"]
        tmpMeta['SeriesNumberRangeNameUID']       = segmentation_identity["grouping"]
        tmpMeta['ImageType']                      = f"DERIVED\\PRIMARY\\M\\{segmentation_identity['type_token']}"
        tmpMeta['ImageTypeValue3']                = 'M'
        tmpMeta['ImageTypeValue4']                = segmentation_identity["display_token"]
        tmpMeta['DicomImageType']                 = f"DERIVED\\PRIMARY\\M\\{segmentation_identity['type_token']}"
        tmpMeta['ComplexImageComponent']          = 'MAGNITUDE'
        tmpMeta['ImageComments']                  = segmentation_identity["image_comment"]
        tmpMeta['ImageComment']                   = segmentation_identity["image_comment"]
        if 'SequenceDescriptionAdditional' in tmpMeta:
            try:
                del tmpMeta['SequenceDescriptionAdditional']
            except Exception:
                tmpMeta['SequenceDescriptionAdditional'] = ''

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                segmentation_identity["sequence_description"],
                segmentation_identity["grouping"],
                source_identity["source_type_token"],
                segmentation_identity["type_token"],
                target_display_token=segmentation_identity["display_token"],
            )
            if minihead_changed:
                tmpMeta['IceMiniHead'] = _encode_ice_minihead(patched_minihead_text)
            else:
                logging.warning(
                    "IceMiniHead was present but not updated for VesselBoost output slice %d",
                    iImg,
                )
        tmpMeta['Keep_image_geometry']            = 1

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get('ImageRowDir') is None:
            tmpMeta['ImageRowDir'] = ["{:.18f}".format(oldHeader.read_dir[0]), "{:.18f}".format(oldHeader.read_dir[1]), "{:.18f}".format(oldHeader.read_dir[2])]

        if tmpMeta.get('ImageColumnDir') is None:
            tmpMeta['ImageColumnDir'] = ["{:.18f}".format(oldHeader.phase_dir[0]), "{:.18f}".format(oldHeader.phase_dir[1]), "{:.18f}".format(oldHeader.phase_dir[2])]

        metaXml = tmpMeta.serialize()
        # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

    if reslice_sagittal or reslice_coronal:
        base_series = segmentation_series_index + 1
        meta_template = meta[0] if meta else None

        if reslice_sagittal:
            sagittal_identity = _build_openrecon_output_identity(
                source_identity,
                orientation="sagittal",
            )
            logging.info(
                "Appending sagittal reformat output series (series_index=%d, name=%s)",
                base_series,
                sagittal_identity["series_description"],
            )
            imagesOut.extend(
                _build_reformatted_images(
                    volume_yxz=volume_yxz,
                    head_template=head[0],
                    meta_template=meta_template,
                    source_type_token=source_identity["source_type_token"],
                    output_identity=sagittal_identity,
                    voxel_size=voxel_size,
                    fov=fov,
                    orientation="sagittal",
                    series_index=base_series,
                    max_val=maxVal,
                )
            )
            base_series += 1

        if reslice_coronal:
            coronal_identity = _build_openrecon_output_identity(
                source_identity,
                orientation="coronal",
            )
            logging.info(
                "Appending coronal reformat output series (series_index=%d, name=%s)",
                base_series,
                coronal_identity["series_description"],
            )
            imagesOut.extend(
                _build_reformatted_images(
                    volume_yxz=volume_yxz,
                    head_template=head[0],
                    meta_template=meta_template,
                    source_type_token=source_identity["source_type_token"],
                    output_identity=coronal_identity,
                    voxel_size=voxel_size,
                    fov=fov,
                    orientation="coronal",
                    series_index=base_series,
                    max_val=maxVal,
                )
            )

    if send_original:
        if called_from_raw:
            logging.warning(
                "sendoriginal is true, but input was raw data, so no original images to return."
            )
        else:
            source_copy_identity = _build_vesselboost_source_copy_identity(source_identity)
            used_output_series_indices = [
                int(getattr(image.getHead(), "image_series_index", 0))
                for image in imagesOut
            ]
            source_copy_series_index = max(
                used_output_series_indices + source_series_indices,
                default=segmentation_series_index,
            ) + 1
            logging.info(
                "Sending original MRA images as derived source-copy series "
                "(series_index=%d, name=%s)",
                source_copy_series_index,
                source_copy_identity["series_description"],
            )
            ordered_original_images = [
                original_images[index] for index in slice_sort_indices
            ]
            for iImg, original_image in enumerate(ordered_original_images):
                original_header = original_image.getHead()
                original_header.image_series_index = source_copy_series_index
                original_header.image_index = iImg
                original_header.slice = iImg
                original_image.setHead(original_header)
                _apply_vesselboost_output_identity(
                    original_image,
                    source_copy_identity,
                    source_identity["source_type_token"],
                    ["PYTHON", "VESSELBOOST", "SOURCE_COPY"],
                )
                imagesOut.append(original_image)

    _validate_vesselboost_output_contract(
        imagesOut,
        source_series_indices,
        source_identity,
        "processed image output",
    )

    return imagesOut


if __name__ == "__main__":
    raise SystemExit(_main())
