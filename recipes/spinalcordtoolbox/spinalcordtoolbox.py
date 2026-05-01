import argparse
import base64
import copy
import ctypes
import itertools
import json
import logging
import os
import re
import shutil
import subprocess
import traceback
import uuid
from pathlib import Path
from time import perf_counter
import xml.dom.minidom

import constants
import ismrmrd
import mrdhelper
import nibabel as nib
import numpy as np
import numpy.fft as fft


# Folder for debug output files
debugFolder = "/tmp/share/debug"
OPENRECON_WORKSPACE_ROOT = "spinalcordtoolbox_openrecon"

SCT_DEEPSEG_TASKS = (
    "spinalcord",
    "sc_epi",
    "sc_lumbar_t2",
    "sc_mouse_t1",
    "graymatter",
    "gm_sc_7t_t2star",
    "gm_wm_exvivo_t2",
    "gm_mouse_t1",
    "gm_wm_mouse_t1",
    "lesion_ms",
    "lesion_ms_axial_t2",
    "lesion_ms_mp2rage",
    "lesion_sci_t2",
    "tumor_edema_cavity_t1_t2",
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
    "totalspineseg",
)

SCT_ANALYSIS_REGISTRY = {
    **{
        f"sct_deepseg_{task}": {
            "kind": "deepseg",
            "task": task,
            "series_suffix": f"sct_deepseg_{task}",
        }
        for task in SCT_DEEPSEG_TASKS
    },
    "sct_label_vertebrae": {
        "kind": "label_vertebrae",
        "series_suffix": "sct_label_vertebrae",
    },
}
SCT_ANALYSIS_REGISTRY["sct_deepseg_graymatter"]["kind"] = "deepseg_gm"

SCT_ANALYSIS_BUNDLES = {
    "sct_bundle_t2_anatomy": (
        "sct_deepseg_spinalcord",
        "sct_label_vertebrae",
        "sct_deepseg_sc_canal_t2",
        "sct_deepseg_totalspineseg",
    ),
    "sct_bundle_t2_ms": (
        "sct_deepseg_spinalcord",
        "sct_deepseg_lesion_ms",
        "sct_deepseg_lesion_ms_axial_t2",
    ),
    "sct_bundle_t2s_gm": (
        "sct_deepseg_spinalcord",
        "sct_deepseg_graymatter",
    ),
    "sct_bundle_mouse_t1": (
        "sct_deepseg_sc_mouse_t1",
        "sct_deepseg_gm_mouse_t1",
    ),
}

OPENRECON_DEFAULTS = {
    "config": "spinalcordtoolbox",
    "sendoriginal": True,
    "segmentationcolormap": False,
    "analysis": "sct_deepseg_spinalcord",
}

OPENRECON_SEND_IMAGE_CHUNK_SIZE = 96

SOURCE_PARENT_REFERENCE_META_KEYS = {
    "DicomEngineDimString",
    "MFInstanceNumber",
    "MultiFrameSOPInstanceUID",
    "PSMultiFrameSOPInstanceUID",
    "PSSeriesInstanceUID",
}

SOURCE_PARENT_REFERENCE_META_PREFIXES = (
    "ReferencedGSPS",
)

SCT_BATCH_PROCESSING_OPENRECON_CASES = (
    {
        "name": "batch_t2_deepseg_spinalcord",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": 'sct_deepseg spinalcord -i t2.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    },
    {
        "name": "batch_t2_label_vertebrae",
        "analysis": "sct_label_vertebrae",
        "source_command": 'sct_label_vertebrae -i t2.nii.gz -s t2_seg.nii.gz -c t2 -qc "$SCT_BP_QC_FOLDER"',
    },
    {
        "name": "batch_t2s_deepseg_spinalcord",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": 'sct_deepseg spinalcord -i t2s.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    },
    {
        "name": "batch_t2s_deepseg_graymatter",
        "analysis": "sct_deepseg_graymatter",
        "source_command": 'sct_deepseg_gm -i t2s.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    },
    {
        "name": "batch_t1_deepseg_spinalcord_t1",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": "sct_deepseg spinalcord -i t1.nii.gz",
    },
    {
        "name": "batch_t1_deepseg_spinalcord_t2",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": "sct_deepseg spinalcord -i t2.nii.gz",
    },
    {
        "name": "batch_mt_deepseg_spinalcord",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": 'sct_deepseg spinalcord -i mt1_crop.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    },
    {
        "name": "batch_dmri_deepseg_spinalcord",
        "analysis": "sct_deepseg_spinalcord",
        "source_command": 'sct_deepseg spinalcord -i dmri_moco_dwi_mean.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    },
)


def process(connection, config, metadata):
    logging.info("Config: \n%s", config)

    # Metadata should be MRD formatted header, but may be a string
    # if it failed conversion earlier
    try:
        # Disabled due to incompatibility between PyXB and Python 3.8:
        # https://github.com/pabigot/pyxb/issues/123
        # # logging.info("Metadata: \n%s", metadata.toxml('utf-8'))

        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info(
            "First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3",
            metadata.encoding[0].trajectory,
            metadata.encoding[0].encodedSpace.matrixSize.x,
            metadata.encoding[0].encodedSpace.matrixSize.y,
            metadata.encoding[0].encodedSpace.matrixSize.z,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z,
        )

    except:
        logging.info("Improperly formatted metadata: \n%s", metadata)

    # Continuously parse incoming data parsed from MRD messages
    currentSeries = 0
    acqGroup = []
    imgGroup = []
    waveformGroup = []
    try:
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                # Accumulate all imaging readouts in a group
                if (
                    not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)
                ):
                    acqGroup.append(item)

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing a group of k-space data")
                    # image = process_raw(acqGroup, connection, config, metadata)
                    _send_images_by_series(connection, image, "processed raw output")
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                # When this criteria is met, run process_group() on the accumulated
                # data, which returns images that are sent back to the client.
                # e.g. when the series number changes:
                if item.image_series_index != currentSeries:
                    logging.info("Processing a group of images because series index changed to %d", item.image_series_index)
                    currentSeries = item.image_series_index
                    image = process_image(imgGroup, connection, config, metadata)
                    _send_images_by_series(connection, image, "processed image output")
                    imgGroup = []

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                else:
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta["Keep_image_geometry"] = 1
                    item.attribute_string = tmpMeta.serialize()

                    connection.send_image(item)
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

        if len(imgGroup) > 0:
            logging.info("Processing a group of images (untriggered)")
            image = process_image(imgGroup, connection, config, metadata)
            _send_images_by_series(connection, image, "processed image output after input drain")
            imgGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()

# from https://github.com/benoitberanger/openrecon-template/blob/main/app/i2i-save-original-images.py
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
        logging.warning("%s slice positions are not increasing along slice_dir", label)
    if duplicate_positions > 0:
        logging.warning("%s has %d duplicate projected slice position(s)", label, duplicate_positions)
    if min_slice_dir_alignment < 0.99:
        logging.warning("%s has slice_dir vectors not aligned with the inferred slice axis", label)

    return slice_axis, records


def _log_slice_sort_mapping(sort_indices):
    if sort_indices == list(range(len(sort_indices))):
        logging.info("SCT input slice order already matches physical slice order")
        return

    logging.warning(
        "Reordering SCT input slices by physical position: first mappings %s",
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:24])
        ),
    )
    if len(sort_indices) > 24:
        logging.warning("Reordering mapping omitted %d additional slice(s)", len(sort_indices) - 24)


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
            "SCT output will still reuse original per-slice MRD positions"
        )


def _clone_mrd_image(image):
    image_copy = ismrmrd.Image.from_array(
        np.array(image.data, copy=True),
        transpose=False,
    )
    image_copy.setHead(image.getHead())
    image_copy.attribute_string = image.attribute_string
    return image_copy


def _send_images_by_series(connection, images, context):
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

    def flush_batch():
        nonlocal batch, batch_series
        if not batch:
            return
        for chunk_start in range(0, len(batch), OPENRECON_SEND_IMAGE_CHUNK_SIZE):
            chunk = batch[chunk_start:chunk_start + OPENRECON_SEND_IMAGE_CHUNK_SIZE]
            logging.info(
                "Sending %s batch: series_index=%s chunk=%d-%d/%d image_count=%d",
                context,
                batch_series,
                chunk_start + 1,
                chunk_start + len(chunk),
                len(batch),
                len(chunk),
            )
            connection.send_image(chunk)
        batch = []
        batch_series = None

    for image in images:
        series_index = int(getattr(image, "image_series_index", 0))
        if batch and series_index != batch_series:
            flush_batch()
        batch.append(image)
        batch_series = series_index

    flush_batch()


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
    try:
        meta_keys = list(meta_obj.keys())
    except Exception:
        return meta_obj

    for key in meta_keys:
        key_text = str(key)
        remove_key = key_text in SOURCE_PARENT_REFERENCE_META_KEYS
        remove_key = remove_key or any(
            key_text == prefix or key_text.startswith(f"{prefix}.")
            for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES
        )
        remove_key = remove_key or (
            "Referenced" in key_text
            and any(token in key_text for token in ("SOP", "Series", "Frame"))
            and any(token in key_text for token in ("UID", "Number"))
        )
        if remove_key:
            try:
                del meta_obj[key]
            except Exception:
                logging.warning("Could not remove source parent metadata key %s", key_text)

    return meta_obj


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


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    if not minihead_text or value is None:
        return minihead_text, False

    value = int(value)
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})'
    )
    match = pattern.search(minihead_text)
    if match:
        current_value = (match.group(2) or "").strip()
        if current_value == str(value):
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _patch_ice_minihead(
    minihead_text,
    sequence_description,
    series_grouping,
    series_instance_uid,
    source_type_token,
    target_type_token,
    target_display_token=None,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    target_display_token = target_display_token or target_type_token

    for param_name, param_value in (
        ("SequenceDescription", sequence_description),
        ("ProtocolName", sequence_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_instance_uid),
        ("ImageType", f"DERIVED\\PRIMARY\\M\\{target_type_token}"),
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


def _set_meta_scalar(meta_obj, name, value):
    meta_obj[name] = str(int(value))


def _build_derived_series_instance_uid(
    source_series_instance_uid,
    analysis,
    output_series_index,
    series_grouping,
    series_description,
):
    stable_source_uid = _first_non_empty_text(source_series_instance_uid)
    if stable_source_uid:
        seed_text = json.dumps(
            {
                "source_series_instance_uid": stable_source_uid,
                "analysis": _first_non_empty_text(analysis),
                "output_series_index": int(output_series_index) if output_series_index is not None else None,
                "series_grouping": _first_non_empty_text(series_grouping),
                "series_description": _first_non_empty_text(series_description),
            },
            sort_keys=True,
        )
        derived_uuid = uuid.uuid5(uuid.NAMESPACE_OID, seed_text)
    else:
        derived_uuid = uuid.uuid4()

    return f"2.25.{derived_uuid.int}"


def _supported_analysis_ids():
    return sorted(set(SCT_ANALYSIS_REGISTRY) | set(SCT_ANALYSIS_BUNDLES))


def _resolve_requested_analyses(analysis):
    if analysis in SCT_ANALYSIS_BUNDLES:
        return SCT_ANALYSIS_BUNDLES[analysis]
    if analysis in SCT_ANALYSIS_REGISTRY:
        return (analysis,)
    supported = ", ".join(_supported_analysis_ids())
    raise ValueError(f"Unsupported SCT analysis '{analysis}'. Supported analyses: {supported}")


def _build_sct_output_identity(source_meta, analysis, output_series_index):
    source_minihead = _decode_ice_minihead(source_meta)
    source_series = _first_non_empty_text(
        _get_meta_text(source_meta, "SeriesDescription"),
        _get_meta_text(source_meta, "SequenceDescription"),
        _extract_minihead_string_value(source_minihead, "SequenceDescription"),
    )
    source_grouping = _first_non_empty_text(
        _get_meta_text(source_meta, "SeriesNumberRangeNameUID"),
        _extract_minihead_string_value(source_minihead, "SeriesNumberRangeNameUID"),
        source_series,
    )
    source_series_instance_uid = _first_non_empty_text(
        _get_meta_text(source_meta, "SeriesInstanceUID"),
        _extract_minihead_string_value(source_minihead, "SeriesInstanceUID"),
    )
    source_type_token = _first_non_empty_text(
        _get_meta_text(source_meta, "ImageTypeValue4"),
        _extract_minihead_array_tokens(source_minihead, "ImageTypeValue4"),
    )
    suffix = SCT_ANALYSIS_REGISTRY[analysis]["series_suffix"]
    series_description = f"{source_series}_{suffix}" if source_series else suffix
    grouping = f"{source_grouping}_{suffix}" if source_grouping else series_description
    type_token = suffix.upper()
    return {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "series_instance_uid": _build_derived_series_instance_uid(
            source_series_instance_uid=source_series_instance_uid,
            analysis=analysis,
            output_series_index=output_series_index,
            series_grouping=grouping,
            series_description=series_description,
        ),
        "source_type_token": source_type_token,
        "type_token": type_token,
        "display_token": suffix,
        "image_comment": suffix,
    }


def _run_command(command, cwd):
    logging.info("Running command: %s", " ".join(str(part) for part in command))
    result = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
    if result.stdout and result.stdout.strip():
        logging.info("SCT stdout:\n%s", result.stdout.rstrip())
    if result.stderr and result.stderr.strip():
        logging.info("SCT stderr:\n%s", result.stderr.rstrip())
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )


def _run_sct_analysis(analysis, input_path, work_dir, precomputed_outputs=None):
    if analysis not in SCT_ANALYSIS_REGISTRY:
        supported = ", ".join(_supported_analysis_ids())
        raise ValueError(f"Unsupported SCT analysis '{analysis}'. Supported analyses: {supported}")
    if precomputed_outputs is None:
        precomputed_outputs = {}

    analysis_config = SCT_ANALYSIS_REGISTRY[analysis]
    qc_dir = work_dir / "qc_singleSubj"
    output_path = work_dir / "output.nii.gz"

    if analysis_config["kind"] == "deepseg":
        _run_command(
            [
                "sct_deepseg",
                analysis_config["task"],
                "-i",
                str(input_path),
                "-o",
                str(output_path),
                "-qc",
                str(qc_dir),
            ],
            cwd=work_dir,
        )
        return output_path

    if analysis_config["kind"] == "deepseg_gm":
        _run_command(
            [
                "sct_deepseg_gm",
                "-i",
                str(input_path),
                "-o",
                str(output_path),
                "-qc",
                str(qc_dir),
            ],
            cwd=work_dir,
        )
        return output_path

    if analysis_config["kind"] == "label_vertebrae":
        seg_path = precomputed_outputs.get("sct_deepseg_spinalcord")
        if seg_path is None:
            seg_path = work_dir / "input_seg.nii.gz"
            _run_command(
                [
                    "sct_deepseg",
                    "spinalcord",
                    "-i",
                    str(input_path),
                    "-o",
                    str(seg_path),
                    "-qc",
                    str(qc_dir),
                ],
                cwd=work_dir,
            )
        _run_command(
            [
                "sct_label_vertebrae",
                "-i",
                str(input_path),
                "-s",
                str(seg_path),
                "-c",
                "t2",
                "-ofolder",
                str(work_dir),
                "-qc",
                str(qc_dir),
            ],
            cwd=work_dir,
        )
        labeled_path = work_dir / "input_seg_labeled.nii.gz"
        if not labeled_path.exists():
            raise FileNotFoundError(f"Could not find SCT vertebral labeling output: {labeled_path}")
        return labeled_path

    raise ValueError(f"Unsupported SCT analysis kind: {analysis_config['kind']}")


def _sct_output_to_mrd_images(
    output_path,
    analysis,
    head,
    meta,
    output_series_index,
    segmentation_colormap=False,
):
    output_identity = _build_sct_output_identity(meta[0], analysis, output_series_index)
    img = nib.load(str(output_path))
    data = img.get_fdata(dtype=np.float32)
    logging.info("Loaded SCT output %s with shape=%s", output_path, data.shape)

    if data.ndim == 2:
        data = data[:, :, None]
    if data.ndim != 3:
        raise ValueError(f"SCT output must be 3D after squeezing, got shape {data.shape}")
    if data.shape[-1] != len(head):
        raise ValueError(
            "SCT output slice count does not match MRD input: "
            f"output_z={data.shape[-1]} input_images={len(head)}"
        )

    data = np.nan_to_num(data, nan=0.0, posinf=0.0, neginf=0.0)
    unique_values = np.unique(data)
    if unique_values.size <= 16:
        unique_summary = ", ".join(f"{float(value):.6g}" for value in unique_values)
    else:
        unique_summary = f"{unique_values.size} unique values"
    logging.info(
        "SCT output voxel statistics before MRD conversion: min=%.6g max=%.6g "
        "nonzero=%d/%d unique=%s",
        float(np.min(data)) if data.size else 0.0,
        float(np.max(data)) if data.size else 0.0,
        int(np.count_nonzero(data)),
        int(data.size),
        unique_summary,
    )
    if np.min(data) < 0:
        logging.warning("Negative values detected in SCT output; clipping to zero.")
        data = np.clip(data, 0, None)

    maxVal = float(np.max(data)) if data.size else 0.0
    if maxVal <= 1.0 and maxVal > 0.0:
        data = data * 1000.0
        maxVal = float(np.max(data))

    if data.dtype != np.int16:
        logging.info("Converting SCT output from %s to int16", data.dtype)
        data = np.rint(data).astype(np.int16)

    data = data[:, :, :, None, None]
    data = data.transpose((0, 1, 4, 3, 2))

    imagesOut = [None] * data.shape[-1]
    for iImg in range(data.shape[-1]):
        imagesOut[iImg] = ismrmrd.Image.from_array(
            data[..., iImg].transpose((3, 2, 0, 1)),
            transpose=False,
        )

        oldHeader = copy.deepcopy(head[iImg])
        oldHeader.data_type = imagesOut[iImg].data_type
        if (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXFLOAT) or (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXDOUBLE):
            oldHeader.image_type = ismrmrd.IMTYPE_COMPLEX
        else:
            oldHeader.image_type = ismrmrd.IMTYPE_MAGNITUDE
        oldHeader.image_series_index = output_series_index
        oldHeader.image_index = iImg + 1
        oldHeader.slice = iImg
        imagesOut[iImg].setHead(oldHeader)

        tmpMeta = _copy_meta(meta[iImg])
        _strip_source_parent_refs(tmpMeta)
        tmpMeta["DataRole"] = "Image"
        tmpMeta["ImageProcessingHistory"] = ["PYTHON", "SPINALCORDTOOLBOX"]
        tmpMeta["WindowCenter"] = str((maxVal + 1) / 2)
        tmpMeta["WindowWidth"] = str(maxVal + 1)
        tmpMeta["SeriesDescription"] = output_identity["series_description"]
        tmpMeta["SequenceDescription"] = output_identity["sequence_description"]
        tmpMeta["ProtocolName"] = output_identity["sequence_description"]
        tmpMeta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
        tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]
        tmpMeta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        tmpMeta["ImageTypeValue3"] = "M"
        tmpMeta["ImageTypeValue4"] = output_identity["display_token"]
        tmpMeta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
        tmpMeta["ImageComments"] = output_identity["image_comment"]
        tmpMeta["ImageComment"] = output_identity["image_comment"]
        if "SequenceDescriptionAdditional" in tmpMeta:
            try:
                del tmpMeta["SequenceDescriptionAdditional"]
            except Exception:
                tmpMeta["SequenceDescriptionAdditional"] = ""
        tmpMeta["Keep_image_geometry"] = 1
        _set_meta_scalar(tmpMeta, "NumberInSeries", iImg + 1)
        _set_meta_scalar(tmpMeta, "SliceNo", iImg)
        _set_meta_scalar(tmpMeta, "AnatomicalSliceNo", iImg)
        _set_meta_scalar(tmpMeta, "ChronSliceNo", iImg)
        _set_meta_scalar(tmpMeta, "ProtocolSliceNumber", iImg)
        _set_meta_scalar(tmpMeta, "IsmrmrdSliceNo", iImg)

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                output_identity["sequence_description"],
                output_identity["grouping"],
                output_identity["series_instance_uid"],
                output_identity["source_type_token"],
                output_identity["type_token"],
                target_display_token=output_identity["display_token"],
            )
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)
            else:
                logging.warning(
                    "IceMiniHead was present but not updated for SCT output slice %d",
                    iImg,
                )
            for long_param_name, long_param_value in (
                ("Actual3DImagePartNumber", iImg),
                ("AnatomicalPartitionNo", iImg),
                ("AnatomicalSliceNo", iImg),
                ("ChronSliceNo", iImg),
                ("NumberInSeries", iImg + 1),
                ("ProtocolSliceNumber", iImg),
                ("SliceNo", iImg),
            ):
                patched_minihead_text, did_change = _replace_or_append_minihead_long_param(
                    patched_minihead_text,
                    long_param_name,
                    long_param_value,
                )
                minihead_changed = minihead_changed or did_change
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)

        tmpMeta["ImageRowDir"] = [
            "{:.18f}".format(oldHeader.read_dir[0]),
            "{:.18f}".format(oldHeader.read_dir[1]),
            "{:.18f}".format(oldHeader.read_dir[2]),
        ]

        tmpMeta["ImageColumnDir"] = [
            "{:.18f}".format(oldHeader.phase_dir[0]),
            "{:.18f}".format(oldHeader.phase_dir[1]),
            "{:.18f}".format(oldHeader.phase_dir[2]),
        ]

        tmpMeta["ImageSliceNormDir"] = [
            "{:.18f}".format(oldHeader.slice_dir[0]),
            "{:.18f}".format(oldHeader.slice_dir[1]),
            "{:.18f}".format(oldHeader.slice_dir[2]),
        ]

        if segmentation_colormap:
            tmpMeta["LUTFileName"] = "MicroDeltaHotMetal.pal"

        imagesOut[iImg].attribute_string = tmpMeta.serialize()

    return imagesOut


def _openrecon_config_for_analysis(analysis, sendoriginal=False):
    return {
        "parameters": {
            "config": OPENRECON_DEFAULTS["config"],
            "sendoriginal": bool(sendoriginal),
            "segmentationcolormap": OPENRECON_DEFAULTS["segmentationcolormap"],
            "analysis": analysis,
        }
    }


def iter_openrecon_batch_processing_test_configs():
    seen_names = set()
    for case in SCT_BATCH_PROCESSING_OPENRECON_CASES:
        name = case["name"]
        if name in seen_names:
            raise ValueError(f"Duplicate SCT OpenRecon batch-processing test case name: {name}")
        seen_names.add(name)
        yield name, _openrecon_config_for_analysis(case["analysis"])

    batch_analysis_ids = {
        case["analysis"] for case in SCT_BATCH_PROCESSING_OPENRECON_CASES
    }
    for analysis in sorted(set(SCT_ANALYSIS_REGISTRY) - batch_analysis_ids):
        yield analysis, _openrecon_config_for_analysis(analysis)
    for analysis in sorted(SCT_ANALYSIS_BUNDLES):
        yield analysis, _openrecon_config_for_analysis(analysis)


def write_openrecon_batch_processing_test_configs(output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written_paths = []
    for name, config in iter_openrecon_batch_processing_test_configs():
        output_path = output_dir / f"{name}.json"
        output_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        written_paths.append(output_path)
    return written_paths


def _main(argv=None):
    parser = argparse.ArgumentParser(
        description="Spinal Cord Toolbox OpenRecon testing helpers"
    )
    parser.add_argument(
        "--write-openrecon-batch-processing-test-configs",
        type=Path,
        metavar="DIR",
        help="Write OpenRecon configs that exercise SCT batch-processing-compatible analyses.",
    )
    args = parser.parse_args(argv)

    if args.write_openrecon_batch_processing_test_configs is None:
        parser.print_help()
        return 0

    written_paths = write_openrecon_batch_processing_test_configs(
        args.write_openrecon_batch_processing_test_configs
    )
    print(f"Wrote {len(written_paths)} OpenRecon test config(s)")
    for path in written_paths:
        print(path)
    return 0


def process_image(imgGroup, connection, config, metadata):
    if len(imgGroup) == 0:
        return []

    os.makedirs(debugFolder, exist_ok=True)

    def boolean_checker(id: str, default_val: bool = False):
        option = mrdhelper.get_json_config_param(config, id, default_val, type="bool")
        if isinstance(option, str):
            return option.strip().lower() in ("1", "true", "yes", "on")
        return bool(option)

    send_original = boolean_checker(
        "sendoriginal",
        default_val=OPENRECON_DEFAULTS["sendoriginal"],
    )
    segmentation_colormap = boolean_checker(
        "segmentationcolormap",
        default_val=OPENRECON_DEFAULTS["segmentationcolormap"],
    )
    called_from_raw = traceback.extract_stack()[-2].name == "process_raw"
    original_images = []
    if send_original and not called_from_raw:
        original_images = [_clone_mrd_image(image) for image in imgGroup]

    analysis = mrdhelper.get_json_config_param(
        config,
        "analysis",
        default=OPENRECON_DEFAULTS["analysis"],
        type="str",
    )
    requested_analysis = analysis
    analyses = _resolve_requested_analyses(requested_analysis)
    logging.info(
        "SCT parameters: analysis=%s resolved_analyses=%s sendoriginal=%s segmentationcolormap=%s",
        requested_analysis,
        ",".join(analyses),
        send_original,
        segmentation_colormap,
    )

    unsorted_head = [img.getHead() for img in imgGroup]
    slice_sort_indices, slice_axis, _ = _slice_sort_indices(unsorted_head)
    _log_slice_geometry("Incoming SCT source", unsorted_head, slice_axis=slice_axis)
    _log_slice_sort_mapping(slice_sort_indices)

    ordered_images = [imgGroup[index] for index in slice_sort_indices]
    data = np.stack([img.data for img in ordered_images])
    head = [unsorted_head[index] for index in slice_sort_indices]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in ordered_images]
    source_series_indices = [
        int(getattr(image_header, "image_series_index", 0))
        for image_header in head
    ]
    output_series_index = max(source_series_indices, default=0) + 1

    _log_slice_geometry(
        "Sorted SCT source",
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

    data = data.transpose((3, 4, 0, 1, 2))
    data = np.squeeze(data)
    if data.ndim != 3:
        logging.warning(
            "OpenRecon input shape after squeeze is %s (%dD). SCT expects 3D input.",
            data.shape,
            data.ndim,
        )
    if np.iscomplexobj(data):
        logging.warning("Complex-valued input received; converting to magnitude for SCT.")
        data = np.abs(data)

    workspace_root = Path(debugFolder) / OPENRECON_WORKSPACE_ROOT
    request_work_dir = workspace_root / requested_analysis
    if request_work_dir.exists():
        shutil.rmtree(request_work_dir)
    request_work_dir.mkdir(parents=True, exist_ok=True)
    input_path = request_work_dir / "input.nii.gz"

    affine = compute_nifti_affine(head[0], voxel_size)
    logging.info("Computed SCT input NIfTI affine:\n%s", affine)

    if data.ndim == 2:
        data_nifti = np.asarray(data[:, :, None])
    elif data.ndim == 3:
        data_nifti = np.asarray(data)
    else:
        data_nifti = np.asarray(data)

    new_img = nib.nifti1.Nifti1Image(data_nifti, affine)
    new_img.header.set_xyzt_units(xyz="mm", t="sec")
    new_img.header.set_dim_info(freq=1, phase=0, slice=2)
    new_img.set_qform(affine, code=1)
    new_img.set_sform(affine, code=1)
    nib.save(new_img, str(input_path))
    logging.info(
        "Saved SCT input image to %s with shape=%s dtype=%s zooms=%s",
        input_path,
        new_img.shape,
        new_img.get_data_dtype(),
        new_img.header.get_zooms(),
    )

    imagesOut = []
    precomputed_outputs = {}
    for analysis_index, member_analysis in enumerate(analyses):
        member_work_dir = request_work_dir
        if len(analyses) > 1:
            member_work_dir = request_work_dir / member_analysis
        member_work_dir.mkdir(parents=True, exist_ok=True)
        output_path = _run_sct_analysis(
            member_analysis,
            input_path,
            member_work_dir,
            precomputed_outputs=precomputed_outputs,
        )
        precomputed_outputs[member_analysis] = output_path
        imagesOut.extend(
            _sct_output_to_mrd_images(
                output_path,
                member_analysis,
                head,
                meta,
                output_series_index + analysis_index,
                segmentation_colormap=segmentation_colormap,
            )
        )

    if send_original:
        if called_from_raw:
            logging.warning("sendoriginal is true, but input was raw data, so no original images to return.")
        else:
            logging.info("Sending original SCT source images with their source series grouping preserved")
            ordered_original_images = [
                original_images[index] for index in slice_sort_indices
            ]
            for original_image in reversed(ordered_original_images):
                tmpMeta = ismrmrd.Meta.deserialize(original_image.attribute_string)
                tmpMeta["Keep_image_geometry"] = 1
                original_image.attribute_string = tmpMeta.serialize()
                imagesOut.insert(0, original_image)

    return imagesOut


if __name__ == "__main__":
    raise SystemExit(_main())
