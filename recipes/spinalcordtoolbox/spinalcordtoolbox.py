import argparse
import base64
import copy
import csv
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
import zipfile
from pathlib import Path
from time import perf_counter
import xml.dom.minidom
import xml.etree.ElementTree as ET

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
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
    "spine",
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
    "sct_spinalcord_area": {
        "kind": "spinalcord_area",
        "series_suffix": "sct_spinalcord_area",
    },
}
SCT_ANALYSIS_REGISTRY["sct_deepseg_graymatter"]["kind"] = "deepseg_gm"

SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN = "MEAN(area)"
SCT_PROCESS_SEGMENTATION_VERT_LEVEL_COLUMN = "VertLevel"
SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX = "sct_spinalcord_area_metrics"
SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX = "sct_lesion_analysis_metrics"

SCT_ANALYSIS_OUTPUTS = {
    "sct_deepseg_gm_sc_7t_t2star": (
        {
            "filename": "output_7t_multiclass_sc.nii.gz",
            "series_suffix": "sct_deepseg_gm_sc_7t_t2star_7t_multiclass_sc",
        },
        {
            "filename": "output_7t_multiclass_gm.nii.gz",
            "series_suffix": "sct_deepseg_gm_sc_7t_t2star_7t_multiclass_gm",
        },
    ),
    "sct_deepseg_gm_wm_exvivo_t2": (
        {
            "filename": "output_gmseg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_exvivo_t2_gmseg",
        },
        {
            "filename": "output_wmseg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_exvivo_t2_wmseg",
        },
    ),
    "sct_deepseg_gm_wm_mouse_t1": (
        {
            "filename": "output_GM_seg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_mouse_t1_gm_seg",
        },
        {
            "filename": "output_WM_seg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_mouse_t1_wm_seg",
        },
    ),
    "sct_deepseg_lesion_ms_axial_t2": (
        {
            "filename": "output_sc_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_ms_axial_t2_sc_seg",
        },
        {
            "filename": "output_lesion_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_ms_axial_t2_lesion_seg",
        },
    ),
    "sct_deepseg_lesion_sci_t2": (
        {
            "filename": "output_lesion_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_sci_t2_lesion_seg",
        },
        {
            "filename": "output_sc_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_sci_t2_sc_seg",
        },
    ),
    "sct_deepseg_spine": (
        {
            "filename": "output_totalspineseg_discs.nii.gz",
            "series_suffix": "sct_deepseg_spine_totalspineseg_discs",
        },
        {
            "filename": "output_totalspineseg_all.nii.gz",
            "series_suffix": "sct_deepseg_spine_totalspineseg_all",
        },
    ),
}

SCT_ANALYSIS_BUNDLES = {
    "sct_bundle_t2_anatomy": (
        "sct_deepseg_spinalcord",
        "sct_label_vertebrae",
        "sct_deepseg_sc_canal_t2",
        "sct_deepseg_spine",
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
    "sctdebugthresholdsegment": False,
    "analysis": "sct_deepseg_spinalcord",
}

OPENRECON_SEND_IMAGE_CHUNK_SIZE = 96
RESERVED_SCANNER_SERIES_INDICES = {99}
# Returned originals stay 2D. SCT segmentations are returned as source-geometry
# 2D slices after originals, without detached/explicit-volume output.
SCANNER_PARTITION_INDEX = 0
SCT_SEGMENT_POSTPROCESSING_META_KEY = "SegmentPostProcessing"
SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY = "SegmentPostProcessingChildRole"
SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY = "SegmentSourceGeometry"
SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY = "SegmentSourceImageHeader"

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
    acqGroup = []
    image_groups = {}
    passthrough_images = []
    input_images_for_series_registry = []
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
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                input_images_for_series_registry.append(item)

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    image_groups.setdefault(_get_image_series_index(item), []).append(item)
                else:
                    passthrough_images.append(item)

            # ----------------------------------------------------------
            # Waveform data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Waveform):
                waveformGroup.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        derived_series_allocator = _build_connection_series_allocator(input_images_for_series_registry)
        output_images = []
        logging.info(
            "Input stream drained before SCT processing: passthrough_images=%d processable_series=%d processable_images=%d",
            len(passthrough_images),
            len(image_groups),
            sum(len(group) for group in image_groups.values()),
        )

        if passthrough_images:
            passthrough_series_index = derived_series_allocator.allocate("PASSTHROUGH")
            output_images.extend(
                _restamp_passthrough_images(
                    passthrough_images,
                    "PASSTHROUGH",
                    passthrough_series_index,
                )
            )

        for series_index, imgGroup in sorted(image_groups.items()):
            logging.info(
                "Processing buffered SCT image series after input drain: image_series_index=%d images=%d",
                series_index,
                len(imgGroup),
            )
            image = process_image(
                imgGroup,
                connection,
                config,
                metadata,
                derived_series_allocator=derived_series_allocator,
            )
            output_images.extend(_as_image_list(image))

        if output_images:
            _log_and_validate_output_series_contract(
                output_images,
                input_images_for_series_registry,
                context="before_connection_send",
            )
            _send_images_by_series(connection, output_images, "validated SCT output")

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
    # process_image writes NIfTI data as (phase, read, slice), while MRD
    # matrix_size/field_of_view store spacing in (read, phase, slice) order.
    # Affine columns must follow the NIfTI data axes.
    affine[:3, :3] = np.column_stack(
        [
            voxel_size[1] * phase_dir,
            voxel_size[0] * read_dir,
            voxel_size[2] * slice_dir,
        ]
    )
    affine[:3, 3] = position

    return affine


def _simple_threshold_segmentation_volume(input_yxz, max_val):
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


def _bright_foreground_threshold(data):
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


def _largest_connected_component_per_plane(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim < 2:
        return _largest_connected_component_2d(mask.reshape(1, -1)).reshape(mask.shape)

    result = np.zeros(mask.shape, dtype=bool)
    planes = mask.reshape((-1,) + mask.shape[-2:])
    result_planes = result.reshape((-1,) + mask.shape[-2:])
    for index, plane in enumerate(planes):
        result_planes[index] = _largest_connected_component_2d(plane)
    return result


def _largest_connected_component_2d(mask):
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


def _header_vector(image_header, field_name):
    try:
        return np.asarray(getattr(image_header, field_name), dtype=float)
    except Exception:
        return np.zeros(3, dtype=float)


def _set_header_sequence_field(image_header, field_name, values):
    sequence = getattr(image_header, field_name)
    for index, value in enumerate(values):
        sequence[index] = value


def _meta_vector(values):
    return [f"{float(value):.18f}" for value in values]


def _explicit_header_geometry_meta(header):
    # Mirrors vesselboost: stamp explicit per-image direction + position vectors
    # into the Meta so the scanner converter/DicomWriter places every returned
    # frame unambiguously and assembles the multi-frame ("concat") series without
    # closing the parent early (the spinalcordtoolbox originals crash).
    return {
        "ImageRowDir": _meta_vector(_header_vector(header, "read_dir")),
        "ImageColumnDir": _meta_vector(_header_vector(header, "phase_dir")),
        "ImageSliceNormDir": _meta_vector(_header_vector(header, "slice_dir")),
        "SlicePosLightMarker": _meta_vector(_header_vector(header, "position")),
    }


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


def _slice_slot_key(record, use_projected_position=True):
    if use_projected_position:
        return (round(float(record["projected_position"]), 3),)
    return (int(record["slice"]),)


def _split_source_images_by_volume(image_headers, slice_axis=None):
    image_headers = list(image_headers)
    if len(image_headers) <= 1:
        return [list(range(len(image_headers)))]

    if slice_axis is None:
        slice_axis = _infer_slice_axis(image_headers)

    _, records = _build_slice_geometry_records(
        image_headers,
        input_indices=list(range(len(image_headers))),
        slice_axis=slice_axis,
    )
    projected_slot_keys = [
        _slice_slot_key(record, use_projected_position=True)
        for record in records
    ]
    use_projected_position = len(set(projected_slot_keys)) > 1
    if use_projected_position:
        slot_keys = projected_slot_keys
    else:
        slot_keys = [
            _slice_slot_key(record, use_projected_position=False)
            for record in records
        ]
    if len(set(slot_keys)) == len(slot_keys):
        return [list(range(len(image_headers)))]

    volumes = []
    volume_slot_keys = []
    for input_index, slot_key in enumerate(slot_keys):
        target_volume_index = None
        for volume_index, seen_slot_keys in enumerate(volume_slot_keys):
            if slot_key not in seen_slot_keys:
                target_volume_index = volume_index
                break

        if target_volume_index is None:
            target_volume_index = len(volumes)
            volumes.append([])
            volume_slot_keys.append(set())

        volumes[target_volume_index].append(input_index)
        volume_slot_keys[target_volume_index].add(slot_key)

    return volumes


def _build_sct_input_volumes(imgGroup):
    unsorted_head = [img.getHead() for img in imgGroup]
    slice_axis = _infer_slice_axis(unsorted_head)
    _log_slice_geometry("Incoming SCT source", unsorted_head, slice_axis=slice_axis)

    volume_input_groups = _split_source_images_by_volume(
        unsorted_head,
        slice_axis=slice_axis,
    )
    if len(volume_input_groups) > 1:
        logging.info(
            "Detected %d SCT input volume(s) from repeated slice geometry: sizes=%s",
            len(volume_input_groups),
            ",".join(str(len(group)) for group in volume_input_groups),
        )

    volumes = []
    include_label_suffix = len(volume_input_groups) > 1
    for volume_index, input_indices in enumerate(volume_input_groups, start=1):
        volume_images = [imgGroup[input_index] for input_index in input_indices]
        volume_unsorted_head = [unsorted_head[input_index] for input_index in input_indices]
        local_sort_indices, volume_slice_axis, _ = _slice_sort_indices(volume_unsorted_head)
        _log_slice_sort_mapping(local_sort_indices)
        ordered_input_indices = [
            input_indices[local_index]
            for local_index in local_sort_indices
        ]
        ordered_images = [imgGroup[input_index] for input_index in ordered_input_indices]
        ordered_head = [unsorted_head[input_index] for input_index in ordered_input_indices]
        ordered_meta = [
            ismrmrd.Meta.deserialize(img.attribute_string)
            for img in ordered_images
        ]
        volume_label = f"volume {volume_index}/{len(volume_input_groups)}"
        _log_slice_geometry(
            f"Sorted SCT source {volume_label}",
            ordered_head,
            input_indices=ordered_input_indices,
            slice_axis=volume_slice_axis,
        )
        volumes.append(
            {
                "index": volume_index,
                "count": len(volume_input_groups),
                "label": volume_label,
                "directory_name": f"volume_{volume_index:03d}",
                "series_label_suffix": (
                    f"vol{volume_index:03d}"
                    if include_label_suffix
                    else ""
                ),
                "input_indices": ordered_input_indices,
                "images": ordered_images,
                "head": ordered_head,
                "meta": ordered_meta,
                "slice_axis": volume_slice_axis,
            }
        )

    return volumes


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
    image_copy.setHead(copy.deepcopy(image.getHead()))
    image_copy.attribute_string = image.attribute_string
    return image_copy


def _as_image_list(images):
    if images is None:
        return []
    if isinstance(images, ismrmrd.Image):
        return [images]
    return list(images)


def _get_image_series_index(image):
    try:
        return int(getattr(image.getHead(), "image_series_index", 0))
    except Exception:
        logging.warning(
            "Could not read image_series_index from header; bucketing as 0",
            exc_info=True,
        )
        return 0


def _send_images_by_series(connection, images, context):
    images = _as_image_list(images)
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
        series_index = _get_image_series_index(image)
        if batch and series_index != batch_series:
            flush_batch()
        batch.append(image)
        batch_series = series_index

    flush_batch()


def _json_log_default(value):
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _log_json_event(event_name, payload, level=logging.INFO):
    logging.log(level, "%s %s", event_name, json.dumps(payload, sort_keys=True, default=_json_log_default))


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
        key_leaf = key_text.rsplit(".", 1)[-1]
        remove_key = key_text in SOURCE_PARENT_REFERENCE_META_KEYS
        remove_key = remove_key or key_leaf in SOURCE_PARENT_REFERENCE_META_KEYS
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


def _strip_scanner_write_unsafe_meta(meta_obj):
    try:
        meta_keys = list(meta_obj.keys())
    except Exception:
        return meta_obj

    for key in meta_keys:
        key_text = str(key)
        key_leaf = key_text.rsplit(".", 1)[-1]
        if key_text not in SCANNER_WRITE_UNSAFE_META_KEYS and key_leaf not in SCANNER_WRITE_UNSAFE_META_KEYS:
            continue
        try:
            del meta_obj[key]
        except Exception:
            logging.warning("Could not remove scanner-unsafe metadata key %s", key_text)

    return meta_obj


class ConnectionSeriesAllocator:
    def __init__(self, observed_indices=None, reserved_indices=None):
        self.observed_indices = set(observed_indices or [])
        self.reserved_indices = set(reserved_indices or [])
        self.allocations = []

    def allocate(self, role):
        role = _first_non_empty_text(role).upper() or "DERIVED"
        allocated_values = {allocation["index"] for allocation in self.allocations}
        candidate = max(self.observed_indices | allocated_values | {0}) + 1
        while candidate in self.reserved_indices:
            candidate += 1
        allocation = {
            "role": role,
            "index": candidate,
            "ordinal": len(self.allocations) + 1,
        }
        self.allocations.append(allocation)
        _log_json_event(
            "SCT_DERIVED_SERIES_ALLOCATION",
            {
                "role": role,
                "allocated_index": candidate,
                "allocation_ordinal": allocation["ordinal"],
                "observed_indices": sorted(self.observed_indices),
                "reserved_indices": sorted(self.reserved_indices),
            },
        )
        return candidate


def _build_connection_series_allocator(images):
    observed_indices = set()
    registry = []
    for image in _as_image_list(images):
        observed_indices.add(_get_image_series_index(image))
        registry.append(_series_contract_entry(image, source="input"))

    allocator = ConnectionSeriesAllocator(
        observed_indices=observed_indices,
        reserved_indices=RESERVED_SCANNER_SERIES_INDICES,
    )
    _log_json_event(
        "SCT_INPUT_SERIES_REGISTRY",
        {
            "observed_indices": sorted(observed_indices),
            "reserved_indices": sorted(RESERVED_SCANNER_SERIES_INDICES),
            "series": registry,
        },
    )
    return allocator


def _series_contract_role(meta_obj, minihead_text):
    if _meta_int(meta_obj, SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY) == 1:
        source_header_role = _first_non_empty_text(
            _get_meta_text(meta_obj, "ImageComment"),
            _get_meta_text(meta_obj, "ImageComments"),
        )
        if source_header_role:
            return source_header_role.upper()

    image_type_value4 = _first_non_empty_text(
        _get_meta_text(meta_obj, "ImageTypeValue4"),
        _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4"),
    ).upper()
    if image_type_value4:
        return image_type_value4

    sequence_description = _first_non_empty_text(
        _get_meta_text(meta_obj, "SequenceDescription"),
        _extract_minihead_string_value(minihead_text, "SequenceDescription"),
    ).upper()
    return sequence_description or "UNKNOWN"


def _series_contract_entry(image, source="output"):
    try:
        meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
    except Exception:
        meta_obj = ismrmrd.Meta()
    minihead_text = _decode_ice_minihead(meta_obj)
    meta_uid = _get_meta_text(meta_obj, "SeriesInstanceUID")
    minihead_uid = _extract_minihead_string_value(minihead_text, "SeriesInstanceUID")
    meta_sop_uid = _get_meta_text(meta_obj, "SOPInstanceUID")
    minihead_sop_uid = _extract_minihead_string_value(minihead_text, "SOPInstanceUID")
    meta_grouping = _get_meta_text(meta_obj, "SeriesNumberRangeNameUID")
    minihead_grouping = _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID")
    meta_protocol_name = _get_meta_text(meta_obj, "ProtocolName")
    minihead_protocol_name = _extract_minihead_string_value(minihead_text, "ProtocolName")
    return {
        "source": source,
        "role": _series_contract_role(meta_obj, minihead_text),
        "image_series_index": _get_image_series_index(image),
        "series_instance_uid": _first_non_empty_text(meta_uid, minihead_uid),
        "meta_series_instance_uid": meta_uid or "N/A",
        "minihead_series_instance_uid": minihead_uid or "N/A",
        "sop_instance_uid": _first_non_empty_text(meta_sop_uid, minihead_sop_uid),
        "meta_sop_instance_uid": meta_sop_uid or "N/A",
        "minihead_sop_instance_uid": minihead_sop_uid or "N/A",
        "series_grouping": _first_non_empty_text(meta_grouping, minihead_grouping) or "N/A",
        "meta_series_grouping": meta_grouping or "N/A",
        "minihead_series_grouping": minihead_grouping or "N/A",
        "meta_protocol_name": meta_protocol_name or "N/A",
        "minihead_protocol_name": minihead_protocol_name or "N/A",
        "sequence_description": _first_non_empty_text(
            _get_meta_text(meta_obj, "SequenceDescription"),
            _extract_minihead_string_value(minihead_text, "SequenceDescription"),
        ) or "N/A",
        "protocol_name": _first_non_empty_text(meta_protocol_name, minihead_protocol_name) or "N/A",
        "series_description": _get_meta_text(meta_obj, "SeriesDescription") or "N/A",
        "keep_image_geometry": _meta_int(meta_obj, "Keep_image_geometry"),
    }


def _series_contract_summary(images, source="output"):
    grouped = {}
    for image in _as_image_list(images):
        entry = _series_contract_entry(image, source=source)
        key = (
            entry["source"],
            entry["image_series_index"],
            entry["role"],
            entry["series_instance_uid"],
            entry["series_grouping"],
        )
        if key not in grouped:
            grouped[key] = dict(entry)
            grouped[key]["count"] = 0
            grouped[key]["sop_instance_uids"] = []
            grouped[key]["meta_sop_instance_uids"] = []
            grouped[key]["minihead_sop_instance_uids"] = []
        grouped[key]["count"] += 1
        for field_name, list_name in (
            ("sop_instance_uid", "sop_instance_uids"),
            ("meta_sop_instance_uid", "meta_sop_instance_uids"),
            ("minihead_sop_instance_uid", "minihead_sop_instance_uids"),
        ):
            value = _first_non_empty_text(entry.get(field_name))
            if value:
                grouped[key][list_name].append(value)
    return list(grouped.values())


def _sct_derived_roles():
    analysis_roles = {
        config["series_suffix"].upper()
        for config in SCT_ANALYSIS_REGISTRY.values()
    }
    output_roles = {
        output["series_suffix"].upper()
        for outputs in SCT_ANALYSIS_OUTPUTS.values()
        for output in outputs
    }
    metric_roles = {
        globals()
        .get("SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX", "sct_spinalcord_area_metrics")
        .upper(),
        globals()
        .get("SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX", "sct_lesion_analysis_metrics")
        .upper(),
    }
    return analysis_roles | output_roles | metric_roles | {"ORIGINAL", "PASSTHROUGH"}


def _log_and_validate_output_series_contract(output_images, input_images, context):
    output_summary = _series_contract_summary(output_images, source="output")
    input_summary = _series_contract_summary(input_images, source="input")
    payload = {
        "context": context,
        "input_series": input_summary,
        "output_series": output_summary,
        "reserved_indices": sorted(RESERVED_SCANNER_SERIES_INDICES),
    }
    _log_json_event("SCT_OUTPUT_SERIES_CONTRACT", payload)
    _validate_output_series_contract(output_summary, input_summary)
    _validate_output_images(output_images, input_images)


def _summary_uid_values(entry, list_name, scalar_name):
    list_values = _non_empty_values(entry.get(list_name))
    if list_values:
        return list_values
    return _non_empty_values(entry.get(scalar_name))


def _validate_output_series_contract(output_summary, input_summary):
    errors = []
    roles_by_index = {}
    derived_series_by_uid = {}
    derived_roles = _sct_derived_roles()
    input_uids = {
        entry["series_instance_uid"]
        for entry in input_summary
        if _first_non_empty_text(entry.get("series_instance_uid"))
    }
    input_sop_uids = {
        value
        for entry in input_summary
        for value in _non_empty_values(entry.get("sop_instance_uids"), entry.get("sop_instance_uid"))
    }
    input_series_indices = {
        int(entry["image_series_index"])
        for entry in input_summary
        if entry.get("image_series_index") is not None
    }
    input_has_minihead_identity = any(
        _first_non_empty_text(entry.get("minihead_series_instance_uid"))
        or _first_non_empty_text(entry.get("minihead_series_grouping"))
        or _first_non_empty_text(entry.get("minihead_protocol_name"))
        for entry in input_summary
    )

    for entry in output_summary:
        role = _first_non_empty_text(entry.get("role")).upper()
        series_index = int(entry.get("image_series_index", 0))
        uid = _first_non_empty_text(entry.get("series_instance_uid"))
        meta_uid = _first_non_empty_text(entry.get("meta_series_instance_uid"))
        minihead_uid = _first_non_empty_text(entry.get("minihead_series_instance_uid"))
        meta_sop_uids = _summary_uid_values(
            entry,
            "meta_sop_instance_uids",
            "meta_sop_instance_uid",
        )
        minihead_sop_uids = _summary_uid_values(
            entry,
            "minihead_sop_instance_uids",
            "minihead_sop_instance_uid",
        )
        meta_grouping = _first_non_empty_text(entry.get("meta_series_grouping"))
        minihead_grouping = _first_non_empty_text(entry.get("minihead_series_grouping"))
        meta_protocol = _first_non_empty_text(entry.get("meta_protocol_name"))
        minihead_protocol = _first_non_empty_text(entry.get("minihead_protocol_name"))
        keep_image_geometry = entry.get("keep_image_geometry")
        explicit_volume_output = False
        try:
            explicit_volume_output = int(keep_image_geometry) == 0
        except (TypeError, ValueError):
            explicit_volume_output = False
        require_minihead_identity = input_has_minihead_identity and not explicit_volume_output

        roles_by_index.setdefault(series_index, set()).add(role)
        if series_index in input_series_indices:
            errors.append(f"output role {role} reuses input image_series_index {series_index}")

        if role in derived_roles:
            if uid:
                derived_series_by_uid.setdefault(uid, set()).add(
                    (
                        role,
                        series_index,
                        _first_non_empty_text(entry.get("series_grouping")) or "N/A",
                    )
                )
            if series_index in RESERVED_SCANNER_SERIES_INDICES:
                errors.append(f"derived role {role} uses reserved scanner series index {series_index}")
            if uid in input_uids:
                errors.append(f"derived role {role} reuses input SeriesInstanceUID {uid}")
            if not meta_uid:
                errors.append(f"derived role {role} is missing Meta SeriesInstanceUID")
            if require_minihead_identity and not minihead_uid:
                errors.append(f"derived role {role} is missing IceMiniHead SeriesInstanceUID")
            if meta_uid and minihead_uid and meta_uid != minihead_uid:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead SeriesInstanceUID mismatch: "
                    f"{meta_uid} != {minihead_uid}"
                )
            if not meta_sop_uids:
                errors.append(f"derived role {role} is missing Meta SOPInstanceUID")
            if require_minihead_identity and not minihead_sop_uids:
                errors.append(f"derived role {role} is missing IceMiniHead SOPInstanceUID")
            if len(meta_sop_uids) != len(set(meta_sop_uids)):
                errors.append(f"derived role {role} has duplicate Meta SOPInstanceUID values")
            if len(minihead_sop_uids) != len(set(minihead_sop_uids)):
                errors.append(f"derived role {role} has duplicate IceMiniHead SOPInstanceUID values")
            if meta_sop_uids and len(meta_sop_uids) != int(entry.get("count", len(meta_sop_uids))):
                errors.append(f"derived role {role} does not have one Meta SOPInstanceUID per image")
            if (
                input_has_minihead_identity
                and minihead_sop_uids
                and len(minihead_sop_uids) != int(entry.get("count", len(minihead_sop_uids)))
            ):
                errors.append(f"derived role {role} does not have one IceMiniHead SOPInstanceUID per image")
            if meta_sop_uids and minihead_sop_uids and set(meta_sop_uids) != set(minihead_sop_uids):
                errors.append(f"derived role {role} has Meta/IceMiniHead SOPInstanceUID mismatch")
            reused_sop_uids = sorted((set(meta_sop_uids) | set(minihead_sop_uids)) & input_sop_uids)
            if reused_sop_uids:
                errors.append(f"derived role {role} reuses input SOPInstanceUID(s): {reused_sop_uids}")
            if not meta_grouping:
                errors.append(f"derived role {role} is missing Meta SeriesNumberRangeNameUID")
            if require_minihead_identity and not minihead_grouping:
                errors.append(f"derived role {role} is missing IceMiniHead SeriesNumberRangeNameUID")
            if meta_grouping and minihead_grouping and meta_grouping != minihead_grouping:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead SeriesNumberRangeNameUID mismatch: "
                    f"{meta_grouping} != {minihead_grouping}"
                )
            if not meta_protocol:
                errors.append(f"derived role {role} is missing Meta ProtocolName")
            if require_minihead_identity and not minihead_protocol:
                errors.append(f"derived role {role} is missing IceMiniHead ProtocolName")
            if meta_protocol and minihead_protocol and meta_protocol != minihead_protocol:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead ProtocolName mismatch: "
                    f"{meta_protocol} != {minihead_protocol}"
                )

    for series_index, roles in sorted(roles_by_index.items()):
        if len(roles) > 1 and any(role in derived_roles for role in roles):
            errors.append(
                f"image_series_index {series_index} is shared by multiple roles: {sorted(roles)}"
            )

    for uid, derived_series in sorted(derived_series_by_uid.items()):
        if len(derived_series) > 1:
            errors.append(
                f"derived SeriesInstanceUID {uid} is shared by multiple derived series: "
                f"{sorted(derived_series)}"
            )

    if errors:
        raise ValueError("Invalid SCT output series contract before send: " + "; ".join(errors))


def _validate_output_images(output_images, input_images):
    errors = []
    seen_image_keys = {}
    seen_storage_keys = {}
    seen_sop_uids = {}
    source_image_count = len(input_images)
    source_slice_count = source_image_count
    input_identity = _identity_values(input_images)
    input_series_indices = {
        _get_image_series_index(image)
        for image in _as_image_list(input_images)
    }
    input_has_minihead = any(_image_minihead(image) for image in input_images)
    series_identity = {}
    series_by_uid = {}
    previous_series_index = None

    for index, image in enumerate(_as_image_list(output_images)):
        header = image.getHead()
        series_index = _get_image_series_index(image)
        if previous_series_index is not None and series_index < previous_series_index:
            errors.append(
                f"image {index} has image_series_index {series_index} after "
                f"{previous_series_index}; output series must be emitted in "
                "ascending allocation order"
            )
        previous_series_index = series_index
        image_key = (
            series_index,
            int(header.slice),
            int(header.image_index),
        )
        if image_key in seen_image_keys:
            errors.append(
                f"image {index} duplicates output image key {image_key} "
                f"from image {seen_image_keys[image_key]}"
            )
        else:
            seen_image_keys[image_key] = index

        if series_index in input_series_indices:
            errors.append(f"output image {index} reuses input image_series_index {series_index}")

        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
        is_original_output = _series_contract_role(meta, minihead) == "ORIGINAL"
        if (
            input_has_minihead
            and not minihead
            and keep_image_geometry != 0
        ):
            errors.append(f"image {index} is missing IceMiniHead")

        identity = {
            "series_description": _get_meta_text(meta, "SeriesDescription"),
            "sequence_description": _get_meta_text(meta, "SequenceDescription"),
            "protocol_name": _get_meta_text(meta, "ProtocolName"),
            "series_grouping": _get_meta_text(meta, "SeriesNumberRangeNameUID"),
            "series_uid": _get_meta_text(meta, "SeriesInstanceUID"),
            "sop_uid": _get_meta_text(meta, "SOPInstanceUID"),
            "minihead_sequence_description": _extract_minihead_string_value(
                minihead, "SequenceDescription"
            ),
            "minihead_protocol_name": _extract_minihead_string_value(minihead, "ProtocolName"),
            "minihead_series_grouping": _extract_minihead_string_value(
                minihead, "SeriesNumberRangeNameUID"
            ),
            "minihead_series_uid": _extract_minihead_string_value(minihead, "SeriesInstanceUID"),
            "minihead_sop_uid": _extract_minihead_string_value(minihead, "SOPInstanceUID"),
        }
        _validate_identity_fields(
            index,
            identity,
            input_identity,
            errors,
            allow_reused_descriptors=is_original_output,
        )
        _validate_storage_fields(
            index,
            image,
            meta,
            minihead,
            input_identity,
            seen_storage_keys,
            seen_sop_uids,
            _series_slice_limit(series_index, source_slice_count, source_image_count),
            errors,
        )

        comparable_identity = (
            identity["sequence_description"],
            identity["protocol_name"],
            identity["series_grouping"],
            identity["series_uid"],
        )
        previous_identity = series_identity.setdefault(series_index, comparable_identity)
        if previous_identity != comparable_identity:
            errors.append(
                f"image {index} in image_series_index {series_index} has "
                f"inconsistent identity values: {comparable_identity} != "
                f"{previous_identity}"
            )
        series_uid = identity["series_uid"]
        if series_uid:
            previous_series = series_by_uid.setdefault(series_uid, series_index)
            if previous_series != series_index:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by "
                    f"image_series_index {previous_series} and {series_index}"
                )

    if errors:
        raise ValueError(
            "Invalid SCT output series contract before send: " + "; ".join(errors)
        )


def _validate_identity_fields(
    index,
    identity,
    input_identity,
    errors,
    allow_reused_descriptors=False,
):
    required = (
        "series_description",
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "sop_uid",
    )
    for key in required:
        if not identity[key]:
            errors.append(f"image {index} is missing Meta {key}")

    for meta_key, minihead_key in (
        ("sequence_description", "minihead_sequence_description"),
        ("protocol_name", "minihead_protocol_name"),
        ("series_grouping", "minihead_series_grouping"),
        ("series_uid", "minihead_series_uid"),
        ("sop_uid", "minihead_sop_uid"),
    ):
        meta_value = identity[meta_key]
        minihead_value = identity[minihead_key]
        if minihead_value and meta_value != minihead_value:
            errors.append(
                f"image {index} has Meta/IceMiniHead {meta_key} mismatch: "
                f"{meta_value} != {minihead_value}"
            )

    for key in (
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "minihead_sequence_description",
        "minihead_protocol_name",
        "minihead_series_grouping",
        "minihead_series_uid",
        "sop_uid",
        "minihead_sop_uid",
    ):
        if allow_reused_descriptors and key in (
            "sequence_description",
            "protocol_name",
            "minihead_sequence_description",
            "minihead_protocol_name",
        ):
            continue
        value = identity[key]
        if value and value in input_identity:
            errors.append(f"image {index} reuses input identity {key}={value}")


def _validate_storage_fields(
    index,
    image,
    meta,
    minihead,
    input_identity,
    seen_storage_keys,
    seen_sop_uids,
    series_slice_limit,
    errors,
):
    header = image.getHead()
    header_slice = int(header.slice)
    header_image_index = int(header.image_index)
    header_matrix_z = int(header.matrix_size[2])
    keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
    image_type_value4_values = _get_meta_values(meta, "ImageTypeValue4")
    is_original_output = _series_contract_role(meta, minihead) == "ORIGINAL"
    is_source_image_header_output = (
        _meta_int(meta, SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY) == 1
    )

    if header_image_index < 1:
        errors.append(
            f"image {index} has image_index {header_image_index}, expected >= 1"
        )

    if (
        series_slice_limit
        and not 0 <= header_slice < series_slice_limit
    ):
        errors.append(
            f"image {index} has slice {header_slice} outside source image "
            f"bounds [0..{series_slice_limit})"
        )

    expected_position_fields = {
        "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
        "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
        "AnatomicalSliceNo": header_slice,
        "ChronSliceNo": max(header_image_index, 1) - 1,
        "NumberInSeries": int(header.image_index),
        "ProtocolSliceNumber": header_slice,
        "SliceNo": header_slice,
        "IsmrmrdSliceNo": header_slice,
    }

    for field, expected in expected_position_fields.items():
        meta_value = _meta_int(meta, field)
        if meta_value is None:
            errors.append(f"image {index} is missing Meta {field}")
        elif meta_value != expected:
            errors.append(
                f"image {index} has Meta {field}={meta_value}, expected {expected}"
            )
        if field == "IsmrmrdSliceNo":
            continue
        minihead_value = _minihead_long_value(minihead, field)
        if minihead and minihead_value is None:
            errors.append(f"image {index} is missing IceMiniHead {field}")
        elif minihead_value is not None and minihead_value != expected:
            errors.append(
                f"image {index} has IceMiniHead {field}={minihead_value}, "
                f"expected {expected}"
            )

    if keep_image_geometry is None:
        errors.append(f"image {index} is missing Meta Keep_image_geometry")
    elif keep_image_geometry == 0:
        partition_count = _meta_int(meta, "partition_count")
        if partition_count is None:
            errors.append(f"image {index} is missing Meta partition_count")
        elif partition_count < 1:
            errors.append(
                f"image {index} has Meta partition_count={partition_count}, "
                "expected at least 1"
            )

        slice_count = _meta_int(meta, "slice_count")
        if slice_count is None:
            errors.append(f"image {index} is missing Meta slice_count")
        elif header_slice >= slice_count:
            errors.append(
                f"image {index} has slice {header_slice} outside explicit "
                f"slice_count {slice_count}"
            )
        elif header_matrix_z > 1 and header_matrix_z != slice_count:
            errors.append(
                f"image {index} has matrix_size[2]={header_matrix_z}, "
                f"expected explicit slice_count {slice_count}"
            )

        number_of_slices = _meta_int(meta, "NumberOfSlices")
        if header_matrix_z > 1 and number_of_slices != slice_count:
            errors.append(
                f"image {index} has Meta NumberOfSlices={number_of_slices}, "
                f"expected {slice_count}"
            )

    sop_uid = _get_meta_text(meta, "SOPInstanceUID")
    minihead_sop_uid = _extract_minihead_string_value(minihead, "SOPInstanceUID")
    if sop_uid:
        previous_index = seen_sop_uids.setdefault(sop_uid, index)
        if previous_index != index:
            errors.append(
                f"SOPInstanceUID {sop_uid} is shared by output images "
                f"{previous_index} and {index}"
            )
    if minihead_sop_uid:
        previous_index = seen_sop_uids.setdefault(minihead_sop_uid, index)
        if previous_index != index:
            errors.append(
                f"IceMiniHead SOPInstanceUID {minihead_sop_uid} is shared by "
                f"output images {previous_index} and {index}"
            )

    minihead_image_type_value4 = _extract_minihead_array_tokens(minihead, "ImageTypeValue4")
    if not image_type_value4_values:
        errors.append(f"image {index} is missing Meta ImageTypeValue4")
    if minihead and minihead_image_type_value4 != image_type_value4_values:
        errors.append(
            f"image {index} has IceMiniHead ImageTypeValue4 "
            f"{minihead_image_type_value4}, expected {image_type_value4_values}"
        )
    if is_original_output or is_source_image_header_output:
        value3_context = "original" if is_original_output else "source-image-header"
        for source, value in (
            ("Meta", _get_meta_text(meta, "ImageTypeValue3")),
            ("IceMiniHead", _extract_minihead_string_value(minihead, "ImageTypeValue3")),
        ):
            if value and value != "M":
                errors.append(
                    f"image {index} has {value3_context} {source} "
                    f"ImageTypeValue3={value}, expected M"
                )
    else:
        meta_image_type_value3 = _get_meta_text(meta, "ImageTypeValue3")
        if meta_image_type_value3:
            errors.append(
                f"image {index} has unsafe scanner Meta ImageTypeValue3={meta_image_type_value3}"
            )
        minihead_image_type_value3 = _extract_minihead_string_value(
            minihead,
            "ImageTypeValue3",
        )
        if minihead_image_type_value3:
            errors.append(
                f"image {index} has unsafe scanner IceMiniHead ImageTypeValue3="
                f"{minihead_image_type_value3}"
            )
        minihead_image_type_value3_tokens = _extract_minihead_array_tokens(
            minihead,
            "ImageTypeValue3",
        )
        if minihead_image_type_value3_tokens:
            errors.append(
                f"image {index} has unsafe scanner IceMiniHead ImageTypeValue3 "
                f"{minihead_image_type_value3_tokens}"
            )

    storage_key = (
        _get_meta_text(meta, "SeriesInstanceUID"),
        _meta_int(meta, "SliceNo"),
        _meta_int(meta, "ChronSliceNo"),
        _meta_int(meta, "NumberInSeries"),
    )
    missing_storage_fields = [
        field
        for field, value in zip(
            ("SeriesInstanceUID", "SliceNo", "ChronSliceNo", "NumberInSeries"),
            storage_key,
        )
        if value is None
    ]
    if missing_storage_fields:
        errors.append(
            f"image {index} is missing scanner storage key field(s) "
            f"{', '.join(missing_storage_fields)}"
        )
    previous_index = seen_storage_keys.setdefault(storage_key, index)
    if previous_index != index:
        errors.append(
            f"image {index} duplicates scanner storage key {storage_key} "
            f"from image {previous_index}"
        )
    if sop_uid and sop_uid in input_identity:
        errors.append(f"image {index} reuses input storage identity SOPInstanceUID={sop_uid}")


def _series_slice_limit(_series_index, source_slice_count, _source_image_count):
    return max(int(source_slice_count), 0)


def _identity_values(images):
    values = set()
    for image in _as_image_list(images):
        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        for key in (
            "SeriesDescription",
            "SequenceDescription",
            "ProtocolName",
            "SeriesNumberRangeNameUID",
            "SeriesInstanceUID",
            "SOPInstanceUID",
        ):
            value = _get_meta_text(meta, key)
            if value:
                values.add(value)
            minihead_value = _extract_minihead_string_value(minihead, key)
            if minihead_value:
                values.add(minihead_value)
    return values


def _meta_int(meta_obj, key):
    text = _get_meta_text(meta_obj, key)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _meta_from_image(image):
    try:
        return ismrmrd.Meta.deserialize(image.attribute_string)
    except Exception:
        return ismrmrd.Meta()


def _image_minihead(image):
    return _decode_ice_minihead(_meta_from_image(image))


def _minihead_long_value(minihead_text, key):
    text = _extract_minihead_string_value(minihead_text, key)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


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


def _non_empty_values(*values):
    collected = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                text = _first_non_empty_text(item)
                if text:
                    collected.append(text)
            continue
        text = _first_non_empty_text(value)
        if text:
            collected.append(text)
    return collected


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

    def clean_helper_value(value):
        text = _first_non_empty_text(value)
        if not text:
            return ""
        text = text.strip()
        wrapped_string = re.fullmatch(r'\{\s*"([^"]*)"\s*\}', text)
        if wrapped_string:
            return wrapped_string.group(1).strip()
        wrapped_value = re.fullmatch(r'\{\s*([^{}"]*)\s*\}', text)
        if wrapped_value:
            return wrapped_value.group(1).strip()
        if text.startswith('{ "'):
            text = text[1:].lstrip()
            text = text[1:] if text.startswith('"') else text
            text = text[:-1].rstrip() if text.endswith("}") else text
            text = text[:-1] if text.endswith('"') else text
        return text.strip()

    string_match = re.search(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"([^"]*)"\s*\}}',
        minihead_text,
    )
    if string_match:
        return string_match.group(1).strip()

    long_match = re.search(
        rf'<ParamLong\."{re.escape(name)}">\s*\{{\s*([^}}]*)\s*\}}',
        minihead_text,
    )
    if long_match:
        return long_match.group(1).strip()

    try:
        return clean_helper_value(mrdhelper.extract_minihead_string_param(minihead_text, name))
    except Exception:
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
    match = pattern.search(minihead_text)
    if match:
        if match.group(2) == value:
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _remove_minihead_string_param(minihead_text, name):
    if not minihead_text:
        return minihead_text, False

    pattern = re.compile(
        rf'^\s*<ParamString\."{re.escape(name)}">\s*\{{\s*"[^"]*"\s*\}}\s*\n?',
        flags=re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _remove_minihead_array_param(minihead_text, name):
    if not minihead_text:
        return minihead_text, False

    pattern = re.compile(
        rf'^\s*<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}\s*\n?',
        flags=re.DOTALL | re.MULTILINE,
    )
    updated_text, count = pattern.subn("", minihead_text)
    return updated_text, bool(count)


def _replace_minihead_array_token(minihead_text, name, source_token, target_token):
    target_token = _sanitize_minihead_param_value(target_token)
    if not minihead_text or not target_token:
        return minihead_text, False

    block_pattern = re.compile(
        rf'(<ParamArray\."{re.escape(name)}">\s*\{{)(.*?)(^\s*\}})',
        flags=re.DOTALL | re.MULTILINE,
    )
    block_match = block_pattern.search(minihead_text)
    if not block_match:
        return minihead_text, False

    block_text = block_match.group(0)
    tokens = [token.strip() for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_text)]
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


def _replace_or_append_minihead_exam_data_role(minihead_text, exam_data_role):
    if not minihead_text or not exam_data_role:
        return minihead_text, False

    literal = str(exam_data_role).replace('"', '""')
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

    appended_param = f'\n<ParamString."ExamDataRole">\t{{ "{literal}" }}\n'
    return minihead_text.rstrip() + appended_param, True


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


def _replace_or_append_minihead_bool_param(minihead_text, name, value):
    # The Siemens IceMiniHead carries scanner-side end-of-series flags
    # (BIsSeriesEnd, ConcatenationEnd) on whichever source slice was last
    # CHRONOLOGICALLY. After SCT reorders inputs by physical position, that
    # flag lands on the wrong output frame and the host's MrDicomWriter
    # closes the parent multi-frame series early, then rejects later frames
    # with "parent frame not found" — see sct.log:5442-5453.
    if not minihead_text or value is None:
        return minihead_text, False

    bool_text = "true" if bool(value) else "false"
    pattern = re.compile(
        rf'<ParamBool\."{re.escape(name)}">\s*\{{\s*("[^"]*")?\s*\}}'
    )
    match = pattern.search(minihead_text)
    if match:
        current_value = match.group(1)
        if current_value is not None:
            current_text_value = current_value.strip('"').strip().lower()
        else:
            current_text_value = "false"
        if current_text_value == bool_text:
            return minihead_text, False
        replacement = f'<ParamBool."{name}">{{ "{bool_text}" }}'
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamBool."{name}">\t{{ "{bool_text}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _source_postprocessing_image_type_identity(source_meta, minihead_text, fallback_token):
    fallback_token = _first_non_empty_text(fallback_token) or "sct_source_image_header"
    fallback_value4 = f"{fallback_token}_source_image_header"
    image_type = (
        _get_meta_text(source_meta, "ImageType")
        or _extract_minihead_string_value(minihead_text, "ImageType")
        or f"DERIVED\\PRIMARY\\SEGMENTATION\\{fallback_value4}"
    )
    dicom_image_type = _get_meta_text(source_meta, "DicomImageType") or image_type
    image_type_value4_tokens = (
        _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4")
        or _get_meta_values(source_meta, "ImageTypeValue4")
        or [fallback_value4]
    )
    return image_type, dicom_image_type, image_type_value4_tokens


def _patch_source_image_header_ice_minihead(
    minihead_text,
    series_description,
    series_grouping,
    series_instance_uid,
    sop_instance_uid,
    image_type,
    image_type_value4_tokens,
    exam_data_role,
    slice_index,
    image_index,
    is_last_in_series=False,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    for remover in (_remove_minihead_string_param, _remove_minihead_array_param):
        current_text, did_change = remover(current_text, "ImageTypeValue3")
        changed = changed or did_change

    for param_name, param_value in (
        ("SeriesDescription", series_description),
        ("SequenceDescription", series_description),
        ("ProtocolName", series_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_instance_uid),
        ("SOPInstanceUID", sop_instance_uid),
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

    current_text, did_change = _replace_or_append_minihead_exam_data_role(
        current_text,
        exam_data_role,
    )
    changed = changed or did_change

    for long_param_name, long_param_value in (
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
            long_param_name,
            long_param_value,
        )
        changed = changed or did_change

    for bool_param_name, bool_param_value in (
        # Same end-of-series remap as _patch_ice_minihead: see comment on
        # _replace_or_append_minihead_bool_param.
        ("BIsSeriesEnd", bool(is_last_in_series)),
        ("ConcatenationEnd", bool(is_last_in_series)),
    ):
        current_text, did_change = _replace_or_append_minihead_bool_param(
            current_text,
            bool_param_name,
            bool_param_value,
        )
        changed = changed or did_change

    return current_text, changed


def _patch_ice_minihead(
    minihead_text,
    sequence_description,
    series_grouping,
    series_instance_uid,
    sop_instance_uid,
    source_type_token,
    target_type_token,
    target_display_token=None,
    output_index=0,
    output_image_index=None,
    preserve_image_type_value3=False,
    is_last_in_series=False,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    target_display_token = target_display_token or target_type_token

    if preserve_image_type_value3:
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            "ImageTypeValue3",
            "M",
        )
        changed = changed or did_change
    else:
        for remover in (_remove_minihead_string_param, _remove_minihead_array_param):
            current_text, did_change = remover(current_text, "ImageTypeValue3")
            changed = changed or did_change

    for param_name, param_value in (
        # SeriesDescription must be re-stamped here too (matches vesselboost):
        # the derived identity already lands in the Meta, but the scanner can use
        # the IceMiniHead identity when assembling the concat parent, so leaving
        # the source SeriesDescription here is a scanner-visible mismatch.
        ("SeriesDescription", sequence_description),
        ("SequenceDescription", sequence_description),
        ("ProtocolName", sequence_description),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_instance_uid),
        ("SOPInstanceUID", sop_instance_uid),
        ("ImageType", f"DERIVED\\PRIMARY\\M\\{target_type_token}"),
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

    output_slice = int(output_index)
    output_image_index = (
        int(output_image_index)
        if output_image_index is not None
        else output_slice + 1
    )
    for long_param_name, long_param_value in (
        ("Actual3DImagePartNumber", SCANNER_PARTITION_INDEX),
        ("AnatomicalPartitionNo", SCANNER_PARTITION_INDEX),
        ("AnatomicalSliceNo", output_slice),
        ("ChronSliceNo", max(output_image_index, 1) - 1),
        ("NumberInSeries", max(output_image_index, 1)),
        ("ProtocolSliceNumber", output_slice),
        ("SliceNo", output_slice),
    ):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            long_param_name,
            long_param_value,
        )
        changed = changed or did_change

    for bool_param_name, bool_param_value in (
        # End-of-series markers must reflect OUTPUT ordering, not the source's
        # chronological order. Otherwise the host's MrDicomWriter closes the
        # series after whichever output frame happens to inherit the source's
        # last-chronological-slice flag, then rejects later frames.
        ("BIsSeriesEnd", bool(is_last_in_series)),
        ("ConcatenationEnd", bool(is_last_in_series)),
    ):
        current_text, did_change = _replace_or_append_minihead_bool_param(
            current_text,
            bool_param_name,
            bool_param_value,
        )
        changed = changed or did_change

    return current_text, changed


def _set_meta_scalar(meta_obj, name, value):
    meta_obj[name] = str(int(value))


def _set_output_position_meta(meta_obj, output_index, image_index=None):
    image_index = int(image_index) if image_index is not None else int(output_index) + 1
    for key in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        _set_meta_scalar(meta_obj, key, SCANNER_PARTITION_INDEX)
    for key, value in (
        ("AnatomicalSliceNo", output_index),
        ("ChronSliceNo", max(image_index, 1) - 1),
        ("NumberInSeries", max(image_index, 1)),
        ("ProtocolSliceNumber", output_index),
        ("SliceNo", output_index),
        ("IsmrmrdSliceNo", output_index),
    ):
        _set_meta_scalar(meta_obj, key, value)


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


def _build_derived_sop_instance_uid(
    source_meta,
    analysis,
    output_series_index,
    output_image_index,
    series_instance_uid,
):
    source_minihead = _decode_ice_minihead(source_meta)
    source_sop_instance_uid = _first_non_empty_text(
        _get_meta_text(source_meta, "SOPInstanceUID"),
        _extract_minihead_string_value(source_minihead, "SOPInstanceUID"),
    )
    seed_text = json.dumps(
        {
            "series_instance_uid": _first_non_empty_text(series_instance_uid),
            "source_sop_instance_uid": source_sop_instance_uid,
            "analysis": _first_non_empty_text(analysis),
            "output_series_index": int(output_series_index) if output_series_index is not None else None,
            "output_image_index": int(output_image_index),
        },
        sort_keys=True,
    )
    derived_uuid = uuid.uuid5(uuid.NAMESPACE_OID, seed_text)
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


def _build_sct_output_identity(
    source_meta,
    analysis,
    output_series_index,
    series_suffix=None,
    series_label_suffix=None,
):
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
    suffix = series_suffix or SCT_ANALYSIS_REGISTRY[analysis]["series_suffix"]
    label_suffix = _first_non_empty_text(series_label_suffix)
    display_suffix = f"{suffix}_{label_suffix}" if label_suffix else suffix
    series_description = f"{source_series}_{display_suffix}" if source_series else display_suffix
    grouping = f"{source_grouping}_{display_suffix}" if source_grouping else series_description
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
        "image_comment": display_suffix,
    }


def _build_passthrough_output_identity(
    source_meta,
    role,
    output_series_index,
    series_label_suffix=None,
):
    role = _first_non_empty_text(role).upper() or "PASSTHROUGH"
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
    suffix = role.lower()
    label_suffix = _first_non_empty_text(series_label_suffix)
    display_suffix = f"{suffix}_{label_suffix}" if label_suffix else suffix
    series_description = f"{source_series}_{display_suffix}" if source_series else display_suffix
    grouping = f"{source_grouping}_{display_suffix}" if source_grouping else series_description
    return {
        "series_description": series_description,
        "sequence_description": series_description,
        "grouping": grouping,
        "series_instance_uid": _build_derived_series_instance_uid(
            source_series_instance_uid=source_series_instance_uid,
            analysis=role,
            output_series_index=output_series_index,
            series_grouping=grouping,
            series_description=series_description,
        ),
        "source_type_token": source_type_token,
        "type_token": role,
        "display_token": role,
        "image_comment": display_suffix,
    }


def _restamp_passthrough_images(
    images,
    role,
    output_series_index,
    series_label_suffix=None,
):
    restamped_images = []
    image_list = _as_image_list(images)
    output_count = len(image_list)
    for iImg, image in enumerate(image_list):
        output_image = _clone_mrd_image(image)
        role_is_original = _first_non_empty_text(role).upper() == "ORIGINAL"
        output_header_image_index = iImg + 1
        output_header_slice = iImg
        if role_is_original:
            output_header_image_index = _source_geometry_header_image_index(image, iImg)
            output_header_slice = _source_geometry_header_slice(image, iImg)

        oldHeader = copy.deepcopy(output_image.getHead())
        oldHeader.image_series_index = output_series_index
        oldHeader.image_index = output_header_image_index
        oldHeader.slice = output_header_slice
        oldHeader.contrast = 0
        oldHeader.image_type = ismrmrd.IMTYPE_MAGNITUDE
        output_image.setHead(oldHeader)

        source_meta = _copy_meta(ismrmrd.Meta.deserialize(image.attribute_string))
        tmpMeta = _copy_meta(source_meta)
        _strip_source_parent_refs(tmpMeta)
        output_identity = _build_passthrough_output_identity(
            tmpMeta,
            role,
            output_series_index,
            series_label_suffix=series_label_suffix,
        )
        is_original_output = output_identity["type_token"] == "ORIGINAL"
        if not is_original_output:
            _strip_scanner_write_unsafe_meta(tmpMeta)
        sop_instance_uid = _build_derived_sop_instance_uid(
            source_meta,
            role,
            output_series_index,
            iImg,
            output_identity["series_instance_uid"],
        )
        tmpMeta["DataRole"] = "Image"
        tmpMeta["ImageProcessingHistory"] = ["PYTHON", "SPINALCORDTOOLBOX", output_identity["type_token"]]
        tmpMeta["SeriesDescription"] = output_identity["series_description"]
        tmpMeta["SequenceDescription"] = output_identity["sequence_description"]
        tmpMeta["ProtocolName"] = output_identity["sequence_description"]
        tmpMeta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
        tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]
        tmpMeta["SOPInstanceUID"] = sop_instance_uid
        tmpMeta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        if is_original_output:
            tmpMeta["ImageTypeValue3"] = "M"
        tmpMeta["ImageTypeValue4"] = output_identity["display_token"]
        tmpMeta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
        tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
        tmpMeta["ImageComments"] = output_identity["image_comment"]
        tmpMeta["ImageComment"] = output_identity["image_comment"]
        tmpMeta["SequenceDescriptionAdditional"] = "or"
        tmpMeta["Keep_image_geometry"] = "1"
        _set_output_position_meta(
            tmpMeta,
            output_header_slice,
            image_index=output_header_image_index,
        )
        if is_original_output:
            # Explicit per-slice geometry is vesselboost's originals-only
            # differentiator. _restamp_passthrough_images is also the PASSTHROUGH
            # path for phase/unknown images, which vesselboost never stamps, so
            # gate this strictly to ORIGINAL outputs.
            for geom_key, geom_value in _explicit_header_geometry_meta(
                output_image.getHead()
            ).items():
                tmpMeta[geom_key] = geom_value

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                output_identity["sequence_description"],
                output_identity["grouping"],
                output_identity["series_instance_uid"],
                sop_instance_uid,
                output_identity["source_type_token"],
                output_identity["type_token"],
                target_display_token=output_identity["display_token"],
                output_index=output_header_slice,
                output_image_index=output_header_image_index,
                preserve_image_type_value3=is_original_output,
                is_last_in_series=(iImg == output_count - 1),
            )
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)

        output_image.attribute_string = tmpMeta.serialize()
        restamped_images.append(output_image)

    return restamped_images


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


def _expected_sct_output_specs(analysis, output_path):
    output_path = Path(output_path)
    outputs = SCT_ANALYSIS_OUTPUTS.get(analysis)
    if not outputs:
        return (
            {
                "path": output_path,
                "series_suffix": SCT_ANALYSIS_REGISTRY[analysis]["series_suffix"],
            },
        )

    return tuple(
        {
            "path": output_path.with_name(output["filename"]),
            "series_suffix": output["series_suffix"],
        }
        for output in outputs
    )


def _require_sct_output_specs(analysis, output_specs):
    missing = [str(output["path"]) for output in output_specs if not output["path"].exists()]
    if missing:
        raise FileNotFoundError(
            f"Could not find expected SCT output(s) for {analysis}: {', '.join(missing)}"
        )
    return output_specs


def _write_debug_threshold_sct_outputs(input_data, affine, output_specs):
    max_val = 2**12 - 1
    volume_yxz = _simple_threshold_segmentation_volume(input_data, max_val)
    for output_spec in output_specs:
        output_path = Path(output_spec["path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        debug_img = nib.nifti1.Nifti1Image(np.asarray(volume_yxz), affine)
        debug_img.header.set_xyzt_units(xyz="mm", t="sec")
        debug_img.header.set_dim_info(freq=1, phase=0, slice=2)
        debug_img.set_qform(affine, code=1)
        debug_img.set_sform(affine, code=1)
        nib.save(debug_img, str(output_path))
        logging.info(
            "Saved debug threshold SCT output to %s with shape=%s dtype=%s",
            output_path,
            debug_img.shape,
            debug_img.get_data_dtype(),
        )
    return output_specs


def _nifti_output_stem(path):
    name = Path(path).name
    if name.endswith(".nii.gz"):
        return name[:-7]
    return Path(name).stem


def _sct_label_vertebrae_output_path(seg_path, work_dir):
    return Path(work_dir) / f"{_nifti_output_stem(seg_path)}_labeled.nii.gz"


def _sct_label_vertebrae_discs_output_path(seg_path, work_dir):
    return Path(work_dir) / f"{_nifti_output_stem(seg_path)}_labeled_discs.nii.gz"


def _format_sct_metric_number(value):
    numeric_value = float(value)
    if not np.isfinite(numeric_value):
        raise ValueError(f"SCT metric value is not finite: {value}")
    return f"{numeric_value:.4f}".rstrip("0").rstrip(".")


def _read_sct_metrics_csv(csv_path):
    csv_path = Path(csv_path)
    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = [
            str(field).strip()
            for field in (reader.fieldnames or [])
            if _first_non_empty_text(field)
        ]
        rows = []
        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned_row[str(key).strip()] = "" if value is None else str(value).strip()
            if any(cleaned_row.values()):
                rows.append(cleaned_row)
    return fieldnames, rows


def _read_sct_mean_area(csv_path):
    fieldnames, rows = _read_sct_metrics_csv(csv_path)
    normalized_fieldnames = {
        field.strip(): field
        for field in fieldnames
        if _first_non_empty_text(field)
    }
    area_field = normalized_fieldnames.get(SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN)
    if area_field is None:
        raise ValueError(
            f"{csv_path} does not contain SCT column "
            f"{SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN!r}; "
            f"columns={fieldnames}"
        )

    for row in rows:
        value_text = _first_non_empty_text(row.get(area_field))
        if not value_text:
            continue
        try:
            value = float(value_text)
        except ValueError as exc:
            raise ValueError(
                f"Could not parse {SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN} "
                f"value {value_text!r} from {csv_path}"
            ) from exc
        if np.isfinite(value):
            return value

    raise ValueError(
        f"{csv_path} does not contain a finite "
        f"{SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN} value"
    )


def _sct_metric_row_value(row, fieldname):
    for key, value in row.items():
        if str(key).strip() == fieldname:
            return _first_non_empty_text(value)
    return ""


def _build_spinalcord_area_rows(fieldnames, rows):
    normalized_fieldnames = {
        field.strip(): field
        for field in fieldnames
        if _first_non_empty_text(field)
    }
    area_field = normalized_fieldnames.get(SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN)
    if area_field is None:
        raise ValueError(
            f"SCT metrics CSV does not contain column "
            f"{SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN!r}; columns={fieldnames}"
        )
    level_field = normalized_fieldnames.get(SCT_PROCESS_SEGMENTATION_VERT_LEVEL_COLUMN)
    metric_rows = []
    for row_index, row in enumerate(rows):
        area_text = _first_non_empty_text(row.get(area_field))
        if not area_text:
            continue
        try:
            area_value = float(area_text)
        except ValueError as exc:
            raise ValueError(
                f"Could not parse {SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN} "
                f"value {area_text!r} from metrics row {row_index + 1}"
            ) from exc
        if not np.isfinite(area_value):
            continue
        level_text = _first_non_empty_text(row.get(level_field)) if level_field else ""
        metric_rows.append(
            {
                "level": level_text,
                "mean_area_mm2": area_value,
                "mean_area_text": _format_sct_metric_number(area_value),
                "slice": _sct_metric_row_value(row, "Slice (I->S)"),
                "std_area": _sct_metric_row_value(row, "STD(area)"),
            }
        )
    if not metric_rows:
        raise ValueError("SCT metrics CSV does not contain finite MEAN(area) rows")
    return metric_rows


def _format_spinalcord_area_summary(metric_rows):
    if not metric_rows:
        return ""
    if len(metric_rows) == 1 and not _first_non_empty_text(metric_rows[0].get("level")):
        return (
            f"Spinal cord area {SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN}="
            f"{metric_rows[0]['mean_area_text']} mm2"
        )

    parts = []
    for row in metric_rows[:8]:
        level = _first_non_empty_text(row.get("level")) or "unknown"
        parts.append(f"{level}={row['mean_area_text']}")
    if len(metric_rows) > 8:
        parts.append(f"+{len(metric_rows) - 8} more")
    return "Spinal cord area per level: " + ", ".join(parts) + " mm2"


def _read_detected_vertebral_levels(vertfile_path):
    img = nib.load(str(vertfile_path))
    data = np.asarray(img.get_fdata(dtype=np.float32))
    values = np.unique(data[np.isfinite(data)])
    levels = []
    for value in values:
        if value <= 0:
            continue
        rounded = int(round(float(value)))
        if abs(float(value) - rounded) > 1e-3:
            continue
        if 1 <= rounded <= 30:
            levels.append(rounded)
    levels = sorted(set(levels))
    if not levels:
        raise ValueError(f"Could not detect vertebral levels in {vertfile_path}")
    return levels


def _format_vertebral_levels_for_sct(levels):
    levels = sorted({int(level) for level in levels})
    if not levels:
        raise ValueError("No vertebral levels supplied for SCT CSA computation")
    return ",".join(str(level) for level in levels)


def _run_sct_label_vertebrae(input_path, seg_path, work_dir, qc_dir):
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
    labeled_path = _sct_label_vertebrae_output_path(seg_path, work_dir)
    discs_path = _sct_label_vertebrae_discs_output_path(seg_path, work_dir)
    if not labeled_path.exists():
        raise FileNotFoundError(f"Could not find SCT vertebral labeling output: {labeled_path}")
    if not discs_path.exists():
        raise FileNotFoundError(f"Could not find SCT disc labeling output: {discs_path}")
    return {
        "labeled_path": labeled_path,
        "discs_path": discs_path,
        "levels": _read_detected_vertebral_levels(labeled_path),
    }


def _read_spinalcord_area_metrics(csv_path):
    fieldnames, rows = _read_sct_metrics_csv(csv_path)
    metric_rows = _build_spinalcord_area_rows(fieldnames, rows)
    return {
        "fieldnames": fieldnames,
        "rows": metric_rows,
    }


def _average_metric_row_area(metric_rows):
    values = [
        float(row["mean_area_mm2"])
        for row in metric_rows
        if np.isfinite(float(row["mean_area_mm2"]))
    ]
    if not values:
        raise ValueError("No finite spinal cord area rows found")
    return float(np.mean(values))


def _level_area_text(metric_rows):
    parts = []
    for row in metric_rows:
        level = _first_non_empty_text(row.get("level"))
        if not level:
            continue
        parts.append(f"{level}:{row['mean_area_text']}")
    return ";".join(parts)


def _build_spinalcord_area_metrics(mean_area_mm2, csv_path, metric_rows=None, label_info=None):
    metric_rows = list(metric_rows or [])
    if not metric_rows:
        metric_rows = [
            {
                "level": "",
                "mean_area_mm2": float(mean_area_mm2),
                "mean_area_text": _format_sct_metric_number(mean_area_mm2),
                "slice": "",
                "std_area": "",
            }
        ]
    mean_area_text = _format_sct_metric_number(mean_area_mm2)
    summary = _format_spinalcord_area_summary(metric_rows)
    levels = [
        _first_non_empty_text(row.get("level"))
        for row in metric_rows
        if _first_non_empty_text(row.get("level"))
    ]
    return {
        "name": "spinal_cord_area",
        "source": "sct_process_segmentation",
        "analysis": "sct_spinalcord_area",
        "report_kind": "spinalcord_area",
        "report_series_suffix": SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX,
        "column": SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN,
        "mean_area_mm2": float(mean_area_mm2),
        "mean_area_text": mean_area_text,
        "rows": metric_rows,
        "levels": levels,
        "level_area_text": _level_area_text(metric_rows),
        "level_count": len(levels),
        "units": "mm2",
        "summary": summary,
        "csv_path": str(csv_path),
        "vertfile_path": str(label_info["labeled_path"]) if label_info else "",
        "discfile_path": str(label_info["discs_path"]) if label_info else "",
    }


SCT_ANALYZE_LESION_VOLUME_COLUMN = "volume [mm^3]"
SCT_ANALYZE_LESION_LENGTH_COLUMN = "length [mm]"
SCT_ANALYZE_LESION_MAX_DIAMETER_COLUMN = "max_equivalent_diameter [mm]"
SCT_ANALYZE_LESION_MAX_DAMAGE_RATIO_COLUMN = "max_axial_damage_ratio []"
SCT_ANALYZE_LESION_DORSAL_BRIDGE_COLUMN = "interpolated_dorsal_bridge_width [mm]"
SCT_ANALYZE_LESION_VENTRAL_BRIDGE_COLUMN = "interpolated_ventral_bridge_width [mm]"
SCT_ANALYZE_LESION_TOTAL_BRIDGE_COLUMN = "interpolated_total_bridge_width [mm]"


def _xml_local_name(tag):
    return str(tag).rsplit("}", 1)[-1]


def _xml_descendants(element, local_name):
    return [
        child
        for child in element.iter()
        if _xml_local_name(child.tag) == local_name
    ]


def _xlsx_column_index(cell_reference):
    match = re.match(r"([A-Za-z]+)", _first_non_empty_text(cell_reference))
    if not match:
        return 0
    index = 0
    for letter in match.group(1).upper():
        index = index * 26 + (ord(letter) - ord("A") + 1)
    return max(index - 1, 0)


def _xlsx_shared_strings(xlsx_zip):
    if "xl/sharedStrings.xml" not in xlsx_zip.namelist():
        return []
    root = ET.fromstring(xlsx_zip.read("xl/sharedStrings.xml"))
    shared_strings = []
    for item in _xml_descendants(root, "si"):
        shared_strings.append("".join(text_node.text or "" for text_node in _xml_descendants(item, "t")))
    return shared_strings


def _xlsx_cell_text(cell, shared_strings):
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(text_node.text or "" for text_node in _xml_descendants(cell, "t")).strip()

    value_nodes = _xml_descendants(cell, "v")
    value_text = value_nodes[0].text if value_nodes else ""
    value_text = "" if value_text is None else str(value_text).strip()
    if cell_type == "s" and value_text:
        try:
            return shared_strings[int(value_text)].strip()
        except (IndexError, ValueError):
            return value_text
    return value_text


def _xlsx_sheet_matrix(xlsx_zip, sheet_name, shared_strings):
    root = ET.fromstring(xlsx_zip.read(sheet_name))
    matrix = []
    for row in _xml_descendants(root, "row"):
        values = []
        for cell in [child for child in row if _xml_local_name(child.tag) == "c"]:
            column_index = _xlsx_column_index(cell.attrib.get("r", ""))
            while len(values) <= column_index:
                values.append("")
            values[column_index] = _xlsx_cell_text(cell, shared_strings)
        if any(_first_non_empty_text(value) for value in values):
            matrix.append(values)
    return matrix


def _uniquify_table_headers(headers):
    result = []
    seen = {}
    for index, header in enumerate(headers):
        text = _first_non_empty_text(header) or f"Column{index + 1}"
        count = seen.get(text, 0) + 1
        seen[text] = count
        result.append(text if count == 1 else f"{text}_{count}")
    return result


def _table_rows_from_matrix(matrix):
    header_index = None
    for index, row in enumerate(matrix):
        if sum(1 for value in row if _first_non_empty_text(value)) >= 2:
            header_index = index
            break
    if header_index is None:
        return [], []

    headers = _uniquify_table_headers(matrix[header_index])
    rows = []
    for raw_row in matrix[header_index + 1:]:
        row = {}
        for index, header in enumerate(headers):
            value = raw_row[index] if index < len(raw_row) else ""
            row[header] = _first_non_empty_text(value)
        if any(row.values()):
            rows.append(row)
    return headers, rows


def _read_xlsx_table_rows(xlsx_path):
    xlsx_path = Path(xlsx_path)
    with zipfile.ZipFile(xlsx_path) as xlsx_zip:
        shared_strings = _xlsx_shared_strings(xlsx_zip)
        worksheet_names = sorted(
            name
            for name in xlsx_zip.namelist()
            if name.startswith("xl/worksheets/") and name.endswith(".xml")
        )
        for worksheet_name in worksheet_names:
            headers, rows = _table_rows_from_matrix(
                _xlsx_sheet_matrix(xlsx_zip, worksheet_name, shared_strings)
            )
            if rows:
                return headers, rows
    raise ValueError(f"Could not find tabular lesion metrics in {xlsx_path}")


def _normalize_metric_column_name(name):
    return re.sub(r"\s+", " ", _first_non_empty_text(name)).lower()


def _row_value_by_column_names(row, *column_names):
    wanted = {_normalize_metric_column_name(column_name) for column_name in column_names}
    for key, value in row.items():
        if _normalize_metric_column_name(key) in wanted:
            return _first_non_empty_text(value)
    return ""


def _metric_float_or_none(value):
    value_text = _first_non_empty_text(value)
    if not value_text:
        return None
    try:
        value_float = float(value_text)
    except ValueError:
        return None
    return value_float if np.isfinite(value_float) else None


def _format_optional_metric_number(value):
    if value is None:
        return ""
    return _format_sct_metric_number(value)


def _sct_lesion_metric_rows(table_rows):
    metric_rows = []
    for index, row in enumerate(table_rows):
        metrics = {
            key: value
            for key, value in row.items()
            if _first_non_empty_text(value)
        }
        if not metrics:
            continue
        lesion_id = _first_non_empty_text(
            _row_value_by_column_names(row, "label", "lesion", "lesion #", "id")
        ) or str(index + 1)
        volume = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_VOLUME_COLUMN)
        )
        length = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_LENGTH_COLUMN)
        )
        max_diameter = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_MAX_DIAMETER_COLUMN)
        )
        max_damage_ratio = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_MAX_DAMAGE_RATIO_COLUMN)
        )
        dorsal_bridge = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_DORSAL_BRIDGE_COLUMN)
        )
        ventral_bridge = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_VENTRAL_BRIDGE_COLUMN)
        )
        total_bridge = _metric_float_or_none(
            _row_value_by_column_names(row, SCT_ANALYZE_LESION_TOTAL_BRIDGE_COLUMN)
        )
        metric_rows.append(
            {
                "lesion_id": lesion_id,
                "metrics": metrics,
                "volume_mm3": volume,
                "volume_text": _format_optional_metric_number(volume),
                "length_mm": length,
                "length_text": _format_optional_metric_number(length),
                "max_equivalent_diameter_mm": max_diameter,
                "max_equivalent_diameter_text": _format_optional_metric_number(max_diameter),
                "max_axial_damage_ratio": max_damage_ratio,
                "max_axial_damage_ratio_text": _format_optional_metric_number(max_damage_ratio),
                "dorsal_bridge_width_mm": dorsal_bridge,
                "dorsal_bridge_width_text": _format_optional_metric_number(dorsal_bridge),
                "ventral_bridge_width_mm": ventral_bridge,
                "ventral_bridge_width_text": _format_optional_metric_number(ventral_bridge),
                "total_bridge_width_mm": total_bridge,
                "total_bridge_width_text": _format_optional_metric_number(total_bridge),
            }
        )
    if not metric_rows:
        raise ValueError("SCT lesion analysis XLSX does not contain lesion metric rows")
    return metric_rows


def _is_total_lesion_metric_row(row):
    lesion_id = _normalize_metric_column_name(row.get("lesion_id"))
    return lesion_id.startswith("total") or lesion_id in {"all", "summary", "mean"}


def _finite_values(metric_rows, key):
    values = []
    for row in metric_rows:
        value = row.get(key)
        if value is not None and np.isfinite(float(value)):
            values.append(float(value))
    return values


def _format_lesion_metrics_summary(metric_rows):
    lesion_rows = [row for row in metric_rows if not _is_total_lesion_metric_row(row)]
    lesion_count = len(lesion_rows)
    volume_values = _finite_values(lesion_rows, "volume_mm3")
    length_values = _finite_values(lesion_rows, "length_mm")
    parts = [f"lesions={lesion_count}"]
    if volume_values:
        parts.append(f"total volume={_format_sct_metric_number(sum(volume_values))} mm3")
    if length_values:
        parts.append(f"max length={_format_sct_metric_number(max(length_values))} mm")
    return "SCI lesion metrics: " + ", ".join(parts)


def _lesion_metrics_text(metric_rows):
    parts = []
    for row in metric_rows[:8]:
        row_parts = []
        if row.get("volume_text"):
            row_parts.append(f"volume={row['volume_text']}")
        if row.get("length_text"):
            row_parts.append(f"length={row['length_text']}")
        if row.get("max_axial_damage_ratio_text"):
            row_parts.append(f"max_damage={row['max_axial_damage_ratio_text']}")
        if row_parts:
            parts.append(f"{row['lesion_id']}:" + ",".join(row_parts))
    if len(metric_rows) > 8:
        parts.append(f"+{len(metric_rows) - 8} more")
    return ";".join(parts)


def _build_sct_lesion_analysis_metrics(xlsx_path, metric_rows, label_path=None):
    metric_rows = list(metric_rows)
    lesion_rows = [row for row in metric_rows if not _is_total_lesion_metric_row(row)]
    volume_values = _finite_values(lesion_rows, "volume_mm3")
    length_values = _finite_values(lesion_rows, "length_mm")
    max_diameter_values = _finite_values(lesion_rows, "max_equivalent_diameter_mm")
    max_damage_values = _finite_values(lesion_rows, "max_axial_damage_ratio")
    dorsal_bridge_values = _finite_values(lesion_rows, "dorsal_bridge_width_mm")
    ventral_bridge_values = _finite_values(lesion_rows, "ventral_bridge_width_mm")
    total_volume = sum(volume_values) if volume_values else None
    max_length = max(length_values) if length_values else None
    max_diameter = max(max_diameter_values) if max_diameter_values else None
    max_damage = max(max_damage_values) if max_damage_values else None
    min_dorsal_bridge = min(dorsal_bridge_values) if dorsal_bridge_values else None
    min_ventral_bridge = min(ventral_bridge_values) if ventral_bridge_values else None
    return {
        "name": "sci_lesion_analysis",
        "source": "sct_analyze_lesion",
        "analysis": "sct_deepseg_lesion_sci_t2",
        "report_kind": "sct_lesion_analysis",
        "report_series_suffix": SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX,
        "summary": _format_lesion_metrics_summary(metric_rows),
        "rows": metric_rows,
        "lesion_count": len(lesion_rows),
        "total_volume_mm3": total_volume,
        "total_volume_text": _format_optional_metric_number(total_volume),
        "max_length_mm": max_length,
        "max_length_text": _format_optional_metric_number(max_length),
        "max_equivalent_diameter_mm": max_diameter,
        "max_equivalent_diameter_text": _format_optional_metric_number(max_diameter),
        "max_axial_damage_ratio": max_damage,
        "max_axial_damage_ratio_text": _format_optional_metric_number(max_damage),
        "min_dorsal_bridge_width_mm": min_dorsal_bridge,
        "min_dorsal_bridge_width_text": _format_optional_metric_number(min_dorsal_bridge),
        "min_ventral_bridge_width_mm": min_ventral_bridge,
        "min_ventral_bridge_width_text": _format_optional_metric_number(min_ventral_bridge),
        "lesion_metrics_text": _lesion_metrics_text(metric_rows),
        "xlsx_path": str(xlsx_path),
        "label_path": str(label_path) if label_path else "",
    }


def _find_sct_analyze_lesion_xlsx(analysis_dir):
    analysis_dir = Path(analysis_dir)
    candidates = sorted(analysis_dir.glob("*lesion_analysis.xlsx"))
    if not candidates:
        candidates = sorted(analysis_dir.glob("*analysis*.xlsx"))
    if not candidates:
        candidates = sorted(analysis_dir.glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"Could not find sct_analyze_lesion XLSX output in {analysis_dir}")
    return candidates[0]


def _find_sct_analyze_lesion_label(analysis_dir):
    analysis_dir = Path(analysis_dir)
    candidates = sorted(analysis_dir.glob("*lesion_label.nii.gz"))
    if not candidates:
        candidates = sorted(analysis_dir.glob("*label.nii.gz"))
    return candidates[0] if candidates else None


def _read_sct_lesion_analysis_metrics(xlsx_path, label_path=None):
    fieldnames, rows = _read_xlsx_table_rows(xlsx_path)
    metric_rows = _sct_lesion_metric_rows(rows)
    metrics = _build_sct_lesion_analysis_metrics(
        xlsx_path,
        metric_rows,
        label_path=label_path,
    )
    metrics["columns"] = fieldnames
    return metrics


def _metric_summary(dicom_metrics):
    if not dicom_metrics:
        return ""
    return _first_non_empty_text(dicom_metrics.get("summary"))


def _apply_sct_dicom_metrics(meta_obj, dicom_metrics):
    summary = _metric_summary(dicom_metrics)
    if not summary:
        return

    existing_comment = _first_non_empty_text(
        meta_obj.get("ImageComments"),
        meta_obj.get("ImageComment"),
    )
    image_comment = summary
    if existing_comment and summary not in existing_comment:
        image_comment = f"{existing_comment}; {summary}"

    mean_area_text = _first_non_empty_text(dicom_metrics.get("mean_area_text"))
    if not mean_area_text:
        mean_area = dicom_metrics.get("mean_area_mm2")
        if mean_area is not None:
            mean_area_text = _format_sct_metric_number(mean_area)

    meta_obj["ImageComments"] = image_comment
    meta_obj["ImageComment"] = image_comment
    meta_obj["DerivationDescription"] = summary
    meta_obj["ContentDescription"] = summary
    meta_obj["SCTMetricName"] = _first_non_empty_text(dicom_metrics.get("name"))
    metric_source = _first_non_empty_text(dicom_metrics.get("source"))
    meta_obj["SCTMetricSource"] = metric_source

    if metric_source == "sct_analyze_lesion":
        meta_obj["SCTAnalyzeLesionCount"] = str(int(dicom_metrics.get("lesion_count") or 0))
        meta_obj["SCTAnalyzeLesionTotalVolumeMm3"] = _first_non_empty_text(
            dicom_metrics.get("total_volume_text")
        )
        meta_obj["SCTAnalyzeLesionMaxLengthMm"] = _first_non_empty_text(
            dicom_metrics.get("max_length_text")
        )
        meta_obj["SCTAnalyzeLesionMaxEquivalentDiameterMm"] = _first_non_empty_text(
            dicom_metrics.get("max_equivalent_diameter_text")
        )
        meta_obj["SCTAnalyzeLesionMaxAxialDamageRatio"] = _first_non_empty_text(
            dicom_metrics.get("max_axial_damage_ratio_text")
        )
        meta_obj["SCTAnalyzeLesionMinDorsalBridgeWidthMm"] = _first_non_empty_text(
            dicom_metrics.get("min_dorsal_bridge_width_text")
        )
        meta_obj["SCTAnalyzeLesionMinVentralBridgeWidthMm"] = _first_non_empty_text(
            dicom_metrics.get("min_ventral_bridge_width_text")
        )
        meta_obj["SCTAnalyzeLesionRows"] = _first_non_empty_text(
            dicom_metrics.get("lesion_metrics_text")
        )
        meta_obj["SCTAnalyzeLesionXlsx"] = _first_non_empty_text(
            dicom_metrics.get("xlsx_path")
        )
        meta_obj["SCTAnalyzeLesionLabel"] = _first_non_empty_text(
            dicom_metrics.get("label_path")
        )
        return

    meta_obj["SCTProcessSegmentationColumn"] = _first_non_empty_text(
        dicom_metrics.get("column")
    )
    meta_obj["SCTProcessSegmentationMeanAreaMm2"] = mean_area_text
    meta_obj["SCTProcessSegmentationMeanAreaUnits"] = _first_non_empty_text(
        dicom_metrics.get("units")
    )
    meta_obj["SCTProcessSegmentationPerLevel"] = "1" if dicom_metrics.get("levels") else "0"
    meta_obj["SCTProcessSegmentationLevelCount"] = str(
        int(dicom_metrics.get("level_count") or 0)
    )
    meta_obj["SCTProcessSegmentationLevels"] = ",".join(
        str(level) for level in dicom_metrics.get("levels", [])
    )
    meta_obj["SCTProcessSegmentationLevelAreasMm2"] = _first_non_empty_text(
        dicom_metrics.get("level_area_text")
    )
    meta_obj["SCTProcessSegmentationVertfile"] = _first_non_empty_text(
        dicom_metrics.get("vertfile_path")
    )
    meta_obj["SCTProcessSegmentationDiscfile"] = _first_non_empty_text(
        dicom_metrics.get("discfile_path")
    )


def _patch_sct_metric_minihead(minihead_text, dicom_metrics):
    summary = _metric_summary(dicom_metrics)
    if not minihead_text or not summary:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    for param_name in (
        "ImageComment",
        "ImageComments",
        "DerivationDescription",
        "ContentDescription",
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            param_name,
            summary,
        )
        changed = changed or did_change
    return current_text, changed


def _pil_text_size(draw, text, font):
    bbox = draw.textbbox((0, 0), str(text), font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _truncate_text_to_width(draw, text, font, max_width):
    text = _first_non_empty_text(text)
    if _pil_text_size(draw, text, font)[0] <= max_width:
        return text
    if max_width <= 0:
        return ""

    suffix = "..."
    low = 0
    high = len(text)
    while low < high:
        mid = (low + high + 1) // 2
        candidate = text[:mid] + suffix
        if _pil_text_size(draw, candidate, font)[0] <= max_width:
            low = mid
        else:
            high = mid - 1
    return text[:low] + suffix if low > 0 else suffix


def _orient_metrics_report_page(page_array):
    return np.rot90(np.asarray(page_array), 2).copy()


def _render_spinalcord_area_report_page(dicom_metrics, width=768, height=512):
    from PIL import Image, ImageDraw, ImageFont

    width = max(int(width), 512)
    height = max(int(height), 384)
    margin = 28
    image = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    line_height = max(14, _pil_text_size(draw, "Ag", font)[1] + 8)
    available_width = width - 2 * margin
    value_text = _first_non_empty_text(dicom_metrics.get("mean_area_text"))
    if not value_text and dicom_metrics.get("mean_area_mm2") is not None:
        value_text = _format_sct_metric_number(dicom_metrics["mean_area_mm2"])

    rows = list(dicom_metrics.get("rows") or [])
    has_per_level_rows = any(_first_non_empty_text(row.get("level")) for row in rows)
    value_label = "Mean across levels" if has_per_level_rows else "Value"

    lines = [
        ("Spinal Cord Area Metrics", 255),
        (_metric_summary(dicom_metrics), 235),
        ("", 0),
        (f"{value_label}: {value_text} mm2", 255),
        (f"Source: {_first_non_empty_text(dicom_metrics.get('source'))}", 220),
        (f"Column: {_first_non_empty_text(dicom_metrics.get('column'))}", 220),
        (f"CSV: {Path(_first_non_empty_text(dicom_metrics.get('csv_path'))).name}", 180),
    ]
    vertfile_name = Path(_first_non_empty_text(dicom_metrics.get("vertfile_path"))).name
    discfile_name = Path(_first_non_empty_text(dicom_metrics.get("discfile_path"))).name
    if vertfile_name or discfile_name:
        lines.append((f"Labels: {vertfile_name} / {discfile_name}", 180))

    y = margin
    for line, fill in lines:
        if not line:
            y += line_height
            continue
        draw.text(
            (margin, y),
            _truncate_text_to_width(draw, line, font, available_width),
            fill=fill,
            font=font,
        )
        y += line_height

    if rows:
        y += max(4, line_height // 2)
        table_columns = (
            ("Level", 0, 90),
            ("MEAN(area) mm2", 100, 145),
            ("STD(area)", 255, 110),
            ("Slice", 375, available_width - 375),
        )
        for heading, x_offset, max_col_width in table_columns:
            draw.text(
                (margin + x_offset, y),
                _truncate_text_to_width(draw, heading, font, max_col_width),
                fill=235,
                font=font,
            )
        y += line_height
        for row_index, row in enumerate(rows):
            if y + line_height > height - margin:
                remaining = len(rows) - row_index
                draw.text(
                    (margin, y),
                    f"+{remaining} more levels",
                    fill=180,
                    font=font,
                )
                break
            values = (
                _first_non_empty_text(row.get("level")) or "-",
                _first_non_empty_text(row.get("mean_area_text")),
                _first_non_empty_text(row.get("std_area")) or "-",
                _first_non_empty_text(row.get("slice")) or "-",
            )
            for (value, (_, x_offset, max_col_width)) in zip(values, table_columns):
                draw.text(
                    (margin + x_offset, y),
                    _truncate_text_to_width(draw, value, font, max_col_width),
                    fill=255,
                    font=font,
                )
            y += line_height

    page = np.asarray(image, dtype=np.uint16) * 16
    return _orient_metrics_report_page(page)


def _render_sct_lesion_analysis_report_page(dicom_metrics, width=768, height=512):
    from PIL import Image, ImageDraw, ImageFont

    width = max(int(width), 512)
    height = max(int(height), 384)
    margin = 28
    image = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    line_height = max(14, _pil_text_size(draw, "Ag", font)[1] + 8)
    available_width = width - 2 * margin
    rows = list(dicom_metrics.get("rows") or [])

    lines = [
        ("SCI Lesion Metrics", 255),
        (_metric_summary(dicom_metrics), 235),
        ("", 0),
        (
            "Total volume: "
            f"{_first_non_empty_text(dicom_metrics.get('total_volume_text')) or '-'} mm3",
            255,
        ),
        (
            "Max length: "
            f"{_first_non_empty_text(dicom_metrics.get('max_length_text')) or '-'} mm",
            255,
        ),
        (
            "Max axial damage ratio: "
            f"{_first_non_empty_text(dicom_metrics.get('max_axial_damage_ratio_text')) or '-'}",
            220,
        ),
        (
            "XLSX: "
            f"{Path(_first_non_empty_text(dicom_metrics.get('xlsx_path'))).name}",
            180,
        ),
    ]

    y = margin
    for line, fill in lines:
        if not line:
            y += line_height
            continue
        draw.text(
            (margin, y),
            _truncate_text_to_width(draw, line, font, available_width),
            fill=fill,
            font=font,
        )
        y += line_height

    if rows:
        y += max(4, line_height // 2)
        table_columns = (
            ("Lesion", 0, 70),
            ("Volume mm3", 80, 115),
            ("Length mm", 210, 100),
            ("Max damage", 325, 110),
            ("Dorsal bridge", 450, available_width - 450),
        )
        for heading, x_offset, max_col_width in table_columns:
            draw.text(
                (margin + x_offset, y),
                _truncate_text_to_width(draw, heading, font, max_col_width),
                fill=235,
                font=font,
            )
        y += line_height
        for row_index, row in enumerate(rows):
            if y + line_height > height - margin:
                draw.text(
                    (margin, y),
                    f"+{len(rows) - row_index} more lesions",
                    fill=180,
                    font=font,
                )
                break
            values = (
                _first_non_empty_text(row.get("lesion_id")) or "-",
                _first_non_empty_text(row.get("volume_text")) or "-",
                _first_non_empty_text(row.get("length_text")) or "-",
                _first_non_empty_text(row.get("max_axial_damage_ratio_text")) or "-",
                _first_non_empty_text(row.get("dorsal_bridge_width_text")) or "-",
            )
            for (value, (_, x_offset, max_col_width)) in zip(values, table_columns):
                draw.text(
                    (margin + x_offset, y),
                    _truncate_text_to_width(draw, value, font, max_col_width),
                    fill=255,
                    font=font,
                )
            y += line_height

    page = np.asarray(image, dtype=np.uint16) * 16
    return _orient_metrics_report_page(page)


def _render_sct_metrics_report_page(dicom_metrics):
    if dicom_metrics.get("report_kind") == "sct_lesion_analysis":
        return _render_sct_lesion_analysis_report_page(dicom_metrics)
    return _render_spinalcord_area_report_page(dicom_metrics)


def _metrics_report_series_suffix(dicom_metrics):
    return _first_non_empty_text(
        dicom_metrics.get("report_series_suffix"),
        SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX,
    )


def _metrics_report_analysis(dicom_metrics):
    return _first_non_empty_text(dicom_metrics.get("analysis"), "sct_spinalcord_area")


def _build_sct_metrics_report_images(
    dicom_metrics,
    source_images,
    output_series_index,
    series_label_suffix=None,
):
    if not dicom_metrics:
        return []
    source_images = _as_image_list(source_images)
    if not source_images:
        return []

    report_page = _render_sct_metrics_report_page(dicom_metrics)
    if report_page.ndim != 2:
        raise ValueError(f"SCT metrics report page must be 2D, got shape {report_page.shape}")

    page_height, page_width = report_page.shape
    report_volume = report_page[np.newaxis, :, :].astype(np.uint16, copy=False)
    report_image = ismrmrd.Image.from_array(report_volume, transpose=False)
    report_header = copy.deepcopy(source_images[0].getHead())
    report_header.data_type = report_image.data_type
    report_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    report_header.image_series_index = output_series_index
    report_header.image_index = 1
    report_header.slice = 0
    report_header.contrast = 0
    _set_header_sequence_field(report_header, "matrix_size", [page_width, page_height, 1])
    _set_header_sequence_field(
        report_header,
        "field_of_view",
        [float(page_width), float(page_height), 1.0],
    )
    _set_header_sequence_field(report_header, "position", [0.0, 0.0, 0.0])
    _set_header_sequence_field(report_header, "read_dir", [1.0, 0.0, 0.0])
    _set_header_sequence_field(report_header, "phase_dir", [0.0, 1.0, 0.0])
    _set_header_sequence_field(report_header, "slice_dir", [0.0, 0.0, 1.0])
    report_image.setHead(report_header)
    report_image.image_series_index = int(output_series_index)

    source_meta = _copy_meta(ismrmrd.Meta.deserialize(source_images[0].attribute_string))
    tmpMeta = _copy_meta(source_meta)
    _strip_source_parent_refs(tmpMeta)
    _strip_scanner_write_unsafe_meta(tmpMeta)
    if "IceMiniHead" in tmpMeta:
        del tmpMeta["IceMiniHead"]

    output_identity = _build_sct_output_identity(
        tmpMeta,
        _metrics_report_analysis(dicom_metrics),
        output_series_index,
        series_suffix=_metrics_report_series_suffix(dicom_metrics),
        series_label_suffix=series_label_suffix,
    )
    sop_instance_uid = _build_derived_sop_instance_uid(
        source_meta,
        _metrics_report_series_suffix(dicom_metrics),
        output_series_index,
        0,
        output_identity["series_instance_uid"],
    )
    tmpMeta["DataRole"] = "Image"
    tmpMeta["ImageProcessingHistory"] = [
        "PYTHON",
        "SPINALCORDTOOLBOX",
        "METRICS_REPORT",
    ]
    tmpMeta["WindowCenter"] = "2040"
    tmpMeta["WindowWidth"] = "4080"
    tmpMeta["SeriesDescription"] = output_identity["series_description"]
    tmpMeta["SequenceDescription"] = output_identity["sequence_description"]
    tmpMeta["ProtocolName"] = output_identity["sequence_description"]
    tmpMeta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
    tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]
    tmpMeta["SOPInstanceUID"] = sop_instance_uid
    tmpMeta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
    tmpMeta["ImageTypeValue4"] = output_identity["type_token"]
    tmpMeta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{output_identity['type_token']}"
    tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
    tmpMeta["ImageComments"] = output_identity["series_description"]
    tmpMeta["ImageComment"] = output_identity["series_description"]
    tmpMeta["SequenceDescriptionAdditional"] = "or"
    tmpMeta["Keep_image_geometry"] = "0"
    tmpMeta["partition_count"] = "1"
    tmpMeta["slice_count"] = "1"
    tmpMeta["NumberOfSlices"] = "1"
    tmpMeta["ImagesInAcquisition"] = "1"
    tmpMeta["SCTMetricReport"] = "1"
    _set_output_position_meta(tmpMeta, 0, image_index=1)
    for geom_key, geom_value in _explicit_header_geometry_meta(report_header).items():
        tmpMeta[geom_key] = geom_value
    _apply_sct_dicom_metrics(tmpMeta, dicom_metrics)

    report_image.attribute_string = tmpMeta.serialize()
    logging.info(
        "Created SCT metrics report image in image_series_index=%d",
        output_series_index,
    )
    return [report_image]


def _build_spinalcord_area_report_images(
    dicom_metrics,
    source_images,
    output_series_index,
    series_label_suffix=None,
):
    return _build_sct_metrics_report_images(
        dicom_metrics,
        source_images,
        output_series_index,
        series_label_suffix=series_label_suffix,
    )


def _output_spec_with_series_suffix(output_specs, series_suffix):
    for output_spec in output_specs:
        if output_spec.get("series_suffix") == series_suffix:
            return output_spec
    raise ValueError(f"Could not find SCT output spec with series_suffix={series_suffix!r}")


def _attach_sci_lesion_analysis_metrics(output_specs, work_dir, qc_dir):
    lesion_spec = _output_spec_with_series_suffix(
        output_specs,
        "sct_deepseg_lesion_sci_t2_lesion_seg",
    )
    cord_spec = _output_spec_with_series_suffix(
        output_specs,
        "sct_deepseg_lesion_sci_t2_sc_seg",
    )
    analysis_dir = Path(work_dir) / "sct_analyze_lesion"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            "sct_analyze_lesion",
            "-m",
            str(lesion_spec["path"]),
            "-s",
            str(cord_spec["path"]),
            "-ofolder",
            str(analysis_dir),
            "-qc",
            str(qc_dir),
        ],
        cwd=work_dir,
    )
    xlsx_path = _find_sct_analyze_lesion_xlsx(analysis_dir)
    label_path = _find_sct_analyze_lesion_label(analysis_dir)
    lesion_spec["dicom_metrics"] = _read_sct_lesion_analysis_metrics(
        xlsx_path,
        label_path=label_path,
    )
    logging.info(
        "Computed SCT SCI lesion metrics: %s",
        lesion_spec["dicom_metrics"]["summary"],
    )
    return output_specs


def _attach_spinalcord_area_metrics(output_specs, input_path, seg_path, work_dir, qc_dir):
    try:
        label_info = _run_sct_label_vertebrae(input_path, seg_path, work_dir, qc_dir)
        vertebral_levels = _format_vertebral_levels_for_sct(label_info["levels"])
        csv_path = Path(work_dir) / "spinalcord_area.csv"
        _run_command(
            [
                "sct_process_segmentation",
                "-i",
                str(seg_path),
                "-vert",
                vertebral_levels,
                "-discfile",
                str(label_info["discs_path"]),
                "-perlevel",
                "1",
                "-o",
                str(csv_path),
            ],
            cwd=work_dir,
        )
        metrics_result = _read_spinalcord_area_metrics(csv_path)
        mean_area = _average_metric_row_area(metrics_result["rows"])
        metrics = _build_spinalcord_area_metrics(
            mean_area,
            csv_path,
            metric_rows=metrics_result["rows"],
            label_info=label_info,
        )
    except Exception as exc:
        logging.warning(
            "SCT per-level spinal cord area metrics failed; falling back to scalar "
            "sct_process_segmentation output: %s",
            exc,
            exc_info=True,
        )
        csv_path = Path(work_dir) / "spinalcord_area_basic.csv"
        try:
            _run_command(
                [
                    "sct_process_segmentation",
                    "-i",
                    str(seg_path),
                    "-o",
                    str(csv_path),
                ],
                cwd=work_dir,
            )
            metrics_result = _read_spinalcord_area_metrics(csv_path)
            mean_area = _average_metric_row_area(metrics_result["rows"])
            metrics = _build_spinalcord_area_metrics(
                mean_area,
                csv_path,
                metric_rows=metrics_result["rows"],
            )
        except Exception as fallback_exc:
            logging.warning(
                "Skipping optional SCT spinal cord area metrics because scalar "
                "metrics fallback failed; returning segmentation without metrics/report: %s",
                fallback_exc,
                exc_info=True,
            )
            return output_specs

    logging.info("Computed SCT spinal cord area metric: %s", metrics["summary"])
    for output_spec in output_specs:
        output_spec["dicom_metrics"] = metrics
    return output_specs


def _run_sct_analysis(analysis, input_path, work_dir, precomputed_outputs=None):
    if analysis not in SCT_ANALYSIS_REGISTRY:
        supported = ", ".join(_supported_analysis_ids())
        raise ValueError(f"Unsupported SCT analysis '{analysis}'. Supported analyses: {supported}")
    if precomputed_outputs is None:
        precomputed_outputs = {}

    analysis_config = SCT_ANALYSIS_REGISTRY[analysis]
    qc_dir = work_dir / "qc_singleSubj"
    output_path = work_dir / "output.nii.gz"
    output_specs = _expected_sct_output_specs(analysis, output_path)

    if analysis_config["kind"] == "deepseg":
        command = [
            "sct_deepseg",
            analysis_config["task"],
            "-i",
            str(input_path),
            "-o",
            str(output_path),
        ]
        if analysis_config["task"] != "rootlets":
            command.extend(["-qc", str(qc_dir)])
        _run_command(command, cwd=work_dir)
        output_specs = _require_sct_output_specs(analysis, output_specs)
        if analysis == "sct_deepseg_lesion_sci_t2":
            return _attach_sci_lesion_analysis_metrics(output_specs, work_dir, qc_dir)
        return output_specs

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
        return _require_sct_output_specs(analysis, output_specs)

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
        labeled_path = _sct_label_vertebrae_output_path(seg_path, work_dir)
        if not labeled_path.exists():
            raise FileNotFoundError(f"Could not find SCT vertebral labeling output: {labeled_path}")
        return _expected_sct_output_specs(analysis, labeled_path)

    if analysis_config["kind"] == "spinalcord_area":
        seg_path = precomputed_outputs.get("sct_deepseg_spinalcord")
        if seg_path is None:
            seg_path = output_path
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
        output_specs = _require_sct_output_specs(
            analysis,
            _expected_sct_output_specs(analysis, seg_path),
        )
        return _attach_spinalcord_area_metrics(
            output_specs,
            input_path,
            seg_path,
            work_dir,
            qc_dir,
        )

    raise ValueError(f"Unsupported SCT analysis kind: {analysis_config['kind']}")


def _sct_output_to_mrd_images(
    output_path,
    analysis,
    head,
    meta,
    output_series_index,
    segmentation_colormap=False,
    source_images=None,
    series_suffix=None,
    dicom_metrics=None,
    series_label_suffix=None,
):
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

    if source_images is None:
        raise ValueError(
            "source_images are required for SCT source-geometry segmentation output"
        )
    return _sct_output_to_source_geometry_images(
        data,
        analysis,
        source_images,
        output_series_index,
        maxVal,
        segmentation_colormap=segmentation_colormap,
        series_suffix=series_suffix,
        dicom_metrics=dicom_metrics,
        series_label_suffix=series_label_suffix,
    )


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


def _sct_output_to_source_geometry_images(
    data,
    analysis,
    source_images,
    output_series_index,
    maxVal,
    segmentation_colormap=False,
    series_suffix=None,
    dicom_metrics=None,
    series_label_suffix=None,
):
    source_images = _as_image_list(source_images)
    output_slice_count = int(data.shape[-1])
    if output_slice_count != len(source_images):
        raise ValueError(
            "SCT source-geometry output slice count does not match MRD input: "
            f"output_z={output_slice_count} input_images={len(source_images)}"
        )

    source_meta = ismrmrd.Meta.deserialize(source_images[0].attribute_string)
    output_identity = _build_sct_output_identity(
        source_meta,
        analysis,
        output_series_index,
        series_suffix=series_suffix,
        series_label_suffix=series_label_suffix,
    )
    outputs = []
    source_headers = [source_image.getHead() for source_image in source_images]
    source_slice_axis = _infer_slice_axis(source_headers)
    source_slice_spacing = _estimate_slice_spacing(
        source_headers,
        slice_axis=source_slice_axis,
    )
    for iImg, source_image in enumerate(source_images):
        slice_yx = np.ascontiguousarray(data[:, :, iImg])
        output_image = ismrmrd.Image.from_array(
            slice_yx[np.newaxis, np.newaxis, :, :],
            transpose=False,
        )
        oldHeader = copy.deepcopy(source_image.getHead())
        oldHeader.data_type = output_image.data_type
        output_slice_spacing = source_slice_spacing
        if output_slice_spacing is None:
            try:
                output_slice_spacing = float(oldHeader.field_of_view[2])
            except Exception:
                output_slice_spacing = 1.0
        _set_header_sequence_field(
            oldHeader,
            "matrix_size",
            [
                int(oldHeader.matrix_size[0]),
                int(oldHeader.matrix_size[1]),
                1,
            ],
        )
        _set_header_sequence_field(
            oldHeader,
            "field_of_view",
            [
                float(oldHeader.field_of_view[0]),
                float(oldHeader.field_of_view[1]),
                float(output_slice_spacing),
            ],
        )
        if np.linalg.norm(_header_vector(oldHeader, "slice_dir")) < 1e-6:
            _set_header_sequence_field(
                oldHeader,
                "slice_dir",
                [float(value) for value in source_slice_axis],
            )
        if (output_image.data_type == ismrmrd.DATATYPE_CXFLOAT) or (output_image.data_type == ismrmrd.DATATYPE_CXDOUBLE):
            oldHeader.image_type = ismrmrd.IMTYPE_COMPLEX
        else:
            oldHeader.image_type = ismrmrd.IMTYPE_MAGNITUDE
        oldHeader.image_series_index = output_series_index
        oldHeader.image_index = _source_geometry_header_image_index(source_image, iImg)
        oldHeader.slice = _source_geometry_header_slice(source_image, iImg)
        oldHeader.contrast = 0
        output_image.setHead(oldHeader)
        output_image.image_series_index = int(output_series_index)
        output_header_slice = int(oldHeader.slice)
        output_header_image_index = int(oldHeader.image_index)

        source_meta = ismrmrd.Meta.deserialize(source_image.attribute_string)
        tmpMeta = _copy_meta(source_meta)
        _strip_source_parent_refs(tmpMeta)
        sop_instance_uid = _build_derived_sop_instance_uid(
            source_meta,
            series_suffix or analysis,
            output_series_index,
            iImg,
            output_identity["series_instance_uid"],
        )
        output_type_token = output_identity["type_token"]
        output_image_type = f"DERIVED\\PRIMARY\\SEGMENTATION\\{output_type_token}"
        output_image_type_value4 = [output_type_token]
        exam_data_role = _format_exam_data_role_sequential_number(output_series_index)
        tmpMeta["DataRole"] = "Segmentation"
        tmpMeta["ImageProcessingHistory"] = [
            "PYTHON",
            "SPINALCORDTOOLBOX",
            "SEGMENT_SOURCE_GEOMETRY",
            output_identity["type_token"],
        ]
        tmpMeta["WindowCenter"] = str((maxVal + 1) / 2)
        tmpMeta["WindowWidth"] = str(maxVal + 1)
        tmpMeta["SeriesDescription"] = output_identity["series_description"]
        tmpMeta["SequenceDescription"] = output_identity["sequence_description"]
        tmpMeta["ProtocolName"] = output_identity["sequence_description"]
        tmpMeta["SeriesNumberRangeNameUID"] = output_identity["grouping"]
        tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]
        tmpMeta["SOPInstanceUID"] = sop_instance_uid
        tmpMeta["ImageType"] = output_image_type
        tmpMeta["ImageTypeValue4"] = output_image_type_value4
        tmpMeta["DicomImageType"] = output_image_type
        tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
        tmpMeta["ImageComments"] = output_identity["series_description"]
        tmpMeta["ImageComment"] = output_identity["series_description"]
        tmpMeta["SequenceDescriptionAdditional"] = "or"
        tmpMeta["Keep_image_geometry"] = "1"
        tmpMeta[SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY] = "1"
        tmpMeta[SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY] = str(int(output_series_index))
        tmpMeta["ExamDataRole"] = exam_data_role
        _strip_scanner_write_unsafe_meta(tmpMeta)
        if SCT_SEGMENT_POSTPROCESSING_META_KEY in tmpMeta:
            del tmpMeta[SCT_SEGMENT_POSTPROCESSING_META_KEY]
        if SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY in tmpMeta:
            del tmpMeta[SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY]
        _set_output_position_meta(
            tmpMeta,
            output_header_slice,
            image_index=output_header_image_index,
        )

        if segmentation_colormap:
            tmpMeta["LUTFileName"] = "MicroDeltaHotMetal.pal"
        _apply_sct_dicom_metrics(tmpMeta, dicom_metrics)

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_source_image_header_ice_minihead(
                minihead_text,
                output_identity["sequence_description"],
                output_identity["grouping"],
                output_identity["series_instance_uid"],
                sop_instance_uid,
                output_image_type,
                output_image_type_value4,
                exam_data_role,
                output_header_slice,
                output_header_image_index,
                is_last_in_series=(iImg == len(source_images) - 1),
            )
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)
            patched_minihead_text, metric_minihead_changed = _patch_sct_metric_minihead(
                patched_minihead_text,
                dicom_metrics,
            )
            if metric_minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)

        output_image.attribute_string = tmpMeta.serialize()
        outputs.append(output_image)

    logging.info(
        "Converted SCT output into %d source-geometry segmentation image(s) "
        "for scanner send path: series_index=%d segmentationcolormap=%s",
        len(outputs),
        output_series_index,
        segmentation_colormap,
    )
    return outputs


def _openrecon_config_for_analysis(analysis, sendoriginal=False):
    return {
        "parameters": {
            "config": OPENRECON_DEFAULTS["config"],
            "sendoriginal": bool(sendoriginal),
            "segmentationcolormap": OPENRECON_DEFAULTS["segmentationcolormap"],
            "sctdebugthresholdsegment": OPENRECON_DEFAULTS["sctdebugthresholdsegment"],
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


def process_image(imgGroup, connection, config, metadata, derived_series_allocator=None):
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
    debug_threshold_segment = boolean_checker(
        "sctdebugthresholdsegment",
        default_val=OPENRECON_DEFAULTS["sctdebugthresholdsegment"],
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
        "SCT parameters: analysis=%s resolved_analyses=%s sendoriginal=%s "
        "segmentationcolormap=%s sctdebugthresholdsegment=%s",
        requested_analysis,
        ",".join(analyses),
        send_original,
        segmentation_colormap,
        debug_threshold_segment,
    )

    if derived_series_allocator is None:
        logging.warning(
            "No connection-level series allocator supplied; building fallback allocator from this image group only"
        )
        derived_series_allocator = _build_connection_series_allocator(imgGroup)

    sct_input_volumes = _build_sct_input_volumes(imgGroup)
    workspace_root = Path(debugFolder) / OPENRECON_WORKSPACE_ROOT
    request_work_dir = workspace_root / requested_analysis
    if request_work_dir.exists():
        shutil.rmtree(request_work_dir)
    request_work_dir.mkdir(parents=True, exist_ok=True)

    imagesOut = []
    if send_original and called_from_raw:
        logging.warning("sendoriginal is true, but input was raw data, so no original images to return.")

    for volume in sct_input_volumes:
        ordered_images = volume["images"]
        head = volume["head"]
        meta = volume["meta"]
        slice_axis = volume["slice_axis"]
        volume_label_suffix = volume["series_label_suffix"]

        logging.info(
            "Processing SCT %s: source_images=%d series_label_suffix=%s",
            volume["label"],
            len(ordered_images),
            volume_label_suffix or "none",
        )

        data = np.stack([img.data for img in ordered_images])
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

        volume_work_dir = request_work_dir
        if volume["count"] > 1:
            volume_work_dir = request_work_dir / volume["directory_name"]
        volume_work_dir.mkdir(parents=True, exist_ok=True)
        input_path = volume_work_dir / "input.nii.gz"

        affine = compute_nifti_affine(head[0], voxel_size)
        logging.info("Computed SCT input NIfTI affine for %s:\n%s", volume["label"], affine)

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

        if send_original and not called_from_raw:
            original_passthrough_index = derived_series_allocator.allocate("ORIGINAL")
            logging.info(
                "Preparing original SCT source images as derived original "
                "series_index=%d for %s",
                original_passthrough_index,
                volume["label"],
            )
            ordered_original_images = [
                original_images[index] for index in volume["input_indices"]
            ]
            imagesOut.extend(
                _restamp_passthrough_images(
                    ordered_original_images,
                    "ORIGINAL",
                    original_passthrough_index,
                    series_label_suffix=volume_label_suffix,
                )
            )

        precomputed_outputs = {}
        for member_analysis in analyses:
            member_work_dir = volume_work_dir
            if len(analyses) > 1:
                member_work_dir = volume_work_dir / member_analysis
            member_work_dir.mkdir(parents=True, exist_ok=True)
            if debug_threshold_segment:
                logging.warning(
                    "sctdebugthresholdsegment is enabled; skipping SCT model execution "
                    "and using a simple threshold segmentation instead."
                )
                output_specs = _expected_sct_output_specs(
                    member_analysis,
                    member_work_dir / "output.nii.gz",
                )
                output_specs = _write_debug_threshold_sct_outputs(
                    data_nifti,
                    affine,
                    output_specs,
                )
            else:
                output_specs = _run_sct_analysis(
                    member_analysis,
                    input_path,
                    member_work_dir,
                    precomputed_outputs=precomputed_outputs,
                )
            precomputed_outputs[member_analysis] = output_specs[0]["path"]
            metrics_report = None
            for output_spec in output_specs:
                output_series_index = derived_series_allocator.allocate(output_spec["series_suffix"])
                imagesOut.extend(
                    _sct_output_to_mrd_images(
                        output_spec["path"],
                        member_analysis,
                        head,
                        meta,
                        output_series_index,
                        segmentation_colormap=segmentation_colormap,
                        source_images=ordered_images,
                        series_suffix=output_spec["series_suffix"],
                        dicom_metrics=output_spec.get("dicom_metrics"),
                        series_label_suffix=volume_label_suffix,
                    )
                )
                if output_spec.get("dicom_metrics"):
                    metrics_report = output_spec["dicom_metrics"]

            if metrics_report:
                report_series_index = derived_series_allocator.allocate(
                    _metrics_report_series_suffix(metrics_report)
                )
                imagesOut.extend(
                    _build_sct_metrics_report_images(
                        metrics_report,
                        ordered_images,
                        report_series_index,
                        series_label_suffix=volume_label_suffix,
                    )
                )

    return imagesOut


if __name__ == "__main__":
    raise SystemExit(_main())
