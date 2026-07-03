"""OpenRecon module for OpenMSK/KneePipeline qDESS knee MRI analysis."""

from __future__ import annotations

import base64
import copy
import csv
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import tempfile
import traceback
import uuid

import constants
import ismrmrd
import mrdhelper
import nibabel as nib
from nibabel.processing import resample_from_to
import numpy as np


OPENMSK_VERSION = os.environ.get("OPENMSK_VERSION", "unknown")
KNEEPIPELINE_DIR = Path(os.environ.get("KNEEPIPELINE_HOME", "/opt/KneePipeline"))
KNEEPIPELINE_CONFIG = Path(
    os.environ.get("KNEEPIPELINE_CONFIG", str(KNEEPIPELINE_DIR / "config.json"))
)
NNUNET_NUMPY_COMPAT_PATH = os.environ.get("OPENMSK_NNUNET_NUMPY_COMPAT_PATH", "/opt/openmsk_compat")
OPENMSK_PIPELINE_TIMEOUT = int(os.environ.get("OPENMSK_PIPELINE_TIMEOUT", "5400"))
DEFAULT_SEG_MODEL = "acl_qdess_bone_july_2024"

ORIGINAL_SERIES_INDEX = 100
SEGMENT_SERIES_INDEX = 101
T2MAP_SERIES_INDEX = 102
SUBREGION_SERIES_INDEX = 103
METRICS_REPORT_SERIES_INDEX = 104
SEGMENT_SERIES_NAME = "openmsk_segmentation"
T2MAP_SERIES_NAME = "openmsk_t2map"
SUBREGION_SERIES_NAME = "openmsk_subregions"
METRICS_REPORT_SERIES_NAME = "openmsk_metrics_report"
SEGMENT_IMAGE_TYPE = f"DERIVED\\PRIMARY\\SEGMENTATION\\{SEGMENT_SERIES_NAME}"
T2MAP_IMAGE_TYPE = f"DERIVED\\PRIMARY\\M\\{T2MAP_SERIES_NAME}"
SUBREGION_IMAGE_TYPE = f"DERIVED\\PRIMARY\\SEGMENTATION\\{SUBREGION_SERIES_NAME}"
METRICS_REPORT_IMAGE_TYPE = f"DERIVED\\PRIMARY\\M\\{METRICS_REPORT_SERIES_NAME}"
SCANNER_PARTITION_INDEX = 0
SEGMENT_POSTPROCESSING_META_KEY = "SegmentPostProcessing"
SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY = "SegmentPostProcessingChildRole"
SCANNER_WRITE_UNSAFE_META_KEYS = ("ImageTypeValue3",)
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
OPENMSK_SEGMENTATION_SUMMARY = "_openmsk_segmentation_summary.json"
OPENMSK_POSTPROCESSING_SUMMARY = "_openmsk_postprocessing_summary.json"
QDESS_GL_AREA_TAG = (0x0019, 0x10B6)
QDESS_TG_TAG = (0x0019, 0x10B7)
QDESS_DEFAULTS = {
    "tr_ms": 25.0,
    "te1_ms": 8.0,
    "te2_ms": 42.0,
    "flip_angle_deg": 30.0,
    "gl_area": 3132.0,
    "tg_us": 1560.0,
}


def process(connection, config, metadata):
    """OpenRecon image-in/image-out entry point."""
    logging.info("openmsk runtime version=%s", OPENMSK_VERSION)
    logging.info("Config: %s", config)
    _log_metadata(metadata)

    input_images = []
    magnitude_images = []
    non_magnitude_images = []
    sent_images = []

    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                input_images.append(item)
                if item.image_type in (ismrmrd.IMTYPE_MAGNITUDE, 0):
                    magnitude_images.append(item)
                else:
                    non_magnitude_images.append(item)
            elif isinstance(item, ismrmrd.Acquisition):
                logging.info("Ignoring raw acquisition message in image-to-image OpenMSK module")
            elif isinstance(item, ismrmrd.Waveform):
                logging.info("Ignoring waveform message in OpenMSK module")
            else:
                logging.warning("Unsupported MRD message type: %s", type(item).__name__)

        send_original = _config_bool(config, "sendoriginal", True)
        seg_model = _config_str(config, "segmodel", DEFAULT_SEG_MODEL)
        legacy_run_nsm_requested = _config_bool_any(config, ("runnsm", "run_nsm"), False)
        legacy_run_bscore = _config_bool_any(config, ("runbscore", "run_bscore"), False)
        if legacy_run_nsm_requested or legacy_run_bscore:
            logging.warning(
                "NSM/BScore options are disabled in this OpenMSK image because "
                "the required ShapeMedKnee weights are gated and not packaged"
            )
        run_nsm_requested = False
        run_bscore = False
        run_nsm = run_nsm_requested or run_bscore
        compute_thickness = _config_bool(config, "computethickness", False)

        logging.info(
            "OpenMSK options: sendoriginal=%s segmodel=%s run_nsm=%s "
            "run_bscore=%s computethickness=%s",
            send_original,
            seg_model,
            run_nsm_requested,
            run_bscore,
            compute_thickness,
        )

        if send_original and input_images:
            originals = _restamp_images(
                input_images,
                ORIGINAL_SERIES_INDEX,
                "openmsk_original",
                "ORIGINAL",
                "OpenMSK original",
            )
            _send_images(connection, originals, "original_passthrough")
            sent_images.extend(originals)
        elif non_magnitude_images:
            passthrough = _restamp_images(
                non_magnitude_images,
                ORIGINAL_SERIES_INDEX,
                "openmsk_passthrough",
                "PASSTHROUGH",
                "OpenMSK passthrough",
            )
            _send_images(connection, passthrough, "non_magnitude_passthrough")
            sent_images.extend(passthrough)

        if not magnitude_images:
            logging.warning("No magnitude image messages received; no OpenMSK derived output")
            return

        source_selection = _select_primary_source(magnitude_images)
        source_group = source_selection.get("primary") if source_selection else []
        if not source_group:
            logging.warning("No processable qDESS source group selected")
            return

        with tempfile.TemporaryDirectory(prefix="openmsk_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            echo1_nifti_path = tmpdir_path / "openmsk_echo1.nii.gz"
            output_dir = tmpdir_path / "out"
            output_dir.mkdir()

            ordered_sources, nifti_shape = _write_source_nifti(
                source_group,
                echo1_nifti_path,
                metadata,
            )
            qdess_params = _resolve_qdess_parameters(
                config,
                metadata,
                source_selection.get("echo_groups", {}),
            )
            qdess_dicom_dir = _write_synthetic_qdess_dicom_input(
                source_selection.get("echo_groups", {}),
                tmpdir_path / "openmsk_qdess_dicom",
                metadata,
                config,
                qdess_params,
            )
            pipeline_input_path = qdess_dicom_dir or echo1_nifti_path
            if qdess_dicom_dir is not None:
                logging.info("Using synthesized qDESS DICOM input for KneePipeline: %s", qdess_dicom_dir)
            else:
                logging.info("Using echo-1 NIfTI input for KneePipeline: %s", echo1_nifti_path)
            run_config_path = _write_run_config(
                tmpdir_path,
                seg_model,
                run_nsm,
                run_bscore,
            )

            segmentation_result = _run_kneepipeline_segmentation(
                pipeline_input_path,
                output_dir,
                seg_model,
                run_config_path,
            )
            segmentation_ok = _step_succeeded(segmentation_result)
            if not segmentation_ok:
                logging.warning(
                    "KneePipeline segmentation process reported an error; attempting "
                    "to return any labels already written to %s",
                    output_dir,
                )

            segment_path = _find_single_output(output_dir, "*_all-labels.nii.gz")
            if segment_path is None:
                message = f"KneePipeline did not write *_all-labels.nii.gz in {output_dir}"
                logging.error(message)
                connection.send_logging(constants.MRD_LOGGING_ERROR, message)
                return

            segment_images = _nifti_to_mrd_images(
                segment_path,
                ordered_sources,
                SEGMENT_SERIES_INDEX,
                SEGMENT_SERIES_NAME,
                SEGMENT_IMAGE_TYPE,
                data_role="Segmentation",
                dtype=np.int16,
                comment="OpenMSK segmentation",
                source_geometry_segment=True,
                reference_nifti_path=echo1_nifti_path,
            )
            _send_images(connection, segment_images, "openmsk_segmentation")
            sent_images.extend(segment_images)

            postprocessing_ok = True
            compute_t2 = _segmentation_is_qdess(segmentation_result)
            if compute_thickness or compute_t2 or run_nsm:
                postprocessing_result = _run_kneepipeline_postprocessing(
                    output_dir,
                    run_config_path,
                    compute_thickness,
                    compute_t2,
                )
                postprocessing_ok = _step_succeeded(postprocessing_result)
                if not postprocessing_ok:
                    logging.warning(
                        "KneePipeline post-processing reported an error after "
                        "segmentation was already sent"
                    )
            elif _segmentation_skips_step(segmentation_result, "t2_mapping"):
                logging.info(
                    "Skipping OpenMSK T2 mapping because KneePipeline identified "
                    "the OpenRecon input as non-qDESS"
                )
            else:
                logging.info("Skipping OpenMSK mesh/thickness post-processing")

            metrics_comment = _collect_metrics_comment(output_dir)
            if metrics_comment:
                logging.info("OpenMSK metrics: %s", metrics_comment)

            subregion_path = _find_single_output(output_dir, "*_subregions-labels.nii.gz")
            if subregion_path is not None:
                subregion_images = _nifti_to_mrd_images(
                    subregion_path,
                    ordered_sources,
                    SUBREGION_SERIES_INDEX,
                    SUBREGION_SERIES_NAME,
                    SUBREGION_IMAGE_TYPE,
                    data_role="Segmentation",
                    dtype=np.int16,
                    comment=_join_comments("OpenMSK subregion segmentation", metrics_comment),
                    source_geometry_segment=True,
                    reference_nifti_path=echo1_nifti_path,
                    extra_meta=_metrics_extra_meta(metrics_comment),
                )
                _send_images(connection, subregion_images, "openmsk_subregions")
                sent_images.extend(subregion_images)
            else:
                logging.info("No OpenMSK subregion segmentation was written")

            metrics_outputs = _collect_metrics_outputs(output_dir)
            if metrics_outputs:
                report_images = _build_metrics_report_images(
                    metrics_outputs,
                    ordered_sources,
                    metrics_comment,
                )
                _send_images(connection, report_images, "openmsk_metrics_report")
                sent_images.extend(report_images)
            else:
                logging.info("No OpenMSK metrics files were written")

            if run_nsm and postprocessing_ok:
                nsm_type = _nsm_type_for_config(run_config_path)
                _run_optional_gpu_step(
                    [
                        "python",
                        "-m",
                        "steps.run_nsm",
                        str(output_dir),
                        "--options",
                        json.dumps({"nsm_type": nsm_type}),
                        "--config",
                        str(run_config_path),
                    ],
                    "NSM",
                )
                if run_bscore:
                    _run_optional_gpu_step(
                        [
                            "python",
                            "-m",
                            "steps.compute_bscore",
                            str(output_dir),
                            "--options",
                            json.dumps({"bscore_type": nsm_type}),
                            "--config",
                            str(run_config_path),
                        ],
                        "BScore",
                    )
            elif run_nsm:
                logging.warning("Skipping NSM/BScore because mesh post-processing failed")

            t2_path = _find_single_output(output_dir, "*_t2map.nii.gz")
            if t2_path is not None:
                t2_images = _nifti_to_mrd_images(
                    t2_path,
                    ordered_sources,
                    T2MAP_SERIES_INDEX,
                    T2MAP_SERIES_NAME,
                    T2MAP_IMAGE_TYPE,
                    data_role="Image",
                    dtype=np.float32,
                    comment=metrics_comment or "OpenMSK T2 map",
                    source_geometry_segment=False,
                    reference_nifti_path=echo1_nifti_path,
                    extra_meta=_metrics_extra_meta(metrics_comment),
                )
                _send_images(connection, t2_images, "openmsk_t2map")
                sent_images.extend(t2_images)
            else:
                logging.info(
                    "No T2 map was written; check the qDESS echo grouping, synthesized "
                    "DICOM parameter logs, and KneePipeline t2_mapping status."
                )

    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        logging.info("OpenMSK sent %d image(s) total", len(sent_images))
        connection.send_close()


def _log_metadata(metadata):
    try:
        first = metadata.encoding[0]
        logging.info(
            "Incoming dataset: encodings=%d matrix=(%s,%s,%s) fov=(%s,%s,%s)",
            len(metadata.encoding),
            first.encodedSpace.matrixSize.x,
            first.encodedSpace.matrixSize.y,
            first.encodedSpace.matrixSize.z,
            first.encodedSpace.fieldOfView_mm.x,
            first.encodedSpace.fieldOfView_mm.y,
            first.encodedSpace.fieldOfView_mm.z,
        )
    except Exception:
        logging.info("Incoming metadata is not a parsed MRD header: %s", metadata)


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


def _config_bool(config, key, default=False):
    value = mrdhelper.get_json_config_param(config, key, default=default, type="bool")
    return _coerce_bool(value, default)


def _config_value_any(config, keys):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return None

    if not isinstance(config, dict):
        return None

    parameters = config.get("parameters")
    for key in keys:
        if key in config:
            return config.get(key)
        if isinstance(parameters, dict) and key in parameters:
            return parameters.get(key)
    return None


def _config_bool_any(config, keys, default=False):
    value = _config_value_any(config, keys)
    if value is not None:
        return _coerce_bool(value, default)
    return default


def _config_float_any(config, keys, default=None):
    value = _config_value_any(config, keys)
    if value is None:
        return default
    return _coerce_float_or_none(value, default)


def _config_str(config, key, default=""):
    value = mrdhelper.get_json_config_param(config, key, default=default, type="str")
    if value is None:
        return default
    return str(value)


def compute_nifti_affine(image_header, voxel_size, slice_axis=None):
    """Return an LPS MRD header to RAS NIfTI affine.

    Copied from the MuscleMap OpenRecon recipe. MRD stores DICOM/LPS
    coordinates, while NIfTI uses RAS coordinates.
    """
    lps_to_ras = np.array([-1, -1, 1], dtype=float)

    position = np.array(image_header.position) * lps_to_ras
    read_dir = np.array(image_header.read_dir) * lps_to_ras
    phase_dir = np.array(image_header.phase_dir) * lps_to_ras
    raw_slice_dir = np.array(image_header.slice_dir, dtype=float)
    if np.linalg.norm(raw_slice_dir) < 1e-8 and slice_axis is not None:
        raw_slice_dir = np.asarray(slice_axis, dtype=float)
    slice_dir = raw_slice_dir * lps_to_ras

    rotation_scaling_matrix = np.column_stack(
        [
            voxel_size[0] * read_dir,
            voxel_size[1] * phase_dir,
            voxel_size[2] * slice_dir,
        ]
    )

    affine = np.eye(4)
    affine[:3, :3] = rotation_scaling_matrix
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
        records.append(
            {
                "local_index": local_index,
                "input_index": int(input_indices[local_index]),
                "image_index": int(getattr(image_header, "image_index", 0)),
                "slice": int(getattr(image_header, "slice", 0)),
                "position": position,
                "projected_position": float(np.dot(position, slice_axis)),
            }
        )
    return slice_axis, records


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


def _select_primary_source(images):
    grouped = {}
    for image in images:
        key = _source_group_key(image)
        grouped.setdefault(key, []).append(image)

    if not grouped:
        return {}

    key, group = max(grouped.items(), key=lambda item: len(item[1]))
    echo_groups = _split_echo_groups(group)
    echo_key = sorted(echo_groups, key=_echo_key_sort)[0]
    selected = echo_groups[echo_key]
    logging.info(
        "Selected OpenMSK source group=%s echo=%s images=%d from echo groups=%s",
        key,
        echo_key,
        len(selected),
        {str(k): len(v) for k, v in echo_groups.items()},
    )
    return {
        "group_key": key,
        "primary_echo_key": echo_key,
        "primary": selected,
        "echo_groups": echo_groups,
    }


def _source_group_key(image):
    header = image.getHead()
    matrix = tuple(int(value) for value in header.matrix_size[:])
    fov = tuple(round(float(value), 4) for value in header.field_of_view[:])
    return (
        int(getattr(image, "average", 0)),
        int(getattr(image, "repetition", 0)),
        int(getattr(image, "set", 0)),
        matrix,
        fov,
    )


def _select_primary_source_group(images):
    selection = _select_primary_source(images)
    return selection.get("primary", [])


def _split_echo_groups(images):
    named_echo_groups = _split_named_dess_echo_groups(images)
    if named_echo_groups:
        return named_echo_groups

    contrast_values = {int(getattr(image, "contrast", 0)) for image in images}
    if len(contrast_values) > 1:
        return _group_by_header_field(images, "contrast")

    echo_time_groups = {}
    for image in images:
        meta = _meta_from_image(image)
        echo_time = _meta_text(meta, "EchoTime") or _meta_text(meta, "TE")
        if echo_time:
            echo_time_groups.setdefault(echo_time, []).append(image)
    if len(echo_time_groups) > 1:
        return echo_time_groups

    source_series_groups = _split_source_series_groups(images)
    if source_series_groups:
        return source_series_groups

    return _split_duplicate_slice_positions(images)


def _echo_key_sort(value):
    try:
        return (0, float(value))
    except Exception:
        return (1, str(value))


def _group_by_header_field(images, field_name):
    groups = {}
    for image in images:
        groups.setdefault(int(getattr(image, field_name, 0)), []).append(image)
    return groups


def _split_named_dess_echo_groups(images):
    groups = {}
    for image in images:
        key = _named_dess_echo_key(image)
        if key is not None:
            groups.setdefault(key, []).append(image)

    selected = {
        key: groups[key]
        for key in ("FID", "SE")
        if key in groups
    }
    if len(selected) >= 2 and _echo_group_counts_match(selected):
        return selected
    return {}


def _named_dess_echo_key(image):
    source_name = _source_series_name(image).lower()
    if not source_name or "dess" not in source_name:
        return None
    if re.search(r"(^|[_\W])fid($|[_\W])", source_name):
        return "FID"
    if re.search(r"(^|[_\W])se($|[_\W])", source_name):
        return "SE"
    return None


def _split_source_series_groups(images):
    groups = _group_by_header_field(images, "image_series_index")
    groups = {key: value for key, value in groups.items() if key}
    if len(groups) > 1 and _echo_group_counts_match(groups):
        return groups
    return {}


def _echo_group_counts_match(groups):
    counts = {len(images) for images in groups.values()}
    return len(counts) == 1 and next(iter(counts), 0) > 0


def _split_duplicate_slice_positions(images):
    headers = [image.getHead() for image in images]
    slice_axis = _infer_slice_axis(headers)
    by_position = {}
    for image in images:
        projection = float(np.dot(_header_vector(image.getHead(), "position"), slice_axis))
        key = round(projection, 4)
        by_position.setdefault(key, []).append(image)

    max_echoes = max(len(group) for group in by_position.values())
    if max_echoes <= 1:
        return {0: list(images)}

    echo_groups = {echo_index: [] for echo_index in range(max_echoes)}
    for position_key in sorted(by_position):
        same_position = sorted(
            by_position[position_key],
            key=lambda image: (
                int(getattr(image, "image_index", 0)),
                int(getattr(image, "slice", 0)),
            ),
        )
        for echo_index, image in enumerate(same_position):
            echo_groups.setdefault(echo_index, []).append(image)
    return echo_groups


def _write_source_nifti(images, output_path, metadata):
    headers = [image.getHead() for image in images]
    sort_indices, slice_axis, _ = _slice_sort_indices(headers)
    ordered_images = [images[index] for index in sort_indices]
    ordered_headers = [headers[index] for index in sort_indices]

    pixels_yxz = np.stack([_slice_pixels(image) for image in ordered_images], axis=2)
    data_xyz = np.asarray(pixels_yxz.transpose((1, 0, 2)), dtype=np.float32)

    matrix = np.array(ordered_headers[0].matrix_size[:], dtype=float)
    matrix[2] = len(ordered_images)
    fov = np.array(ordered_headers[0].field_of_view[:], dtype=float)
    measured_slice_spacing = _estimate_slice_spacing(ordered_headers, slice_axis=slice_axis)
    if measured_slice_spacing is not None:
        fov[2] = measured_slice_spacing * len(ordered_images)
    else:
        fov[2] = max(float(fov[2]), 1.0) * len(ordered_images)
    voxel_size = fov / matrix
    affine = compute_nifti_affine(ordered_headers[0], voxel_size, slice_axis=slice_axis)

    image = nib.Nifti1Image(data_xyz, affine)
    header = image.header
    header.set_data_dtype(np.float32)
    header.set_dim_info(freq=1, phase=0, slice=2)
    header.set_xyzt_units(xyz="mm", t="sec")
    header["descrip"] = "OpenMSK qDESS echo-1 MRD reconstruction"
    header["aux_file"] = "Not for diagnostic use"
    try:
        tr = metadata.sequenceParameters.TR[0]
        if tr > 1.0:
            tr = tr / 1000.0
        header["pixdim"][4] = float(tr)
    except Exception:
        pass
    image.set_qform(affine, code=1)
    image.set_sform(affine, code=1)
    nib.save(image, str(output_path))

    logging.info(
        "Wrote OpenMSK NIfTI input %s shape=%s voxel_size=%s",
        output_path,
        list(data_xyz.shape),
        [float(v) for v in voxel_size],
    )
    return ordered_images, data_xyz.shape


def _ordered_echo_groups(echo_groups):
    ordered = []
    for echo_key in sorted(echo_groups, key=_echo_key_sort):
        images = list(echo_groups[echo_key])
        if not images:
            continue
        sort_indices, _slice_axis, _records = _slice_sort_indices([image.getHead() for image in images])
        ordered.append((echo_key, [images[index] for index in sort_indices]))
    return ordered


def _resolve_qdess_parameters(config, metadata, echo_groups):
    params = {"sources": {}}

    tr_ms, source = _first_available_float(
        (
            ("mrd.sequenceParameters.TR", lambda: _metadata_sequence_float(metadata, "TR", 0, unit="ms")),
            ("openrecon.qdess_tr_ms", lambda: _config_float_any(config, ("qdess_tr_ms", "qdesstrms"))),
            ("default.qdess_tr_ms", lambda: QDESS_DEFAULTS["tr_ms"]),
        )
    )
    params["tr_ms"] = tr_ms
    params["sources"]["tr_ms"] = source

    te_values, te_sources = _resolve_qdess_echo_times(config, metadata, echo_groups)
    params["te_ms"] = te_values
    params["sources"]["te_ms"] = te_sources

    flip_angle, source = _first_available_float(
        (
            ("mrd.sequenceParameters.flipAngle_deg", lambda: _metadata_sequence_float(metadata, "flipAngle_deg", 0)),
            ("openrecon.qdess_flip_angle_deg", lambda: _config_float_any(config, ("qdess_flip_angle_deg", "qdessflipangledeg"))),
            ("image_meta.FlipAngle", lambda: _first_image_meta_float(echo_groups, ("FlipAngle", "FA"))),
            ("default.qdess_flip_angle_deg", lambda: QDESS_DEFAULTS["flip_angle_deg"]),
        )
    )
    params["flip_angle_deg"] = flip_angle
    params["sources"]["flip_angle_deg"] = source

    gl_area, source = _first_available_float(
        (
            ("mrd.userParameters.qdess_gl_area", lambda: _metadata_user_parameter_float(metadata, ("qdess_gl_area", "GL_AREA", "gl_area"))),
            ("image_meta.qdess_gl_area", lambda: _first_image_meta_float(echo_groups, ("qdess_gl_area", "GL_AREA", "gl_area", "SpoilerGradientArea"))),
            ("openrecon.qdess_gl_area", lambda: _config_float_any(config, ("qdess_gl_area", "qdessglarea"))),
            ("default.qdess_gl_area", lambda: QDESS_DEFAULTS["gl_area"]),
        )
    )
    params["gl_area"] = gl_area
    params["sources"]["gl_area"] = source

    tg_us, source = _first_available_float(
        (
            ("mrd.userParameters.qdess_tg_us", lambda: _metadata_user_parameter_float(metadata, ("qdess_tg_us", "TG", "tg_us", "tg"))),
            ("image_meta.qdess_tg_us", lambda: _first_image_meta_float(echo_groups, ("qdess_tg_us", "TG", "tg_us", "tg", "SpoilerGradientDuration"))),
            ("openrecon.qdess_tg_us", lambda: _config_float_any(config, ("qdess_tg_us", "qdesstgus"))),
            ("default.qdess_tg_us", lambda: QDESS_DEFAULTS["tg_us"]),
        )
    )
    params["tg_us"] = tg_us
    params["sources"]["tg_us"] = source

    logging.info("Resolved qDESS synthesis parameters: %s", params)
    return params


def _resolve_qdess_echo_times(config, metadata, echo_groups):
    te_values = []
    te_sources = []
    seq_te = _metadata_sequence_values_ms(metadata, "TE")
    if len(seq_te) >= 2:
        return seq_te[:2], ["mrd.sequenceParameters.TE[0]", "mrd.sequenceParameters.TE[1]"]

    meta_te = _echo_group_meta_echo_times(echo_groups)
    config_te = [
        _config_float_any(config, ("qdess_te1_ms", "qdesste1ms")),
        _config_float_any(config, ("qdess_te2_ms", "qdesste2ms")),
    ]
    defaults = [QDESS_DEFAULTS["te1_ms"], QDESS_DEFAULTS["te2_ms"]]
    for index in range(2):
        if index < len(seq_te) and seq_te[index] is not None:
            te_values.append(seq_te[index])
            te_sources.append(f"mrd.sequenceParameters.TE[{index}]")
        elif index < len(meta_te) and meta_te[index] is not None:
            te_values.append(meta_te[index])
            te_sources.append(f"image_meta.echo_group[{index}]")
        elif config_te[index] is not None:
            te_values.append(config_te[index])
            te_sources.append(f"openrecon.qdess_te{index + 1}_ms")
        else:
            te_values.append(defaults[index])
            te_sources.append(f"default.qdess_te{index + 1}_ms")
    return te_values, te_sources


def _first_available_float(candidates):
    for source, getter in candidates:
        value = _coerce_float_or_none(getter())
        if value is not None:
            return value, source
    return None, ""


def _metadata_sequence_float(metadata, field_name, index=0, *, unit=None):
    values = _metadata_sequence_values(metadata, field_name)
    if len(values) <= index:
        return None
    value = _coerce_float_or_none(values[index])
    if value is None:
        return None
    return _seconds_to_ms_if_needed(value) if unit == "ms" else value


def _metadata_sequence_values_ms(metadata, field_name):
    return [_seconds_to_ms_if_needed(value) for value in _metadata_sequence_values(metadata, field_name)]


def _metadata_sequence_values(metadata, field_name):
    try:
        values = getattr(metadata.sequenceParameters, field_name)
    except Exception:
        return []
    if values is None:
        return []
    return [_coerce_float_or_none(value) for value in list(values) if _coerce_float_or_none(value) is not None]


def _metadata_user_parameter_float(metadata, names):
    names = {str(name).lower() for name in names}
    try:
        user_parameters = metadata.userParameters
    except Exception:
        return None
    for attr in ("userParameterDouble", "userParameterLong", "userParameterString"):
        for item in list(getattr(user_parameters, attr, []) or []):
            name = str(getattr(item, "name", "")).lower()
            if name in names:
                return _coerce_float_or_none(getattr(item, "value", None))
    return None


def _echo_group_meta_echo_times(echo_groups):
    values = []
    for _echo_key, images in _ordered_echo_groups(echo_groups):
        value = _first_meta_float(images, ("EchoTime", "TE", "EffectiveEchoTime"))
        values.append(_seconds_to_ms_if_needed(value) if value is not None else None)
    return values


def _first_image_meta_float(echo_groups, keys):
    for _echo_key, images in _ordered_echo_groups(echo_groups):
        value = _first_meta_float(images, keys)
        if value is not None:
            return value
    return None


def _first_meta_float(images, keys):
    for image in images:
        meta = _meta_from_image(image)
        for key in keys:
            value = _coerce_float_or_none(_meta_text(meta, key))
            if value is not None:
                return value
    return None


def _seconds_to_ms_if_needed(value):
    value = _coerce_float_or_none(value)
    if value is None:
        return None
    return value * 1000.0 if 0 < value <= 1.0 else value


def _metadata_protocol_name(metadata):
    try:
        return str(metadata.measurementInformation.protocolName or "").strip()
    except Exception:
        return ""


def _write_synthetic_qdess_dicom_input(echo_groups, output_dir, metadata, config, qdess_params):
    ordered_groups = _ordered_echo_groups(echo_groups)
    if len(ordered_groups) < 2:
        logging.info("Cannot synthesize qDESS DICOM: only %d echo group(s) available", len(ordered_groups))
        return None

    echo_pairs = ordered_groups[:2]
    slice_count = len(echo_pairs[0][1])
    if slice_count == 0 or any(len(images) != slice_count for _key, images in echo_pairs):
        logging.warning(
            "Cannot synthesize qDESS DICOM: echo group slice counts differ: %s",
            [len(images) for _key, images in echo_pairs],
        )
        return None

    missing = [key for key in ("tr_ms", "te_ms", "flip_angle_deg", "gl_area", "tg_us") if qdess_params.get(key) is None]
    if missing:
        logging.warning("Cannot synthesize qDESS DICOM: missing qDESS parameter(s) %s", missing)
        return None

    try:
        import pydicom
        from pydicom.dataset import FileDataset, FileMetaDataset
        from pydicom.uid import ExplicitVRLittleEndian, MRImageStorage, generate_uid
    except Exception:
        logging.warning("Cannot synthesize qDESS DICOM because pydicom import failed:\n%s", traceback.format_exc())
        return None

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    study_uid = generate_uid(prefix="2.25.")
    series_uid = generate_uid(prefix="2.25.")
    frame_uid = generate_uid(prefix="2.25.")
    protocol_name = _metadata_protocol_name(metadata) or _source_series_name(echo_pairs[0][1][0]) or "openmsk_qdess"
    now = _dicom_now()

    all_pixels = []
    for _echo_key, images in echo_pairs:
        for image in images:
            all_pixels.append(_slice_pixels(image))
    pixel_scale = _dicom_pixel_scale(all_pixels)

    logging.info(
        "Synthesizing qDESS DICOM series: out=%s slices=%d echo_keys=%s TR=%sms TE=%s flip=%sdeg "
        "GL_AREA=%s TG=%sus sources=%s",
        output_dir,
        slice_count,
        [str(key) for key, _images in echo_pairs],
        qdess_params["tr_ms"],
        qdess_params["te_ms"],
        qdess_params["flip_angle_deg"],
        qdess_params["gl_area"],
        qdess_params["tg_us"],
        qdess_params.get("sources", {}),
    )

    written = []
    for echo_index, (echo_key, images) in enumerate(echo_pairs, start=1):
        echo_time = float(qdess_params["te_ms"][echo_index - 1])
        for slice_index, image in enumerate(images, start=1):
            header = image.getHead()
            pixels = _dicom_uint16_pixels(_slice_pixels(image), pixel_scale)
            rows, cols = pixels.shape
            spacing = _dicom_pixel_spacing(header, rows, cols)
            file_meta = FileMetaDataset()
            file_meta.MediaStorageSOPClassUID = MRImageStorage
            file_meta.MediaStorageSOPInstanceUID = generate_uid(prefix="2.25.")
            file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
            file_meta.ImplementationClassUID = generate_uid(prefix="2.25.")

            path = output_dir / f"echo{echo_index:02d}_slice{slice_index:04d}.dcm"
            ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
            ds.SpecificCharacterSet = "ISO_IR 100"
            ds.SOPClassUID = MRImageStorage
            ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
            ds.StudyInstanceUID = study_uid
            ds.SeriesInstanceUID = series_uid
            ds.FrameOfReferenceUID = frame_uid
            ds.PatientName = "OpenMSK^SyntheticQDESS"
            ds.PatientID = "OPENMSK"
            ds.StudyDate = now["date"]
            ds.StudyTime = now["time"]
            ds.SeriesDate = now["date"]
            ds.SeriesTime = now["time"]
            ds.ContentDate = now["date"]
            ds.ContentTime = now["time"]
            ds.Modality = "MR"
            ds.Manufacturer = "OpenMSK"
            ds.ManufacturerModelName = "OpenRecon"
            ds.SeriesDescription = protocol_name
            ds.ProtocolName = protocol_name
            ds.ImageType = ["DERIVED", "PRIMARY", "M", "QDESS_SYNTHETIC"]
            ds.ScanningSequence = "GR"
            ds.SequenceVariant = "SS"
            ds.ScanOptions = ""
            ds.MRAcquisitionType = "3D"
            ds.InstanceNumber = (echo_index - 1) * slice_count + slice_index
            ds.EchoNumbers = echo_index
            ds.add_new((0x0019, 0x107E), "IS", 2)
            ds.EchoTime = _dicom_ds(echo_time)
            ds.RepetitionTime = _dicom_ds(qdess_params["tr_ms"])
            ds.FlipAngle = _dicom_ds(qdess_params["flip_angle_deg"])
            ds.Rows = rows
            ds.Columns = cols
            ds.PixelSpacing = [_dicom_ds(spacing["row_mm"]), _dicom_ds(spacing["col_mm"])]
            ds.SliceThickness = _dicom_ds(spacing["slice_mm"])
            ds.SpacingBetweenSlices = _dicom_ds(spacing["slice_mm"])
            ds.ImageOrientationPatient = _dicom_image_orientation(header)
            ds.ImagePositionPatient = [_dicom_ds(v) for v in header.position]
            ds.SliceLocation = _dicom_ds(header.position[2]) if len(header.position) >= 3 else _dicom_ds(slice_index - 1)
            ds.SamplesPerPixel = 1
            ds.PhotometricInterpretation = "MONOCHROME2"
            ds.BitsAllocated = 16
            ds.BitsStored = 16
            ds.HighBit = 15
            ds.PixelRepresentation = 0
            ds.RescaleIntercept = "0"
            ds.RescaleSlope = "1"
            ds.add_new((0x0019, 0x0010), "LO", "SIEMENS MR HEADER")
            ds.add_new(QDESS_GL_AREA_TAG, "DS", _dicom_ds(qdess_params["gl_area"]))
            ds.add_new(QDESS_TG_TAG, "DS", _dicom_ds(qdess_params["tg_us"]))
            ds.PixelData = pixels.tobytes()
            try:
                ds.save_as(str(path), enforce_file_format=True)
            except TypeError:
                ds.save_as(str(path), write_like_original=False)
            written.append(path)

    logging.info("Wrote %d synthetic qDESS DICOM file(s) to %s", len(written), output_dir)
    return output_dir


def _dicom_now():
    from datetime import datetime

    now = datetime.now()
    return {"date": now.strftime("%Y%m%d"), "time": now.strftime("%H%M%S.%f")}


def _dicom_ds(value):
    value = _coerce_float_or_none(value)
    if value is None or not np.isfinite(value):
        return "0"
    text = f"{value:.8g}"
    if len(text) <= 16:
        return text
    for precision in range(8, 0, -1):
        text = f"{value:.{precision}e}"
        if len(text) <= 16:
            return text
    return "0"


def _dicom_pixel_scale(pixel_arrays):
    max_value = 0.0
    for array in pixel_arrays:
        finite = np.asarray(array, dtype=np.float32)
        if finite.size:
            value = float(np.nanmax(finite))
            if np.isfinite(value):
                max_value = max(max_value, value)
    if max_value <= 0:
        return 1.0
    return 4095.0 / max_value


def _dicom_uint16_pixels(pixel_array, scale):
    pixels = np.asarray(pixel_array, dtype=np.float32)
    pixels = np.nan_to_num(pixels, nan=0.0, posinf=0.0, neginf=0.0)
    pixels = np.clip(np.rint(pixels * float(scale)), 0, np.iinfo(np.uint16).max)
    return np.ascontiguousarray(pixels.astype(np.uint16, copy=False))


def _dicom_pixel_spacing(header, rows, cols):
    matrix = np.asarray(header.matrix_size[:], dtype=float)
    fov = np.asarray(header.field_of_view[:], dtype=float)
    col_mm = fov[0] / matrix[0] if matrix.size > 0 and matrix[0] > 0 else 1.0
    row_mm = fov[1] / matrix[1] if matrix.size > 1 and matrix[1] > 0 else 1.0
    slice_mm = fov[2] / matrix[2] if matrix.size > 2 and matrix[2] > 0 else 1.0
    if not np.isfinite(row_mm) or row_mm <= 0:
        row_mm = 1.0
    if not np.isfinite(col_mm) or col_mm <= 0:
        col_mm = 1.0
    if not np.isfinite(slice_mm) or slice_mm <= 0:
        slice_mm = 1.0
    return {"row_mm": float(row_mm), "col_mm": float(col_mm), "slice_mm": float(slice_mm)}


def _dicom_image_orientation(header):
    read_dir = _normalize_vector(_header_vector(header, "read_dir"))
    if read_dir is None:
        read_dir = np.array([1.0, 0.0, 0.0])
    phase_dir = _normalize_vector(_header_vector(header, "phase_dir"))
    if phase_dir is None:
        phase_dir = np.array([0.0, 1.0, 0.0])
    return [_dicom_ds(v) for v in np.concatenate([read_dir, phase_dir])]


def _slice_pixels(image):
    data = np.asarray(image.data)
    if np.iscomplexobj(data):
        data = np.abs(data)
    if data.ndim == 2:
        return np.asarray(data, dtype=np.float32)
    data = np.squeeze(data)
    if data.ndim == 2:
        return np.asarray(data, dtype=np.float32)
    if data.ndim == 3:
        return np.sqrt(np.sum(np.asarray(data, dtype=np.float32) ** 2, axis=0))
    if data.ndim == 4:
        return np.sqrt(np.sum(np.asarray(data, dtype=np.float32) ** 2, axis=(0, 1)))
    raise ValueError(f"Unsupported MRD image data shape for OpenMSK: {data.shape}")


def _write_run_config(tmpdir, seg_model, run_nsm, run_bscore):
    with open(KNEEPIPELINE_CONFIG) as f:
        config = json.load(f)

    nsm_type = _requested_nsm_type(config, run_nsm)
    config["default_seg_model"] = seg_model
    # The main run_pipeline.py couples NSM and BScore. Keep the main pass
    # CPU-safe, then run those optional GPU steps explicitly below.
    config["perform_bone_only_nsm"] = False
    config["perform_bone_and_cart_nsm"] = False
    config["_openmsk_run_nsm"] = bool(run_nsm)
    config["_openmsk_run_bscore"] = bool(run_bscore)
    config["_openmsk_nsm_type"] = nsm_type

    if run_nsm and not _nsm_assets_available(config, nsm_type):
        logging.info("NSM model files are not present; GPU-only NSM/BScore will be skipped if requested")

    config_path = Path(tmpdir) / "openmsk_config.json"
    config_path.write_text(json.dumps(config, indent=2))
    return config_path


def _requested_nsm_type(config, run_nsm):
    if not run_nsm:
        return "bone_only"
    bone_only = bool(config.get("perform_bone_only_nsm"))
    bone_and_cart = bool(config.get("perform_bone_and_cart_nsm"))
    if bone_only and bone_and_cart:
        return "both"
    if bone_and_cart:
        return "bone_and_cart"
    return "bone_only"


def _nsm_assets_available(config, nsm_type=None):
    nsm_type = nsm_type or config.get("_openmsk_nsm_type", "bone_only")
    keys = []
    if nsm_type in ("bone_and_cart", "both"):
        keys.append("nsm")
    if nsm_type in ("bone_only", "both"):
        keys.append("nsm_bone_only")
    if not keys:
        keys.append("nsm")
    for key in keys:
        section = config.get(key, {})
        for path_key in ("path_model_config", "path_model_state"):
            path = section.get(path_key)
            if path and not os.path.exists(path):
                return False
    return True


def _nsm_type_for_config(config_path):
    try:
        config = json.loads(Path(config_path).read_text())
    except Exception:
        return "bone_and_cart"
    openmsk_type = config.get("_openmsk_nsm_type")
    if openmsk_type:
        return openmsk_type
    if config.get("perform_bone_only_nsm") and config.get("perform_bone_and_cart_nsm"):
        return "both"
    if config.get("perform_bone_and_cart_nsm"):
        return "bone_and_cart"
    return "bone_only"


def _read_openmsk_step_summary(output_dir, filename):
    path = Path(output_dir) / filename
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        logging.warning("Failed to read OpenMSK step summary %s:\n%s", path, traceback.format_exc())
        return {}
    return payload if isinstance(payload, dict) else {}


def _step_succeeded(result):
    if isinstance(result, bool):
        return result
    if not isinstance(result, dict):
        return bool(result)
    if "ok" in result:
        return bool(result["ok"])
    if "returncode" in result:
        return int(result["returncode"]) == 0
    return not bool(result.get("errors"))


def _segmentation_summary(result):
    if not isinstance(result, dict):
        return {}
    summary = result.get("segmentation")
    return summary if isinstance(summary, dict) else result


def _segmentation_is_qdess(result):
    summary = _segmentation_summary(result)
    return bool(summary.get("is_qdess"))


def _segmentation_skips_step(result, step_name):
    summary = _segmentation_summary(result)
    skip_steps = summary.get("skip_steps") or []
    return step_name in skip_steps


def _run_kneepipeline_segmentation(input_path, output_dir, seg_model, config_path):
    script = r"""
import json
from pathlib import Path
import shutil
import sys

pipeline_dir = Path("/opt/KneePipeline")
sys.path.insert(0, str(pipeline_dir))

import os
import subprocess as _sp

from run_pipeline import _get_remap_table
from steps._common import STEP_RESULT_FILENAME, load_config
from steps.label_remap import run as label_remap

# KneePipeline's own _run_step_subprocess hardcodes a 600s timeout, too short
# for CPU-only / emulated hosts where DOSMA qDESS inference can take much
# longer. Use a generous, configurable timeout instead.
_STEP_TIMEOUT = int(os.environ.get("OPENMSK_STEP_TIMEOUT", "5400"))


def _summarize_stream(text, max_chars=12000):
    text = text or ""
    if len(text) <= max_chars:
        return text
    keep = max_chars // 2
    omitted = len(text) - (2 * keep)
    return text[:keep] + "\n... [%d chars omitted] ...\n" % omitted + text[-keep:]


def _run_step(module_name, wd, options=None, config_path=None):
    cmd = [sys.executable, "-m", module_name, str(wd)]
    if options:
        cmd += ["--options", json.dumps(options)]
    if config_path:
        cmd += ["--config", str(config_path)]
    r = _sp.run(cmd, capture_output=True, text=True, timeout=_STEP_TIMEOUT)
    if r.stdout:
        print(r.stdout)
    if r.stderr:
        print("%s stderr:\n%s" % (module_name, _summarize_stream(r.stderr)), file=sys.stderr)
    if r.returncode != 0:
        raise RuntimeError("%s failed (exit %s)" % (module_name, r.returncode))
    rp = Path(wd) / STEP_RESULT_FILENAME
    res = json.loads(rp.read_text())
    rp.unlink()
    return res

input_path = Path(sys.argv[1])
working_dir = Path(sys.argv[2])
seg_model = sys.argv[3]
config_path = Path(sys.argv[4])

config = load_config(str(config_path))
working_dir.mkdir(parents=True, exist_ok=True)
link_path = working_dir / input_path.name
if not link_path.exists():
    try:
        link_path.symlink_to(input_path.resolve())
    except Exception:
        shutil.copy2(input_path, link_path)

summary = {
    "segmodel": seg_model,
    "errors": {},
}

seg_result = _run_step(
    "steps.segment",
    working_dir,
    options={"model": seg_model},
    config_path=str(config_path),
)
summary["segmentation"] = seg_result

remap_table = _get_remap_table(seg_result["model_name"], config)
if remap_table:
    label_remap(working_dir, options={"remap_table": remap_table}, config=config)
summary["label_remap"] = bool(remap_table)

summary_path = working_dir / "_openmsk_segmentation_summary.json"
summary_path.write_text(json.dumps(summary, default=str, indent=2))
print(json.dumps(summary, default=str))
"""
    cmd = [
        "python",
        "-c",
        script,
        str(input_path),
        str(output_dir),
        seg_model,
        str(config_path),
    ]
    result = subprocess.run(
        cmd,
        cwd=str(KNEEPIPELINE_DIR),
        env=_kneepipeline_subprocess_env(),
        capture_output=True,
        text=True,
        timeout=OPENMSK_PIPELINE_TIMEOUT,
    )
    _log_subprocess_result("KneePipeline segmentation", result)
    summary = _read_openmsk_step_summary(output_dir, OPENMSK_SEGMENTATION_SUMMARY)
    summary["returncode"] = int(result.returncode)
    summary["ok"] = result.returncode == 0
    return summary


def _run_kneepipeline_postprocessing(output_dir, config_path, compute_thickness, compute_t2):
    script = r"""
import json
from pathlib import Path
import sys
import traceback

pipeline_dir = Path("/opt/KneePipeline")
sys.path.insert(0, str(pipeline_dir))

from steps._common import load_config

working_dir = Path(sys.argv[1])
config_path = Path(sys.argv[2])
compute_thickness = json.loads(sys.argv[3])
compute_t2 = json.loads(sys.argv[4])

config = load_config(str(config_path))
summary = {
    "compute_thickness": compute_thickness,
    "compute_t2": compute_t2,
    "errors": {},
}

try:
    from steps.generate_meshes import run as generate_meshes
    summary["generate_meshes"] = generate_meshes(
        working_dir,
        options={"compute_thickness": compute_thickness},
        config=config,
    )
except Exception:
    summary["errors"]["generate_meshes"] = traceback.format_exc()

if compute_t2:
    try:
        from steps.t2_mapping import run as t2_mapping
        summary["t2_mapping"] = t2_mapping(working_dir, config=config)
    except Exception:
        summary["errors"]["t2_mapping"] = traceback.format_exc()
else:
    summary["t2_mapping"] = {
        "skipped": True,
        "reason": "KneePipeline segmentation did not identify the input as qDESS",
    }

summary_path = working_dir / "_openmsk_postprocessing_summary.json"
summary_path.write_text(json.dumps(summary, default=str, indent=2))
print(json.dumps(summary, default=str))
if summary["errors"]:
    sys.exit(1)
"""
    cmd = [
        "python",
        "-c",
        script,
        str(output_dir),
        str(config_path),
        json.dumps(bool(compute_thickness)),
        json.dumps(bool(compute_t2)),
    ]
    result = subprocess.run(
        cmd,
        cwd=str(KNEEPIPELINE_DIR),
        env=_kneepipeline_subprocess_env(),
        capture_output=True,
        text=True,
        timeout=OPENMSK_PIPELINE_TIMEOUT,
    )
    _log_subprocess_result("KneePipeline post-processing", result)
    summary = _read_openmsk_step_summary(output_dir, OPENMSK_POSTPROCESSING_SUMMARY)
    summary["returncode"] = int(result.returncode)
    summary["ok"] = result.returncode == 0
    return summary


def _run_optional_gpu_step(cmd, label):
    config_path = Path(cmd[-1])
    try:
        config = json.loads(config_path.read_text())
    except Exception:
        config = {}
    nsm_type = config.get("_openmsk_nsm_type", "bone_only")
    if not _nsm_assets_available(config, nsm_type):
        logging.warning("%s requested but NSM model files are missing; skipping", label)
        return False

    try:
        result = subprocess.run(
            cmd,
            cwd=str(KNEEPIPELINE_DIR),
            env=_kneepipeline_subprocess_env(),
            capture_output=True,
            text=True,
            timeout=1200,
        )
        _log_subprocess_result(label, result)
        return result.returncode == 0
    except Exception:
        logging.warning("%s failed; segmentation output will still be returned:\n%s", label, traceback.format_exc())
        return False


def _kneepipeline_subprocess_env():
    env = os.environ.copy()
    if not NNUNET_NUMPY_COMPAT_PATH:
        return env

    current_pythonpath = env.get("PYTHONPATH")
    parts = [NNUNET_NUMPY_COMPAT_PATH]
    if current_pythonpath:
        parts.extend(part for part in current_pythonpath.split(os.pathsep) if part)
    env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(parts))
    return env


def _log_subprocess_result(label, result):
    if result.stdout:
        logging.info("%s stdout:\n%s", label, _summarize_log_stream(result.stdout))
    if result.stderr:
        log_fn = logging.warning if result.returncode else logging.info
        log_fn("%s stderr:\n%s", label, _summarize_log_stream(result.stderr))
    if result.returncode:
        logging.warning("%s exited with code %s", label, result.returncode)


def _summarize_log_stream(text, max_chars=12000):
    text = text or ""
    if len(text) <= max_chars:
        return text
    keep = max_chars // 2
    omitted = len(text) - (2 * keep)
    return text[:keep] + f"\n... [{omitted} chars omitted] ...\n" + text[-keep:]


def _find_single_output(output_dir, pattern):
    matches = sorted(Path(output_dir).glob(pattern))
    if not matches:
        return None
    if len(matches) > 1:
        logging.warning("Multiple files match %s in %s; using %s", pattern, output_dir, matches[0])
    return matches[0]


def _nifti_to_mrd_images(
    nifti_path,
    source_images,
    series_index,
    series_name,
    image_type,
    *,
    data_role,
    dtype,
    comment,
    source_geometry_segment,
    reference_nifti_path=None,
    extra_meta=None,
):
    img = nib.load(str(nifti_path))
    img = _nifti_on_reference_grid(
        img,
        reference_nifti_path,
        order=0 if np.issubdtype(dtype, np.integer) else 1,
    )
    data_xyz = np.asarray(img.dataobj)
    if data_xyz.ndim > 3:
        data_xyz = np.squeeze(data_xyz)
    if data_xyz.ndim == 2:
        data_xyz = data_xyz[:, :, None]
    if data_xyz.ndim != 3:
        raise ValueError(f"Unsupported NIfTI output shape for {nifti_path}: {data_xyz.shape}")

    data_yxz = data_xyz.transpose((1, 0, 2))
    target_shape = (
        _slice_pixels(source_images[0]).shape[0],
        _slice_pixels(source_images[0]).shape[1],
        len(source_images),
    )
    data_yxz = _match_volume_shape_yxz(data_yxz, target_shape)
    if np.issubdtype(dtype, np.integer):
        rounded = np.rint(data_yxz)
        if not np.allclose(data_yxz, rounded, atol=1e-3):
            raise ValueError(f"{nifti_path} contains non-integer label data")
        info = np.iinfo(dtype)
        rounded = np.clip(rounded, info.min, info.max)
        data_yxz = rounded.astype(dtype, copy=False)
    else:
        data_yxz = data_yxz.astype(dtype, copy=False)

    max_value = float(np.nanmax(data_yxz)) if data_yxz.size else 1.0
    if not np.isfinite(max_value) or max_value <= 0:
        max_value = 1.0
    min_value = float(np.nanmin(data_yxz)) if data_yxz.size else 0.0
    if not np.isfinite(min_value):
        min_value = 0.0
    if source_geometry_segment:
        labels = np.unique(data_yxz)
        logging.info(
            "Converted OpenMSK segmentation %s shape=%s min=%s max=%s labels=%s",
            nifti_path,
            list(data_yxz.shape),
            min_value,
            max_value,
            labels[:32].tolist(),
        )

    series_uid = _derived_uid(series_name, series_index)
    outputs = []
    for index, source_image in enumerate(source_images):
        slice_data = np.ascontiguousarray(data_yxz[:, :, index])
        output = ismrmrd.Image.from_array(slice_data, transpose=False)
        header = copy.deepcopy(source_image.getHead())
        header.data_type = output.data_type
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.image_series_index = series_index
        header.image_index = index + 1
        header.slice = index
        _set_header_sequence_field(header, "matrix_size", [slice_data.shape[1], slice_data.shape[0], 1])
        output.setHead(header)
        output.image_series_index = series_index
        output.image_index = index + 1
        output.attribute_string = _derived_meta(
            source_image,
            series_name,
            series_uid,
            series_index,
            index,
            image_type,
            data_role,
            comment,
            max_value,
            source_geometry_segment=source_geometry_segment,
            slice_count=len(source_images),
            extra_meta=extra_meta,
        ).serialize()
        outputs.append(output)
    return outputs


def _nifti_on_reference_grid(img, reference_nifti_path, *, order):
    if reference_nifti_path is None:
        return img

    reference = nib.load(str(reference_nifti_path))
    reference_shape = tuple(reference.shape[:3])
    image_shape = tuple(img.shape[:3])
    if image_shape == reference_shape and np.allclose(img.affine, reference.affine, atol=1e-4):
        return img

    reindexed = _reindex_axis_aligned_nifti(img, reference)
    if reindexed is not None:
        return reindexed

    logging.info(
        "Resampling OpenMSK NIfTI output from shape=%s affine=%s to source shape=%s affine=%s",
        image_shape,
        np.array2string(img.affine, precision=4, suppress_small=True),
        reference_shape,
        np.array2string(reference.affine, precision=4, suppress_small=True),
    )
    return resample_from_to(
        img,
        (reference_shape, reference.affine),
        order=order,
        mode="constant",
        cval=0,
    )


def _reindex_axis_aligned_nifti(img, reference):
    if len(img.shape) < 3 or len(reference.shape) < 3:
        return None

    transform = np.linalg.inv(img.affine) @ reference.affine
    linear = transform[:3, :3]
    offset = transform[:3, 3]
    rounded_linear = np.rint(linear).astype(int)
    rounded_offset = np.rint(offset).astype(int)
    if not (
        np.allclose(linear, rounded_linear, atol=1e-4)
        and np.allclose(offset, rounded_offset, atol=1e-4)
    ):
        return None
    if not (
        np.all(np.sum(np.abs(rounded_linear), axis=0) == 1)
        and np.all(np.sum(np.abs(rounded_linear), axis=1) == 1)
    ):
        return None

    source_shape = tuple(img.shape[:3])
    target_shape = tuple(reference.shape[:3])
    source_axis_to_target_axis = []
    source_indices = []
    for source_axis in range(3):
        target_axis = int(np.argmax(np.abs(rounded_linear[source_axis, :])))
        sign = int(rounded_linear[source_axis, target_axis])
        indices = sign * np.arange(target_shape[target_axis]) + rounded_offset[source_axis]
        if indices.size and (indices.min() < 0 or indices.max() >= source_shape[source_axis]):
            return None
        source_axis_to_target_axis.append(target_axis)
        source_indices.append(indices.astype(int, copy=False))

    data = np.asarray(img.dataobj)
    indexed = data[np.ix_(*source_indices)]
    axis_order = np.argsort(source_axis_to_target_axis).tolist()
    if indexed.ndim > 3:
        axis_order.extend(range(3, indexed.ndim))
    reindexed = np.transpose(indexed, axis_order)
    logging.info(
        "Reindexed axis-aligned OpenMSK NIfTI output from shape=%s to source shape=%s without interpolation",
        source_shape,
        target_shape,
    )
    return nib.Nifti1Image(reindexed, reference.affine)


def _match_volume_shape_yxz(data, target_shape):
    result = data
    for axis, target in enumerate(target_shape):
        current = result.shape[axis]
        if current == target:
            continue
        if current > target:
            start = (current - target) // 2
            slices = [slice(None)] * result.ndim
            slices[axis] = slice(start, start + target)
            result = result[tuple(slices)]
        else:
            pad_before = (target - current) // 2
            pad_after = target - current - pad_before
            pad_width = [(0, 0)] * result.ndim
            pad_width[axis] = (pad_before, pad_after)
            result = np.pad(result, pad_width, mode="constant")
    return result


def _restamp_images(images, series_index, series_name, type_token, comment):
    series_uid = _derived_uid(series_name, series_index)
    outputs = []
    for index, image in enumerate(images):
        output = ismrmrd.Image.from_array(np.array(image.data, copy=True), transpose=False)
        header = copy.deepcopy(image.getHead())
        header.image_series_index = series_index
        header.image_index = index + 1
        output.setHead(header)
        output.image_series_index = series_index
        output.image_index = index + 1
        output.attribute_string = _passthrough_meta(
            image,
            series_name,
            series_uid,
            series_index,
            index,
            type_token,
            comment,
            _image_abs_max(image.data),
            slice_count=len(images),
        ).serialize()
        outputs.append(output)
    return outputs


def _image_abs_max(data):
    array = np.asarray(data)
    if array.size == 0:
        return 1.0
    if np.iscomplexobj(array):
        array = np.abs(array)
    value = float(np.nanmax(array))
    if not np.isfinite(value) or value <= 0:
        return 1.0
    return value


def _derived_meta(
    source_image,
    series_name,
    series_uid,
    series_index,
    output_index,
    image_type,
    data_role,
    comment,
    max_value,
    *,
    source_geometry_segment,
    slice_count,
    extra_meta=None,
):
    meta = _copy_meta(_meta_from_image(source_image))
    _strip_source_parent_refs(meta)
    sop_uid = _derived_uid(series_name, series_index, output_index)

    meta["DataRole"] = data_role
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["SeriesNumberRangeNameUID"] = f"{series_name}_{series_index}"
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = series_name
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["ImageComments"] = comment
    meta["ImageComment"] = comment
    meta["ImageProcessingHistory"] = ["OPENRECON", "OPENMSK"]
    meta["SequenceDescriptionAdditional"] = "openrecon"
    meta["Keep_image_geometry"] = 1
    window_width = max_value + 1.0 if source_geometry_segment else max(max_value, 1.0)
    meta["WindowCenter"] = str(window_width / 2.0)
    meta["WindowWidth"] = str(max(window_width, 1.0))
    meta["partition_count"] = "1"
    meta["slice_count"] = str(slice_count)
    meta["NumberOfSlices"] = str(slice_count)
    meta["ImagesInAcquisition"] = str(slice_count)
    _set_output_storage_meta(meta, output_index)
    exam_data_role = None
    if source_geometry_segment:
        exam_data_role = _format_exam_data_role_sequential_number(series_index)
        meta["SegmentSourceGeometry"] = "1"
        meta["SegmentOutputGeometry"] = "2d"
        meta[SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY] = str(int(series_index))
        meta["ExamDataRole"] = exam_data_role
        if SEGMENT_POSTPROCESSING_META_KEY in meta:
            del meta[SEGMENT_POSTPROCESSING_META_KEY]
        meta["LUTFileName"] = "MicroDeltaHotMetal.pal"
        _strip_scanner_write_unsafe_meta(meta)
    else:
        meta["ImageTypeValue3"] = "M"
    if extra_meta:
        for key, value in extra_meta.items():
            if value is not None:
                meta[key] = str(value)
    minihead = _decode_ice_minihead(meta)
    if minihead:
        patched_minihead, changed = _patch_derived_ice_minihead(
            minihead,
            series_name,
            f"{series_name}_{series_index}",
            series_uid,
            sop_uid,
            image_type,
            exam_data_role=exam_data_role,
            source_geometry_segment=source_geometry_segment,
            output_index=output_index,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _passthrough_meta(
    source_image,
    series_name,
    series_uid,
    series_index,
    output_index,
    type_token,
    comment,
    max_value,
    *,
    slice_count,
):
    meta = _copy_meta(_meta_from_image(source_image))
    _strip_source_parent_refs(meta)
    sop_uid = _derived_uid(series_name, series_index, output_index)
    source_name = _source_series_name(source_image)
    fallback_name = source_name or series_name

    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        if not _meta_text(meta, key):
            meta[key] = fallback_name
    if not _meta_text(meta, "DataRole"):
        meta["DataRole"] = "Image"
    if not _meta_text(meta, "ComplexImageComponent"):
        meta["ComplexImageComponent"] = "MAGNITUDE"
    if not _meta_text(meta, "ImageType"):
        meta["ImageType"] = type_token
    if not _meta_text(meta, "DicomImageType"):
        meta["DicomImageType"] = _meta_text(meta, "ImageType")
    if not _meta_text(meta, "ImageTypeValue4"):
        meta["ImageTypeValue4"] = type_token
    if not _meta_text(meta, "ImageComments"):
        meta["ImageComments"] = comment
    if not _meta_text(meta, "ImageComment"):
        meta["ImageComment"] = comment

    meta["SeriesNumberRangeNameUID"] = f"{series_name}_{series_index}"
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["Keep_image_geometry"] = 1
    window_width = max(max_value, 1.0)
    meta["WindowCenter"] = str(window_width / 2.0)
    meta["WindowWidth"] = str(window_width)
    meta["partition_count"] = "1"
    meta["slice_count"] = str(slice_count)
    meta["NumberOfSlices"] = str(slice_count)
    meta["ImagesInAcquisition"] = str(slice_count)
    _set_output_storage_meta(meta, output_index)
    _strip_scanner_write_unsafe_meta(meta)

    minihead = _decode_ice_minihead(meta)
    if minihead:
        patched_minihead, changed = _patch_passthrough_ice_minihead(
            minihead,
            source_name,
            f"{series_name}_{series_index}",
            series_uid,
            sop_uid,
            output_index,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _set_output_storage_meta(meta, output_index):
    for key, value in (
        ("Actual3DImagePartNumber", SCANNER_PARTITION_INDEX),
        ("AnatomicalPartitionNo", SCANNER_PARTITION_INDEX),
        ("AnatomicalSliceNo", output_index),
        ("ChronSliceNo", output_index),
        ("NumberInSeries", output_index + 1),
        ("ProtocolSliceNumber", output_index),
        ("SliceNo", output_index),
        ("IsmrmrdSliceNo", output_index),
    ):
        meta[key] = str(int(value))


def _strip_scanner_write_unsafe_meta(meta):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta:
            del meta[key]


def _decode_ice_minihead(meta):
    encoded = _meta_text(meta, "IceMiniHead")
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _patch_derived_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    *,
    exam_data_role=None,
    source_geometry_segment,
    output_index,
):
    current_text = minihead_text
    changed = False
    for name, value in (
        ("SeriesDescription", series_name),
        ("SequenceDescription", series_name),
        ("ProtocolName", series_name),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
        ("ImageType", image_type),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change

    current_text, did_change = _replace_or_append_minihead_array_tokens(
        current_text,
        "ImageTypeValue4",
        [series_name],
    )
    changed = changed or did_change
    current_text, did_change = _replace_or_append_minihead_exam_data_role(
        current_text,
        exam_data_role,
    )
    changed = changed or did_change

    if source_geometry_segment:
        current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    else:
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            "ImageTypeValue3",
            "M",
        )
    changed = changed or did_change

    for name, value in (
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
            name,
            value,
        )
        changed = changed or did_change

    return current_text, changed


def _patch_passthrough_ice_minihead(
    minihead_text,
    source_name,
    series_grouping,
    series_uid,
    sop_uid,
    output_index,
):
    current_text = minihead_text
    changed = False
    current_text, did_change = _strip_scanner_write_unsafe_minihead(current_text)
    changed = changed or did_change
    for name in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        current_text, did_change = _ensure_minihead_string_param(
            current_text,
            name,
            source_name,
        )
        changed = changed or did_change
    for name, value in (
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    for name, value in (
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
            name,
            value,
        )
        changed = changed or did_change
    return current_text, changed


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


def _strip_scanner_write_unsafe_minihead(minihead_text):
    current_text = minihead_text
    changed = False
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        current_text, did_change = _remove_minihead_string_param(current_text, key)
        changed = changed or did_change
        current_text, did_change = _remove_minihead_array_param(current_text, key)
        changed = changed or did_change
    return current_text, changed


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


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = str(value).replace('"', "'")
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

    return minihead_text.rstrip() + f'\n<ParamString."{name}">\t{{ "{value}" }}\n', True


def _ensure_minihead_string_param(minihead_text, name, value):
    if not value:
        return minihead_text, False
    pattern = re.compile(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"[^"]*"\s*\}}'
    )
    if pattern.search(minihead_text):
        return minihead_text, False
    return _replace_or_append_minihead_string_param(minihead_text, name, value)


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    value = str(int(value))
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d+)(\s*\}})'
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

    return minihead_text.rstrip() + f'\n<ParamLong."{name}">\t{{ {value} }}\n', True


def _replace_or_append_minihead_array_tokens(minihead_text, name, values):
    values = [str(value).replace('"', "'") for value in values if str(value)]
    if not values:
        return minihead_text, False

    line_ending = "\r\n" if "\r\n" in minihead_text else "\n"
    token_lines = "".join(f'    {{ "{value}" }}{line_ending}' for value in values)
    array_text = (
        f'<ParamArray."{name}">{line_ending}'
        f"{{{line_ending}"
        f"    <DefaultSize> {len(values)}{line_ending}"
        f"    <MaxSize> 2147483647{line_ending}"
        f'    <Default> <ParamString."">{{ }}{line_ending}'
        f"{token_lines}"
        f"}}{line_ending}"
    )
    pattern = re.compile(
        rf'^\s*<ParamArray\."{re.escape(name)}">\s*\{{.*?^\s*\}}\s*\n?',
        flags=re.DOTALL | re.MULTILINE,
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        token_pattern = re.compile(r'^[ \t]*\{\s*"[^"]*"\s*\}\s*$', flags=re.MULTILINE)

        def replace_block(match):
            block_text = match.group(0)
            if (
                "<DefaultSize>" not in block_text
                or "<MaxSize>" not in block_text
                or "<Default>" not in block_text
            ):
                return array_text
            if token_pattern.search(block_text):
                return token_pattern.sub(lambda _match: token_lines.rstrip(), block_text)

            close_matches = list(re.finditer(r'^[ \t]*\}\s*$', block_text, flags=re.MULTILINE))
            if not close_matches:
                return array_text
            close_match = close_matches[-1]
            return (
                block_text[: close_match.start()]
                + token_lines
                + block_text[close_match.start() :]
            )

        updated_text = pattern.sub(replace_block, minihead_text)
        return updated_text, updated_text != minihead_text

    return minihead_text.rstrip() + "\n" + array_text, True


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

    return minihead_text.rstrip() + f'\n<ParamString."ExamDataRole">\t{{ "{literal}" }}\n', True


def _copy_meta(meta_obj):
    try:
        return ismrmrd.Meta.deserialize(meta_obj.serialize())
    except Exception:
        return copy.deepcopy(meta_obj)


def _meta_from_image(image):
    try:
        return ismrmrd.Meta.deserialize(image.attribute_string)
    except Exception:
        return ismrmrd.Meta()


def _meta_text(meta, key):
    value = meta.get(key)
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        for item in value:
            text = str(item).strip()
            if text:
                return text
        return ""
    return str(value).strip()


def _minihead_string_value(minihead_text, name):
    match = re.search(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"([^"]*)"\s*\}}',
        minihead_text or "",
    )
    if not match:
        return ""
    return match.group(1).strip()


def _source_series_name(source_image):
    meta = _meta_from_image(source_image)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _meta_text(meta, key)
        if value:
            return value

    minihead = _decode_ice_minihead(meta)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _minihead_string_value(minihead, key)
        if value:
            return value
    return ""


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta[key]


def _set_header_sequence_field(image_header, field_name, values):
    values = list(values)
    current_value = getattr(image_header, field_name)
    try:
        current_value[:] = values
    except Exception:
        setattr(image_header, field_name, tuple(values))


def _derived_uid(*parts):
    source = ".".join(str(part) for part in parts if part is not None)
    return "2.25." + str(uuid.uuid5(uuid.NAMESPACE_URL, source).int)


def _collect_metrics_comment(output_dir):
    summaries = [_summarize_metrics_output(output) for output in _collect_metrics_outputs(output_dir)]
    return "; ".join(item for item in summaries if item)[:1000]


def _collect_metrics_outputs(output_dir):
    summaries = []
    for json_pattern, csv_pattern, label in (
        ("*_thickness_results.json", "*_thickness_results.csv", "thickness"),
        ("*_t2_results.json", None, "t2"),
        ("bscore_results.json", None, "bscore"),
    ):
        json_path = _find_single_output(output_dir, json_pattern)
        csv_path = _find_single_output(output_dir, csv_pattern) if csv_pattern else None
        payload = None
        rows = []

        if json_path is not None:
            try:
                payload = json.loads(json_path.read_text())
            except Exception:
                payload = None

        if csv_path is not None:
            rows = _read_metrics_csv_rows(csv_path)

        if payload is None and not rows:
            continue
        summaries.append(
            {
                "label": label,
                "json_path": json_path,
                "csv_path": csv_path,
                "payload": payload,
                "rows": rows,
            }
        )
    return summaries


def _read_metrics_csv_rows(path):
    try:
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        logging.warning("Failed to read OpenMSK metrics CSV %s:\n%s", path, traceback.format_exc())
        return []


def _summarize_metrics_output(metrics_output):
    label = metrics_output["label"]
    summaries = []
    summary = _summarize_json_metrics(f"{label}.json", metrics_output.get("payload"))
    if summary:
        summaries.append(summary)

    summary = _summarize_csv_metrics(f"{label}.csv", metrics_output.get("rows") or [])
    if summary:
        summaries.append(summary)

    return "; ".join(summaries)


def _summarize_csv_metrics(label, rows):
    if not rows:
        return ""
    parts = []
    for key in _csv_fieldnames(rows[0])[:6]:
        value = _coerce_float_or_none(rows[0].get(key))
        if value is not None:
            parts.append(f"{key}={value:.4g}")
    return f"{label}: " + ", ".join(parts) if parts else label


def _csv_fieldnames(row):
    return sorted(str(key) for key in row.keys() if key is not None)


def _summarize_json_metrics(label, payload):
    if not isinstance(payload, dict) or not payload:
        return ""
    parts = []
    for key in sorted(payload.keys())[:6]:
        value = payload[key]
        if isinstance(value, (int, float)):
            parts.append(f"{key}={value:.4g}")
    if not parts:
        return label
    return f"{label}: " + ", ".join(parts)


def _metrics_extra_meta(metrics_comment):
    if not metrics_comment:
        return {}
    return {
        "OpenMSKMetrics": metrics_comment,
        "DerivationDescription": metrics_comment,
    }


def _join_comments(prefix, metrics_comment):
    if prefix and metrics_comment:
        return f"{prefix} | {metrics_comment}"
    return prefix or metrics_comment or ""


def _metrics_report_rows(metrics_outputs):
    rows = []
    for metrics_output in metrics_outputs:
        label = metrics_output["label"]
        payload = metrics_output.get("payload")
        if isinstance(payload, dict):
            for key in sorted(payload.keys()):
                value = payload[key]
                if isinstance(value, (int, float)):
                    rows.append(
                        {
                            "source": f"{label}.json",
                            "metric": str(key),
                            "value": _format_metric_value(value),
                        }
                    )

        csv_rows = metrics_output.get("rows") or []
        for row_index, csv_row in enumerate(csv_rows, start=1):
            source = f"{label}.csv"
            if len(csv_rows) > 1:
                source = f"{source} row {row_index}"
            for key in _csv_fieldnames(csv_row):
                value = csv_row.get(key)
                if value not in (None, ""):
                    rows.append(
                        {
                            "source": source,
                            "metric": str(key),
                            "value": str(value),
                        }
                    )
    return rows


def _format_metric_value(value):
    if isinstance(value, (int, float)):
        return f"{float(value):.5g}"
    return str(value)


def _coerce_float_or_none(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _pil_text_size(draw, text, font):
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    except Exception:
        return draw.textsize(text, font=font)


def _truncate_text_to_width(draw, text, font, max_width):
    text = str(text)
    if _pil_text_size(draw, text, font)[0] <= max_width:
        return text
    ellipsis = "..."
    while text and _pil_text_size(draw, text + ellipsis, font)[0] > max_width:
        text = text[:-1]
    return text + ellipsis if text else ellipsis


def _render_metrics_report_pages(metrics_outputs, width, height):
    rows = _metrics_report_rows(metrics_outputs)
    if not rows:
        return []

    from PIL import Image, ImageDraw, ImageFont

    width = max(int(width), 768)
    height = max(int(height), 768)
    margin = 24
    probe = Image.new("L", (width, height), 0)
    draw_probe = ImageDraw.Draw(probe)
    font = ImageFont.load_default()
    line_height = max(14, _pil_text_size(draw_probe, "Ag", font)[1] + 7)
    title_lines = [
        "OpenMSK Metrics",
        f"Version: {OPENMSK_VERSION}",
        "Files: "
        + ", ".join(
            path.name
            for output in metrics_outputs
            for path in (output.get("json_path"), output.get("csv_path"))
            if path is not None
        ),
    ]
    table_top = margin + (len(title_lines) + 1) * line_height + 8
    footer_height = line_height + 8
    rows_per_page = max(1, (height - table_top - margin - footer_height) // line_height - 1)
    pages = []
    total_pages = max(1, (len(rows) + rows_per_page - 1) // rows_per_page)
    column_widths = [
        int((width - 2 * margin) * 0.18),
        int((width - 2 * margin) * 0.57),
        int((width - 2 * margin) * 0.25),
    ]
    headers = ("Source", "Metric", "Value")

    for page_index, start in enumerate(range(0, len(rows), rows_per_page), start=1):
        page_rows = rows[start : start + rows_per_page]
        image = Image.new("L", (width, height), 0)
        draw = ImageDraw.Draw(image)
        y = margin
        for title_line in title_lines:
            draw.text(
                (margin, y),
                _truncate_text_to_width(draw, title_line, font, width - 2 * margin),
                fill=255,
                font=font,
            )
            y += line_height
        draw.text(
            (margin, y),
            f"Page {page_index}/{total_pages}    Rows {start + 1}-{start + len(page_rows)} of {len(rows)}",
            fill=200,
            font=font,
        )

        y = table_top
        x = margin
        for header, column_width in zip(headers, column_widths):
            draw.text((x, y), header, fill=255, font=font)
            x += column_width
        y += line_height
        draw.line((margin, y - 2, width - margin, y - 2), fill=120)

        for row in page_rows:
            x = margin
            for key, column_width in zip(("source", "metric", "value"), column_widths):
                draw.text(
                    (x, y),
                    _truncate_text_to_width(draw, row.get(key, ""), font, column_width - 8),
                    fill=220,
                    font=font,
                )
                x += column_width
            y += line_height

        pages.append(_orient_metrics_report_page(np.asarray(image, dtype=np.uint16) * 16))
    return pages


def _orient_metrics_report_page(page_array):
    return np.rot90(np.asarray(page_array), 2).copy()


def _build_metrics_report_images(metrics_outputs, source_images, metrics_comment):
    if not source_images:
        return []
    rows = _metrics_report_rows(metrics_outputs)
    if not rows:
        return []

    base_header = source_images[0].getHead()
    source_matrix = np.array(base_header.matrix_size[:], dtype=float)
    source_width = int(source_matrix[0]) if source_matrix.size > 0 and source_matrix[0] > 0 else 512
    source_height = int(source_matrix[1]) if source_matrix.size > 1 and source_matrix[1] > 0 else 512

    try:
        pages = _render_metrics_report_pages(
            metrics_outputs,
            max(source_width, 768),
            max(source_height, 768),
        )
    except Exception:
        logging.warning("Failed to render OpenMSK metrics report:\n%s", traceback.format_exc())
        return []

    if not pages:
        return []

    series_uid = _derived_uid(METRICS_REPORT_SERIES_NAME, METRICS_REPORT_SERIES_INDEX)
    outputs = []
    page_count = len(pages)
    for index, page in enumerate(pages):
        page = np.ascontiguousarray(page.astype(np.uint16, copy=False))
        output = ismrmrd.Image.from_array(page, transpose=False)
        header = copy.deepcopy(base_header)
        header.data_type = output.data_type
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.image_series_index = METRICS_REPORT_SERIES_INDEX
        header.image_index = index + 1
        header.slice = index
        _set_header_sequence_field(header, "matrix_size", [page.shape[1], page.shape[0], 1])
        _set_header_sequence_field(header, "field_of_view", [float(page.shape[1]), float(page.shape[0]), float(page_count)])
        _set_header_sequence_field(header, "position", [0.0, 0.0, float(index)])
        _set_header_sequence_field(header, "read_dir", [1.0, 0.0, 0.0])
        _set_header_sequence_field(header, "phase_dir", [0.0, 1.0, 0.0])
        _set_header_sequence_field(header, "slice_dir", [0.0, 0.0, 1.0])
        output.setHead(header)
        output.image_series_index = METRICS_REPORT_SERIES_INDEX
        output.image_index = index + 1
        output.attribute_string = _derived_meta(
            source_images[0],
            METRICS_REPORT_SERIES_NAME,
            series_uid,
            METRICS_REPORT_SERIES_INDEX,
            index,
            METRICS_REPORT_IMAGE_TYPE,
            "Image",
            metrics_comment or "OpenMSK metrics report",
            float(np.nanmax(page)) if page.size else 4095.0,
            source_geometry_segment=False,
            slice_count=page_count,
            extra_meta={
                "Keep_image_geometry": "0",
                "OpenMSKMetricsRows": str(len(rows)),
                **_metrics_extra_meta(metrics_comment),
            },
        ).serialize()
        outputs.append(output)

    logging.info(
        "Created OpenMSK metrics report image series with %d page(s) in image_series_index=%d",
        page_count,
        METRICS_REPORT_SERIES_INDEX,
    )
    return outputs


def _send_images(connection, images, context):
    if not images:
        return
    batch = []
    current_series = None
    for image in images:
        series_index = int(getattr(image, "image_series_index", getattr(image.getHead(), "image_series_index", 0)))
        if batch and series_index != current_series:
            logging.info("Sending %s series=%s images=%d", context, current_series, len(batch))
            connection.send_image(batch)
            batch = []
        batch.append(image)
        current_series = series_index
    if batch:
        logging.info("Sending %s series=%s images=%d", context, current_series, len(batch))
        connection.send_image(batch)
