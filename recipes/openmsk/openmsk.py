"""OpenRecon module for OpenMSK/KneePipeline qDESS knee MRI analysis."""

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

import constants
import ismrmrd
import mrdhelper
import nibabel as nib
import numpy as np


OPENMSK_VERSION = os.environ.get("OPENMSK_VERSION", "unknown")
KNEEPIPELINE_DIR = Path(os.environ.get("KNEEPIPELINE_HOME", "/opt/KneePipeline"))
KNEEPIPELINE_CONFIG = Path(
    os.environ.get("KNEEPIPELINE_CONFIG", str(KNEEPIPELINE_DIR / "config.json"))
)
OPENMSK_PIPELINE_TIMEOUT = int(os.environ.get("OPENMSK_PIPELINE_TIMEOUT", "5400"))

ORIGINAL_SERIES_INDEX = 100
SEGMENT_SERIES_INDEX = 101
T2MAP_SERIES_INDEX = 102
SEGMENT_SERIES_NAME = "openmsk_segmentation"
T2MAP_SERIES_NAME = "openmsk_t2map"
SEGMENT_IMAGE_TYPE = f"DERIVED\\PRIMARY\\SEGMENTATION\\{SEGMENT_SERIES_NAME}"
T2MAP_IMAGE_TYPE = f"DERIVED\\PRIMARY\\M\\{T2MAP_SERIES_NAME}"
SCANNER_PARTITION_INDEX = 0
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
        seg_model = _config_str(config, "segmodel", "acl_qdess_bone_july_2024")
        run_nsm_requested = _config_bool_any(config, ("runnsm", "run_nsm"), False)
        run_bscore = _config_bool_any(config, ("runbscore", "run_bscore"), False)
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

        source_group = _select_primary_source_group(magnitude_images)
        if not source_group:
            logging.warning("No processable qDESS source group selected")
            return

        with tempfile.TemporaryDirectory(prefix="openmsk_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            input_path = tmpdir_path / "openmsk_echo1.nii.gz"
            output_dir = tmpdir_path / "out"
            output_dir.mkdir()

            ordered_sources, nifti_shape = _write_source_nifti(
                source_group,
                input_path,
                metadata,
            )
            run_config_path = _write_run_config(
                tmpdir_path,
                seg_model,
                run_nsm,
                run_bscore,
            )

            segmentation_ok = _run_kneepipeline_segmentation(
                input_path,
                output_dir,
                seg_model,
                run_config_path,
            )
            if not segmentation_ok:
                logging.warning(
                    "KneePipeline segmentation process reported an error; attempting "
                    "to return any labels already written to %s",
                    output_dir,
                )

            segment_path = _find_single_output(output_dir, "*_all-labels.nii.gz")
            if segment_path is None:
                raise FileNotFoundError(f"KneePipeline did not write *_all-labels.nii.gz in {output_dir}")

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
            )
            _send_images(connection, segment_images, "openmsk_segmentation")
            sent_images.extend(segment_images)

            postprocessing_ok = True
            if compute_thickness or run_nsm:
                postprocessing_ok = _run_kneepipeline_postprocessing(
                    output_dir,
                    run_config_path,
                    compute_thickness,
                )
                if not postprocessing_ok:
                    logging.warning(
                        "KneePipeline post-processing reported an error after "
                        "segmentation was already sent"
                    )
            else:
                logging.info("Skipping OpenMSK mesh/thickness post-processing")

            metrics_comment = _collect_metrics_comment(output_dir)
            if metrics_comment:
                logging.info("OpenMSK metrics: %s", metrics_comment)

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
                )
                _send_images(connection, t2_images, "openmsk_t2map")
                sent_images.extend(t2_images)
            else:
                logging.info(
                    "No T2 map was written. This is expected for OpenRecon MRD/NIfTI "
                    "input because qDESS GL/TG private tags are not present."
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


def _select_primary_source_group(images):
    grouped = {}
    for image in images:
        key = (
            int(getattr(image, "image_series_index", 0)),
            int(getattr(image, "average", 0)),
            int(getattr(image, "repetition", 0)),
            int(getattr(image, "set", 0)),
        )
        grouped.setdefault(key, []).append(image)

    if not grouped:
        return []

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
    return selected


def _split_echo_groups(images):
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
        capture_output=True,
        text=True,
        timeout=OPENMSK_PIPELINE_TIMEOUT,
    )
    _log_subprocess_result("KneePipeline segmentation", result)
    return result.returncode == 0


def _run_kneepipeline_postprocessing(output_dir, config_path, compute_thickness):
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

config = load_config(str(config_path))
summary = {
    "compute_thickness": compute_thickness,
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

summary["t2_mapping"] = {
    "skipped": True,
    "reason": "OpenRecon MRD path uses reconstructed NIfTI input without qDESS private tags",
}

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
    ]
    result = subprocess.run(
        cmd,
        cwd=str(KNEEPIPELINE_DIR),
        capture_output=True,
        text=True,
        timeout=OPENMSK_PIPELINE_TIMEOUT,
    )
    _log_subprocess_result("KneePipeline post-processing", result)
    return result.returncode == 0


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
            capture_output=True,
            text=True,
            timeout=1200,
        )
        _log_subprocess_result(label, result)
        return result.returncode == 0
    except Exception:
        logging.warning("%s failed; segmentation output will still be returned:\n%s", label, traceback.format_exc())
        return False


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
):
    img = nib.load(str(nifti_path))
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
        ).serialize()
        outputs.append(output)
    return outputs


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
        image_type = f"DERIVED\\PRIMARY\\M\\{type_token}"
        output.attribute_string = _derived_meta(
            image,
            series_name,
            series_uid,
            series_index,
            index,
            image_type,
            "Image",
            comment,
            _image_abs_max(image.data),
            source_geometry_segment=False,
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
    meta["WindowCenter"] = str(max_value / 2.0)
    meta["WindowWidth"] = str(max(max_value, 1.0))
    meta["partition_count"] = "1"
    meta["slice_count"] = str(slice_count)
    meta["NumberOfSlices"] = str(slice_count)
    meta["ImagesInAcquisition"] = str(slice_count)
    _set_output_storage_meta(meta, output_index)
    if source_geometry_segment:
        meta["SegmentSourceGeometry"] = "1"
        meta["SegmentOutputGeometry"] = "2d"
        meta["LUTFileName"] = "MicroDeltaHotMetal.pal"
        _strip_scanner_write_unsafe_meta(meta)
    else:
        meta["ImageTypeValue3"] = "M"
    minihead = _decode_ice_minihead(meta)
    if minihead:
        patched_minihead, changed = _patch_derived_ice_minihead(
            minihead,
            series_name,
            f"{series_name}_{series_index}",
            series_uid,
            sop_uid,
            image_type,
            source_geometry_segment=source_geometry_segment,
            output_index=output_index,
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
    summaries = []
    for pattern, label in (
        ("*_thickness_results.json", "thickness"),
        ("*_t2_results.json", "t2"),
        ("bscore_results.json", "bscore"),
    ):
        path = _find_single_output(output_dir, pattern)
        if path is None:
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        summaries.append(_summarize_json_metrics(label, payload))
    return "; ".join(item for item in summaries if item)[:1000]


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
