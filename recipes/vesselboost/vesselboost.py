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
from time import perf_counter
import traceback
import xml.dom.minidom

import ismrmrd
import nibabel as nib
import numpy as np
import numpy.fft as fft

import constants
import mrdhelper


# Folder for debug output files
debugFolder = "/tmp/share/debug"
OPENRECON_WORKSPACE_ROOT = "vesselboost_openrecon"
OPENRECON_OUTPUT_NAME_PARAM = "vboutputname"

# Keep this overview aligned with recipes/vesselboost/OpenReconLabel.json.
# Tests can import it to build a minimal OpenRecon config payload.
OPENRECON_DEFAULTS = {
    "config": "vesselboost",
    "vbmodules": "prediction",
    "vbepochs": "200",
    "vbrate": "0.001",
    "vbuseblending": False,
    "vboverlap": 50,
    "vbbiasfieldcorrection": False,
    "vbdenoising": False,
    "vbbrainextraction": False,
    "vbreslicesagittal": False,
    "vbreslicecoronal": False,
}
OPENRECON_DEFAULT_TEST_CONFIG = {
    "parameters": OPENRECON_DEFAULTS.copy(),
}

OPENRECON_COMBINATION_PARAMETER_VALUES = {
    "vbmodules": ("prediction", "tta", "booster"),
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
    module_values = modules or OPENRECON_COMBINATION_PARAMETER_VALUES["vbmodules"]
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
        parameters["vbmodules"] = module
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
    valid_modules = set(OPENRECON_COMBINATION_PARAMETER_VALUES["vbmodules"])
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
        default=",".join(OPENRECON_COMBINATION_PARAMETER_VALUES["vbmodules"]),
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


def _build_reformatted_images(
    volume_yxz: np.ndarray,
    head_template,
    meta_template,
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
        new_fov = (float(fov[1]), float(fov[2]), float(fov[0]))
    else:  # coronal
        n_slices = N_y
        new_read_dir = read_dir
        new_phase_dir = slice_dir
        new_slice_dir = phase_dir
        slice_spacing = float(voxel_size[1])
        new_fov = (float(fov[0]), float(fov[2]), float(fov[1]))

    logging.info(
        "Building %s reformat: n_slices=%d slice_spacing=%.4f new_fov=%s "
        "series_index=%d",
        orientation,
        n_slices,
        slice_spacing,
        new_fov,
        series_index,
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
        new_header.image_index = j
        new_header.image_series_index = series_index
        new_header.slice = j

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
        tmp_meta["SequenceDescriptionAdditional"] = f"VesselBoost_{orientation}"
        tmp_meta["Keep_image_geometry"] = 1
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
        mrd_image.attribute_string = tmp_meta.serialize()
        images_out.append(mrd_image)

    return images_out


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
                    connection.send_image(image)
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
                    connection.send_image(image)
                    imgGroup = []

                # Only process magnitude images -- send phase images back without modification (fallback for images with unknown type)
                if (item.image_type is ismrmrd.IMTYPE_MAGNITUDE) or (item.image_type == 0):
                    imgGroup.append(item)
                else:
                    tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                    tmpMeta['Keep_image_geometry']    = 1
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

        # Extract raw ECG waveform data. Basic sorting to make sure that data 
        # is time-ordered, but no additional checking for missing data.
        # ecgData has shape (5 x timepoints)
        if len(waveformGroup) > 0:
            waveformGroup.sort(key = lambda item: item.time_stamp)
            ecgData = [item.data for item in waveformGroup if item.waveform_id == 0]
            ecgData = np.concatenate(ecgData,1)

        # Process any remaining groups of raw or image data.  This can 
        # happen if the trigger condition for these groups are not met.
        # This is also a fallback for handling image data, as the last
        # image in a series is typically not separately flagged.
        if len(acqGroup) > 0:
            logging.info("Processing a group of k-space data (untriggered)")
            image = process_raw(acqGroup, connection, config, metadata)
            connection.send_image(image)
            acqGroup = []

        if len(imgGroup) > 0:
            logging.info("Processing a group of images (untriggered)")
            image = process_image(imgGroup, connection, config, metadata)
            connection.send_image(image)
            imgGroup = []

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
        default=OPENRECON_DEFAULTS["vbmodules"],
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
        default=OPENRECON_DEFAULTS["vbepochs"],
        type='str',
    )
    l_rate = mrdhelper.get_json_config_param(
        config,
        "vbrate",
        default=OPENRECON_DEFAULTS["vbrate"],
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

        # Unused example, as images are grouped by series before being passed into this function now
        # oldHeader.image_series_index = currentSeries

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], 'IceMiniHead') is not None:
            if mrdhelper.extract_minihead_bool_param(base64.b64decode(meta[iImg]['IceMiniHead']).decode('utf-8'), 'BIsSeriesEnd') is True:
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]
        tmpMeta['DataRole']                       = 'Image'
        tmpMeta['ImageProcessingHistory']         = ['PYTHON', 'VESSELBOOST']
        tmpMeta['WindowCenter']                   = str((maxVal+1)/2)
        tmpMeta['WindowWidth']                    = str((maxVal+1))
        tmpMeta['SequenceDescriptionAdditional']  = 'VesselBoost'
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
        base_series = max(
            (int(getattr(h, "image_series_index", 0)) for h in head),
            default=0,
        ) + 1
        meta_template = meta[0] if meta else None

        if reslice_sagittal:
            logging.info(
                "Appending sagittal reformat output series (series_index=%d)",
                base_series,
            )
            imagesOut.extend(
                _build_reformatted_images(
                    volume_yxz=volume_yxz,
                    head_template=head[0],
                    meta_template=meta_template,
                    voxel_size=voxel_size,
                    fov=fov,
                    orientation="sagittal",
                    series_index=base_series,
                    max_val=maxVal,
                )
            )
            base_series += 1

        if reslice_coronal:
            logging.info(
                "Appending coronal reformat output series (series_index=%d)",
                base_series,
            )
            imagesOut.extend(
                _build_reformatted_images(
                    volume_yxz=volume_yxz,
                    head_template=head[0],
                    meta_template=meta_template,
                    voxel_size=voxel_size,
                    fov=fov,
                    orientation="coronal",
                    series_index=base_series,
                    max_val=maxVal,
                )
            )

    return imagesOut


if __name__ == "__main__":
    raise SystemExit(_main())
