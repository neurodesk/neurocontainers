#!/usr/bin/env python3
"""OpenRecon entrypoint for multi-echo palindromic TPI gridding.

This module adapts ``ptpi_dual_echo_lowrank_core.py`` from its standalone
file-based workflow into the same MRD streaming contract used by
``sodiumgridding``. The reconstruction math stays in the core module; this file
only resolves OpenRecon config, converts incoming acquisitions into the
interleaved echo array layout, and emits one magnitude volume per echo.
"""

import logging
import os
from pathlib import Path
from time import perf_counter
import traceback

import h5py
import ismrmrd
import numpy as np

import constants
import sodiumgridding as openrecon_base
import ptpi_dual_echo_lowrank_core as core


DEFAULT_TRAJECTORY_FILE = "/opt/sodiumgridding_ptpi/23Na_pTPI_n28_g50_p5traj.h5"
OUTPUT_SERIES_BASE = "pTPI"
debugFolder = "/tmp/share/debug"

OPENRECON_DEFAULTS = {
    "config": "sodiumgridding_ptpi",
    "matrixsize": 128,
    "fovcm": 22.0,
    "trajectoryfile": DEFAULT_TRAJECTORY_FILE,
    "trajectorydataset": "k",
    "numechoes": 2,
    "dcfiterations": 5,
    "maxcoils": 0,
    "maxworkers": 8,
    "coilcompression": True,
    "coilvarianceretention": 0.9,
    "coilcompressionsource": "both",
    "coilcombinemode": "AC",
    "echonormalizationmode": "none",
    "fieldmapdeltates": 0.005,
    "fieldmapunwrapmethod": "skimage",
    "runphasecorrection": True,
    "phaselowrankrank": 10,
    "applyn4biascorrection": True,
    "denoisetemporalsvd": True,
    "svdpatchsize": 7,
    "svdstride": 4,
    "svdretainfraction": 0.2,
    "sharedechodisplayscale": True,
    "orientation": "zyx",
    "orientationflipslice": False,
    "orientationdebugseries": False,
}


def _ensure_debug_folder():
    os.makedirs(debugFolder, exist_ok=True)


def _config_bool(config, key):
    return openrecon_base._config_bool(config, key, OPENRECON_DEFAULTS[key])


def _config_int(config, key):
    return openrecon_base._config_int(config, key, OPENRECON_DEFAULTS[key])


def _config_float(config, key):
    return openrecon_base._config_float(config, key, OPENRECON_DEFAULTS[key])


def _config_str(config, key):
    return openrecon_base._config_str(config, key, OPENRECON_DEFAULTS[key])


def _load_base_trajectories(config):
    trajectory_file = _config_str(config, "trajectoryfile").strip()
    if not trajectory_file:
        trajectory_file = DEFAULT_TRAJECTORY_FILE
    trajectory_dataset = _config_str(config, "trajectorydataset").strip() or "k"

    path = Path(trajectory_file).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"pTPI trajectory file does not exist: {path}")

    with h5py.File(path, "r") as h5_file:
        if "k_echo1" in h5_file and "k_echo2_forward_order" in h5_file:
            k_cm_te1 = h5_file["k_echo1"][...]
            k_cm_te2 = h5_file["k_echo2_forward_order"][...]
            trajectory_mode = "forward/reverse echo trajectories"
        elif trajectory_dataset in h5_file:
            k_cm_te1 = h5_file[trajectory_dataset][...]
            k_cm_te2 = k_cm_te1
            trajectory_mode = f"single-trajectory fallback [{trajectory_dataset}]"
        else:
            raise KeyError(
                f"Trajectory dataset '{trajectory_dataset}' not found in {path}. "
                f"Available datasets: {list(h5_file.keys())}"
            )

        if core.TE1_PHASE_TIME_DATASET in h5_file:
            t_te1 = h5_file[core.TE1_PHASE_TIME_DATASET][...].astype(np.float32)
            te1_time_mode = core.TE1_PHASE_TIME_DATASET
        else:
            sample_source = trajectory_dataset if trajectory_dataset in h5_file else next(iter(h5_file.keys()))
            dt_s = float(h5_file[sample_source].attrs.get("sampling_time_us", 10.0)) * 1e-6
            t_te1 = np.arange(k_cm_te1.shape[0], dtype=np.float32) * dt_s
            te1_time_mode = f"inferred +{dt_s:.3e} s/sample"

        if core.TE2_PHASE_TIME_DATASET in h5_file:
            t_te2 = h5_file[core.TE2_PHASE_TIME_DATASET][...].astype(np.float32)
            te2_time_mode = core.TE2_PHASE_TIME_DATASET
        else:
            sample_source = trajectory_dataset if trajectory_dataset in h5_file else next(iter(h5_file.keys()))
            dt_s = float(h5_file[sample_source].attrs.get("sampling_time_us", 10.0)) * 1e-6
            t_te2 = -np.arange(k_cm_te2.shape[0], dtype=np.float32) * dt_s
            te2_time_mode = f"inferred {-dt_s:.3e} s/sample"

    logging.info(
        "Loaded pTPI trajectory from %s: mode=%s te1_time=%s te2_time=%s "
        "k_te1=%s k_te2=%s t_te1=%s t_te2=%s",
        path,
        trajectory_mode,
        te1_time_mode,
        te2_time_mode,
        k_cm_te1.shape,
        k_cm_te2.shape,
        t_te1.shape,
        t_te2.shape,
    )
    return (
        openrecon_base._normalize_trajectory_array(k_cm_te1),
        openrecon_base._normalize_trajectory_array(k_cm_te2),
        np.asarray(t_te1, dtype=np.float32),
        np.asarray(t_te2, dtype=np.float32),
    )


def _metadata_contrast_count(metadata):
    try:
        contrast = metadata.encoding[0].encodingLimits.contrast
        return int(contrast.maximum) - int(contrast.minimum) + 1
    except Exception:
        return 0


def _acquisition_contrast(acquisition):
    try:
        return int(acquisition.getHead().idx.contrast)
    except Exception:
        return 0


def _detect_echo_count(acquisitions, metadata, config):
    header_contrasts = [_acquisition_contrast(item) for item in acquisitions]
    if header_contrasts:
        header_count = max(header_contrasts) + 1
        if header_count > 1:
            return header_count, "acquisition contrast index"

    metadata_count = _metadata_contrast_count(metadata)
    if metadata_count > 0:
        return metadata_count, "MRD encodingLimits.contrast"

    configured_count = max(1, _config_int(config, "numechoes"))
    return configured_count, "config/default numechoes"


def _build_interleaved_data_array(acquisitions):
    """Return raw data as (coils, samples, acquisitions)."""
    base_data = openrecon_base._build_data_array(acquisitions)
    return np.ascontiguousarray(base_data.transpose(0, 2, 1))


def _split_echo_data(data_all, echo_count):
    echo_count = int(echo_count)
    if echo_count < 1:
        raise ValueError(f"numechoes must be at least 1, got {echo_count}")
    if data_all.shape[2] < echo_count:
        raise ValueError(
            "Multi-echo pTPI reconstruction needs at least one acquisition per echo; "
            f"got {data_all.shape[2]} acquisition(s) for {echo_count} echoes."
        )
    if data_all.shape[2] % echo_count:
        logging.warning(
            "Incoming pTPI readout count %d is not divisible by detected echo count %d; "
            "later echoes may have one fewer readout after interleaved splitting.",
            data_all.shape[2],
            echo_count,
        )

    echo_data = []
    for echo_index in range(echo_count):
        data_echo = data_all[:, :, echo_index::echo_count].copy()
        # Echoes 2, 4, 6, ... are acquired in reverse sample order.
        if echo_index % 2 == 1:
            data_echo = data_echo[:, ::-1, :]
        echo_data.append(data_echo)
    return echo_data


def _split_dual_echo_data(data_all):
    echo_data = _split_echo_data(data_all, 2)
    return echo_data[0], echo_data[1]


def _echo_label(echo_index):
    return f"TE{int(echo_index) + 1}"


def _series_index_for_echo(echo_index, output_repetition_index, total_repetitions):
    series_offset = int(output_repetition_index) if int(total_repetitions) > 1 else 0
    if int(echo_index) == 0:
        return 1 + series_offset
    return 1000 * int(echo_index) + 1 + series_offset


def _echo_sample_times(base_times, echo_index):
    base_times = np.asarray(base_times, dtype=np.float32)
    pair_offset = 2 * (int(echo_index) // 2) * float(core.FIELD_MAP_DELTA_TE_S)
    if pair_offset == 0:
        return base_times.copy()
    return (base_times + np.float32(pair_offset)).astype(np.float32, copy=False)


def _sequence_parameter_values(metadata, name):
    try:
        values = getattr(metadata.sequenceParameters, name)
    except Exception:
        return []
    if values is None:
        return []
    if isinstance(values, (str, bytes)):
        values = [values]
    try:
        return [float(value) for value in values]
    except TypeError:
        return [float(values)]
    except ValueError:
        return []


def _metadata_echo_times_s(metadata, echo_count, delta_te_s):
    values = _sequence_parameter_values(metadata, "TE")
    values = [value for value in values if np.isfinite(value) and value >= 0.0]
    if not values:
        echo_times = np.arange(echo_count, dtype=np.float32) * np.float32(delta_te_s)
        return echo_times, "fallback delta TE offsets"

    # ISMRMRD sequenceParameters.TE is normally in milliseconds. Some Siemens
    # mappings expose alTE-style microseconds, and some tools pass seconds.
    max_value = max(values)
    if max_value > 100.0:
        scale = 1e-6
    elif max_value >= 0.1:
        scale = 1e-3
    else:
        scale = 1.0
    values_s = np.asarray(values, dtype=np.float32) * np.float32(scale)
    if values_s.size >= echo_count:
        return values_s[:echo_count], "MRD sequenceParameters.TE"

    echo_times = values_s[0] + np.arange(echo_count, dtype=np.float32) * np.float32(delta_te_s)
    return echo_times, "MRD first TE plus configured delta TE"


def _patch_starts(n, patch_size, stride):
    n = int(n)
    patch_size = int(patch_size)
    stride = int(stride)
    if n <= patch_size:
        return [0]
    starts = list(range(0, n - patch_size + 1, stride))
    final_start = n - patch_size
    if starts[-1] != final_start:
        starts.append(final_start)
    return starts


def _denoise_temporal_svd_final_images(
    echo_images,
    patch_size,
    stride,
    retain_fraction,
):
    if len(echo_images) < 2:
        logging.info("Temporal SVD denoising skipped: need at least two echoes")
        return list(echo_images)

    combined_echoes = np.asarray(echo_images, dtype=np.float32)
    n_echoes, nx, ny, nz = combined_echoes.shape
    patch_size = int(patch_size)
    stride = int(stride)
    retain_fraction = float(retain_fraction)
    if patch_size < 2:
        raise ValueError("svdpatchsize must be >= 2")
    if stride < 1:
        raise ValueError("svdstride must be >= 1")
    if not (0.0 < retain_fraction <= 1.0):
        raise ValueError("svdretainfraction must be > 0 and <= 1")

    patch_size = min(patch_size, nx, ny, nz)
    xs = _patch_starts(nx, patch_size, stride)
    ys = _patch_starts(ny, patch_size, stride)
    zs = _patch_starts(nz, patch_size, stride)
    total_patches = len(xs) * len(ys) * len(zs)
    logging.info(
        "Temporal SVD denoising final magnitude images: patch=%d^3 stride=%d "
        "contrasts=%d patches=%d singular_value_threshold_fraction=%.3g",
        patch_size,
        stride,
        n_echoes,
        total_patches,
        retain_fraction,
    )

    accum = np.zeros_like(combined_echoes, dtype=np.float32)
    weights = np.zeros((nx, ny, nz), dtype=np.float32)
    rank_sum = 0.0
    patch_counter = 0

    for ix in xs:
        for iy in ys:
            for iz in zs:
                patch = combined_echoes[
                    :,
                    ix : ix + patch_size,
                    iy : iy + patch_size,
                    iz : iz + patch_size,
                ]
                x = patch.reshape(n_echoes, -1).T.astype(np.float32, copy=False)
                if not np.all(np.isfinite(x)):
                    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
                try:
                    u, s, vt = np.linalg.svd(x, full_matrices=False)
                except np.linalg.LinAlgError:
                    x_denoised = x
                    rank = n_echoes
                else:
                    if s.size:
                        threshold = retain_fraction * float(s[0])
                        rank = max(1, int(np.count_nonzero(s >= threshold)))
                    else:
                        rank = 0
                    if rank >= s.size:
                        x_denoised = x
                    else:
                        x_denoised = (u[:, :rank] * s[:rank]) @ vt[:rank, :]

                accum[
                    :,
                    ix : ix + patch_size,
                    iy : iy + patch_size,
                    iz : iz + patch_size,
                ] += x_denoised.T.reshape(
                    n_echoes,
                    patch_size,
                    patch_size,
                    patch_size,
                ).astype(np.float32, copy=False)
                weights[
                    ix : ix + patch_size,
                    iy : iy + patch_size,
                    iz : iz + patch_size,
                ] += 1.0

                rank_sum += float(rank)
                patch_counter += 1
                if patch_counter == 1 or patch_counter % 5000 == 0 or patch_counter == total_patches:
                    logging.info(
                        "Temporal SVD patches %d/%d, mean retained rank=%.2f",
                        patch_counter,
                        total_patches,
                        rank_sum / patch_counter,
                    )

    denoised = accum / np.maximum(weights[None, ...], 1.0)
    denoised = np.maximum(denoised, 0.0).astype(np.float32, copy=False)
    finite_signal = combined_echoes[np.isfinite(combined_echoes)]
    signal_scale = float(np.percentile(finite_signal, 99.0)) if finite_signal.size else 1.0
    abs_diff = np.abs(denoised - combined_echoes)
    logging.info(
        "Temporal SVD denoising complete; mean retained rank=%.2f, "
        "mean_abs_change=%.4g, p95_abs_change=%.4g, change_vs_p99_signal=%.3f%%",
        rank_sum / max(patch_counter, 1),
        float(np.mean(abs_diff)),
        float(np.percentile(abs_diff, 95.0)),
        100.0 * float(np.mean(abs_diff)) / max(signal_scale, 1e-12),
    )
    return [denoised[index] for index in range(n_echoes)]


def _shared_echo_display_range(echo_images):
    finite_values = [
        np.asarray(image, dtype=np.float32)[np.isfinite(image)]
        for image in echo_images
    ]
    finite_values = [values for values in finite_values if values.size]
    if not finite_values:
        return None, None
    finite = np.concatenate(finite_values)
    input_min = float(np.min(finite))
    input_max = float(np.max(finite))
    if not np.isfinite(input_max - input_min) or input_max <= input_min:
        return None, None
    return input_min, input_max


def _clip_echo_inputs(echo_inputs):
    samples = min(
        min(echo_input["k"].shape[0], echo_input["t"].shape[0], echo_input["data"].shape[1])
        for echo_input in echo_inputs
    )
    readouts = min(
        min(echo_input["k"].shape[1], echo_input["data"].shape[2])
        for echo_input in echo_inputs
    )
    if samples <= 0 or readouts <= 0:
        raise ValueError("No overlapping pTPI samples/readouts between data and trajectory")

    clipped = []
    for echo_input in echo_inputs:
        item = dict(echo_input)
        item["k"] = echo_input["k"][:samples, :readouts, :]
        item["t"] = echo_input["t"][:samples]
        item["data"] = echo_input["data"][:, :samples, :readouts]
        clipped.append(item)
    return clipped


def _clip_to_common_shape(k_te1, k_te2, t_te1, t_te2, data_te1, data_te2):
    clipped = _clip_echo_inputs(
        [
            {"k": k_te1, "t": t_te1, "data": data_te1},
            {"k": k_te2, "t": t_te2, "data": data_te2},
        ]
    )

    return (
        clipped[0]["k"],
        clipped[1]["k"],
        clipped[0]["t"],
        clipped[1]["t"],
        clipped[0]["data"],
        clipped[1]["data"],
    )


def _configure_core(config, matrix_size, fov_cm):
    core.N = int(matrix_size)
    core.FOV_CM = float(fov_cm)
    core.DCF_ITER = max(0, _config_int(config, "dcfiterations"))
    core.MAX_WORKERS = max(1, _config_int(config, "maxworkers"))
    core.COIL_COMPRESSION = _config_bool(config, "coilcompression")
    core.COIL_VARIANCE_RETENTION = float(
        np.clip(_config_float(config, "coilvarianceretention"), 0.0, 1.0)
    )
    core.COIL_COMPRESSION_SOURCE = _config_str(config, "coilcompressionsource")
    core.COIL_COMBINE_MODE = _config_str(config, "coilcombinemode")
    core.ECHO_NORMALIZATION_MODE = _config_str(config, "echonormalizationmode")
    core.FIELD_MAP_DELTA_TE_S = _config_float(config, "fieldmapdeltates")
    core.FIELD_MAP_UNWRAP_METHOD = _config_str(config, "fieldmapunwrapmethod")
    core.RUN_TE1_PHASE_CORRECTION = _config_bool(config, "runphasecorrection")
    core.RUN_TE2_PHASE_CORRECTION = _config_bool(config, "runphasecorrection")
    core.TE2_PHASE_LOWRANK_RANK = max(1, _config_int(config, "phaselowrankrank"))
    core.APPLY_N4_BIAS_CORRECTION = _config_bool(config, "applyn4biascorrection")
    core.PLOT_RESULTS = False
    core.SAVE_C0_FILES = False
    core.SAVE_NIFTI_FILES = False


def _build_echo_images(
    volume,
    echo_label,
    echo_time_s,
    series_index,
    echo_index,
    total_echoes,
    repetition_index,
    total_repetitions,
    output_repetition_index,
    reference_head,
    metadata,
    output_fov_mm,
    orientation,
    orientation_flip_slice,
    orientation_debug_series,
    display_input_min=None,
    display_input_max=None,
):
    old_description = openrecon_base.OUTPUT_SERIES_DESCRIPTION
    old_index = openrecon_base.OUTPUT_IMAGE_SERIES_INDEX
    old_prefix_protocol = getattr(openrecon_base, "OUTPUT_SERIES_PREFIX_PROTOCOL", True)
    try:
        repetition_suffix = (
            f"_rep{int(output_repetition_index) + 1}"
            if int(total_repetitions) > 1
            else ""
        )
        openrecon_base.OUTPUT_SERIES_DESCRIPTION = (
            f"{OUTPUT_SERIES_BASE}_{echo_label}{repetition_suffix}"
        )
        openrecon_base.OUTPUT_IMAGE_SERIES_INDEX = int(series_index)
        openrecon_base.OUTPUT_SERIES_PREFIX_PROTOCOL = False
        return openrecon_base._build_output_images(
            np.abs(volume).astype(np.float32, copy=False),
            reference_head,
            metadata,
            output_fov_mm=output_fov_mm,
            orientation=orientation,
            flip_slice=orientation_flip_slice,
            emit_debug_series=orientation_debug_series,
            echo_index=echo_index,
            total_echoes=total_echoes,
            repetition_index=repetition_index,
            total_repetitions=total_repetitions,
            echo_time_s=echo_time_s,
            display_input_min=display_input_min,
            display_input_max=display_input_max,
        )
    finally:
        openrecon_base.OUTPUT_SERIES_DESCRIPTION = old_description
        openrecon_base.OUTPUT_IMAGE_SERIES_INDEX = old_index
        openrecon_base.OUTPUT_SERIES_PREFIX_PROTOCOL = old_prefix_protocol


def _build_echo_normalization_scales(echo_data, bad_readouts_by_coil=None):
    if len(echo_data) == 2:
        return list(
            core.build_echo_normalization_scales(
                echo_data[0],
                echo_data[1],
                bad_readouts_by_coil,
            )
        )

    mode = core.ECHO_NORMALIZATION_MODE.lower()
    n_coils = echo_data[0].shape[0]
    if mode in ("none", "off", "false"):
        logging.info("Echo normalization: disabled")
        return [np.ones(n_coils, dtype=np.float32) for _echo in echo_data]

    if mode not in ("te1", "shared", "independent"):
        raise ValueError("ECHO_NORMALIZATION_MODE must be 'te1', 'independent', or 'none'")

    scales = []
    reference_scales = np.ones(n_coils, dtype=np.float32)
    center_medians = []
    for echo_index, data_echo in enumerate(echo_data):
        echo_scales = np.ones(n_coils, dtype=np.float32)
        echo_centers = np.zeros(n_coils, dtype=np.float32)
        for coil in range(n_coils):
            bad = None if bad_readouts_by_coil is None else bad_readouts_by_coil[coil]
            scale, center = core.echo_center_normalization_scale(data_echo[coil], bad)
            echo_centers[coil] = center
            if echo_index == 0:
                reference_scales[coil] = scale
                echo_scales[coil] = scale
            elif mode == "independent":
                echo_scales[coil] = scale
            else:
                echo_scales[coil] = reference_scales[coil]
        scales.append(echo_scales)
        center_medians.append(float(np.nanmedian(echo_centers)))

    logging.info(
        "Echo normalization: mode=%s center_medians=%s scale_medians=%s",
        core.ECHO_NORMALIZATION_MODE,
        ", ".join(f"{value:.4g}" for value in center_medians),
        ", ".join(f"{float(np.nanmedian(value)):.4g}" for value in scales),
    )
    return scales


def _phase_correction_sign(echo_index):
    if int(echo_index) == 0:
        return core.TE1_PHASE_CORRECTION_SIGN
    return core.TE2_PHASE_CORRECTION_SIGN


def _phase_correction_enabled(echo_index):
    if int(echo_index) == 0:
        return bool(core.RUN_TE1_PHASE_CORRECTION)
    return bool(core.RUN_TE2_PHASE_CORRECTION)


def _apply_n4_bias_correction_to_echoes(echo_images):
    if not core.APPLY_N4_BIAS_CORRECTION:
        return list(echo_images), None, None
    if len(echo_images) < 2:
        image = np.asarray(echo_images[0])
        reference_mag = np.abs(image).astype(np.float32, copy=False)
        mask = core.make_bias_mask(reference_mag)
        logging.info(
            "Preparing single-echo SimpleITK N4 bias correction: mask_voxels=%d",
            int(mask.sum()),
        )
        bias_field, mask = core.estimate_bias_field_sitk_n4(reference_mag, mask)
        corrected_image, output_mask = core.apply_bias_field_to_image(
            image,
            bias_field,
            mask,
            True,
        )
        return [corrected_image], bias_field, output_mask

    corrected_te1, corrected_te2, bias_field, output_mask = core.apply_n4_bias_correction(
        echo_images[0],
        echo_images[1],
    )
    corrected = [corrected_te1, corrected_te2]
    if len(echo_images) == 2:
        return corrected, bias_field, output_mask

    apply_to = core.N4_APPLY_TO_ECHOES.lower()
    correct_later_echoes = apply_to == "both"
    for image in echo_images[2:]:
        corrected_image, output_mask = core.apply_bias_field_to_image(
            image,
            bias_field,
            output_mask,
            correct_later_echoes,
        )
        corrected.append(corrected_image)
    return corrected, bias_field, output_mask


def _acquisition_repetition(acquisition):
    try:
        return int(acquisition.getHead().idx.repetition)
    except Exception:
        return 0


def _split_acquisitions_by_repetition(acquisitions):
    groups = {}
    for acquisition in acquisitions:
        groups.setdefault(_acquisition_repetition(acquisition), []).append(acquisition)
    return list(groups.items())


def _process_acquisitions(acquisitions, connection, config, metadata):
    repetition_groups = _split_acquisitions_by_repetition(acquisitions)
    if len(repetition_groups) > 1:
        logging.info(
            "Detected %d repetitions in incoming pTPI measurement: %s",
            len(repetition_groups),
            ", ".join(
                f"rep{repetition_index}={len(group)} readouts"
                for repetition_index, group in repetition_groups
            ),
        )

    images = []
    total_repetitions = len(repetition_groups)
    for output_repetition_index, (repetition_index, repetition_group) in enumerate(
        repetition_groups
    ):
        logging.info(
            "Processing repetition %d/%d (MRD repetition=%d, %d readouts)",
            output_repetition_index + 1,
            total_repetitions,
            repetition_index,
            len(repetition_group),
        )
        images.extend(
            process_raw(
                repetition_group,
                connection,
                config,
                metadata,
                repetition_index=repetition_index,
                total_repetitions=total_repetitions,
                output_repetition_index=output_repetition_index,
            )
        )
    return images


def process(connection, config, metadata):
    logging.info("Config:\n%s", config)
    acquisitions = []
    passthrough_images = []
    try:
        for item in connection:
            if isinstance(item, ismrmrd.Acquisition):
                if (
                    not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)
                ):
                    acquisitions.append(item)

                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_MEASUREMENT):
                    logging.info("Processing %d acquired readouts", len(acquisitions))
                    connection.send_image(
                        _process_acquisitions(acquisitions, connection, config, metadata)
                    )
                    acquisitions = []
            elif isinstance(item, ismrmrd.Image):
                passthrough_images.append(item)
            elif item is None:
                break
            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        if acquisitions:
            logging.info("Processing %d acquired readouts (end of stream)", len(acquisitions))
            connection.send_image(
                _process_acquisitions(acquisitions, connection, config, metadata)
            )

        if passthrough_images:
            logging.warning(
                "Received %d images instead of raw data; returning them unchanged",
                len(passthrough_images),
            )
            connection.send_image(
                openrecon_base.process_image(passthrough_images, connection, config, metadata)
            )
    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
    finally:
        connection.send_close()


def process_raw(
    group,
    connection,
    config,
    metadata,
    repetition_index=0,
    total_repetitions=1,
    output_repetition_index=0,
):
    if not group:
        return []

    tic = perf_counter()
    _ensure_debug_folder()

    matrix_size = max(1, openrecon_base._config_int(
        config,
        "matrixsize",
        openrecon_base._compute_default_matrix_size(metadata),
    ))
    fov_cm = openrecon_base._config_float(
        config,
        "fovcm",
        openrecon_base._compute_default_fov_cm(metadata),
    )
    if fov_cm <= 0:
        raise ValueError(f"fovcm must be positive, got {fov_cm}")

    _configure_core(config, matrix_size, fov_cm)
    denoise_temporal_svd = _config_bool(config, "denoisetemporalsvd")
    svd_patch_size = _config_int(config, "svdpatchsize")
    svd_stride = _config_int(config, "svdstride")
    svd_retain_fraction = _config_float(config, "svdretainfraction")
    shared_echo_display_scale = _config_bool(config, "sharedechodisplayscale")
    orientation, orientation_debug_series = openrecon_base._resolve_orientation_config(config)
    orientation_flip_slice = openrecon_base._config_bool(
        config,
        "orientationflipslice",
        OPENRECON_DEFAULTS["orientationflipslice"],
    )

    logging.info(
        "Resolved pTPI configuration: matrixsize=%d fovcm=%.3f dcfiterations=%d "
        "maxworkers=%d coilcompression=%s coilvarianceretention=%.3f "
        "coilcompressionsource=%s coilcombinemode=%s echonormalizationmode=%s "
        "fieldmapdeltates=%.6f fieldmapunwrapmethod=%s runphasecorrection=%s "
        "phaselowrankrank=%d applyn4biascorrection=%s denoisetemporalsvd=%s "
        "svdpatchsize=%d svdstride=%d svdretainfraction=%.3g "
        "sharedechodisplayscale=%s orientation=%s "
        "orientationflipslice=%s orientationdebugseries=%s",
        core.N,
        core.FOV_CM,
        core.DCF_ITER,
        core.MAX_WORKERS,
        core.COIL_COMPRESSION,
        core.COIL_VARIANCE_RETENTION,
        core.COIL_COMPRESSION_SOURCE,
        core.COIL_COMBINE_MODE,
        core.ECHO_NORMALIZATION_MODE,
        core.FIELD_MAP_DELTA_TE_S,
        core.FIELD_MAP_UNWRAP_METHOD,
        core.RUN_TE2_PHASE_CORRECTION,
        core.TE2_PHASE_LOWRANK_RANK,
        core.APPLY_N4_BIAS_CORRECTION,
        denoise_temporal_svd,
        svd_patch_size,
        svd_stride,
        svd_retain_fraction,
        shared_echo_display_scale,
        orientation,
        orientation_flip_slice,
        orientation_debug_series,
    )

    data_all = _build_interleaved_data_array(group)
    max_coils = max(0, _config_int(config, "maxcoils"))
    if 0 < max_coils < data_all.shape[0]:
        logging.warning("Limiting reconstruction to first %d of %d coils", max_coils, data_all.shape[0])
        data_all = data_all[:max_coils]

    data_all = core.compress_coils_covariance(data_all)
    echo_count, echo_count_source = _detect_echo_count(group, metadata, config)
    single_echo_mode = echo_count < 2
    if single_echo_mode:
        logging.info(
            "Single-echo pTPI input detected; field-map phase correction, "
            "and temporal SVD denoising require at least two echoes and will "
            "be skipped. N4 bias correction can still run from TE1."
        )
    effective_denoise_temporal_svd = denoise_temporal_svd and not single_echo_mode
    echo_times_s, echo_time_source = _metadata_echo_times_s(
        metadata,
        echo_count,
        core.FIELD_MAP_DELTA_TE_S,
    )
    echo_data_raw = _split_echo_data(data_all, echo_count)
    k_cm_forward, k_cm_reverse, t_forward_s, t_reverse_s = _load_base_trajectories(config)
    echo_inputs = []
    for echo_index, data_echo in enumerate(echo_data_raw):
        is_reverse = echo_index % 2 == 1
        echo_inputs.append(
            {
                "echo_index": echo_index,
                "label": _echo_label(echo_index),
                "echo_time_s": float(echo_times_s[echo_index]),
                "data": data_echo,
                "k": k_cm_reverse if is_reverse else k_cm_forward,
                "t": _echo_sample_times(t_reverse_s if is_reverse else t_forward_s, echo_index),
                "trajectory_parity": "reverse" if is_reverse else "forward",
            }
        )
    echo_inputs = _clip_echo_inputs(echo_inputs)

    logging.info(
        "Detected %d pTPI echoes from %s; echo times from %s: %s; "
        "input after clipping: coils=%d samples=%d readouts=%d trajectories=%s",
        echo_count,
        echo_count_source,
        echo_time_source,
        ", ".join(f"{value * 1e3:.3f} ms" for value in echo_times_s),
        echo_inputs[0]["data"].shape[0],
        echo_inputs[0]["data"].shape[1],
        echo_inputs[0]["data"].shape[2],
        ", ".join(
            f"{item['label']}:{item['trajectory_parity']} k={item['k'].shape} "
            f"t={item['t'].shape} data={item['data'].shape}"
            for item in echo_inputs
        ),
    )
    openrecon_base._log_trajectory_extent(echo_inputs[0]["k"])
    if len(echo_inputs) > 1:
        openrecon_base._log_trajectory_extent(echo_inputs[1]["k"])

    k_nyquist = 1.0 / (2.0 * fov_cm / matrix_size)
    kb_beta = core.default_kb_beta(core.KB_WIDTH, core.OVERSAMP) if core.KB_BETA is None else float(core.KB_BETA)
    logging.info("Kaiser-Bessel beta=%.4f LUT size=%d", kb_beta, core.KB_LUT_SIZE)
    kernel_lut = core.make_kaiser_bessel_lut(core.KB_WIDTH, kb_beta, core.KB_LUT_SIZE)
    deapo = core.compute_deapodisation_kb(matrix_size, core.OVERSAMP, kernel_lut, core.KB_WIDTH)

    dcf_cache = {}
    for echo_input in echo_inputs:
        echo_input["k"] = core.apply_trajectory_delay_correction(
            echo_input["k"],
            echo_input["t"],
            echo_input["label"],
        )
        echo_input["k_norm"] = (
            echo_input["k"].reshape(-1, 3) / k_nyquist / 2.0
        ).astype(np.float32)
        cache_key = (echo_input["trajectory_parity"], echo_input["k"].shape)
        if cache_key not in dcf_cache:
            logging.info(
                "Preparing %s trajectory weights from %s",
                echo_input["trajectory_parity"],
                echo_input["label"],
            )
            dcf_cache[cache_key] = core.compute_dcf_kb(
                echo_input["k_norm"],
                matrix_size,
                core.OVERSAMP,
                core.DCF_ITER,
                kernel_lut,
                core.KB_WIDTH,
            )
        echo_input["dcf"] = dcf_cache[cache_key]

    first_echo_bad_readouts_by_coil = None
    if core.USE_TE1_BAD_READOUTS_FOR_TE2:
        first_echo_bad_readouts_by_coil = core.find_bad_readouts_by_coil(
            echo_inputs[0]["data"],
            echo_inputs[0]["label"],
        )

    normalization_scales = _build_echo_normalization_scales(
        [item["data"] for item in echo_inputs],
        first_echo_bad_readouts_by_coil,
    )

    uncorrected_coil_images = []
    for echo_input, scales in zip(echo_inputs, normalization_scales):
        uncorrected_coil_images.append(
            core.reconstruct_echo_all_coils(
                echo_input["data"],
                echo_input["k"],
                echo_input["k_norm"],
                echo_input["dcf"],
                deapo,
                kernel_lut,
                echo_input["label"],
                bad_readouts_by_coil=first_echo_bad_readouts_by_coil,
                bad_readout_source="TE1 precomputed",
                normalization_scales=scales,
            )
        )

    field_map_hz = None
    if len(uncorrected_coil_images) >= 2:
        field_map_hz = core.compute_field_map_from_coils(
            uncorrected_coil_images[0],
            uncorrected_coil_images[1],
        )
    elif core.RUN_TE1_PHASE_CORRECTION or core.RUN_TE2_PHASE_CORRECTION:
        logging.info("Phase correction skipped: at least two echoes are required")

    output_coil_images = []
    for echo_input, coil_images, scales in zip(
        echo_inputs,
        uncorrected_coil_images,
        normalization_scales,
    ):
        if field_map_hz is not None and _phase_correction_enabled(echo_input["echo_index"]):
            output_coil_images.append(
                core.reconstruct_phase_corrected_all_coils(
                    echo_input["data"],
                    echo_input["k"],
                    echo_input["k_norm"],
                    echo_input["dcf"],
                    deapo,
                    kernel_lut,
                    field_map_hz,
                    echo_input["t"],
                    _phase_correction_sign(echo_input["echo_index"]),
                    echo_input["label"],
                    bad_readouts_by_coil=first_echo_bad_readouts_by_coil,
                    bad_readout_source="TE1 precomputed",
                    normalization_scales=scales,
                )
            )
        else:
            output_coil_images.append(coil_images)

    uncorrected_echo_images = [
        core.combine_single_echo_coils(coil_images, mode=core.COIL_COMBINE_MODE)
        for coil_images in uncorrected_coil_images
    ]
    phase_corrected_echo_images = [
        core.combine_single_echo_coils(coil_images, mode=core.COIL_COMBINE_MODE)
        for coil_images in output_coil_images
    ]
    final_echo_images, bias_field, bias_mask = _apply_n4_bias_correction_to_echoes(
        phase_corrected_echo_images
    )
    if effective_denoise_temporal_svd:
        pre_denoise_echo_images = [image.copy() for image in final_echo_images]
        final_echo_images = _denoise_temporal_svd_final_images(
            final_echo_images,
            patch_size=svd_patch_size,
            stride=svd_stride,
            retain_fraction=svd_retain_fraction,
        )
    else:
        pre_denoise_echo_images = None
        if denoise_temporal_svd and single_echo_mode:
            logging.info("Temporal SVD denoising skipped: at least two echoes are required")
        else:
            logging.info("Temporal SVD denoising: disabled")

    debug_suffix = (
        f"_rep{int(output_repetition_index) + 1}"
        if int(total_repetitions) > 1
        else ""
    )
    for echo_input, uncorrected_image, final_image in zip(
        echo_inputs,
        uncorrected_echo_images,
        final_echo_images,
    ):
        label = echo_input["label"].lower()
        np.save(
            os.path.join(debugFolder, f"sodiumgridding_ptpi_{label}_uncorrected{debug_suffix}.npy"),
            uncorrected_image,
        )
        np.save(
            os.path.join(debugFolder, f"sodiumgridding_ptpi_{label}_final{debug_suffix}.npy"),
            final_image,
        )
    if pre_denoise_echo_images is not None:
        for echo_input, pre_denoise_image in zip(echo_inputs, pre_denoise_echo_images):
            label = echo_input["label"].lower()
            np.save(
                os.path.join(debugFolder, f"sodiumgridding_ptpi_{label}_pre_svd{debug_suffix}.npy"),
                pre_denoise_image,
            )
    if field_map_hz is not None:
        np.save(os.path.join(debugFolder, f"sodiumgridding_ptpi_field_map_hz{debug_suffix}.npy"), field_map_hz)
    if bias_field is not None:
        np.save(os.path.join(debugFolder, f"sodiumgridding_ptpi_n4_bias_field{debug_suffix}.npy"), bias_field)
    if bias_mask is not None:
        np.save(os.path.join(debugFolder, f"sodiumgridding_ptpi_n4_bias_mask{debug_suffix}.npy"), bias_mask)

    elapsed_ms = (perf_counter() - tic) * 1000.0
    recon_mode = "single-echo" if single_echo_mode else f"{echo_count}-echo low-rank"
    message = f"pTPI {recon_mode} reconstruction time: {elapsed_ms:.2f} ms"
    logging.info(message)
    connection.send_logging(constants.MRD_LOGGING_INFO, message)

    reference_head = group[len(group) // 2].getHead()
    output_fov_mm = float(fov_cm) * 10.0
    if shared_echo_display_scale:
        display_input_min, display_input_max = _shared_echo_display_range(final_echo_images)
        if display_input_min is not None:
            logging.info(
                "Using shared echo display scale across %d final echoes: input_min=%.6g input_max=%.6g",
                len(final_echo_images),
                display_input_min,
                display_input_max,
            )
        else:
            logging.info("Shared echo display scale requested but no finite common range was available")
    else:
        display_input_min = display_input_max = None
        logging.info("Shared echo display scale: disabled")

    images = []
    for echo_input, final_image in zip(echo_inputs, final_echo_images):
        images.extend(
            _build_echo_images(
                final_image,
                echo_input["label"],
                echo_input["echo_time_s"],
                _series_index_for_echo(
                    echo_input["echo_index"],
                    output_repetition_index,
                    total_repetitions,
                ),
                echo_input["echo_index"],
                echo_count,
                repetition_index,
                total_repetitions,
                output_repetition_index,
                reference_head,
                metadata,
                output_fov_mm,
                orientation,
                orientation_flip_slice,
                orientation_debug_series,
                display_input_min=display_input_min,
                display_input_max=display_input_max,
            )
        )
    return images


def process_image(images, connection, config, metadata):
    return openrecon_base.process_image(images, connection, config, metadata)
