#!/usr/bin/env python3
"""OpenRecon entrypoint for dual-echo palindromic TPI gridding.

This module adapts ``ptpi_dual_echo_lowrank_core.py`` from its standalone
file-based workflow into the same MRD streaming contract used by
``sodiumgridding``. The reconstruction math stays in the core module; this file
only resolves OpenRecon config, converts incoming acquisitions into the
dual-echo array layout, and emits TE1/TE2 magnitude volumes.
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
OUTPUT_SERIES_BASE = "sodiumgridding_pTPI"
debugFolder = "/tmp/share/debug"

OPENRECON_DEFAULTS = {
    "config": "sodiumgridding_ptpi",
    "matrixsize": 128,
    "fovcm": 22.0,
    "trajectoryfile": DEFAULT_TRAJECTORY_FILE,
    "trajectorydataset": "k",
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


def _load_dual_trajectory(config):
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
            trajectory_mode = "dual-echo forward-order"
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


def _build_interleaved_data_array(acquisitions):
    """Return raw data as (coils, samples, acquisitions)."""
    base_data = openrecon_base._build_data_array(acquisitions)
    return np.ascontiguousarray(base_data.transpose(0, 2, 1))


def _split_dual_echo_data(data_all):
    if data_all.shape[2] < 2:
        raise ValueError(
            "Dual-echo pTPI reconstruction needs alternating TE1/TE2 acquisitions; "
            f"got only {data_all.shape[2]} acquisition(s)."
        )
    data_te1 = data_all[:, :, 0::2].copy()
    data_te2 = data_all[:, :, 1::2].copy()
    # TE2 is acquired in reverse sample order in the standalone pTPI workflow.
    data_te2 = data_te2[:, ::-1, :]
    return data_te1, data_te2


def _clip_to_common_shape(k_te1, k_te2, t_te1, t_te2, data_te1, data_te2):
    samples = min(
        k_te1.shape[0],
        k_te2.shape[0],
        t_te1.shape[0],
        t_te2.shape[0],
        data_te1.shape[1],
        data_te2.shape[1],
    )
    readouts = min(
        k_te1.shape[1],
        k_te2.shape[1],
        data_te1.shape[2],
        data_te2.shape[2],
    )
    if samples <= 0 or readouts <= 0:
        raise ValueError("No overlapping pTPI samples/readouts between data and trajectory")

    return (
        k_te1[:samples, :readouts, :],
        k_te2[:samples, :readouts, :],
        t_te1[:samples],
        t_te2[:samples],
        data_te1[:, :samples, :readouts],
        data_te2[:, :samples, :readouts],
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
    series_index,
    reference_head,
    metadata,
    output_fov_mm,
    orientation,
    orientation_flip_slice,
    orientation_debug_series,
):
    old_description = openrecon_base.OUTPUT_SERIES_DESCRIPTION
    old_index = openrecon_base.OUTPUT_IMAGE_SERIES_INDEX
    try:
        openrecon_base.OUTPUT_SERIES_DESCRIPTION = f"{OUTPUT_SERIES_BASE}_{echo_label.lower()}"
        openrecon_base.OUTPUT_IMAGE_SERIES_INDEX = int(series_index)
        return openrecon_base._build_output_images(
            np.abs(volume).astype(np.float32, copy=False),
            reference_head,
            metadata,
            output_fov_mm=output_fov_mm,
            orientation=orientation,
            flip_slice=orientation_flip_slice,
            emit_debug_series=orientation_debug_series,
        )
    finally:
        openrecon_base.OUTPUT_SERIES_DESCRIPTION = old_description
        openrecon_base.OUTPUT_IMAGE_SERIES_INDEX = old_index


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
                    connection.send_image(process_raw(acquisitions, connection, config, metadata))
                    acquisitions = []
            elif isinstance(item, ismrmrd.Image):
                passthrough_images.append(item)
            elif item is None:
                break
            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        if acquisitions:
            logging.info("Processing %d acquired readouts (end of stream)", len(acquisitions))
            connection.send_image(process_raw(acquisitions, connection, config, metadata))

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


def process_raw(group, connection, config, metadata):
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
        "phaselowrankrank=%d applyn4biascorrection=%s orientation=%s "
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
    data_te1_raw, data_te2_raw = _split_dual_echo_data(data_all)
    k_cm_te1, k_cm_te2, t_te1_s, t_te2_s = _load_dual_trajectory(config)
    (
        k_cm_te1,
        k_cm_te2,
        t_te1_s,
        t_te2_s,
        data_te1_raw,
        data_te2_raw,
    ) = _clip_to_common_shape(
        k_cm_te1,
        k_cm_te2,
        t_te1_s,
        t_te2_s,
        data_te1_raw,
        data_te2_raw,
    )

    logging.info(
        "pTPI input after clipping: coils=%d samples=%d readouts=%d "
        "k_te1=%s k_te2=%s",
        data_te1_raw.shape[0],
        data_te1_raw.shape[1],
        data_te1_raw.shape[2],
        k_cm_te1.shape,
        k_cm_te2.shape,
    )
    openrecon_base._log_trajectory_extent(k_cm_te1)
    openrecon_base._log_trajectory_extent(k_cm_te2)

    k_cm_te1 = core.apply_trajectory_delay_correction(k_cm_te1, t_te1_s, "TE1")
    k_cm_te2 = core.apply_trajectory_delay_correction(k_cm_te2, t_te2_s, "TE2")

    k_nyquist = 1.0 / (2.0 * fov_cm / matrix_size)
    k_norm_te1 = (k_cm_te1.reshape(-1, 3) / k_nyquist / 2.0).astype(np.float32)
    k_norm_te2 = (k_cm_te2.reshape(-1, 3) / k_nyquist / 2.0).astype(np.float32)

    kb_beta = core.default_kb_beta(core.KB_WIDTH, core.OVERSAMP) if core.KB_BETA is None else float(core.KB_BETA)
    logging.info("Kaiser-Bessel beta=%.4f LUT size=%d", kb_beta, core.KB_LUT_SIZE)
    kernel_lut = core.make_kaiser_bessel_lut(core.KB_WIDTH, kb_beta, core.KB_LUT_SIZE)

    logging.info("Preparing TE1 trajectory weights")
    dcf_te1 = core.compute_dcf_kb(k_norm_te1, matrix_size, core.OVERSAMP, core.DCF_ITER, kernel_lut, core.KB_WIDTH)
    logging.info("Preparing TE2 trajectory weights")
    dcf_te2 = core.compute_dcf_kb(k_norm_te2, matrix_size, core.OVERSAMP, core.DCF_ITER, kernel_lut, core.KB_WIDTH)
    deapo = core.compute_deapodisation_kb(matrix_size, core.OVERSAMP, kernel_lut, core.KB_WIDTH)

    te1_bad_readouts_by_coil = None
    if core.USE_TE1_BAD_READOUTS_FOR_TE2:
        te1_bad_readouts_by_coil = core.find_bad_readouts_by_coil(data_te1_raw, "TE1")

    te1_normalization_scales, te2_normalization_scales = core.build_echo_normalization_scales(
        data_te1_raw,
        data_te2_raw,
        te1_bad_readouts_by_coil,
    )

    imgs_te1 = core.reconstruct_echo_all_coils(
        data_te1_raw,
        k_cm_te1,
        k_norm_te1,
        dcf_te1,
        deapo,
        kernel_lut,
        "TE1",
        bad_readouts_by_coil=te1_bad_readouts_by_coil,
        bad_readout_source="TE1 precomputed",
        normalization_scales=te1_normalization_scales,
    )
    imgs_te2 = core.reconstruct_echo_all_coils(
        data_te2_raw,
        k_cm_te2,
        k_norm_te2,
        dcf_te2,
        deapo,
        kernel_lut,
        "TE2",
        bad_readouts_by_coil=te1_bad_readouts_by_coil,
        bad_readout_source="TE1 precomputed",
        normalization_scales=te2_normalization_scales,
    )

    field_map_hz = core.compute_field_map_from_coils(imgs_te1, imgs_te2)
    img_te1_uncorrected, img_te2_uncorrected, _ = core.combine_coils(imgs_te1, imgs_te2)

    if core.RUN_TE1_PHASE_CORRECTION:
        imgs_te1_output = core.reconstruct_phase_corrected_all_coils(
            data_te1_raw,
            k_cm_te1,
            k_norm_te1,
            dcf_te1,
            deapo,
            kernel_lut,
            field_map_hz,
            t_te1_s,
            core.TE1_PHASE_CORRECTION_SIGN,
            "TE1",
            bad_readouts_by_coil=te1_bad_readouts_by_coil,
            bad_readout_source="TE1 precomputed",
            normalization_scales=te1_normalization_scales,
        )
    else:
        imgs_te1_output = imgs_te1

    if core.RUN_TE2_PHASE_CORRECTION:
        imgs_te2_output = core.reconstruct_phase_corrected_all_coils(
            data_te2_raw,
            k_cm_te2,
            k_norm_te2,
            dcf_te2,
            deapo,
            kernel_lut,
            field_map_hz,
            t_te2_s,
            core.TE2_PHASE_CORRECTION_SIGN,
            "TE2",
            bad_readouts_by_coil=te1_bad_readouts_by_coil,
            bad_readout_source="TE1 precomputed",
            normalization_scales=te2_normalization_scales,
        )
    else:
        imgs_te2_output = imgs_te2

    img_te1_phasecorr, img_te2_phasecorr, _ = core.combine_coils(imgs_te1_output, imgs_te2_output)
    img_te1_final, img_te2_final, bias_field, bias_mask = core.apply_n4_bias_correction(
        img_te1_phasecorr,
        img_te2_phasecorr,
    )

    np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_te1_uncorrected.npy"), img_te1_uncorrected)
    np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_te2_uncorrected.npy"), img_te2_uncorrected)
    np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_te1_final.npy"), img_te1_final)
    np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_te2_final.npy"), img_te2_final)
    np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_field_map_hz.npy"), field_map_hz)
    if bias_field is not None:
        np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_n4_bias_field.npy"), bias_field)
    if bias_mask is not None:
        np.save(os.path.join(debugFolder, "sodiumgridding_ptpi_n4_bias_mask.npy"), bias_mask)

    elapsed_ms = (perf_counter() - tic) * 1000.0
    message = f"pTPI dual-echo low-rank reconstruction time: {elapsed_ms:.2f} ms"
    logging.info(message)
    connection.send_logging(constants.MRD_LOGGING_INFO, message)

    reference_head = group[len(group) // 2].getHead()
    output_fov_mm = float(fov_cm) * 10.0
    images = []
    images.extend(
        _build_echo_images(
            img_te1_final,
            "TE1",
            1,
            reference_head,
            metadata,
            output_fov_mm,
            orientation,
            orientation_flip_slice,
            orientation_debug_series,
        )
    )
    images.extend(
        _build_echo_images(
            img_te2_final,
            "TE2",
            1001,
            reference_head,
            metadata,
            output_fov_mm,
            orientation,
            orientation_flip_slice,
            orientation_debug_series,
        )
    )
    return images


def process_image(images, connection, config, metadata):
    return openrecon_base.process_image(images, connection, config, metadata)
