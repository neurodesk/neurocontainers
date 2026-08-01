#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dual-echo pTPI reconstruction with the N256 AC KB gridding pipeline.

This keeps the Kaiser-Bessel gridding, variance-retention coil compression,
adaptive coil combination, and optional N4 correction style from
TPI_gridding_N256_AC_coil_compression_kb.py, while adding the pTPI dual-echo
loader and low-rank phase correction used for palindromic forward/reverse
echoes.
"""

from concurrent.futures import ThreadPoolExecutor
import threading

import h5py
import matplotlib.pyplot as plt
import numba
import numpy as np
import scipy.fft as sp_fft
import scipy.ndimage as ndi

try:
    import pyfftw
    pyfftw.interfaces.cache.enable()
    USE_PYFFTW = True
except ImportError:
    USE_PYFFTW = False

try:
    import nibabel as nib
except ImportError as exc:
    nib = None
    NIBABEL_IMPORT_ERROR = exc

try:
    from skimage.restoration import unwrap_phase as skimage_unwrap_phase
except ImportError:
    skimage_unwrap_phase = None

try:
    import SimpleITK as sitk
except ImportError as exc:
    sitk = None
    SIMPLEITK_IMPORT_ERROR = exc


# --- Config ------------------------------------------------------------------
# These defaults intentionally mirror TPI_gridding_N256_AC_coil_compression_kb.py.
N = 128
FOV_CM = 22
OVERSAMP = 2
DCF_ITER = 5

FERMI_CUTOFF = 0.98
FERMI_WIDTH = 0.05

MAX_WORKERS = 8
MAX_SIMULTANEOUS_GRIDS = 2
GRID_SEMAPHORE = threading.Semaphore(MAX_SIMULTANEOUS_GRIDS)
FFT_WORKERS = -1

## 23Na
#TRAJ_FILE = "/Users/clicht/Desktop/Python/n28_TPI_23Na.h5"
# DATA_FILE = "/Users/clicht/Data_3T/TPI/TPI_2TE/20260604/23Na_n14_TPI_TE2_3T_Pht_20260604.h5"
#DATA_FILE = "/Users/clicht/Data_3T/TPI/TPI_2TE/20260605/1H_n14_TPI_TE2_3T_Pht_20260605.h5"
#DATA_FILE = "/Users/clicht/Data_7T/23Na/TPI_2TE/20260608/23Na_n28_TPI_TE2_7T_Pht_20260608.h5"
#OUTPUT_FILE = "/Users/clicht/Data_7T/TPI_2TE/20260611_invivo/20260611_InVivo_Liliana/tpi_dual_echo_gridded_multi_coil.h5"

## 1H
#TRAJ_FILE = "/Users/clicht/Desktop/Python/1H_n14_TPI_traj_new.h5"
# For the new dual-echo trajectory file, this script uses k_echo1 and
# k_echo2_forward_order automatically when those datasets are present.
#TRAJ_FILE = '/Users/clicht/Desktop/Python/n72_TPI_traj_new.h5'
#TRAJ_FILE = '/Users/clicht/Desktop/Python/1H_n72_TPI_dual_echo_traj_new.h5'
#TRAJ_FILE ="/Users/clicht/Desktop/Python/23Na_n28_TPI_dual_echo_traj_g90.h5"
TRAJ_FILE = '/Users/clicht/Desktop/Python/23Na_pTPI_n28_g50_p5traj.h5'

DATA_FILE ='/Users/clicht/Data_7T/TPI_2TE/pTPI_20260724/23Na_pTPI_n28_g50_p5.h5'
#DATA_FILE = "/Users/clicht/Data_7T/TPI_2TE/20260611_invivo/20260611_InVivo_Liliana/1H_n72_TPI_TE2_7T_TR10_Rx64_Lil.h5"
#DATA_FILE = '/Users/clicht/Data_7T/23Na/20260618/23Na_n50_7T_g90_TE05.h5'
#'/Users/clicht/Data_7T/TPI_2TE/20260612_redHeadPhantom/1H_TPI2TE_TE007_RHP.h5'

#DATA_FILE = "/Users/clicht/Data_3T/TPI/TPI_2TE/20260605/1H_n14_TPI_TE2_3T_Pht_20260605.h5"

# Set to None to use all available coils. Use 16 to match the N256 AC script.
NUM_COILS = None
COIL_COMPRESSION = True
COIL_VARIANCE_RETENTION = 0.9
COIL_COMPRESSION_SOURCE = "both"  # "both", "te1", or "te2"; used to estimate the PCA basis.
COIL_COMPRESSION_MAX_CALIB_POINTS = 2500000
COIL_COMBINE_MODE = "AC"  # "AC" or "SoS", matching the N256 AC script.
ADAPTIVE_COMBINE_SMOOTH_SIGMA = None  # None uses N / 32 voxels.
PLOT_RESULTS = True
SAVE_C0_FILES = True
C0_ECHO1_FILE = "/Users/clicht/Data_7T/TPI_2TE/pTPI_20260724/23Na_pTPI_TE05_RHP.c0"
C0_ECHO2_FILE = "/Users/clicht/Data_7T/TPI_2TE/pTPI_20260724/23Na_pTPI_TE50_RHP.c0"
SAVE_NIFTI_FILES = False
NIFTI_OUTPUT_PREFIX = "/Users/clicht/Data_7T/TPI_2TE/pTPI_20260724/PC_lowrank_recon"
NIFTI_FIELD_MAP_NAN_VALUE = 0.0
# Keep saved reconstructed arrays in the same native grid orientation as
# TPI_gridding_N256_AC_coil_compression_kb.py. That script applies swapaxes(0, 2)
# only for display.
SAVE_OUTPUTS_IN_GRIDDING_DISPLAY_ORIENTATION = False
DISPLAY_ROT90_K = 1
DISPLAY_FIELD_MAP = False
DISPLAY_VMIN_PERCENTILE = 0.0
DISPLAY_VMAX_PERCENTILE = 99.5
SAVE_COIL_IMAGES = False
USE_TE1_BAD_READOUTS_FOR_TE2 = True
ECHO_NORMALIZATION_MODE = "none"  # Use "none" for quantitative TE1/TE2 scaling; "independent" matches old behavior.
APPLY_TRAJECTORY_DELAY_CORRECTION = False
TRAJ_DELAY_US = [0.0, 0.0, 0.0]  # Per-axis [kx, ky, kz] delay in microseconds.
TRAJ_DELAY_APPLY_TO = "te2"  # "te1", "te2", or "both".

FIELD_MAP_DELTA_TE_S = .005 #0.0045
FIELD_MAP_SIGN = 1.0
FIELD_MAP_ESTIMATION_MODE = "cross_sum"  # "coilwise" or "cross_sum"
FIELD_MAP_MAG_MASK_FRACTION = .08 #0.08
FIELD_MAP_PHASE_SMOOTH_SIGMA = .7
FIELD_MAP_UNWRAP_METHOD = "skimage"  # "skimage", "axis", or "none"
FIELD_MAP_SMOOTH_SIGMA_HZ = 1.0
FIELD_MAP_DISPLAY_CLIP_HZ = 250.0
FIELD_MAP_COIL_WEIGHT_POWER = 0.5
FIELD_MAP_COIL_WEIGHT_NORMALIZE = True
FIELD_MAP_COIL_WEIGHT_CLIP_PERCENTILE = 95.0
FIELD_MAP_COIL_WEIGHT_MIN_FRACTION = 0.02

RUN_TE2_PHASE_CORRECTION = True #True
RUN_TE1_PHASE_CORRECTION = True
TE1_PHASE_TIME_DATASET = "t_echo1_relative_s"
TE1_PHASE_CORRECTION_SIGN = -1.0
TE2_PHASE_TIME_DATASET = "t_echo2_forward_relative_s"
TE2_PHASE_SEGMENTS = 10 # Used only by the legacy hard-segment correction.
TE2_PHASE_CORRECTION_SIGN = -1.0
TE2_PHASE_CORRECTION_MODEL = "lowrank"  # "lowrank" or "hard_segments"
TE2_PHASE_LOWRANK_RANK = 10
TE2_PHASE_LOWRANK_FIELD_BINS = 256
TE2_PHASE_LOWRANK_FIELD_CLIP_HZ = None  # None uses FIELD_MAP_DISPLAY_CLIP_HZ.
FIELD_MODEL_RANGE_MODE = "percentile"  # "percentile", "minmax", or "symmetric_clip"
FIELD_MODEL_RANGE_PERCENTILES = (1.0, 99.0)
PLOT_PHASE_TABLE_ROWS = False
PHASE_TABLE_ROW_INDICES = None  # None plots 4 evenly spaced B0 rows.
PHASE_TABLE_ROW_COUNT = 4

APPLY_N4_BIAS_CORRECTION = True
N4_MASK_ECHO = "te1"  # "te1" or "te2"
N4_BIAS_ECHO = "te2"  # "te1" or "te2"
N4_APPLY_TO_ECHOES = "both"  # "both", "te1", or "te2"
N4_MASK_THRESHOLD_FRACTION = 0.05
N4_MASK_CLOSE_ITERATIONS = 2
N4_MASK_ERODE_ITERATIONS = 1
N4_BIAS_FLOOR_FRACTION = 0.05
N4_CLIP_PERCENTILES = (1.0, 99.0)
N4_SHRINK_FACTOR = 4
N4_MAX_ITERATIONS = [50, 50, 50, 50]
N4_CONVERGENCE_THRESHOLD = 1e-6
N4_WIENER_FILTER_NOISE = 0.1
N4_BIAS_FIELD_FWHM = 0.25
N4_OUTPUT_MASK_DILATE_ITERATIONS = 2  # Used only when N4_BACKGROUND_MODE is "zero" or "original".
N4_BACKGROUND_MODE = "corrected"  # "corrected" keeps the full image; "zero"/"original" mask the background.

KB_WIDTH = 3.0
KB_BETA = np.pi * np.sqrt((KB_WIDTH / OVERSAMP * (OVERSAMP - 0.5)) ** 2 - 0.8)
KB_LUT_SIZE = 2048
KB_DEAPODIZATION_FLOOR = 1e-8


def default_kb_beta(width, oversamp):
    return np.pi * np.sqrt((width / oversamp * (oversamp - 0.5)) ** 2 - 0.8)


def make_kaiser_bessel_lut(width, beta, lut_size):
    radius = np.linspace(0.0, width / 2.0, lut_size, dtype=np.float64)
    arg = beta * np.sqrt(np.maximum(1.0 - (2.0 * radius / width) ** 2, 0.0))
    lut = np.i0(arg) / np.i0(beta)
    lut[-1] = 0.0
    return lut.astype(np.float64)


@numba.jit(nopython=True, fastmath=True)
def _kb_lut_value(distance, kernel_lut, width):
    half_width = 0.5 * width
    if distance >= half_width:
        return 0.0
    pos = distance / half_width * (len(kernel_lut) - 1)
    idx = int(np.floor(pos))
    frac = pos - idx
    if idx >= len(kernel_lut) - 1:
        return kernel_lut[len(kernel_lut) - 1]
    return kernel_lut[idx] * (1.0 - frac) + kernel_lut[idx + 1] * frac


@numba.jit(nopython=True)
def _grid_kb_complex(grid, k_coords_norm, weights, grid_size, oversamp, kernel_lut, width):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width))
        ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width))
        iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width))
        iz_max = int(np.floor(gz + half_width))
        w = weights[i]
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kb_lut_value(abs(gz - iz), kernel_lut, width)
                if wz > 0.0:
                    for iy in range(iy_min, iy_max + 1):
                        if 0 <= iy < og:
                            wy = _kb_lut_value(abs(gy - iy), kernel_lut, width)
                            if wy > 0.0:
                                for ix in range(ix_min, ix_max + 1):
                                    if 0 <= ix < og:
                                        wx = _kb_lut_value(abs(gx - ix), kernel_lut, width)
                                        if wx > 0.0:
                                            grid[iz, iy, ix] += w * wx * wy * wz


@numba.jit(nopython=True, fastmath=True)
def _grid_kb_real(grid, k_coords_norm, weights, grid_size, oversamp, kernel_lut, width):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width))
        ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width))
        iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width))
        iz_max = int(np.floor(gz + half_width))
        w = weights[i]
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kb_lut_value(abs(gz - iz), kernel_lut, width)
                if wz > 0.0:
                    for iy in range(iy_min, iy_max + 1):
                        if 0 <= iy < og:
                            wy = _kb_lut_value(abs(gy - iy), kernel_lut, width)
                            if wy > 0.0:
                                for ix in range(ix_min, ix_max + 1):
                                    if 0 <= ix < og:
                                        wx = _kb_lut_value(abs(gx - ix), kernel_lut, width)
                                        if wx > 0.0:
                                            grid[iz, iy, ix] += w * wx * wy * wz


@numba.jit(nopython=True, fastmath=True)
def _sample_kb(values, grid, k_coords_norm, grid_size, oversamp, kernel_lut, width):
    og = grid_size * oversamp
    num_points = len(k_coords_norm)
    half_width = 0.5 * width
    for i in range(num_points):
        gx = (k_coords_norm[i, 0] + 0.5) * og - 0.5
        gy = (k_coords_norm[i, 1] + 0.5) * og - 0.5
        gz = (k_coords_norm[i, 2] + 0.5) * og - 0.5
        ix_min = int(np.ceil(gx - half_width))
        ix_max = int(np.floor(gx + half_width))
        iy_min = int(np.ceil(gy - half_width))
        iy_max = int(np.floor(gy + half_width))
        iz_min = int(np.ceil(gz - half_width))
        iz_max = int(np.floor(gz + half_width))
        val = 0.0
        for iz in range(iz_min, iz_max + 1):
            if 0 <= iz < og:
                wz = _kb_lut_value(abs(gz - iz), kernel_lut, width)
                if wz > 0.0:
                    for iy in range(iy_min, iy_max + 1):
                        if 0 <= iy < og:
                            wy = _kb_lut_value(abs(gy - iy), kernel_lut, width)
                            if wy > 0.0:
                                for ix in range(ix_min, ix_max + 1):
                                    if 0 <= ix < og:
                                        wx = _kb_lut_value(abs(gx - ix), kernel_lut, width)
                                        if wx > 0.0:
                                            val += grid[iz, iy, ix] * wx * wy * wz
        values[i] = val


def compute_dcf_kb(k_coords_norm, grid_size, oversamp, n_iter, kernel_lut, width):
    og = grid_size * oversamp
    dcf = np.ones(len(k_coords_norm), dtype=np.float64)
    print("Computing KB DCF:", end=" ")
    for i in range(n_iter):
        print(f"{i + 1}", end=" ", flush=True)
        wsum = np.zeros((og, og, og), dtype=np.float64)
        _grid_kb_real(wsum, k_coords_norm, dcf, grid_size, oversamp, kernel_lut, width)
        sampled_density = np.zeros_like(dcf)
        _sample_kb(sampled_density, wsum, k_coords_norm, grid_size, oversamp, kernel_lut, width)
        sampled_density[sampled_density < 1e-9] = 1.0
        dcf /= sampled_density
        dcf /= np.median(dcf)
    print("done")
    return dcf.astype(np.float32)


def compute_deapodisation_kb(grid_size, oversamp, kernel_lut, width):
    og = grid_size * oversamp
    kernel_1d = np.zeros(og, dtype=np.complex128)
    g_center = 0.5 * og - 0.5
    half_width = 0.5 * width
    ix_min = int(np.ceil(g_center - half_width))
    ix_max = int(np.floor(g_center + half_width))
    for ix in range(ix_min, ix_max + 1):
        if 0 <= ix < og:
            distance = abs(g_center - ix)
            pos = distance / half_width * (len(kernel_lut) - 1)
            idx = int(np.floor(pos))
            frac = pos - idx
            value = kernel_lut[idx] * (1.0 - frac) + kernel_lut[min(idx + 1, len(kernel_lut) - 1)] * frac
            kernel_1d[ix] = value

    response_1d = np.fft.ifftshift(np.fft.ifft(np.fft.ifftshift(kernel_1d))) * og
    start = (og - grid_size) // 2
    end = start + grid_size
    deapo_1d = np.abs(response_1d[start:end])
    deapo = np.multiply.outer(np.multiply.outer(deapo_1d, deapo_1d).ravel(), deapo_1d)
    deapo = deapo.reshape(grid_size, grid_size, grid_size)
    deapo /= np.nanmax(deapo)
    deapo = np.where(deapo < KB_DEAPODIZATION_FLOOR, KB_DEAPODIZATION_FLOOR, deapo)
    return deapo.astype(np.float32)


def regrid_3d_kb(kspace_data, k_coords_norm, grid_size, dcf, deapo, oversamp, kernel_lut, width):
    og = grid_size * oversamp
    data_dcf = (kspace_data * dcf).astype(np.complex64, copy=False)
    grid = np.zeros((og, og, og), dtype=np.complex64)
    _grid_kb_complex(grid, k_coords_norm, data_dcf, grid_size, oversamp, kernel_lut, width)

    shifted_grid = np.fft.ifftshift(grid)
    if USE_PYFFTW:
        transformed_grid = pyfftw.builders.ifftn(
            shifted_grid,
            auto_align_input=True,
            auto_contiguous=True,
            threads=FFT_WORKERS,
        )()
    else:
        transformed_grid = sp_fft.ifftn(
            shifted_grid,
            workers=FFT_WORKERS,
            overwrite_x=True,
        )

    img_os = np.fft.ifftshift(transformed_grid) * (og**3)
    start = (og - grid_size) // 2
    end = start + grid_size
    img = img_os[start:end, start:end, start:end].copy()
    img /= deapo
    return img.astype(np.complex64)


def find_bad_readouts(data):
    max_vals = np.abs(data).max(axis=0)
    hist_counts, hist_edges = np.histogram(ndi.gaussian_filter1d(max_vals, 5), bins=40)
    most_common_max = hist_edges[np.argmax(hist_counts)]
    reference = max_vals[max_vals >= 0.95 * most_common_max]
    if reference.size == 0:
        return np.array([], dtype=int)
    threshold = most_common_max - 3.0 * reference.std()
    return np.where(max_vals < threshold)[0]


def preprocess_echo(
    data,
    k_cm,
    coil_index=None,
    echo_label="",
    bad_readouts=None,
    bad_readout_source="detected",
    normalization_scale=None,
):
    if bad_readouts is None:
        bad_readouts = find_bad_readouts(data)
        bad_readout_source = "detected"
    else:
        bad_readouts = np.asarray(bad_readouts, dtype=int)
    prefix = f"coil {coil_index} {echo_label}".strip()
    print(f"  {prefix}: bad readouts {bad_readouts.size} ({bad_readout_source})")

    weights = np.ones(data.shape, dtype=np.uint8)
    if bad_readouts.size:
        weights[:, bad_readouts] = 0
    weights[0, :] = 0

    processed = data * weights
    if normalization_scale is None:
        center_signal = np.abs(processed[1:11, :]).mean()
        if center_signal > 1e-8:
            processed *= 2.0 / center_signal
    else:
        processed *= np.float32(normalization_scale)

    abs_k = np.linalg.norm(k_cm, axis=-1)
    abs_k_norm = abs_k / np.nanmax(abs_k)
    fermi_filter = 1.0 / (1.0 + np.exp((abs_k_norm - FERMI_CUTOFF) / FERMI_WIDTH))
    processed *= fermi_filter

    return processed.astype(np.complex64)


def echo_center_normalization_scale(data, bad_readouts=None):
    if bad_readouts is None:
        bad_readouts = np.array([], dtype=int)
    else:
        bad_readouts = np.asarray(bad_readouts, dtype=int)

    weights = np.ones(data.shape, dtype=np.uint8)
    if bad_readouts.size:
        weights[:, bad_readouts] = 0
    #weights[0, :] = 0

    center_signal = np.abs(data * weights)[1:11, :].mean()
    if center_signal > 1e-8:
        return np.float32(2.0 / center_signal), float(center_signal)
    return np.float32(1.0), float(center_signal)


def build_echo_normalization_scales(data_te1, data_te2, bad_readouts_by_coil=None):
    mode = ECHO_NORMALIZATION_MODE.lower()
    n_coils = data_te1.shape[0]

    if mode in ("none", "off", "false"):
        print("Echo normalization: disabled")
        return np.ones(n_coils, dtype=np.float32), np.ones(n_coils, dtype=np.float32)

    if mode not in ("te1", "shared", "independent"):
        raise ValueError("ECHO_NORMALIZATION_MODE must be 'te1', 'independent', or 'none'")

    te1_scales = np.ones(n_coils, dtype=np.float32)
    te2_scales = np.ones(n_coils, dtype=np.float32)
    te1_centers = np.zeros(n_coils, dtype=np.float32)
    te2_centers = np.zeros(n_coils, dtype=np.float32)

    for coil in range(n_coils):
        bad = None if bad_readouts_by_coil is None else bad_readouts_by_coil[coil]
        te1_scales[coil], te1_centers[coil] = echo_center_normalization_scale(data_te1[coil], bad)
        if mode == "independent":
            te2_scales[coil], te2_centers[coil] = echo_center_normalization_scale(data_te2[coil], bad)
        else:
            te2_scales[coil] = te1_scales[coil]
            _, te2_centers[coil] = echo_center_normalization_scale(data_te2[coil], bad)

    raw_ratio = np.divide(
        te2_centers,
        te1_centers,
        out=np.full_like(te2_centers, np.nan, dtype=np.float32),
        where=te1_centers > 1e-8,
    )
    print(
        "Echo normalization: "
        f"mode={ECHO_NORMALIZATION_MODE}, "
        f"TE1 center median={np.nanmedian(te1_centers):.4g}, "
        f"TE2 center median={np.nanmedian(te2_centers):.4g}, "
        f"raw TE2/TE1 center median={np.nanmedian(raw_ratio):.4g}, "
        f"TE1 scale median={np.nanmedian(te1_scales):.4g}, "
        f"TE2 scale median={np.nanmedian(te2_scales):.4g}"
    )
    return te1_scales, te2_scales


def find_bad_readouts_by_coil(data_echo, echo_label):
    bad_readouts_by_coil = []
    counts = []
    for coil in range(data_echo.shape[0]):
        bad_readouts = find_bad_readouts(data_echo[coil])
        bad_readouts_by_coil.append(bad_readouts)
        counts.append(bad_readouts.size)
    print(
        f"{echo_label} bad-readout detection summary: "
        f"min={np.min(counts)}, median={np.median(counts):.1f}, max={np.max(counts)}"
    )
    return bad_readouts_by_coil


def compress_coils_by_variance(coil_data, variance_retention=0.90, eps=1e-12):
    """Compress coils with the variance-retention PCA method from the N256 AC recon."""
    num_input_coils = coil_data.shape[0]
    if num_input_coils <= 1:
        return coil_data, np.array([1.0], dtype=np.float32), np.eye(num_input_coils, dtype=np.complex64)

    data_2d = coil_data.reshape(num_input_coils, -1)
    covariance = data_2d @ data_2d.conj().T
    covariance /= max(data_2d.shape[1], 1)

    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    sort_idx = np.argsort(eigenvalues)[::-1]
    eigenvalues = np.maximum(eigenvalues[sort_idx].real, 0.0)
    eigenvectors = eigenvectors[:, sort_idx]

    total_variance = eigenvalues.sum()
    if total_variance <= eps:
        print("Coil compression skipped: coil covariance has near-zero total variance.")
        return coil_data, np.ones(num_input_coils, dtype=np.float32), np.eye(num_input_coils, dtype=np.complex64)

    cumulative_variance = np.cumsum(eigenvalues) / total_variance
    variance_retention = np.clip(variance_retention, 0.0, 1.0)
    num_virtual_coils = np.searchsorted(cumulative_variance, variance_retention) + 1
    compression_matrix = eigenvectors[:, :num_virtual_coils].astype(np.complex64)
    compressed_data = np.einsum("cv,c...->v...", compression_matrix.conj(), coil_data, optimize=True)

    print(
        f"Coil compression: {num_input_coils} physical coils -> {num_virtual_coils} virtual coils "
        f"({100.0 * cumulative_variance[num_virtual_coils - 1]:.2f}% variance retained)"
    )
    print("Cumulative coil variance:", np.array2string(cumulative_variance[:num_virtual_coils], precision=4))
    return compressed_data.astype(np.complex64), cumulative_variance.astype(np.float32), compression_matrix


def compress_coils_covariance(data_all):
    if not COIL_COMPRESSION:
        return data_all

    n_coils, n_samples, n_acqs = data_all.shape
    if n_coils <= 1:
        return data_all

    print(
        f"Compressing coils by variance retention: source={COIL_COMPRESSION_SOURCE}, "
        f"target={COIL_VARIANCE_RETENTION:.3f}"
    )

    source = COIL_COMPRESSION_SOURCE.lower()
    if source == "te1":
        calib = data_all[:, :, 0::2]
    elif source == "te2":
        calib = data_all[:, :, 1::2]
    elif source == "both":
        calib = data_all
    else:
        raise ValueError(
            f"Unsupported COIL_COMPRESSION_SOURCE={COIL_COMPRESSION_SOURCE!r}. "
            "Use 'both', 'te1', or 'te2'."
        )

    calib_flat = calib.reshape(n_coils, -1)
    n_points = calib_flat.shape[1]
    stride = max(1, int(np.ceil(n_points / COIL_COMPRESSION_MAX_CALIB_POINTS)))
    calib_sub = calib_flat[:, ::stride].reshape(n_coils, -1, 1).astype(np.complex64, copy=False)
    print(f"  calibration points: {calib_sub.shape[1]} of {n_points} (stride {stride})")

    _, _, basis = compress_coils_by_variance(
        calib_sub,
        variance_retention=COIL_VARIANCE_RETENTION,
    )
    data_flat = data_all.reshape(n_coils, -1)
    compressed = basis.conj().T @ data_flat
    return compressed.reshape(basis.shape[1], n_samples, n_acqs).astype(np.complex64, copy=False)


def load_dual_echo_multi_coil():
    print("Loading trajectory and multi-coil dual-echo data...")
    with h5py.File(TRAJ_FILE, "r") as f:
        if "k_echo1" in f and "k_echo2_forward_order" in f:
            k_cm_te1_full = f["k_echo1"][...]
            k_cm_te2_full = f["k_echo2_forward_order"][...]
            trajectory_mode = "dual-echo forward-order"
        else:
            k_cm_te1_full = f["k"][...]
            k_cm_te2_full = k_cm_te1_full
            trajectory_mode = "single-trajectory fallback"
        if TE1_PHASE_TIME_DATASET in f:
            t_te1_full = f[TE1_PHASE_TIME_DATASET][...].astype(np.float32)
            te1_time_mode = TE1_PHASE_TIME_DATASET
        else:
            dt_s = float(f["k"].attrs.get("sampling_time_us", 10.0)) * 1e-6
            t_te1_full = np.arange(k_cm_te1_full.shape[0], dtype=np.float32) * dt_s
            te1_time_mode = f"inferred +{dt_s:.3e} s/sample"

        if TE2_PHASE_TIME_DATASET in f:
            t_te2_full = f[TE2_PHASE_TIME_DATASET][...].astype(np.float32)
            te2_time_mode = TE2_PHASE_TIME_DATASET
        else:
            dt_s = float(f["k"].attrs.get("sampling_time_us", 10.0)) * 1e-6
            t_te2_full = -np.arange(k_cm_te2_full.shape[0], dtype=np.float32) * dt_s
            te2_time_mode = f"inferred {-dt_s:.3e} s/sample"

    with h5py.File(DATA_FILE, "r") as f:
        raw = f["data"]
        if raw.ndim == 3:
            available_coils = raw.shape[0]
            n_coils = available_coils if NUM_COILS is None else min(NUM_COILS, available_coils)
            data_all = raw[:n_coils, :, :].astype(np.complex64)
        elif raw.ndim == 2:
            data_all = raw[None, :, :].astype(np.complex64)
        else:
            raise ValueError(f"Unsupported data shape: {raw.shape}")

    data_all = compress_coils_covariance(data_all)

    data_echo_1 = data_all[:, :, 0::2].copy()
    data_echo_2 = data_all[:, :, 1::2].copy()

    # Echo 2 is acquired in the reverse sample order. Put it in forward order
    # for gridding with k_echo2_forward_order when available.
    data_echo_2 = data_echo_2[:, ::-1, :]

    n_samples = min(
        k_cm_te1_full.shape[0],
        k_cm_te2_full.shape[0],
        data_echo_1.shape[1],
        data_echo_2.shape[1],
    )
    n_readouts = min(
        k_cm_te1_full.shape[1],
        k_cm_te2_full.shape[1],
        data_echo_1.shape[2],
        data_echo_2.shape[2],
    )

    k_cm_te1 = k_cm_te1_full[:n_samples, :n_readouts, :]
    k_cm_te2 = k_cm_te2_full[:n_samples, :n_readouts, :]
    t_te1 = t_te1_full[:n_samples]
    t_te2 = t_te2_full[:n_samples]
    data_echo_1 = data_echo_1[:, :n_samples, :n_readouts]
    data_echo_2 = data_echo_2[:, :n_samples, :n_readouts]

    print(f"  coils      : {data_echo_1.shape[0]}")
    print(f"  traj mode  : {trajectory_mode}")
    print(f"  TE1 time   : {te1_time_mode}")
    print(f"  TE2 time   : {te2_time_mode}")
    print(f"  k TE1 shape: {k_cm_te1.shape}")
    print(f"  k TE2 shape: {k_cm_te2.shape}")
    print(f"  t TE1 shape: {t_te1.shape}")
    print(f"  t TE2 shape: {t_te2.shape}")
    print(f"  TE1 shape  : {data_echo_1.shape}")
    print(f"  TE2 shape  : {data_echo_2.shape}")
    return k_cm_te1, k_cm_te2, t_te1, t_te2, data_echo_1, data_echo_2


def trajectory_delay_applies(echo_label):
    apply_to = TRAJ_DELAY_APPLY_TO.lower()
    echo_label = echo_label.lower()
    if apply_to == "both":
        return True
    if apply_to in ("te1", "echo1"):
        return echo_label in ("te1", "echo1")
    if apply_to in ("te2", "echo2"):
        return echo_label in ("te2", "echo2")
    if apply_to in ("none", "off", "false"):
        return False
    raise ValueError("TRAJ_DELAY_APPLY_TO must be 'te1', 'te2', 'both', or 'none'")


def apply_trajectory_delay_correction(k_cm, sample_times_s, echo_label):
    if not APPLY_TRAJECTORY_DELAY_CORRECTION or not trajectory_delay_applies(echo_label):
        return k_cm

    delays_s = np.asarray(TRAJ_DELAY_US, dtype=np.float32) * 1e-6
    if delays_s.shape != (3,):
        raise ValueError("TRAJ_DELAY_US must contain exactly three values: [kx, ky, kz]")
    if np.allclose(delays_s, 0.0):
        print(f"Trajectory delay correction for {echo_label}: enabled, delays are all zero")
        return k_cm

    t = np.asarray(sample_times_s, dtype=np.float32)
    if t.ndim != 1 or t.size != k_cm.shape[0]:
        raise ValueError(
            f"{echo_label} trajectory delay correction needs one sample time per k-space sample"
        )

    order = np.argsort(t)
    t_sorted = t[order]
    k_sorted = k_cm[order, :, :]
    corrected_sorted = np.empty_like(k_sorted, dtype=np.float32)

    for axis in range(3):
        shifted_t = t_sorted + delays_s[axis]
        for readout in range(k_sorted.shape[1]):
            corrected_sorted[:, readout, axis] = np.interp(
                shifted_t,
                t_sorted,
                k_sorted[:, readout, axis],
                left=k_sorted[0, readout, axis],
                right=k_sorted[-1, readout, axis],
            )

    corrected = np.empty_like(k_cm, dtype=np.float32)
    corrected[order, :, :] = corrected_sorted
    print(
        f"Applied trajectory delay correction to {echo_label}: "
        f"[kx, ky, kz]=[{TRAJ_DELAY_US[0]:.3f}, {TRAJ_DELAY_US[1]:.3f}, {TRAJ_DELAY_US[2]:.3f}] us"
    )
    return corrected


def reconstruct_one_coil(
    coil_index,
    echo_data,
    k_cm,
    k_norm,
    dcf,
    deapo,
    kernel_lut,
    echo_label,
    bad_readouts=None,
    bad_readout_source="detected",
    normalization_scale=None,
):
    processed = preprocess_echo(
        echo_data,
        k_cm,
        coil_index=coil_index,
        echo_label=echo_label,
        bad_readouts=bad_readouts,
        bad_readout_source=bad_readout_source,
        normalization_scale=normalization_scale,
    )
    with GRID_SEMAPHORE:
        img = regrid_3d_kb(processed.ravel(), k_norm, N, dcf, deapo, OVERSAMP, kernel_lut, KB_WIDTH)
    return coil_index, img


def reconstruct_echo_all_coils(
    data_echo,
    k_cm,
    k_norm,
    dcf,
    deapo,
    kernel_lut,
    echo_label,
    bad_readouts_by_coil=None,
    bad_readout_source="detected",
    normalization_scales=None,
):
    n_coils = data_echo.shape[0]
    imgs = np.zeros((n_coils, N, N, N), dtype=np.complex64)
    print(f"Reconstructing {echo_label}: {n_coils} coils")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                reconstruct_one_coil,
                coil,
                data_echo[coil],
                k_cm,
                k_norm,
                dcf,
                deapo,
                kernel_lut,
                echo_label,
                None if bad_readouts_by_coil is None else bad_readouts_by_coil[coil],
                bad_readout_source,
                None if normalization_scales is None else normalization_scales[coil],
            )
            for coil in range(n_coils)
        ]
        for i, future in enumerate(futures, start=1):
            coil, img = future.result()
            imgs[coil] = img
            print(f"  completed {echo_label} coil {coil + 1}/{n_coils} ({i}/{n_coils})")
    return imgs


def phase_segment_bounds(num_samples, num_segments):
    num_segments = max(1, int(num_segments))
    edges = np.linspace(0, num_samples, num_segments + 1, dtype=int)
    return [(int(edges[i]), int(edges[i + 1])) for i in range(num_segments) if edges[i + 1] > edges[i]]


def determine_lowrank_field_range(field_map_hz):
    field = np.asarray(field_map_hz, dtype=np.float32)
    finite_field = field[np.isfinite(field)]
    mode = FIELD_MODEL_RANGE_MODE.lower()

    if finite_field.size and mode in ("percentile", "percentiles"):
        p_low, p_high = FIELD_MODEL_RANGE_PERCENTILES
        lo, hi = np.percentile(finite_field, [float(p_low), float(p_high)])
        if hi > lo:
            return float(lo), float(hi), f"percentile p{p_low:g}-p{p_high:g}"

    if finite_field.size and mode in ("minmax", "min_max", "data"):
        lo = float(np.min(finite_field))
        hi = float(np.max(finite_field))
        if hi > lo:
            return lo, hi, "minmax"

    if mode not in ("symmetric_clip", "clip", "display_clip", "percentile", "percentiles", "minmax", "min_max", "data"):
        raise ValueError("FIELD_MODEL_RANGE_MODE must be 'percentile', 'minmax', or 'symmetric_clip'")

    clip_hz = (
        FIELD_MAP_DISPLAY_CLIP_HZ
        if TE2_PHASE_LOWRANK_FIELD_CLIP_HZ is None
        else float(TE2_PHASE_LOWRANK_FIELD_CLIP_HZ)
    )
    if clip_hz <= 0:
        clip_hz = float(np.nanpercentile(np.abs(finite_field), 99.0)) if finite_field.size else 250.0
        clip_hz = max(clip_hz, 1.0)
    return -float(clip_hz), float(clip_hz), f"symmetric_clip +/-{clip_hz:.1f}Hz"


def plot_phase_table_rows(freq_bins, sample_times_s, phase_table, echo_label):
    if not PLOT_PHASE_TABLE_ROWS:
        return

    if PHASE_TABLE_ROW_INDICES is None:
        count = max(1, int(PHASE_TABLE_ROW_COUNT))
        row_indices = np.linspace(0, phase_table.shape[0] - 1, count, dtype=int)
    else:
        row_indices = np.asarray(PHASE_TABLE_ROW_INDICES, dtype=int)
        row_indices = np.clip(row_indices, 0, phase_table.shape[0] - 1)

    t_ms = np.asarray(sample_times_s, dtype=np.float32) * 1e3
    fig, axes = plt.subplots(2, 1, figsize=(8, 6), sharex=True, constrained_layout=True)

    for row in row_indices:
        curve = phase_table[row]
        label = f"{freq_bins[row]:.1f} Hz"
        axes[0].plot(t_ms, np.unwrap(np.angle(curve)), label=label)
        axes[1].plot(t_ms, curve.real, label=f"Re {label}")
        axes[1].plot(t_ms, curve.imag, linestyle="--", label=f"Im {label}")

    axes[0].set_title(f"{echo_label} phase table rows P(B0, t)")
    axes[0].set_ylabel("unwrapped phase [rad]")
    axes[0].legend(loc="best", fontsize=8)
    axes[0].grid(True, alpha=0.25)
    axes[1].set_xlabel("ADC sample time [ms]")
    axes[1].set_ylabel("complex value")
    axes[1].legend(loc="best", fontsize=8, ncol=2)
    axes[1].grid(True, alpha=0.25)
    plt.show()


def build_lowrank_phase_model(field_map_hz, sample_times_s, correction_sign, echo_label):
    field_hz = np.nan_to_num(field_map_hz, nan=0.0).astype(np.float32, copy=False)
    field_min_hz, field_max_hz, field_range_label = determine_lowrank_field_range(field_map_hz)

    rank = int(TE2_PHASE_LOWRANK_RANK)
    n_bins = int(TE2_PHASE_LOWRANK_FIELD_BINS)
    if rank < 1:
        raise ValueError("TE2_PHASE_LOWRANK_RANK must be >= 1")
    if n_bins < rank:
        raise ValueError("TE2_PHASE_LOWRANK_FIELD_BINS must be >= TE2_PHASE_LOWRANK_RANK")

    freq_bins = np.linspace(field_min_hz, field_max_hz, n_bins, dtype=np.float32)
    field_hz = np.clip(field_hz, field_min_hz, field_max_hz).astype(np.float32, copy=False)
    sample_times_s = np.asarray(sample_times_s, dtype=np.float32)

    phase_table = np.exp(
        1j
        * float(correction_sign)
        * 2.0
        * np.pi
        * freq_bins[:, None].astype(np.float32)
        * sample_times_s[None, :]
    ).astype(np.complex64)
    plot_phase_table_rows(freq_bins, sample_times_s, phase_table, echo_label)

    u, s, vh = np.linalg.svd(phase_table, full_matrices=False)
    rank = min(rank, s.size)
    coeff_table = (u[:, :rank] * s[:rank][None, :]).astype(np.complex64)
    time_basis = vh[:rank, :].astype(np.complex64)

    approx = coeff_table @ time_basis
    rel_error = np.linalg.norm(phase_table - approx) / max(np.linalg.norm(phase_table), 1e-12)
    print(
        f"Built low-rank {echo_label} phase model: "
        f"rank={rank}, field_bins={n_bins}, "
        f"field_range=[{field_min_hz:.1f}, {field_max_hz:.1f}] Hz ({field_range_label}), "
        f"time_range=[{1e3 * float(sample_times_s.min()):.3f}, "
        f"{1e3 * float(sample_times_s.max()):.3f}] ms, "
        f"sign={correction_sign}, "
        f"relative_table_error={rel_error:.3e}"
    )

    return {
        "field_hz": field_hz,
        "freq_bins": freq_bins,
        "coeff_table": coeff_table,
        "time_basis": time_basis,
    }


def lowrank_spatial_coeff(phase_model, basis_index):
    coeff = phase_model["coeff_table"][:, basis_index]
    field_flat = phase_model["field_hz"].reshape(-1)
    freq_bins = phase_model["freq_bins"]
    coeff_real = np.interp(field_flat, freq_bins, coeff.real).astype(np.float32)
    coeff_imag = np.interp(field_flat, freq_bins, coeff.imag).astype(np.float32)
    return (coeff_real + 1j * coeff_imag).reshape(phase_model["field_hz"].shape).astype(np.complex64)


def reconstruct_one_coil_phase_corrected(
    coil_index,
    echo_data,
    k_cm,
    k_norm,
    dcf,
    deapo,
    kernel_lut,
    field_map_hz,
    sample_times_s,
    correction_sign,
    echo_label,
    phase_model=None,
    bad_readouts=None,
    bad_readout_source="detected",
    normalization_scale=None,
):
    processed = preprocess_echo(
        echo_data,
        k_cm,
        coil_index=coil_index,
        echo_label=f"{echo_label} phase-corrected",
        bad_readouts=bad_readouts,
        bad_readout_source=bad_readout_source,
        normalization_scale=normalization_scale,
    )

    n_samples, n_readouts = processed.shape
    field_hz = np.nan_to_num(field_map_hz, nan=0.0).astype(np.float32, copy=False)

    if TE2_PHASE_CORRECTION_MODEL.lower() in ("lowrank", "low-rank", "svd"):
        if phase_model is None:
            phase_model = build_lowrank_phase_model(
                field_hz,
                sample_times_s,
                correction_sign,
                echo_label,
            )

        img = np.zeros((N, N, N), dtype=np.complex64)
        time_basis = phase_model["time_basis"]
        for basis_index in range(time_basis.shape[0]):
            weighted = processed * time_basis[basis_index, :, None]
            with GRID_SEMAPHORE:
                basis_img = regrid_3d_kb(
                    weighted.reshape(-1),
                    k_norm,
                    N,
                    dcf,
                    deapo,
                    OVERSAMP,
                    kernel_lut,
                    KB_WIDTH,
                )

            spatial_coeff = lowrank_spatial_coeff(phase_model, basis_index)
            img += basis_img * spatial_coeff
            print(
                f"    coil {coil_index} {echo_label} low-rank basis "
                f"{basis_index + 1}/{time_basis.shape[0]}"
            )

        return coil_index, img

    if TE2_PHASE_CORRECTION_MODEL.lower() not in ("hard_segments", "hard-segments", "segments"):
        raise ValueError("TE2_PHASE_CORRECTION_MODEL must be 'lowrank' or 'hard_segments'")

    k_norm_3d = k_norm.reshape(n_samples, n_readouts, 3)
    dcf_2d = dcf.reshape(n_samples, n_readouts)

    img = np.zeros((N, N, N), dtype=np.complex64)
    for seg_index, (start, stop) in enumerate(phase_segment_bounds(n_samples, TE2_PHASE_SEGMENTS), start=1):
        seg_data = processed[start:stop, :]
        seg_k_norm = k_norm_3d[start:stop, :, :].reshape(-1, 3)
        seg_dcf = dcf_2d[start:stop, :].reshape(-1)
        t_mid = float(np.mean(sample_times_s[start:stop]))

        with GRID_SEMAPHORE:
            seg_img = regrid_3d_kb(
                seg_data.reshape(-1),
                seg_k_norm,
                N,
                seg_dcf,
                deapo,
                OVERSAMP,
                kernel_lut,
                KB_WIDTH,
            )

        phase = np.exp(
            1j * float(correction_sign) * 2.0 * np.pi * field_hz * t_mid
        ).astype(np.complex64)
        img += seg_img * phase
        print(
            f"    coil {coil_index} {echo_label} segment {seg_index}: "
            f"samples {start}:{stop}, t_mid={1e3 * t_mid:.3f} ms"
        )

    return coil_index, img


def reconstruct_phase_corrected_all_coils(
    data_echo,
    k_cm,
    k_norm,
    dcf,
    deapo,
    kernel_lut,
    field_map_hz,
    sample_times_s,
    correction_sign,
    echo_label,
    bad_readouts_by_coil=None,
    bad_readout_source="detected",
    normalization_scales=None,
):
    n_coils = data_echo.shape[0]
    imgs = np.zeros((n_coils, N, N, N), dtype=np.complex64)
    correction_model = TE2_PHASE_CORRECTION_MODEL.lower()
    phase_model = None
    if correction_model in ("lowrank", "low-rank", "svd"):
        phase_model = build_lowrank_phase_model(
            field_map_hz,
            sample_times_s,
            correction_sign,
            echo_label,
        )
    print(
        f"Reconstructing {echo_label} with phase correction: "
        f"{n_coils} coils, model={TE2_PHASE_CORRECTION_MODEL}, "
        f"segments={TE2_PHASE_SEGMENTS}, lowrank_rank={TE2_PHASE_LOWRANK_RANK}, "
        f"sign={correction_sign}"
    )
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                reconstruct_one_coil_phase_corrected,
                coil,
                data_echo[coil],
                k_cm,
                k_norm,
                dcf,
                deapo,
                kernel_lut,
                field_map_hz,
                sample_times_s,
                correction_sign,
                echo_label,
                phase_model,
                None if bad_readouts_by_coil is None else bad_readouts_by_coil[coil],
                bad_readout_source,
                None if normalization_scales is None else normalization_scales[coil],
            )
            for coil in range(n_coils)
        ]
        for i, future in enumerate(futures, start=1):
            coil, img = future.result()
            imgs[coil] = img
            print(f"  completed corrected {echo_label} coil {coil + 1}/{n_coils} ({i}/{n_coils})")
    return imgs


def estimate_sensitivities(img_coils, smooth_sigma=None, eps=1e-8):
    n_coils, nx, _, _ = img_coils.shape
    if smooth_sigma is None:
        smooth_sigma = nx / 32.0
    print(f"Using sensitivity smoothing sigma: {smooth_sigma:.1f} voxels")
    sens_smooth = np.zeros_like(img_coils, dtype=np.complex64)
    for coil in range(n_coils):
        real = ndi.gaussian_filter(img_coils[coil].real, smooth_sigma)
        imag = ndi.gaussian_filter(img_coils[coil].imag, smooth_sigma)
        sens_smooth[coil] = real + 1j * imag
    rss = np.sqrt(np.sum(np.abs(sens_smooth) ** 2, axis=0)) + eps
    sens_smooth /= rss[None, ...]
    return sens_smooth.astype(np.complex64)


def combine_sum_of_squares(img_coils):
    return np.sqrt(np.sum(np.abs(img_coils) ** 2, axis=0)).astype(np.float32)


def combine_single_echo_coils(img_coils, mode="AC"):
    mode_normalized = mode.strip().upper()
    if img_coils.shape[0] == 1:
        print("Single-channel data: skipping coil combination.")
        return np.abs(img_coils[0])

    if mode_normalized == "SOS":
        print("Performing sum-of-squares coil combination...")
        return combine_sum_of_squares(img_coils)

    if mode_normalized == "AC":
        print("Estimating coil sensitivities...")
        sens_maps = estimate_sensitivities(
            img_coils,
            smooth_sigma=ADAPTIVE_COMBINE_SMOOTH_SIGMA,
        )
        print("Performing adaptive coil combination...")
        img_combined = np.sum(np.conj(sens_maps) * img_coils, axis=0)
        return np.abs(img_combined).astype(np.float32)

    raise ValueError("COIL_COMBINE_MODE must be 'AC' or 'SoS'")


def combine_coils(imgs_te1, imgs_te2):
    print(f"Combining TE1/TE2 coils with N256 AC mode={COIL_COMBINE_MODE}")
    return (
        combine_single_echo_coils(imgs_te1, mode=COIL_COMBINE_MODE),
        combine_single_echo_coils(imgs_te2, mode=COIL_COMBINE_MODE),
        None,
    )


def unwrap_field_map_phase(phase, mask):
    method = FIELD_MAP_UNWRAP_METHOD.lower()
    if method in ("none", "off", "false"):
        return phase.astype(np.float32, copy=False)

    if not np.any(mask):
        return phase.astype(np.float32, copy=False)

    if method == "skimage":
        if skimage_unwrap_phase is None:
            print("Field-map warning: scikit-image unavailable; falling back to axis unwrap")
        else:
            masked_phase = np.ma.array(phase, mask=~mask)
            unwrapped = skimage_unwrap_phase(masked_phase)
            return np.asarray(unwrapped.filled(np.nan), dtype=np.float32)

    if method == "axis" or (method == "skimage" and skimage_unwrap_phase is None):
        phase = np.unwrap(phase, axis=0)
        phase = np.unwrap(phase, axis=1)
        phase = np.unwrap(phase, axis=2)
        return phase.astype(np.float32, copy=False)

    raise ValueError("FIELD_MAP_UNWRAP_METHOD must be 'skimage', 'axis', or 'none'")


def compute_cross_sum_phase_reference(imgs_te1, imgs_te2):
    cross = np.sum(imgs_te2 * np.conj(imgs_te1), axis=0).astype(np.complex64, copy=False)
    return cross, None


def compute_coilwise_phase_reference(imgs_te1, imgs_te2):
    n_coils = imgs_te1.shape[0]
    phase_sum = np.zeros(imgs_te1.shape[1:], dtype=np.complex64)
    weight_sum = np.zeros(imgs_te1.shape[1:], dtype=np.float32)
    eps = np.float32(1e-8)

    for coil in range(n_coils):
        cross = (imgs_te2[coil] * np.conj(imgs_te1[coil])).astype(np.complex64, copy=False)
        abs_cross = np.abs(cross).astype(np.float32, copy=False)
        phase_unit = np.divide(
            cross,
            abs_cross + eps,
            out=np.zeros_like(cross, dtype=np.complex64),
            where=abs_cross > eps,
        )

        if FIELD_MAP_COIL_WEIGHT_NORMALIZE:
            valid = abs_cross[np.isfinite(abs_cross) & (abs_cross > eps)]
            if valid.size:
                scale = np.percentile(valid, FIELD_MAP_COIL_WEIGHT_CLIP_PERCENTILE)
            else:
                scale = 1.0
            weight = np.clip(abs_cross / max(float(scale), 1e-8), 0.0, 1.0).astype(np.float32)
        else:
            weight = abs_cross

        if FIELD_MAP_COIL_WEIGHT_POWER != 1.0:
            weight = np.power(weight, float(FIELD_MAP_COIL_WEIGHT_POWER)).astype(np.float32)

        phase_sum += phase_unit * weight
        weight_sum += weight

    phase_ref = np.divide(
        phase_sum,
        weight_sum + eps,
        out=np.zeros_like(phase_sum, dtype=np.complex64),
        where=weight_sum > eps,
    )
    return phase_ref.astype(np.complex64, copy=False), weight_sum


def compute_field_map_from_coils(imgs_te1, imgs_te2):
    if FIELD_MAP_DELTA_TE_S <= 0:
        raise ValueError("FIELD_MAP_DELTA_TE_S must be positive")

    mode = FIELD_MAP_ESTIMATION_MODE.lower()
    if mode in ("coilwise", "coil-wise", "weighted"):
        cross, coil_weight_sum = compute_coilwise_phase_reference(imgs_te1, imgs_te2)
    elif mode in ("cross_sum", "cross-sum", "sum"):
        cross, coil_weight_sum = compute_cross_sum_phase_reference(imgs_te1, imgs_te2)
    else:
        raise ValueError("FIELD_MAP_ESTIMATION_MODE must be 'coilwise' or 'cross_sum'")

    if FIELD_MAP_PHASE_SMOOTH_SIGMA > 0:
        sigma = float(FIELD_MAP_PHASE_SMOOTH_SIGMA)
        cross = (
            ndi.gaussian_filter(cross.real, sigma)
            + 1j * ndi.gaussian_filter(cross.imag, sigma)
        ).astype(np.complex64)

    mag_te1 = np.sqrt(np.sum(np.abs(imgs_te1) ** 2, axis=0))
    mag_te2 = np.sqrt(np.sum(np.abs(imgs_te2) ** 2, axis=0))
    mag_ref = np.sqrt(mag_te1 * mag_te2)
    threshold = FIELD_MAP_MAG_MASK_FRACTION * np.nanmax(mag_ref)
    mask = mag_ref > threshold
    if coil_weight_sum is not None and FIELD_MAP_COIL_WEIGHT_MIN_FRACTION > 0:
        weight_threshold = FIELD_MAP_COIL_WEIGHT_MIN_FRACTION * np.nanmax(coil_weight_sum)
        mask &= coil_weight_sum > weight_threshold

    phase = np.angle(cross).astype(np.float32, copy=False)
    phase = unwrap_field_map_phase(phase, mask)

    field_map_hz = FIELD_MAP_SIGN * phase / (2.0 * np.pi * FIELD_MAP_DELTA_TE_S)
    mask &= np.isfinite(field_map_hz)

    field_map_hz = field_map_hz.astype(np.float32, copy=False)
    if FIELD_MAP_SMOOTH_SIGMA_HZ > 0:
        sigma = float(FIELD_MAP_SMOOTH_SIGMA_HZ)
        mask_float = mask.astype(np.float32)
        field_filled = np.where(mask, field_map_hz, 0.0).astype(np.float32, copy=False)
        smooth_num = ndi.gaussian_filter(field_filled * mask_float, sigma)
        smooth_den = ndi.gaussian_filter(mask_float, sigma)
        field_map_hz = np.divide(
            smooth_num,
            smooth_den,
            out=np.zeros_like(smooth_num, dtype=np.float32),
            where=smooth_den > 1e-6,
        )
        mask &= smooth_den > 1e-6

    field_map_hz[~mask] = np.nan
    print(
        "Computed TE1/TE2 field map: "
        f"mode={FIELD_MAP_ESTIMATION_MODE}, "
        f"delta_te={FIELD_MAP_DELTA_TE_S * 1e3:.3f} ms, "
        f"unwrap={FIELD_MAP_UNWRAP_METHOD}, "
        f"smooth_sigma_hz={FIELD_MAP_SMOOTH_SIGMA_HZ}, "
        f"mask_voxels={int(mask.sum())}"
    )
    return field_map_hz


def make_bias_mask(reference_mag):
    threshold = N4_MASK_THRESHOLD_FRACTION * np.nanmax(reference_mag)
    mask = np.isfinite(reference_mag) & (reference_mag > threshold)
    if N4_MASK_CLOSE_ITERATIONS > 0:
        mask = ndi.binary_closing(mask, iterations=N4_MASK_CLOSE_ITERATIONS)
    mask = ndi.binary_fill_holes(mask)
    if N4_MASK_ERODE_ITERATIONS > 0:
        mask = ndi.binary_erosion(mask, iterations=N4_MASK_ERODE_ITERATIONS)
    return mask


def get_echo_magnitude(img_te1, img_te2, echo_name):
    echo_name = echo_name.lower()
    if echo_name == "te1":
        return np.abs(img_te1)
    if echo_name == "te2":
        return np.abs(img_te2)
    raise ValueError("Echo selector must be 'te1' or 'te2'")


def estimate_bias_field_sitk_n4(reference_mag, mask):
    if sitk is None:
        raise ImportError(
            "SimpleITK is required for this script. Install it with:\n"
            "  /opt/anaconda3/bin/python3 -m pip install SimpleITK"
        ) from SIMPLEITK_IMPORT_ERROR

    reference_mag = np.asarray(reference_mag, dtype=np.float32)
    if not np.any(mask):
        print("N4 warning: empty object mask; skipping correction")
        return np.ones_like(reference_mag, dtype=np.float32), mask

    p_low, p_high = N4_CLIP_PERCENTILES
    lo, hi = np.percentile(reference_mag[mask], [p_low, p_high])
    reference_clip = np.clip(reference_mag, max(lo, 1e-8), max(hi, lo + 1e-8)).astype(np.float32)
    scale = np.percentile(reference_clip[mask], 99)
    reference_norm = reference_clip / max(float(scale), 1e-8)

    reference_norm_zyx = reference_norm.swapaxes(0, 2).astype(np.float32)
    mask_zyx = mask.swapaxes(0, 2).astype(np.uint8)
    image = sitk.GetImageFromArray(reference_norm_zyx)
    mask_image = sitk.GetImageFromArray(mask_zyx)

    if N4_SHRINK_FACTOR > 1:
        shrink = [int(N4_SHRINK_FACTOR)] * 3
        n4_image = sitk.Shrink(image, shrink)
        n4_mask = sitk.Shrink(mask_image, shrink)
    else:
        n4_image = image
        n4_mask = mask_image

    corrector = sitk.N4BiasFieldCorrectionImageFilter()
    corrector.SetMaximumNumberOfIterations([int(v) for v in N4_MAX_ITERATIONS])
    corrector.SetConvergenceThreshold(float(N4_CONVERGENCE_THRESHOLD))
    if N4_WIENER_FILTER_NOISE is not None:
        corrector.SetWienerFilterNoise(float(N4_WIENER_FILTER_NOISE))
    if N4_BIAS_FIELD_FWHM is not None:
        corrector.SetBiasFieldFullWidthAtHalfMaximum(float(N4_BIAS_FIELD_FWHM))

    print(
        "Running SimpleITK N4 bias correction: "
        f"shrink={N4_SHRINK_FACTOR}, iterations={N4_MAX_ITERATIONS}, "
        f"wiener_noise={N4_WIENER_FILTER_NOISE}, "
        f"bias_fwhm={N4_BIAS_FIELD_FWHM}, "
        f"mask_voxels={int(mask.sum())}"
    )
    corrector.Execute(n4_image, n4_mask)

    log_bias = corrector.GetLogBiasFieldAsImage(image)
    bias = np.exp(sitk.GetArrayFromImage(log_bias)).swapaxes(0, 2).astype(np.float32)
    if np.any(mask):
        bias /= np.median(bias[mask])

    bias_floor = N4_BIAS_FLOOR_FRACTION * np.nanmax(bias)
    bias = np.maximum(bias, bias_floor).astype(np.float32)
    print(
        "SimpleITK N4 bias field: "
        f"median={np.median(bias[mask]):.3f}, "
        f"p05={np.percentile(bias[mask], 5):.3f}, "
        f"p95={np.percentile(bias[mask], 95):.3f}"
    )
    return bias, mask


def apply_bias_field_to_image(img, bias, mask, should_correct):
    if N4_OUTPUT_MASK_DILATE_ITERATIONS > 0:
        output_mask = ndi.binary_dilation(mask, iterations=N4_OUTPUT_MASK_DILATE_ITERATIONS)
    else:
        output_mask = mask

    if should_correct:
        corrected = img / bias
    else:
        corrected = img.copy()

    background_mode = N4_BACKGROUND_MODE.lower()
    if background_mode == "zero":
        corrected = np.where(output_mask, corrected, 0.0)
    elif background_mode == "original":
        corrected = np.where(output_mask, corrected, img)
    elif background_mode == "corrected":
        pass
    else:
        raise ValueError("N4_BACKGROUND_MODE must be 'zero', 'original', or 'corrected'")

    return corrected, output_mask


def apply_n4_bias_correction(img_te1, img_te2):
    if not APPLY_N4_BIAS_CORRECTION:
        return img_te1, img_te2, None, None

    mask_echo = N4_MASK_ECHO.lower()
    bias_echo = N4_BIAS_ECHO.lower()
    mask_reference = get_echo_magnitude(img_te1, img_te2, mask_echo)
    bias_reference = get_echo_magnitude(img_te1, img_te2, bias_echo)
    mask = make_bias_mask(mask_reference)

    print(
        "Preparing SimpleITK N4 bias correction: "
        f"mask_echo={mask_echo}, bias_echo={bias_echo}, "
        f"mask_voxels={int(mask.sum())}"
    )
    bias, mask = estimate_bias_field_sitk_n4(bias_reference, mask)
    apply_to = N4_APPLY_TO_ECHOES.lower()
    if apply_to not in ("both", "te1", "te2"):
        raise ValueError("N4_APPLY_TO_ECHOES must be 'both', 'te1', or 'te2'")
    img_te1, output_mask = apply_bias_field_to_image(img_te1, bias, mask, apply_to in ("both", "te1"))
    img_te2, output_mask = apply_bias_field_to_image(img_te2, bias, mask, apply_to in ("both", "te2"))

    return img_te1, img_te2, bias, output_mask


def orient_like_gridding_script(volume):
    """Match the display orientation used by TPI_gridding_N256_AC_coil_compression_kb.py."""
    return np.asarray(volume).swapaxes(0, 2)


def save_echoes_as_c0(echo1, echo2):
    if not SAVE_C0_FILES:
        return
    if SAVE_OUTPUTS_IN_GRIDDING_DISPLAY_ORIENTATION:
        echo1 = orient_like_gridding_script(echo1)
        echo2 = orient_like_gridding_script(echo2)
    echo1.astype(np.complex64).tofile(C0_ECHO1_FILE)
    echo2.astype(np.complex64).tofile(C0_ECHO2_FILE)
    print(f"Saved {C0_ECHO1_FILE}: shape={echo1.shape}, dtype=complex64")
    print(f"Saved {C0_ECHO2_FILE}: shape={echo2.shape}, dtype=complex64")


def nifti_affine():
    voxel_mm = float(FOV_CM) * 10.0 / float(N)
    return np.diag([voxel_mm, voxel_mm, voxel_mm, 1.0]).astype(np.float32)


def save_nifti_volume(volume, filename, is_field_map=False):
    if nib is None:
        raise ImportError(
            "nibabel is required to write NIfTI files. Install it with:\n"
            "  /opt/anaconda3/bin/python3 -m pip install nibabel"
        ) from NIBABEL_IMPORT_ERROR

    if np.iscomplexobj(volume):
        data = np.abs(volume)
    else:
        data = np.asarray(volume)
    if SAVE_OUTPUTS_IN_GRIDDING_DISPLAY_ORIENTATION:
        data = orient_like_gridding_script(data)
    data = data.astype(np.float32, copy=False)
    if is_field_map:
        data = np.nan_to_num(data, nan=float(NIFTI_FIELD_MAP_NAN_VALUE)).astype(np.float32)

    img = nib.Nifti1Image(data, nifti_affine())
    img.header.set_xyzt_units("mm", "sec")
    img.header["descrip"] = f"FOV={FOV_CM}cm, N={N}".encode("ascii")[:80]
    nib.save(img, filename)
    print(f"Saved {filename}: shape={data.shape}, dtype=float32")


def save_nifti_outputs(
    te1_uncorrected_n4,
    te2_uncorrected_n4,
    te1_final,
    te2_final,
    field_map_hz,
):
    if not SAVE_NIFTI_FILES:
        return

    prefix = NIFTI_OUTPUT_PREFIX
    save_nifti_volume(te1_uncorrected_n4, f"{prefix}_te1_phaseuncorrected_n4.nii.gz")
    save_nifti_volume(te2_uncorrected_n4, f"{prefix}_te2_phaseuncorrected_n4.nii.gz")
    save_nifti_volume(te1_final, f"{prefix}_te1_phasecorrected_n4.nii.gz")
    save_nifti_volume(te2_final, f"{prefix}_te2_phasecorrected_n4.nii.gz")
    save_nifti_volume(field_map_hz, f"{prefix}_b0_map_hz.nii.gz", is_field_map=True)


def plot_te1_te2_n4_corrected(te1_n4_corrected, te2_n4_corrected):
    axial_idx = N // 2 - 20
    sagittal_idx = N // 2
    coronal_idx = N // 2 - 10

    def display_slice(slice_2d):
        return np.rot90(slice_2d, k=DISPLAY_ROT90_K)

    te1_img = orient_like_gridding_script(np.abs(te1_n4_corrected))
    te2_img = orient_like_gridding_script(np.abs(te2_n4_corrected))
    panels = [
        ("TE1 N4 Corrected", te1_img),
        ("TE2 N4 Corrected", te2_img),
    ]
    slice_getters = [
        (lambda img: img[:, :, axial_idx], f"Axial (z={axial_idx})"),
        (lambda img: img[sagittal_idx, :, :], f"Sagittal (x={sagittal_idx})"),
        (lambda img: img[:, coronal_idx, :], f"Coronal (y={coronal_idx})"),
    ]

    fig, axes = plt.subplots(2, 3, figsize=(12, 7))
    for row, (row_label, img) in enumerate(panels):
        row_slices = [display_slice(get_slice(img)) for get_slice, _ in slice_getters]
        values = np.concatenate([slc.ravel() for slc in row_slices])
        values = values[np.isfinite(values)]
        vmax = np.percentile(values, DISPLAY_VMAX_PERCENTILE) if values.size else 1.0
        vmin = np.percentile(values, DISPLAY_VMIN_PERCENTILE) if values.size else 0.0
        if vmax <= vmin:
            vmax = vmin + 1.0

        for col, (slice_2d, (_, col_title)) in enumerate(zip(row_slices, slice_getters)):
            axes[row, col].imshow(slice_2d, cmap="gray", vmin=vmin, vmax=vmax)
            axes[row, col].set_title(f"{row_label} {col_title}")

    for ax in axes.ravel():
        ax.axis("off")
    plt.tight_layout()
    plt.show()


def plot_field_map(field_map_hz):
    if not DISPLAY_FIELD_MAP:
        return
    axial_idx = N // 2 - 20
    sagittal_idx = N // 2
    coronal_idx = N // 2 - 10
    field = field_map_hz
    field = orient_like_gridding_script(field)

    values = field[np.isfinite(field)]
    if values.size == 0:
        vmin, vmax = -1.0, 1.0
    elif FIELD_MAP_DISPLAY_CLIP_HZ is not None:
        vmax = float(FIELD_MAP_DISPLAY_CLIP_HZ)
        vmin = -vmax
    else:
        vmax = float(np.percentile(np.abs(values), 98.0))
        vmax = max(vmax, 1e-6)
        vmin = -vmax

    slices = [
        (field[:, :, axial_idx], f"Field map Axial (z={axial_idx})"),
        (field[sagittal_idx, :, :], f"Field map Sagittal (x={sagittal_idx})"),
        (field[:, coronal_idx, :], f"Field map Coronal (y={coronal_idx})"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    im = None
    for ax, (slc, title) in zip(axes, slices):
        im = ax.imshow(np.rot90(slc, k=DISPLAY_ROT90_K), cmap="coolwarm", vmin=vmin, vmax=vmax)
        ax.set_title(title)
        ax.axis("off")
    fig.colorbar(im, ax=axes.ravel().tolist(), shrink=0.85, label="Hz")
    plt.show()


def plot_results(
    te1_final,
    te2_final,
    field_map_hz,
):
    plot_te1_te2_n4_corrected(te1_final, te2_final)
    plot_field_map(field_map_hz)


def main():
    print(
        f"Configuration: N={N}, oversampling={OVERSAMP}, DCF_ITER={DCF_ITER}, "
        f"kernel=kaiser-bessel, kb_width={KB_WIDTH}, "
        f"fft={'pyfftw' if USE_PYFFTW else 'scipy'}, fft_workers={FFT_WORKERS}, max_workers={MAX_WORKERS}, "
        f"coil_combine={COIL_COMBINE_MODE}, "
        f"coil_compression={COIL_COMPRESSION}, variance_retention={COIL_VARIANCE_RETENTION}, "
        f"save_nifti={SAVE_NIFTI_FILES}, nifti_prefix={NIFTI_OUTPUT_PREFIX}, "
        f"save_gridding_display_orientation={SAVE_OUTPUTS_IN_GRIDDING_DISPLAY_ORIENTATION}, "
        f"simpleitk_n4_bias={APPLY_N4_BIAS_CORRECTION}, "
        f"n4_mask_echo={N4_MASK_ECHO}, n4_bias_echo={N4_BIAS_ECHO}, "
        f"n4_background={N4_BACKGROUND_MODE}, "
        f"te2_bad_readouts_from_te1={USE_TE1_BAD_READOUTS_FOR_TE2}, "
        f"echo_normalization={ECHO_NORMALIZATION_MODE}, "
        f"traj_delay_correction={APPLY_TRAJECTORY_DELAY_CORRECTION}, "
        f"traj_delay_apply_to={TRAJ_DELAY_APPLY_TO}, "
        f"traj_delay_us={TRAJ_DELAY_US}, "
        f"field_map_mode={FIELD_MAP_ESTIMATION_MODE}, "
        f"field_map_delta_te_ms={FIELD_MAP_DELTA_TE_S * 1e3:.3f}, "
        f"te1_phase_correction={RUN_TE1_PHASE_CORRECTION}, "
        f"te1_phase_sign={TE1_PHASE_CORRECTION_SIGN}, "
        f"te2_phase_correction={RUN_TE2_PHASE_CORRECTION}, "
        f"te2_phase_model={TE2_PHASE_CORRECTION_MODEL}, "
        f"te2_phase_segments={TE2_PHASE_SEGMENTS}, "
        f"te2_lowrank_rank={TE2_PHASE_LOWRANK_RANK}, "
        f"field_model_range={FIELD_MODEL_RANGE_MODE}, "
        f"te2_phase_sign={TE2_PHASE_CORRECTION_SIGN}"
    )
    k_cm_te1, k_cm_te2, t_te1_s, t_te2_s, data_te1_raw, data_te2_raw = load_dual_echo_multi_coil()

    k_cm_te1 = apply_trajectory_delay_correction(k_cm_te1, t_te1_s, "TE1")
    k_cm_te2 = apply_trajectory_delay_correction(k_cm_te2, t_te2_s, "TE2")

    k_nyquist = 1.0 / (2.0 * FOV_CM / N)
    k_norm_te1 = (k_cm_te1.reshape(-1, 3) / k_nyquist / 2.0).astype(np.float32)
    k_norm_te2 = (k_cm_te2.reshape(-1, 3) / k_nyquist / 2.0).astype(np.float32)

    kb_beta = default_kb_beta(KB_WIDTH, OVERSAMP) if KB_BETA is None else float(KB_BETA)
    print(f"Kaiser-Bessel beta={kb_beta:.4f}, LUT size={KB_LUT_SIZE}")
    kernel_lut = make_kaiser_bessel_lut(KB_WIDTH, kb_beta, KB_LUT_SIZE)

    print("Preparing TE1 trajectory weights")
    dcf_te1 = compute_dcf_kb(
        k_norm_te1,
        grid_size=N,
        oversamp=OVERSAMP,
        n_iter=DCF_ITER,
        kernel_lut=kernel_lut,
        width=KB_WIDTH,
    )
    print("Preparing TE2 trajectory weights")
    dcf_te2 = compute_dcf_kb(
        k_norm_te2,
        grid_size=N,
        oversamp=OVERSAMP,
        n_iter=DCF_ITER,
        kernel_lut=kernel_lut,
        width=KB_WIDTH,
    )
    deapo = compute_deapodisation_kb(N, OVERSAMP, kernel_lut, KB_WIDTH)

    te1_bad_readouts_by_coil = None
    if USE_TE1_BAD_READOUTS_FOR_TE2:
        te1_bad_readouts_by_coil = find_bad_readouts_by_coil(data_te1_raw, "TE1")

    te1_normalization_scales, te2_normalization_scales = build_echo_normalization_scales(
        data_te1_raw,
        data_te2_raw,
        te1_bad_readouts_by_coil,
    )

    imgs_te1 = reconstruct_echo_all_coils(
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
    imgs_te2 = reconstruct_echo_all_coils(
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
    field_map_hz = compute_field_map_from_coils(imgs_te1, imgs_te2)
    img_te1_uncorrected_combined, img_te2_uncorrected_combined, _ = combine_coils(imgs_te1, imgs_te2)

    if RUN_TE1_PHASE_CORRECTION:
        imgs_te1_for_output = reconstruct_phase_corrected_all_coils(
            data_te1_raw,
            k_cm_te1,
            k_norm_te1,
            dcf_te1,
            deapo,
            kernel_lut,
            field_map_hz,
            t_te1_s,
            TE1_PHASE_CORRECTION_SIGN,
            "TE1",
            bad_readouts_by_coil=te1_bad_readouts_by_coil,
            bad_readout_source="TE1 precomputed",
            normalization_scales=te1_normalization_scales,
        )
    else:
        imgs_te1_for_output = imgs_te1

    if RUN_TE2_PHASE_CORRECTION:
        imgs_te2_for_output = reconstruct_phase_corrected_all_coils(
            data_te2_raw,
            k_cm_te2,
            k_norm_te2,
            dcf_te2,
            deapo,
            kernel_lut,
            field_map_hz,
            t_te2_s,
            TE2_PHASE_CORRECTION_SIGN,
            "TE2",
            bad_readouts_by_coil=te1_bad_readouts_by_coil,
            bad_readout_source="TE1 precomputed",
            normalization_scales=te2_normalization_scales,
        )
    else:
        imgs_te2_for_output = imgs_te2

    img_te1_phasecorr_combined, img_te2_phasecorr_combined, combine_weights = combine_coils(
        imgs_te1_for_output,
        imgs_te2_for_output,
    )
    img_te1_combined, img_te2_combined, bias_field, bias_mask = apply_n4_bias_correction(
        img_te1_phasecorr_combined,
        img_te2_phasecorr_combined,
    )
    if APPLY_N4_BIAS_CORRECTION and bias_field is not None and bias_mask is not None:
        apply_to = N4_APPLY_TO_ECHOES.lower()
        img_te1_uncorrected_n4, _ = apply_bias_field_to_image(
            img_te1_uncorrected_combined,
            bias_field,
            bias_mask,
            apply_to in ("both", "te1"),
        )
        img_te2_uncorrected_n4, _ = apply_bias_field_to_image(
            img_te2_uncorrected_combined,
            bias_field,
            bias_mask,
            apply_to in ("both", "te2"),
        )
    else:
        img_te1_uncorrected_n4 = img_te1_uncorrected_combined
        img_te2_uncorrected_n4 = img_te2_uncorrected_combined

    save_echoes_as_c0(img_te1_combined, img_te2_combined)
    save_nifti_outputs(
        img_te1_uncorrected_n4,
        img_te2_uncorrected_n4,
        img_te1_combined,
        img_te2_combined,
        field_map_hz,
    )

    # with h5py.File(OUTPUT_FILE, "w") as f:
    #     if np.iscomplexobj(img_te1_combined):
    #         f.create_dataset("img_te1_complex", data=img_te1_combined, compression="gzip")
    #         f.create_dataset("img_te2_complex", data=img_te2_combined, compression="gzip")
    #     f.create_dataset("img_te1_abs", data=np.abs(img_te1_combined).astype(np.float32), compression="gzip")
    #     f.create_dataset("img_te2_abs", data=np.abs(img_te2_combined).astype(np.float32), compression="gzip")
    #     if combine_weights is not None:
    #         f.create_dataset("combine_weights", data=combine_weights, compression="gzip")
    #     if SAVE_COIL_IMAGES:
    #         f.create_dataset("coil_imgs_te1", data=imgs_te1, compression="gzip")
    #         f.create_dataset("coil_imgs_te2", data=imgs_te2, compression="gzip")
    #     f.attrs["fov_cm"] = FOV_CM
    #     f.attrs["matrix_size"] = N
    #     f.attrs["oversampling"] = OVERSAMP
    #     f.attrs["num_coils"] = data_te1_raw.shape[0]
    #     f.attrs["coil_combine_method"] = COIL_COMBINE_MODE

    # print(f"Saved: {OUTPUT_FILE}")
    # print(f"Voxel size: {FOV_CM / N:.4f} cm")

    if PLOT_RESULTS:
        plot_results(
            img_te1_combined,
            img_te2_combined,
            field_map_hz,
        )


if __name__ == "__main__":
    main()
