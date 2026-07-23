#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
import ctypes
import logging
import os
from pathlib import Path
import threading
from time import perf_counter
import traceback
import uuid

import h5py
import ismrmrd
import numba
import numpy as np
import scipy.ndimage as ndi

import constants
import mrdhelper


try:
    import pyfftw

    pyfftw.interfaces.cache.enable()
    USE_PYFFTW = True
except ImportError:
    pyfftw = None
    USE_PYFFTW = False


debugFolder = "/tmp/share/debug"
BUNDLED_TRAJECTORIES = {
    "sodiumn50": "/opt/sodiumgridding/23Na_n50_trajectory.h5",
    "sodiumn28": "/opt/sodiumgridding/23Na_n28_trajectory.h5",
    "23Na_n50": "/opt/sodiumgridding/23Na_n50_trajectory.h5",
    "23Na_n28": "/opt/sodiumgridding/23Na_n28_trajectory.h5",
}
OPENRECON_DEFAULTS = {
    "config": "sodiumgridding",
    "matrixsize": 128,
    "fovcm": 22.0,
    "trajectorypreset": "23Na_n28",
    "trajectoryfile": "",
    "trajectorydataset": "k",
    "trajectorysampleoffset": 0,
    "rejectbadreadouts": True,
    "badreadoutsigma": 3.0,
    "centerwindow": 5,
    "applyfermifilter": True,
    "fermiwidth": 0.05,
    "fermicutoff": 0.98,
    "dcfiterations": 5,
    "maxcoils": 16,
    "maxworkers": 8,
    "coilvarianceretention": 0.9,
    "coilcombinemode": "AC",
    "applyn4biascorrection": True,
}
OUTPUT_SERIES_DESCRIPTION = "sodiumgridding"
OUTPUT_IMAGE_COMMENT = "23Na Kaiser-Bessel Gridding"
OUTPUT_IMAGE_SERIES_INDEX = 1
SCANNER_DISPLAY_MIN = 0
SCANNER_DISPLAY_MAX = 4096
OVERSAMPLING = 2
KB_KERNEL_WIDTH = 3.0
KB_LUT_SIZE = 2048
MAX_SIMULTANEOUS_GRIDS = 2
N4_SHRINK_FACTOR = 2
N4_MAX_ITERATIONS = [50, 50, 50, 50]
GRID_SEMAPHORE = threading.Semaphore(MAX_SIMULTANEOUS_GRIDS)


def _kaiser_bessel_parameters(kernel_width=KB_KERNEL_WIDTH, oversampling=OVERSAMPLING):
    radicand = (kernel_width / oversampling * (oversampling - 0.5)) ** 2 - 0.8
    if radicand <= 0:
        raise ValueError(
            "Kaiser-Bessel parameters require a positive beta radicand; "
            f"got kernel_width={kernel_width} oversampling={oversampling}"
        )
    beta = float(np.pi * np.sqrt(radicand))
    distances = np.linspace(
        0.0,
        0.5 * kernel_width,
        KB_LUT_SIZE,
        dtype=np.float64,
    )
    lut = np.i0(
        beta
        * np.sqrt(
            np.maximum(
                0.0,
                1.0 - (distances / (0.5 * kernel_width)) ** 2,
            )
        )
    ) / np.i0(beta)
    return beta, np.asarray(lut, dtype=np.float64)


KB_BETA, KB_LUT = _kaiser_bessel_parameters()


def _get_config_value(config, key, default, value_type):
    try:
        return mrdhelper.get_json_config_param(config, key, default=default, type=value_type)
    except Exception:
        return default


def _config_bool(config, key, default):
    value = _get_config_value(config, key, default, "bool")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _config_int(config, key, default):
    value = _get_config_value(config, key, default, "int")
    try:
        return int(value)
    except Exception:
        return int(default)


def _config_float(config, key, default):
    value = _get_config_value(config, key, default, "float")
    try:
        return float(value)
    except Exception:
        return float(default)


def _config_str(config, key, default):
    value = _get_config_value(config, key, default, "str")
    if value is None:
        return default
    return str(value)


def _ensure_debug_folder():
    os.makedirs(debugFolder, exist_ok=True)


def _read_runtime_file(*paths):
    for path_text in paths:
        try:
            value = Path(path_text).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return "unavailable"


def _cgroup_cpu_limit():
    cpu_max = _read_runtime_file("/sys/fs/cgroup/cpu.max")
    if cpu_max != "unavailable":
        return cpu_max

    quota = _read_runtime_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period = _read_runtime_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota == "unavailable" and period == "unavailable":
        return "unavailable"
    return f"quota={quota} period={period}"


def _cgroup_cpuset():
    return _read_runtime_file(
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/cpuset/cpuset.cpus",
    )


def _log_cpu_resources(configured_max_workers, effective_coil_workers):
    logical_cpu_count = os.cpu_count()
    try:
        affinity = sorted(os.sched_getaffinity(0))
        affinity_count = len(affinity)
        affinity_cpus = ",".join(str(cpu) for cpu in affinity)
    except (AttributeError, OSError):
        affinity_count = "unavailable"
        affinity_cpus = "unavailable"

    logging.info(
        "FIRE CPU resources: os_cpu_count=%s affinity_count=%s "
        "affinity_cpus=%s cgroup_cpu_limit='%s' cgroup_cpuset='%s' "
        "configured_maxworkers=%d effective_coil_workers=%d",
        logical_cpu_count,
        affinity_count,
        affinity_cpus,
        _cgroup_cpu_limit(),
        _cgroup_cpuset(),
        configured_max_workers,
        effective_coil_workers,
    )


def _safe_protocol_name(metadata):
    try:
        protocol_name = getattr(metadata.measurementInformation, "protocolName", "")
        if protocol_name:
            return str(protocol_name)
    except Exception:
        pass
    return OUTPUT_SERIES_DESCRIPTION


@numba.njit(fastmath=True)
def _kaiser_bessel_weight_lut(distance, kernel_width, lut):
    half_width = 0.5 * kernel_width
    if distance > half_width:
        return 0.0

    lut_position = distance / half_width * (len(lut) - 1)
    index = int(np.floor(lut_position))
    if index >= len(lut) - 1:
        return lut[len(lut) - 1]

    fraction = lut_position - index
    return lut[index] * (1.0 - fraction) + lut[index + 1] * fraction


@numba.njit
def _grid_kb_complex(
    grid,
    normalized_coordinates,
    weights,
    grid_size,
    oversampling,
    kernel_width,
    lut,
):
    oversampled_size = grid_size * oversampling
    half_width = 0.5 * kernel_width

    for point_index in range(len(normalized_coordinates)):
        grid_x = (normalized_coordinates[point_index, 0] + 0.5) * oversampled_size - 0.5
        grid_y = (normalized_coordinates[point_index, 1] + 0.5) * oversampled_size - 0.5
        grid_z = (normalized_coordinates[point_index, 2] + 0.5) * oversampled_size - 0.5
        x_min = int(np.ceil(grid_x - half_width))
        x_max = int(np.floor(grid_x + half_width))
        y_min = int(np.ceil(grid_y - half_width))
        y_max = int(np.floor(grid_y + half_width))
        z_min = int(np.ceil(grid_z - half_width))
        z_max = int(np.floor(grid_z + half_width))
        value = weights[point_index]

        for z_index in range(z_min, z_max + 1):
            if 0 <= z_index < oversampled_size:
                z_weight = _kaiser_bessel_weight_lut(
                    abs(grid_z - z_index), kernel_width, lut
                )
                for y_index in range(y_min, y_max + 1):
                    if 0 <= y_index < oversampled_size:
                        y_weight = _kaiser_bessel_weight_lut(
                            abs(grid_y - y_index), kernel_width, lut
                        )
                        for x_index in range(x_min, x_max + 1):
                            if 0 <= x_index < oversampled_size:
                                x_weight = _kaiser_bessel_weight_lut(
                                    abs(grid_x - x_index), kernel_width, lut
                                )
                                grid[z_index, y_index, x_index] += (
                                    value * x_weight * y_weight * z_weight
                                )


@numba.njit(fastmath=True)
def _grid_kb_real(
    grid,
    normalized_coordinates,
    weights,
    grid_size,
    oversampling,
    kernel_width,
    lut,
):
    oversampled_size = grid_size * oversampling
    half_width = 0.5 * kernel_width

    for point_index in range(len(normalized_coordinates)):
        grid_x = (normalized_coordinates[point_index, 0] + 0.5) * oversampled_size - 0.5
        grid_y = (normalized_coordinates[point_index, 1] + 0.5) * oversampled_size - 0.5
        grid_z = (normalized_coordinates[point_index, 2] + 0.5) * oversampled_size - 0.5
        x_min = int(np.ceil(grid_x - half_width))
        x_max = int(np.floor(grid_x + half_width))
        y_min = int(np.ceil(grid_y - half_width))
        y_max = int(np.floor(grid_y + half_width))
        z_min = int(np.ceil(grid_z - half_width))
        z_max = int(np.floor(grid_z + half_width))
        value = weights[point_index]

        for z_index in range(z_min, z_max + 1):
            if 0 <= z_index < oversampled_size:
                z_weight = _kaiser_bessel_weight_lut(
                    abs(grid_z - z_index), kernel_width, lut
                )
                for y_index in range(y_min, y_max + 1):
                    if 0 <= y_index < oversampled_size:
                        y_weight = _kaiser_bessel_weight_lut(
                            abs(grid_y - y_index), kernel_width, lut
                        )
                        for x_index in range(x_min, x_max + 1):
                            if 0 <= x_index < oversampled_size:
                                x_weight = _kaiser_bessel_weight_lut(
                                    abs(grid_x - x_index), kernel_width, lut
                                )
                                grid[z_index, y_index, x_index] += (
                                    value * x_weight * y_weight * z_weight
                                )


@numba.njit(fastmath=True)
def _sample_kb(
    values,
    grid,
    normalized_coordinates,
    grid_size,
    oversampling,
    kernel_width,
    lut,
):
    oversampled_size = grid_size * oversampling
    half_width = 0.5 * kernel_width

    for point_index in range(len(normalized_coordinates)):
        grid_x = (normalized_coordinates[point_index, 0] + 0.5) * oversampled_size - 0.5
        grid_y = (normalized_coordinates[point_index, 1] + 0.5) * oversampled_size - 0.5
        grid_z = (normalized_coordinates[point_index, 2] + 0.5) * oversampled_size - 0.5
        x_min = int(np.ceil(grid_x - half_width))
        x_max = int(np.floor(grid_x + half_width))
        y_min = int(np.ceil(grid_y - half_width))
        y_max = int(np.floor(grid_y + half_width))
        z_min = int(np.ceil(grid_z - half_width))
        z_max = int(np.floor(grid_z + half_width))
        value = 0.0

        for z_index in range(z_min, z_max + 1):
            if 0 <= z_index < oversampled_size:
                z_weight = _kaiser_bessel_weight_lut(
                    abs(grid_z - z_index), kernel_width, lut
                )
                for y_index in range(y_min, y_max + 1):
                    if 0 <= y_index < oversampled_size:
                        y_weight = _kaiser_bessel_weight_lut(
                            abs(grid_y - y_index), kernel_width, lut
                        )
                        for x_index in range(x_min, x_max + 1):
                            if 0 <= x_index < oversampled_size:
                                x_weight = _kaiser_bessel_weight_lut(
                                    abs(grid_x - x_index), kernel_width, lut
                                )
                                value += (
                                    grid[z_index, y_index, x_index]
                                    * x_weight
                                    * y_weight
                                    * z_weight
                                )
        values[point_index] = value


def _normalize_grid_coordinates(trajectory, matrix_size, fov_cm):
    physical_coordinates = np.asarray(trajectory, dtype=np.float32).reshape(-1, 3)
    normalized = physical_coordinates * (float(fov_cm) / float(matrix_size))
    max_coordinate = float(np.max(np.abs(normalized))) if normalized.size else 0.0
    if max_coordinate > 0.5 + 1e-5:
        logging.warning(
            "Normalized trajectory extends beyond the gridding FOV: max_abs=%.6f",
            max_coordinate,
        )
    return np.asarray(normalized, dtype=np.float32)


def _compute_dcf_kb(
    normalized_coordinates,
    grid_size,
    oversampling=OVERSAMPLING,
    num_iterations=5,
):
    oversampled_size = grid_size * oversampling
    dcf = np.ones(len(normalized_coordinates), dtype=np.float64)

    for iteration in range(num_iterations):
        logging.info("Computing Kaiser-Bessel DCF iteration %d/%d", iteration + 1, num_iterations)
        density_grid = np.zeros(
            (oversampled_size, oversampled_size, oversampled_size),
            dtype=np.float64,
        )
        _grid_kb_real(
            density_grid,
            normalized_coordinates,
            dcf,
            grid_size,
            oversampling,
            KB_KERNEL_WIDTH,
            KB_LUT,
        )
        sampled_density = np.zeros_like(dcf)
        _sample_kb(
            sampled_density,
            density_grid,
            normalized_coordinates,
            grid_size,
            oversampling,
            KB_KERNEL_WIDTH,
            KB_LUT,
        )
        sampled_density[sampled_density < 1e-9] = 1.0
        dcf /= sampled_density
        median = float(np.median(dcf))
        if median > 0.0 and np.isfinite(median):
            dcf /= median

    return np.asarray(dcf, dtype=np.float32)


def _compute_deapodization_kb(grid_size, oversampling=OVERSAMPLING):
    oversampled_size = grid_size * oversampling
    kernel_1d = np.zeros(oversampled_size, dtype=np.complex128)
    grid_center = 0.5 * oversampled_size - 0.5
    half_width = 0.5 * KB_KERNEL_WIDTH
    index_min = int(np.ceil(grid_center - half_width))
    index_max = int(np.floor(grid_center + half_width))

    for index in range(index_min, index_max + 1):
        if 0 <= index < oversampled_size:
            distance = abs(grid_center - index)
            ratio = distance / half_width
            kernel_1d[index] = np.i0(
                KB_BETA * np.sqrt(max(0.0, 1.0 - ratio * ratio))
            ) / np.i0(KB_BETA)

    response_1d = (
        np.fft.ifftshift(np.fft.ifft(np.fft.ifftshift(kernel_1d)))
        * oversampled_size
    )
    start = (oversampled_size - grid_size) // 2
    stop = start + grid_size
    deapodization_1d = np.abs(response_1d[start:stop])
    deapodization = np.multiply.outer(
        np.multiply.outer(deapodization_1d, deapodization_1d).ravel(),
        deapodization_1d,
    ).reshape(grid_size, grid_size, grid_size)
    deapodization /= np.nanmax(deapodization)
    deapodization = np.where(deapodization < 1e-8, 1e-8, deapodization)
    return np.asarray(deapodization, dtype=np.float32)


def _regrid_3d_kb(
    kspace_data,
    normalized_coordinates,
    grid_size,
    dcf,
    deapodization,
    oversampling=OVERSAMPLING,
):
    oversampled_size = grid_size * oversampling
    weighted_data = (
        np.asarray(kspace_data).ravel() * np.asarray(dcf).ravel()
    ).astype(np.complex128)
    grid = np.zeros(
        (oversampled_size, oversampled_size, oversampled_size),
        dtype=np.complex128,
    )
    _grid_kb_complex(
        grid,
        normalized_coordinates,
        weighted_data,
        grid_size,
        oversampling,
        KB_KERNEL_WIDTH,
        KB_LUT,
    )

    shifted_grid = np.fft.ifftshift(grid)
    if USE_PYFFTW:
        inverse_fft = pyfftw.builders.ifftn(
            shifted_grid,
            auto_align_input=True,
            auto_contiguous=True,
            threads=-1,
        )
        transformed_grid = inverse_fft()
    else:
        transformed_grid = np.fft.ifftn(shifted_grid)

    oversampled_image = np.fft.ifftshift(transformed_grid) * (oversampled_size**3)
    start = (oversampled_size - grid_size) // 2
    stop = start + grid_size
    image = oversampled_image[start:stop, start:stop, start:stop].copy()
    image /= deapodization
    return np.asarray(image, dtype=np.complex64)


def _normalize_trajectory_array(traj):
    array = np.asarray(traj)
    if array.size == 0:
        raise ValueError("Trajectory array is empty")

    if array.ndim == 2:
        if array.shape[-1] in (2, 3):
            array = array[None, :, :]
        elif array.shape[0] in (2, 3):
            array = array.T[None, :, :]
        else:
            raise ValueError(f"Unsupported 2D trajectory shape: {array.shape}")
    elif array.ndim == 3:
        if array.shape[-1] in (2, 3):
            pass
        elif array.shape[0] in (2, 3):
            array = np.moveaxis(array, 0, -1)
        elif array.shape[1] in (2, 3):
            array = np.moveaxis(array, 1, -1)
        else:
            raise ValueError(f"Unsupported 3D trajectory shape: {array.shape}")
    else:
        raise ValueError(f"Unsupported trajectory shape: {array.shape}")

    if array.shape[-1] == 2:
        array = np.concatenate(
            [array, np.zeros(array.shape[:-1] + (1,), dtype=array.dtype)],
            axis=-1,
        )

    return np.asarray(array, dtype=np.float32)


def _load_trajectory_from_file(path_text, dataset_name):
    path = Path(path_text).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Trajectory file does not exist: {path}")

    with h5py.File(path, "r") as h5_file:
        if dataset_name not in h5_file:
            raise KeyError(
                f"Trajectory dataset '{dataset_name}' not found in {path}. "
                f"Available datasets: {list(h5_file.keys())}"
            )
        trajectory = h5_file[dataset_name][...]

    logging.info("Loaded trajectory from %s[%s] with shape %s", path, dataset_name, trajectory.shape)
    return _normalize_trajectory_array(trajectory)


def _resolve_trajectory_file(config):
    explicit_file = _config_str(
        config,
        "trajectoryfile",
        OPENRECON_DEFAULTS["trajectoryfile"],
    ).strip()
    if explicit_file:
        if explicit_file in BUNDLED_TRAJECTORIES:
            trajectory_file = BUNDLED_TRAJECTORIES[explicit_file]
            logging.info("Using bundled trajectory %s: %s", explicit_file, trajectory_file)
            return trajectory_file
        logging.info("Using trajectory file override: %s", explicit_file)
        return explicit_file

    preset = _config_str(
        config,
        "trajectorypreset",
        OPENRECON_DEFAULTS["trajectorypreset"],
    ).strip() or OPENRECON_DEFAULTS["trajectorypreset"]

    if preset in BUNDLED_TRAJECTORIES:
        trajectory_file = BUNDLED_TRAJECTORIES[preset]
        logging.info("Using bundled trajectory preset %s: %s", preset, trajectory_file)
        return trajectory_file

    valid_presets = ", ".join(sorted(BUNDLED_TRAJECTORIES))
    raise ValueError(
        f"Unknown trajectory preset '{preset}'. "
        f"Expected one of: {valid_presets}. "
        "Use 'trajectoryfile' to provide an explicit external HDF5 path."
    )


def _load_trajectory(acquisitions, config):
    embedded_trajectory = []
    for acquisition in acquisitions:
        traj = getattr(acquisition, "traj", None)
        if traj is None:
            continue
        traj_array = np.asarray(traj)
        if traj_array.size == 0:
            continue
        embedded_trajectory.append(_normalize_trajectory_array(traj_array)[0])

    if embedded_trajectory:
        trajectory = np.stack(embedded_trajectory, axis=0)
        logging.info("Using embedded ISMRMRD trajectory with shape %s", trajectory.shape)
        return trajectory

    trajectory_file = _resolve_trajectory_file(config)
    if not trajectory_file:
        raise ValueError(
            "No embedded trajectory found in the MRD acquisitions and no "
            "'trajectorypreset' or 'trajectoryfile' parameter was provided."
        )

    trajectory_dataset = _config_str(
        config,
        "trajectorydataset",
        OPENRECON_DEFAULTS["trajectorydataset"],
    ).strip() or OPENRECON_DEFAULTS["trajectorydataset"]
    return _load_trajectory_from_file(trajectory_file, trajectory_dataset)


def _build_data_array(acquisitions):
    num_readouts = len(acquisitions)
    num_coils = int(acquisitions[0].data.shape[0])
    num_samples = min(int(acq.data.shape[1]) for acq in acquisitions)

    data = np.zeros((num_coils, num_readouts, num_samples), dtype=np.complex64)
    for readout_index, acquisition in enumerate(acquisitions):
        acquisition_data = np.asarray(acquisition.data, dtype=np.complex64)
        if acquisition_data.shape[0] != num_coils:
            raise ValueError(
                "All acquisitions must contain the same number of coils. "
                f"Expected {num_coils}, got {acquisition_data.shape[0]}"
            )
        data[:, readout_index, :] = acquisition_data[:, :num_samples]

    return data


def _compute_default_fov_cm(metadata):
    try:
        fov_cm = float(metadata.encoding[0].reconSpace.fieldOfView_mm.x) / 10.0
        if fov_cm <= 0:
            raise ValueError(f"Invalid reconSpace FOV from metadata: {fov_cm}")
        return fov_cm
    except Exception:
        return OPENRECON_DEFAULTS["fovcm"]


def _compute_default_matrix_size(metadata):
    try:
        matrix_size = int(metadata.encoding[0].reconSpace.matrixSize.x)
        if matrix_size <= 1:
            raise ValueError(f"Invalid reconSpace matrix size from metadata: {matrix_size}")
        return matrix_size
    except Exception:
        return OPENRECON_DEFAULTS["matrixsize"]


def _clip_data_to_trajectory(data, trajectory, sample_offset):
    data_readouts = data.shape[1]
    data_samples = data.shape[2]

    if trajectory.ndim == 3:
        direct_score = abs(trajectory.shape[0] - data_samples) + abs(trajectory.shape[1] - data_readouts)
        swapped_score = abs(trajectory.shape[1] - data_samples) + abs(trajectory.shape[0] - data_readouts)

        if swapped_score < direct_score:
            logging.warning(
                "Swapping trajectory axes to match standalone dimensions: trajectory=%s data=(samples=%d, readouts=%d)",
                trajectory.shape,
                data_samples,
                data_readouts,
            )
            trajectory = np.swapaxes(trajectory, 0, 1)

    available_sample_offset = max(0, trajectory.shape[0] - data_samples)
    applied_sample_offset = max(0, min(sample_offset, available_sample_offset))
    if applied_sample_offset > 0:
        logging.info(
            "Applying trajectory sample offset %d to align %d trajectory samples with %d raw samples",
            applied_sample_offset,
            trajectory.shape[0],
            data_samples,
        )

    samples = min(data.shape[2], trajectory.shape[0] - applied_sample_offset)
    readouts = min(data.shape[1], trajectory.shape[1])
    if readouts != data.shape[1] or samples != data.shape[2]:
        logging.warning(
            "Cropping raw data to match trajectory dimensions: data=%s trajectory=%s offset=%d -> (%d, %d)",
            data.shape,
            trajectory.shape,
            applied_sample_offset,
            readouts,
            samples,
        )
    return (
        data[:, :readouts, :samples],
        trajectory[applied_sample_offset:applied_sample_offset + samples, :readouts, :],
    )


def _compute_sample_mask_and_scale(coil_data, sigma):
    column_max = np.abs(coil_data).max(axis=0)
    smoothed = ndi.gaussian_filter1d(column_max.astype(np.float32), 5)
    histogram, edges = np.histogram(smoothed, bins=40)
    modal_edge = float(edges[int(np.argmax(histogram))])

    nearby = column_max[column_max >= 0.95 * modal_edge]
    if nearby.size == 0:
        bad_columns = np.array([], dtype=np.int64)
    else:
        threshold = modal_edge - sigma * float(nearby.std())
        bad_columns = np.where(column_max < threshold)[0]

    filtered = np.array(coil_data, copy=True)
    if bad_columns.size > 0:
        filtered[:, bad_columns] = 0

    return filtered, bad_columns


def _build_fermi_filter(
    trajectory,
    apply_fermi_filter,
    fermi_width,
    fermi_cutoff,
):
    if not apply_fermi_filter:
        return np.ones(trajectory.shape[:2], dtype=np.float32)

    abs_k = np.linalg.norm(trajectory, axis=-1)
    abs_k_max = float(np.max(abs_k)) if abs_k.size else 0.0
    if abs_k_max <= 0.0:
        return np.ones(trajectory.shape[:2], dtype=np.float32)

    normalized_radius = abs_k / abs_k_max
    fermi_filter = 1.0 / (
        1.0
        + np.exp(
            (normalized_radius - float(fermi_cutoff))
            / max(float(fermi_width), 1e-6)
        )
    )
    return np.asarray(fermi_filter, dtype=np.float32)


def _prepare_single_coil_data(
    coil_index,
    coil_data,
    reject_bad_readouts,
    bad_readout_sigma,
    center_window,
    fermi_filter,
):
    working_data = np.asarray(coil_data.T, dtype=np.complex64)

    if reject_bad_readouts:
        working_data, bad_columns = _compute_sample_mask_and_scale(
            working_data,
            sigma=bad_readout_sigma,
        )
        logging.info(
            "Coil %d: rejected %d low-signal sample columns",
            coil_index,
            bad_columns.size,
        )
    else:
        bad_columns = np.array([], dtype=np.int64)
        working_data = np.array(working_data, copy=True)

    center = working_data.shape[1] // 2
    start = max(0, center - center_window)
    stop = min(working_data.shape[1], center + center_window)
    reference_mean = float(np.abs(working_data[:, start:stop]).mean())
    if reference_mean > 0:
        working_data *= 2.0 / reference_mean

    return np.asarray(working_data * fermi_filter, dtype=np.complex64)


def _compress_coils_by_variance(coil_data, variance_retention=0.9, epsilon=1e-12):
    num_input_coils = int(coil_data.shape[0])
    if num_input_coils <= 1:
        return (
            np.asarray(coil_data, dtype=np.complex64),
            np.array([1.0], dtype=np.float32),
            np.eye(num_input_coils, dtype=np.complex64),
        )

    data_2d = np.asarray(coil_data).reshape(num_input_coils, -1)
    covariance = data_2d @ data_2d.conj().T
    covariance /= max(data_2d.shape[1], 1)
    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = np.maximum(eigenvalues[order].real, 0.0)
    eigenvectors = eigenvectors[:, order]

    total_variance = float(eigenvalues.sum())
    if total_variance <= epsilon:
        logging.warning(
            "Coil compression skipped because covariance has near-zero variance"
        )
        return (
            np.asarray(coil_data, dtype=np.complex64),
            np.ones(num_input_coils, dtype=np.float32),
            np.eye(num_input_coils, dtype=np.complex64),
        )

    cumulative_variance = np.cumsum(eigenvalues) / total_variance
    retention = float(np.clip(variance_retention, 0.0, 1.0))
    num_virtual_coils = min(
        num_input_coils,
        int(np.searchsorted(cumulative_variance, retention) + 1),
    )
    compression_matrix = np.asarray(
        eigenvectors[:, :num_virtual_coils],
        dtype=np.complex64,
    )
    compressed_data = np.einsum(
        "cv,c...->v...",
        compression_matrix.conj(),
        coil_data,
        optimize=True,
    )

    logging.info(
        "Coil compression: %d physical coils -> %d virtual coils "
        "(%.2f%% variance retained)",
        num_input_coils,
        num_virtual_coils,
        100.0 * cumulative_variance[num_virtual_coils - 1],
    )
    logging.info(
        "Cumulative coil variance: %s",
        np.array2string(cumulative_variance[:num_virtual_coils], precision=4),
    )
    return (
        np.asarray(compressed_data, dtype=np.complex64),
        np.asarray(cumulative_variance, dtype=np.float32),
        compression_matrix,
    )


def _reconstruct_single_virtual_coil(
    coil_index,
    coil_data,
    normalized_coordinates,
    dcf,
    deapodization,
    matrix_size,
):
    logging.info("Starting Kaiser-Bessel gridding for virtual coil %d", coil_index)
    with GRID_SEMAPHORE:
        reconstructed = _regrid_3d_kb(
            coil_data.ravel(),
            normalized_coordinates,
            grid_size=matrix_size,
            dcf=dcf,
            deapodization=deapodization,
            oversampling=OVERSAMPLING,
        )
    logging.info("Finished Kaiser-Bessel gridding for virtual coil %d", coil_index)
    return coil_index, reconstructed


def _estimate_sensitivities(coil_images, smooth_sigma=None, epsilon=1e-8):
    num_coils, matrix_x, _, _ = coil_images.shape
    if smooth_sigma is None:
        smooth_sigma = matrix_x / 32.0
    logging.info(
        "Estimating sensitivities for %d virtual coils with sigma %.2f voxels",
        num_coils,
        smooth_sigma,
    )

    smoothed = np.zeros_like(coil_images, dtype=np.complex64)
    for coil_index in range(num_coils):
        real = ndi.gaussian_filter(coil_images[coil_index].real, smooth_sigma)
        imaginary = ndi.gaussian_filter(coil_images[coil_index].imag, smooth_sigma)
        smoothed[coil_index] = real + 1j * imaginary

    root_sum_of_squares = np.sqrt(np.sum(np.abs(smoothed) ** 2, axis=0)) + epsilon
    smoothed /= root_sum_of_squares[None, ...]
    return np.asarray(smoothed, dtype=np.complex64)


def _combine_coils(coil_images, mode="AC"):
    normalized_mode = str(mode).strip().upper()
    if coil_images.shape[0] == 1:
        logging.info("Single virtual coil: skipping coil combination")
        return np.asarray(np.abs(coil_images[0]), dtype=np.float32)

    if normalized_mode == "SOS":
        logging.info("Combining virtual coils with root-sum-of-squares")
        return np.asarray(
            np.sqrt(np.sum(np.abs(coil_images) ** 2, axis=0)),
            dtype=np.float32,
        )

    if normalized_mode == "AC":
        sensitivity_maps = _estimate_sensitivities(coil_images)
        logging.info("Combining virtual coils adaptively")
        combined = np.sum(np.conj(sensitivity_maps) * coil_images, axis=0)
        return np.asarray(np.abs(combined), dtype=np.float32)

    raise ValueError("coilcombinemode must be 'AC' or 'SoS'")


def _n4_bias_field_correct(volume):
    try:
        import SimpleITK as sitk
    except ImportError as error:
        raise RuntimeError(
            "N4 bias correction requires the SimpleITK runtime dependency"
        ) from error

    values = np.asarray(volume, dtype=np.float32)
    if not values.size or float(np.max(values)) <= 0.0:
        logging.warning("Skipping N4 bias correction for an empty image")
        return values

    image_zyx = values.swapaxes(0, 2)
    sitk_image = sitk.GetImageFromArray(image_zyx)
    mask = sitk.OtsuThreshold(sitk_image, 0, 1, 512)
    mask = sitk.BinaryMorphologicalClosing(mask, [9, 9, 9])
    mask = sitk.BinaryFillhole(mask)

    if N4_SHRINK_FACTOR > 1:
        shrink = [int(N4_SHRINK_FACTOR)] * sitk_image.GetDimension()
        correction_image = sitk.Shrink(sitk_image, shrink)
        correction_mask = sitk.Shrink(mask, shrink)
    else:
        correction_image = sitk_image
        correction_mask = mask

    corrector = sitk.N4BiasFieldCorrectionImageFilter()
    corrector.SetMaximumNumberOfIterations(N4_MAX_ITERATIONS)
    corrector.SetConvergenceThreshold(0.001)
    corrector.SetSplineOrder(3)
    corrector.SetWienerFilterNoise(0.1)
    corrector.SetBiasFieldFullWidthAtHalfMaximum(0.15)
    corrector.Execute(correction_image, correction_mask)
    log_bias_field = corrector.GetLogBiasFieldAsImage(sitk_image)
    corrected = sitk_image / sitk.Exp(log_bias_field)
    return np.asarray(
        sitk.GetArrayFromImage(corrected).swapaxes(0, 2),
        dtype=np.float32,
    )


def _format_display_number(value):
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6g}"


def _scale_volume_to_display_range(volume):
    values = np.asarray(volume, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": 0.0,
            "input_max": 0.0,
            "scale": 1.0,
            "display_min": 0,
            "display_max": 0,
            "formula": "value = display",
        }

    input_min = float(np.min(finite))
    input_max = float(np.max(finite))
    input_range = input_max - input_min
    if input_range <= 0.0 or not np.isfinite(input_range):
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": input_min,
            "input_max": input_max,
            "scale": 1.0,
            "display_min": 0,
            "display_max": 0,
            "formula": f"value = display + {_format_display_number(input_min)}",
        }

    scale = float(SCANNER_DISPLAY_MAX - SCANNER_DISPLAY_MIN) / input_range
    cleaned = np.nan_to_num(values, nan=input_min, posinf=input_max, neginf=input_min)
    display = np.rint((cleaned - input_min) * scale + SCANNER_DISPLAY_MIN)
    display = np.clip(display, SCANNER_DISPLAY_MIN, SCANNER_DISPLAY_MAX)
    display = display.astype(np.uint16, copy=False)
    scale_text = _format_display_number(scale)
    min_text = _format_display_number(input_min)
    return display, {
        "input_min": input_min,
        "input_max": input_max,
        "scale": scale,
        "display_min": int(np.min(display)) if display.size else 0,
        "display_max": int(np.max(display)) if display.size else 0,
        "formula": f"value = display / {scale_text} + {min_text}",
    }


def _scanner_display_comment(display_meta):
    return (
        f"{OUTPUT_IMAGE_COMMENT}; scanner display uint16 "
        f"{SCANNER_DISPLAY_MIN}-{SCANNER_DISPLAY_MAX}; {display_meta['formula']}"
    )


def _new_dicom_uid():
    return f"2.25.{uuid.uuid4().int}"


def _build_output_images(volume, reference_head, metadata, output_fov_mm):
    volume = np.asarray(volume, dtype=np.float32)
    if volume.ndim != 3:
        raise ValueError(f"Reconstructed volume must be 3D, got shape {volume.shape}")

    # The display data is flipped along both in-plane axes below. Reverse the
    # corresponding direction vectors so scanner orientation markers continue
    # to describe the displayed pixels correctly.
    read_dir = -np.asarray(reference_head.read_dir, dtype=float)
    phase_dir = -np.asarray(reference_head.phase_dir, dtype=float)
    slice_dir = np.asarray(reference_head.slice_dir, dtype=float)
    slice_dir_norm = float(np.linalg.norm(slice_dir))
    if slice_dir_norm < 1e-6:
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    else:
        slice_dir = slice_dir / slice_dir_norm

    center_position = np.asarray(reference_head.position, dtype=float)

    series_description = f"{_safe_protocol_name(metadata)}_{OUTPUT_SERIES_DESCRIPTION}"
    series_grouping = f"{series_description}_{OUTPUT_IMAGE_SERIES_INDEX}"
    series_uid = _new_dicom_uid()
    output_fov_mm = float(output_fov_mm)
    display_volume, display_meta = _scale_volume_to_display_range(volume)
    slice_count = int(display_volume.shape[2])
    image_comment = _scanner_display_comment(display_meta)

    # Pack the complete matrix as one explicit 3D MRD image. Sending 64
    # separate 2D messages lets ICE refill each mini-header from the source
    # protocol's NoImagesPerSlab=32 and causes the DICOM writer to flush two
    # 32-frame volumes. One [z, y, x] image gives the writer one 64-frame
    # volume, matching the native ICE reconstruction contract.
    packed_volume = np.ascontiguousarray(
        display_volume[::-1, ::-1, :].transpose(2, 1, 0)
    )
    image = ismrmrd.Image.from_array(packed_volume, transpose=False)

    new_header = mrdhelper.update_img_header_from_raw(image.getHead(), reference_head)
    new_header.data_type = image.data_type
    new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    new_header.image_series_index = OUTPUT_IMAGE_SERIES_INDEX
    new_header.image_index = 1
    new_header.slice = 0
    new_header.matrix_size = tuple(int(value) for value in image.getHead().matrix_size)
    new_header.position = tuple(float(value) for value in center_position)
    new_header.read_dir = tuple(float(value) for value in read_dir)
    new_header.phase_dir = tuple(float(value) for value in phase_dir)
    new_header.slice_dir = tuple(float(value) for value in slice_dir)
    new_header.field_of_view = (
        output_fov_mm,
        output_fov_mm,
        output_fov_mm,
    )
    image.setHead(new_header)
    image.image_series_index = OUTPUT_IMAGE_SERIES_INDEX
    image.field_of_view = (
        ctypes.c_float(output_fov_mm),
        ctypes.c_float(output_fov_mm),
        ctypes.c_float(output_fov_mm),
    )

    meta = ismrmrd.Meta()
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "NUMBA", "KAISERBESSEL", "GRIDDING"]
    meta["ImageType"] = "DERIVED\\PRIMARY\\M\\SODIUMGRIDDING"
    meta["DicomImageType"] = "DERIVED\\PRIMARY\\M\\SODIUMGRIDDING"
    meta["ImageTypeValue4"] = "SODIUMGRIDDING"
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SequenceDescriptionAdditional"] = OUTPUT_IMAGE_COMMENT
    meta["SeriesDescription"] = series_description
    meta["SequenceDescription"] = series_description
    meta["ProtocolName"] = series_description
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = _new_dicom_uid()
    meta["ImageComment"] = image_comment
    meta["ImageComments"] = image_comment
    meta["Keep_image_geometry"] = 0
    meta["partition_count"] = 1
    meta["slice_count"] = slice_count
    meta["NumberOfSlices"] = slice_count
    meta["ImagesInAcquisition"] = slice_count
    meta["NumberInSeries"] = 1
    meta["SliceNo"] = 0
    meta["IsmrmrdSliceNo"] = 0
    meta["AnatomicalSliceNo"] = 0
    meta["ChronSliceNo"] = 0
    meta["ProtocolSliceNumber"] = 0
    meta["Actual3DImagePartNumber"] = 0
    meta["Actual3DImaPartNumber"] = 0
    meta["AnatomicalPartitionNo"] = 0
    meta["ImageRowDir"] = [f"{float(value):.18f}" for value in read_dir]
    meta["ImageColumnDir"] = [f"{float(value):.18f}" for value in phase_dir]
    meta["ImageSliceNormDir"] = [f"{float(value):.18f}" for value in slice_dir]
    meta["SlicePosLightMarker"] = [
        f"{float(value):.18f}" for value in new_header.position
    ]
    meta["SodiumGriddingDisplayScale"] = _format_display_number(display_meta["scale"])
    meta["SodiumGriddingDisplayInputMin"] = f"{float(display_meta['input_min']):.6g}"
    meta["SodiumGriddingDisplayInputMax"] = f"{float(display_meta['input_max']):.6g}"
    meta["SodiumGriddingDisplayMin"] = str(int(display_meta["display_min"]))
    meta["SodiumGriddingDisplayMax"] = str(int(display_meta["display_max"]))
    meta["SodiumGriddingDisplayFormula"] = display_meta["formula"]
    image.attribute_string = meta.serialize()

    return [image]


def process(connection, config, metadata):
    logging.info("Config:\n%s", config)

    try:
        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info(
            "First encoding trajectory=%s matrix=(%s x %s x %s) fov=(%s x %s x %s)mm^3",
            metadata.encoding[0].trajectory,
            metadata.encoding[0].encodedSpace.matrixSize.x,
            metadata.encoding[0].encodedSpace.matrixSize.y,
            metadata.encoding[0].encodedSpace.matrixSize.z,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z,
        )
    except Exception:
        logging.info("Improperly formatted metadata: %s", metadata)

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
                    images = process_raw(acquisitions, connection, config, metadata)
                    connection.send_image(images)
                    acquisitions = []

            elif isinstance(item, ismrmrd.Image):
                passthrough_images.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        if acquisitions:
            logging.info("Processing %d acquired readouts (end of stream)", len(acquisitions))
            images = process_raw(acquisitions, connection, config, metadata)
            connection.send_image(images)

        if passthrough_images:
            logging.warning(
                "Received %d images instead of raw data; returning them unchanged",
                len(passthrough_images),
            )
            connection.send_image(process_image(passthrough_images, connection, config, metadata))

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

    matrix_size = max(1, _config_int(config, "matrixsize", _compute_default_matrix_size(metadata)))
    fov_cm = _config_float(config, "fovcm", _compute_default_fov_cm(metadata))
    reject_bad_readouts = _config_bool(
        config,
        "rejectbadreadouts",
        OPENRECON_DEFAULTS["rejectbadreadouts"],
    )
    bad_readout_sigma = _config_float(
        config,
        "badreadoutsigma",
        OPENRECON_DEFAULTS["badreadoutsigma"],
    )
    center_window = max(1, _config_int(config, "centerwindow", OPENRECON_DEFAULTS["centerwindow"]))
    apply_fermi_filter = _config_bool(
        config,
        "applyfermifilter",
        OPENRECON_DEFAULTS["applyfermifilter"],
    )
    fermi_width = _config_float(config, "fermiwidth", OPENRECON_DEFAULTS["fermiwidth"])
    fermi_cutoff = _config_float(config, "fermicutoff", OPENRECON_DEFAULTS["fermicutoff"])
    dcf_iterations = max(0, _config_int(config, "dcfiterations", OPENRECON_DEFAULTS["dcfiterations"]))
    trajectory_sample_offset = max(
        0,
        _config_int(
            config,
            "trajectorysampleoffset",
            OPENRECON_DEFAULTS["trajectorysampleoffset"],
        ),
    )
    max_coils = max(0, _config_int(config, "maxcoils", OPENRECON_DEFAULTS["maxcoils"]))
    configured_max_workers = max(
        1,
        _config_int(config, "maxworkers", OPENRECON_DEFAULTS["maxworkers"]),
    )
    coil_variance_retention = float(
        np.clip(
            _config_float(
                config,
                "coilvarianceretention",
                OPENRECON_DEFAULTS["coilvarianceretention"],
            ),
            0.0,
            1.0,
        )
    )
    coil_combine_mode = _config_str(
        config,
        "coilcombinemode",
        OPENRECON_DEFAULTS["coilcombinemode"],
    )
    apply_n4_bias_correction = _config_bool(
        config,
        "applyn4biascorrection",
        OPENRECON_DEFAULTS["applyn4biascorrection"],
    )

    data = _build_data_array(group)
    trajectory = _load_trajectory(group, config)
    data, trajectory = _clip_data_to_trajectory(
        data,
        trajectory,
        sample_offset=trajectory_sample_offset,
    )
    if 0 < max_coils < data.shape[0]:
        logging.warning(
            "Limiting reconstruction to first %d of %d coils",
            max_coils,
            data.shape[0],
        )
        data = data[:max_coils]

    logging.info(
        "Running sodium Kaiser-Bessel gridding with physical_coils=%d "
        "readouts=%d samples=%d matrix=%d oversampling=%d fov_cm=%.3f",
        data.shape[0],
        data.shape[1],
        data.shape[2],
        matrix_size,
        OVERSAMPLING,
        fov_cm,
    )
    logging.info("Trajectory shape after clipping: %s", trajectory.shape)
    logging.info(
        "Gridding configuration: kernel_width=%.1f beta=%.6f dcf_iterations=%d "
        "coil_variance_retention=%.3f coil_combine_mode=%s n4=%s pyfftw=%s",
        KB_KERNEL_WIDTH,
        KB_BETA,
        dcf_iterations,
        coil_variance_retention,
        coil_combine_mode,
        apply_n4_bias_correction,
        USE_PYFFTW,
    )
    reference_head = group[len(group) // 2].getHead()
    fermi_filter = _build_fermi_filter(
        trajectory,
        apply_fermi_filter=apply_fermi_filter,
        fermi_width=fermi_width,
        fermi_cutoff=fermi_cutoff,
    )

    prepared_data = []
    logging.info("Preparing %d physical coils before compression", data.shape[0])
    for coil_index in range(data.shape[0]):
        prepared_data.append(
            _prepare_single_coil_data(
                coil_index,
                data[coil_index],
                reject_bad_readouts=reject_bad_readouts,
                bad_readout_sigma=bad_readout_sigma,
                center_window=center_window,
                fermi_filter=fermi_filter,
            )
        )
    logging.info("Finished coil data preparation")
    prepared_data = np.asarray(prepared_data, dtype=np.complex64)
    virtual_coil_data, cumulative_variance, compression_matrix = (
        _compress_coils_by_variance(
            prepared_data,
            variance_retention=coil_variance_retention,
        )
    )

    normalized_coordinates = _normalize_grid_coordinates(
        trajectory,
        matrix_size=matrix_size,
        fov_cm=fov_cm,
    )
    logging.info("Computing Kaiser-Bessel density compensation")
    dcf = _compute_dcf_kb(
        normalized_coordinates,
        grid_size=matrix_size,
        oversampling=OVERSAMPLING,
        num_iterations=dcf_iterations,
    )
    logging.info("Computing Kaiser-Bessel deapodization")
    deapodization = _compute_deapodization_kb(
        matrix_size,
        oversampling=OVERSAMPLING,
    )

    max_workers = min(configured_max_workers, virtual_coil_data.shape[0])
    _log_cpu_resources(configured_max_workers, max_workers)

    coil_images = np.zeros(
        (virtual_coil_data.shape[0], matrix_size, matrix_size, matrix_size),
        dtype=np.complex64,
    )

    logging.info(
        "Launching full-resolution gridding for %d virtual coils with up to "
        "%d workers and %d simultaneous grids",
        virtual_coil_data.shape[0],
        max_workers,
        MAX_SIMULTANEOUS_GRIDS,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _reconstruct_single_virtual_coil,
                coil_index,
                virtual_coil_data[coil_index],
                normalized_coordinates,
                dcf,
                deapodization,
                matrix_size,
            )
            for coil_index in range(virtual_coil_data.shape[0])
        ]
        for future in futures:
            coil_index, reconstructed = future.result()
            coil_images[coil_index] = reconstructed
            logging.info(
                "Collected full-resolution reconstruction for virtual coil %d",
                coil_index,
            )

    combined_magnitude_volume = _combine_coils(coil_images, mode=coil_combine_mode)
    output_volume = combined_magnitude_volume
    if apply_n4_bias_correction:
        logging.info("Running N4 bias field correction")
        output_volume = _n4_bias_field_correct(combined_magnitude_volume)
        logging.info("Finished N4 bias field correction")

    np.save(os.path.join(debugFolder, "sodiumgridding_virtual_coil_data.npy"), virtual_coil_data)
    np.save(os.path.join(debugFolder, "sodiumgridding_coil_variance.npy"), cumulative_variance)
    np.save(os.path.join(debugFolder, "sodiumgridding_compression_matrix.npy"), compression_matrix)
    np.save(os.path.join(debugFolder, "sodiumgridding_coil_images.npy"), coil_images)
    np.save(
        os.path.join(debugFolder, "sodiumgridding_magnitude_volume.npy"),
        combined_magnitude_volume,
    )
    np.save(os.path.join(debugFolder, "sodiumgridding_output_volume.npy"), output_volume)

    process_time_ms = (perf_counter() - tic) * 1000.0
    message = f"Sodium gridding processing time: {process_time_ms:.2f} ms"
    logging.info(message)
    connection.send_logging(constants.MRD_LOGGING_INFO, message)

    return _build_output_images(
        output_volume,
        reference_head,
        metadata,
        output_fov_mm=float(fov_cm) * 10.0,
    )


def process_image(images, connection, config, metadata):
    del connection, config, metadata

    images_out = []
    for image in images:
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        meta["Keep_image_geometry"] = 1
        image.attribute_string = meta.serialize()
        images_out.append(image)
    return images_out
