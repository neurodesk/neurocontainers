#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
import ctypes
import logging
import os
from pathlib import Path
from time import perf_counter
import traceback

import h5py
import ismrmrd
import numpy as np
import scipy.ndimage as ndi
import sigpy

import constants
import mrdhelper


debugFolder = "/tmp/share/debug"
OPENRECON_DEFAULTS = {
    "config": "sodiumnufft",
    "matrixsize": 128,
    "fovcm": 22.0,
    "trajectoryfile": "/opt/sodiumnufft/23NA_n50_trajectory.h5",
    "trajectorydataset": "k",
    "rejectbadreadouts": True,
    "badreadoutsigma": 3.0,
    "centerwindow": 5,
    "applyfermifilter": False,
    "fermiwidth": 0.15,
    "fermicutoff": 0.9,
    "dcfiterations": 0,
    "maxcoils": 0,
    "maxworkers": 6,
}
OUTPUT_SERIES_DESCRIPTION = "sodiumnufft"
OUTPUT_IMAGE_COMMENT = "23Na NUFFT Sum-of-Squares"


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


def _safe_protocol_name(metadata):
    try:
        protocol_name = getattr(metadata.measurementInformation, "protocolName", "")
        if protocol_name:
            return str(protocol_name)
    except Exception:
        pass
    return OUTPUT_SERIES_DESCRIPTION


def estimate_dcf_iterative(coord, img_shape, num_iter=10):
    dcf = np.ones(coord.shape[0], dtype=np.complex64)
    for idx in range(num_iter):
        psf = sigpy.nufft_adjoint(dcf, coord, img_shape)
        back = sigpy.nufft(psf, coord)
        dcf = dcf / (np.abs(back) + 1e-8)
        logging.info("DCF iter %d/%d", idx + 1, num_iter)
    return np.real(dcf).astype(np.float32, copy=False)


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

    trajectory_file = _config_str(
        config,
        "trajectoryfile",
        OPENRECON_DEFAULTS["trajectoryfile"],
    ).strip()
    if not trajectory_file:
        raise ValueError(
            "No embedded trajectory found in the MRD acquisitions and no "
            "'trajectoryfile' parameter was provided."
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
        return float(metadata.encoding[0].reconSpace.fieldOfView_mm.x) / 10.0
    except Exception:
        return OPENRECON_DEFAULTS["fovcm"]


def _compute_default_matrix_size(metadata):
    try:
        return int(metadata.encoding[0].reconSpace.matrixSize.x)
    except Exception:
        return OPENRECON_DEFAULTS["matrixsize"]


def _clip_data_to_trajectory(data, trajectory):
    data_readouts = data.shape[1]
    data_samples = data.shape[2]

    if trajectory.ndim == 3:
        direct_score = abs(trajectory.shape[0] - data_readouts) + abs(trajectory.shape[1] - data_samples)
        swapped_score = abs(trajectory.shape[1] - data_readouts) + abs(trajectory.shape[0] - data_samples)

        if swapped_score < direct_score:
            logging.warning(
                "Swapping trajectory axes to match data dimensions: trajectory=%s data=(readouts=%d, samples=%d)",
                trajectory.shape,
                data_readouts,
                data_samples,
            )
            trajectory = np.swapaxes(trajectory, 0, 1)

    readouts = min(data.shape[1], trajectory.shape[0])
    samples = min(data.shape[2], trajectory.shape[1])
    if readouts != data.shape[1] or samples != data.shape[2]:
        logging.warning(
            "Cropping raw data to match trajectory dimensions: data=%s trajectory=%s -> (%d, %d)",
            data.shape,
            trajectory.shape,
            readouts,
            samples,
        )
    return data[:, :readouts, :samples], trajectory[:readouts, :samples, :]


def _compute_sample_mask_and_scale(coil_data, sigma, center_window):
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

    center = filtered.shape[1] // 2
    start = max(0, center - center_window)
    stop = min(filtered.shape[1], center + center_window)
    if stop <= start:
        return filtered, bad_columns

    reference_mean = float(np.abs(filtered[:, start:stop]).mean())
    if reference_mean > 0:
        filtered *= 2.0 / reference_mean

    return filtered, bad_columns


def _scale_coil_data(coil_data, center_window):
    filtered = np.array(coil_data, copy=True)
    center = filtered.shape[1] // 2
    start = max(0, center - center_window)
    stop = min(filtered.shape[1], center + center_window)
    if stop <= start:
        return filtered

    reference_mean = float(np.abs(filtered[:, start:stop]).mean())
    if reference_mean > 0:
        filtered *= 2.0 / reference_mean
    return filtered


def _compute_clipped_radial_dcf(abs_k):
    if abs_k.size == 0:
        return np.array([], dtype=np.float32)

    dcf = np.square(abs_k, dtype=np.float32)
    if abs_k.shape[0] < 2:
        return dcf.ravel()

    dk = abs_k[1:, 0] - abs_k[:-1, 0]
    if dk.size == 0 or float(np.max(dk)) <= 0:
        return dcf.ravel()

    twist_indices = np.where(dk > 0.99 * float(np.max(dk)))[0]
    if twist_indices.size == 0:
        return dcf.ravel()

    abs_k_twist = float(abs_k[int(twist_indices.max()), 0])
    return np.asarray(np.clip(dcf, None, abs_k_twist ** 2), dtype=np.float32).ravel()


def _build_reconstruction_weights(
    trajectory,
    matrix_size,
    fov_cm,
    apply_fermi_filter,
    fermi_width,
    fermi_cutoff,
    dcf_iterations,
):
    abs_k = np.linalg.norm(trajectory, axis=-1)

    if dcf_iterations > 0:
        coordinates = np.asarray(trajectory.reshape(-1, 3) * float(fov_cm), dtype=np.float32)
        weights = estimate_dcf_iterative(
            coordinates,
            (matrix_size, matrix_size, matrix_size),
            num_iter=dcf_iterations,
        )
    else:
        weights = _compute_clipped_radial_dcf(abs_k)

    if apply_fermi_filter:
        abs_k_max = float(np.max(abs_k)) if abs_k.size else 0.0
        if abs_k_max > 0:
            abs_k_norm = abs_k / abs_k_max
            fermi_filter = 1.0 / (
                1.0 + np.exp((abs_k_norm - float(fermi_cutoff)) / max(float(fermi_width), 1e-6))
            )
            weights = weights * fermi_filter.ravel().astype(np.float32, copy=False)

    return np.asarray(weights, dtype=np.float32), abs_k


def _prepare_single_coil_data(
    coil_index,
    coil_data,
    reject_bad_readouts,
    bad_readout_sigma,
    center_window,
):
    working_data = np.asarray(coil_data, dtype=np.complex64)

    if reject_bad_readouts:
        working_data, bad_columns = _compute_sample_mask_and_scale(
            working_data,
            sigma=bad_readout_sigma,
            center_window=center_window,
        )
        logging.info(
            "Coil %d: rejected %d low-signal sample columns",
            coil_index,
            bad_columns.size,
        )
    else:
        bad_columns = np.array([], dtype=np.int64)
        working_data = _scale_coil_data(working_data, center_window=center_window)

    return np.asarray(working_data, dtype=np.complex64)


def _reconstruct_single_coil(coil_index, coil_data, coordinates, weights, matrix_size, stage_label):
    logging.info("Starting %s NUFFT for coil %d", stage_label, coil_index)
    reconstructed = sigpy.nufft_adjoint(
        coil_data.ravel() * weights,
        coordinates,
        (matrix_size, matrix_size, matrix_size),
        oversamp=1, width=2
    )
    logging.info("Finished %s NUFFT for coil %d", stage_label, coil_index)
    return coil_index, np.asarray(reconstructed, dtype=np.complex64)


def _combine_sum_of_squares(coil_images):
    logging.info("Combining coils with root-sum-of-squares")
    combined = np.sqrt(np.sum(np.abs(coil_images) ** 2, axis=0)).astype(np.float32)
    logging.info("Finished root-sum-of-squares coil combination")
    return combined


def _build_output_images(volume, reference_head, metadata, output_fov_mm):
    matrix_size = int(volume.shape[2])

    read_dir = np.asarray(reference_head.read_dir, dtype=float)
    phase_dir = np.asarray(reference_head.phase_dir, dtype=float)
    slice_dir = np.asarray(reference_head.slice_dir, dtype=float)
    slice_dir_norm = float(np.linalg.norm(slice_dir))
    if slice_dir_norm < 1e-6:
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    else:
        slice_dir = slice_dir / slice_dir_norm

    slice_spacing = output_fov_mm / max(matrix_size, 1)
    center_position = np.asarray(reference_head.position, dtype=float)
    first_slice_position = center_position - 0.5 * (matrix_size - 1) * slice_spacing * slice_dir

    series_description = f"{_safe_protocol_name(metadata)}_{OUTPUT_SERIES_DESCRIPTION}"
    images_out = []
    for slice_index in range(matrix_size):
        slice_data = np.asarray(volume[:, :, slice_index], dtype=np.float32)
        image = ismrmrd.Image.from_array(slice_data, transpose=False)

        new_header = mrdhelper.update_img_header_from_raw(image.getHead(), reference_head)
        new_header.data_type = image.data_type
        new_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        new_header.image_index = slice_index + 1
        new_header.slice = slice_index
        new_header.position = tuple(
            float(value)
            for value in (first_slice_position + slice_index * slice_spacing * slice_dir)
        )
        new_header.read_dir = tuple(float(value) for value in read_dir)
        new_header.phase_dir = tuple(float(value) for value in phase_dir)
        new_header.slice_dir = tuple(float(value) for value in slice_dir)
        new_header.field_of_view = (
            float(output_fov_mm),
            float(output_fov_mm),
            float(output_fov_mm),
        )
        image.setHead(new_header)
        image.field_of_view = (
            ctypes.c_float(output_fov_mm),
            ctypes.c_float(output_fov_mm),
            ctypes.c_float(output_fov_mm),
        )

        meta = ismrmrd.Meta()
        meta["DataRole"] = "Image"
        meta["ImageProcessingHistory"] = ["PYTHON", "SIGPY", "NUFFT"]
        meta["SequenceDescriptionAdditional"] = OUTPUT_IMAGE_COMMENT
        meta["SeriesDescription"] = series_description
        meta["ImageComment"] = OUTPUT_IMAGE_COMMENT
        meta["Keep_image_geometry"] = 1
        meta["ImageRowDir"] = [f"{float(value):.18f}" for value in read_dir]
        meta["ImageColumnDir"] = [f"{float(value):.18f}" for value in phase_dir]
        image.attribute_string = meta.serialize()
        images_out.append(image)

    return images_out


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
    max_coils = max(0, _config_int(config, "maxcoils", OPENRECON_DEFAULTS["maxcoils"]))
    max_workers = max(1, _config_int(config, "maxworkers", OPENRECON_DEFAULTS["maxworkers"]))

    data = _build_data_array(group)
    trajectory = _load_trajectory(group, config)
    data, trajectory = _clip_data_to_trajectory(data, trajectory)
    if 0 < max_coils < data.shape[0]:
        logging.warning(
            "Limiting reconstruction to first %d of %d coils",
            max_coils,
            data.shape[0],
        )
        data = data[:max_coils]

    logging.info(
        "Running sodium NUFFT with coils=%d readouts=%d samples=%d matrix=%d fov_cm=%.3f",
        data.shape[0],
        data.shape[1],
        data.shape[2],
        matrix_size,
        fov_cm,
    )
    logging.info("Trajectory shape after clipping: %s", trajectory.shape)

    reference_head = group[len(group) // 2].getHead()
    max_workers = min(max_workers, data.shape[0])
    coordinates = np.asarray(trajectory.reshape(-1, 3) * float(fov_cm), dtype=np.float32)
    reconstruction_weights, _ = _build_reconstruction_weights(
        trajectory,
        matrix_size=matrix_size,
        fov_cm=fov_cm,
        apply_fermi_filter=apply_fermi_filter,
        fermi_width=fermi_width,
        fermi_cutoff=fermi_cutoff,
        dcf_iterations=dcf_iterations,
    )

    prepared_data = np.zeros_like(data, dtype=np.complex64)
    logging.info("Preparing %d coils for reconstruction", data.shape[0])
    for coil_index in range(data.shape[0]):
        prepared_data[coil_index] = _prepare_single_coil_data(
            coil_index,
            data[coil_index],
            reject_bad_readouts=reject_bad_readouts,
            bad_readout_sigma=bad_readout_sigma,
            center_window=center_window,
        )
    logging.info("Finished coil data preparation")

    coil_images = np.zeros(
        (data.shape[0], matrix_size, matrix_size, matrix_size),
        dtype=np.complex64,
    )

    logging.info(
        "Launching full-resolution NUFFTs for %d coils with up to %d workers",
        data.shape[0],
        max_workers,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _reconstruct_single_coil,
                coil_index,
                prepared_data[coil_index],
                coordinates,
                reconstruction_weights,
                matrix_size,
                "full-resolution",
            )
            for coil_index in range(data.shape[0])
        ]
        for future in futures:
            coil_index, reconstructed = future.result()
            coil_images[coil_index] = reconstructed
            logging.info("Collected full-resolution reconstruction for coil %d", coil_index)

    combined_magnitude_volume = _combine_sum_of_squares(coil_images)
    np.save(os.path.join(debugFolder, "sodiumnufft_coil_images.npy"), coil_images)
    np.save(os.path.join(debugFolder, "sodiumnufft_magnitude_volume.npy"), combined_magnitude_volume)

    process_time_ms = (perf_counter() - tic) * 1000.0
    message = f"Sodium NUFFT processing time: {process_time_ms:.2f} ms"
    logging.info(message)
    connection.send_logging(constants.MRD_LOGGING_INFO, message)

    return _build_output_images(
        combined_magnitude_volume,
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
