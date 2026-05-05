import base64
import copy
import json
import logging
import os
import re
import time
import traceback
import uuid
from collections import defaultdict

import constants
import ismrmrd
import numpy as np


DEBUG_FOLDER = "/tmp/share/debug"
RESERVED_SCANNER_SERIES_INDICES = {0, 999, 1000, 1001}
DOWNSAMPLED_ROLE = "DOWNSAMPLED"
PASSTHROUGH_ROLE = "PASSTHROUGH"
DOWNSAMPLE_FACTOR = 2
OUTPUT_MODE_SINGLE_SERIES = "single_series"
OUTPUT_MODE_MULTI_SERIES = "multi_series"
DEFAULT_OUTPUT_MODE = OUTPUT_MODE_SINGLE_SERIES
DIAGNOSTIC_SINGLE_SERIES_ROLE = "THRESH_MID"
DERIVED_ROLES = {
    "THRESH_LOW",
    "THRESH_MID",
    "THRESH_HIGH",
    DOWNSAMPLED_ROLE,
    "ORIGINAL",
    PASSTHROUGH_ROLE,
}
OUTPUT_SERIES_CONTRACT_EVENT = "OPENRECONI2I_OUTPUT_SERIES_CONTRACT"
INPUT_SERIES_REGISTRY_EVENT = "OPENRECONI2I_INPUT_SERIES_REGISTRY"
SEND_IMAGE_CHUNK_SIZE = 96
SEND_SERIES_DRAIN_SECONDS = 0.25
CLOSE_DRAIN_SECONDS_PER_IMAGE = 0.25
CLOSE_DRAIN_SECONDS_MAX = 30.0
SEND_SERIES_DRAIN_SECONDS_ENV = "OPENRECONI2I_SEND_SERIES_DRAIN_SECONDS"
CLOSE_DRAIN_SECONDS_ENV = "OPENRECONI2I_CLOSE_DRAIN_SECONDS"
CLOSE_DRAIN_SECONDS_MAX_ENV = "OPENRECONI2I_CLOSE_DRAIN_SECONDS_MAX"
OUTPUT_MODE_ENV = "OPENRECONI2I_OUTPUT_MODE"
ALLOW_SENDORIGINAL_ENV = "OPENRECONI2I_ALLOW_SENDORIGINAL"


def process(connection, config, metadata):
    logging.info("Config:\n%s", config)
    _log_metadata_summary(metadata)

    all_images = []
    processable_images_by_series = defaultdict(list)
    passthrough_images = []
    waveform_count = 0
    acquisition_count = 0
    sent_output_image_count = 0

    try:
        for item in connection:
            if item is None:
                break

            if isinstance(item, ismrmrd.Image):
                all_images.append(item)
                if item.image_type in (ismrmrd.IMTYPE_MAGNITUDE, 0):
                    processable_images_by_series[_get_image_series_index(item)].append(item)
                else:
                    logging.info(
                        "Buffering non-magnitude image for scanner passthrough: "
                        "series_index=%s image_type=%s",
                        _get_image_series_index(item),
                        item.image_type,
                    )
                    passthrough_images.append(item)
            elif isinstance(item, ismrmrd.Acquisition):
                acquisition_count += 1
                logging.info("Ignoring acquisition message in image-to-image example")
            elif isinstance(item, ismrmrd.Waveform):
                waveform_count += 1
            else:
                logging.warning("Ignoring unsupported data type %s", type(item).__name__)

        logging.info(
            "Input drain complete: images=%d processable_series=%d passthrough_images=%d "
            "acquisitions=%d waveforms=%d",
            len(all_images),
            len(processable_images_by_series),
            len(passthrough_images),
            acquisition_count,
            waveform_count,
        )

        if not all_images:
            logging.warning("No image messages were received; closing without output")
            return

        output_mode = _configured_output_mode(config)
        logging.info("OpenRecon i2i output mode resolved to %s", output_mode)
        send_original = _send_original_enabled(config)
        derived_series_allocator = _build_connection_series_allocator(all_images)
        output_images = []

        for series_index in sorted(processable_images_by_series):
            input_group = processable_images_by_series[series_index]
            output_images.extend(
                _threshold_outputs_for_series(
                    input_group,
                    derived_series_allocator,
                    output_mode=output_mode,
                )
            )

            if send_original:
                original_index = derived_series_allocator.allocate("ORIGINAL")
                output_images.extend(
                    _restamp_passthrough_images(input_group, "ORIGINAL", original_index)
                )

        if passthrough_images:
            if output_mode == OUTPUT_MODE_MULTI_SERIES:
                logging.info(
                    "Returning %d unsupported/non-magnitude images on a fresh passthrough series",
                    len(passthrough_images),
                )
                passthrough_index = derived_series_allocator.allocate(PASSTHROUGH_ROLE)
                output_images.extend(
                    _restamp_passthrough_images(
                        passthrough_images,
                        PASSTHROUGH_ROLE,
                        passthrough_index,
                    )
                )
            else:
                logging.warning(
                    "Suppressing %d passthrough image(s) in %s mode so the diagnostic "
                    "scanner run does not add passthrough output series",
                    len(passthrough_images),
                    output_mode,
                )

        _log_and_validate_output_series_contract(output_images, all_images, "before_send")
        _send_images_by_series(connection, output_images, "validated openreconi2i output")
        sent_output_image_count = len(output_images)

    except Exception:
        error_text = traceback.format_exc()
        logging.error(error_text)
        connection.send_logging(constants.MRD_LOGGING_ERROR, error_text)
    finally:
        _wait_for_downstream_drain_before_close(sent_output_image_count)
        connection.send_close()


def _log_metadata_summary(metadata):
    try:
        encodings = metadata.encoding
        first = encodings[0]
        logging.info(
            "Incoming MRD header: encodings=%d trajectory=%s matrix=(%s,%s,%s) "
            "fov_mm=(%s,%s,%s)",
            len(encodings),
            first.trajectory,
            first.encodedSpace.matrixSize.x,
            first.encodedSpace.matrixSize.y,
            first.encodedSpace.matrixSize.z,
            first.encodedSpace.fieldOfView_mm.x,
            first.encodedSpace.fieldOfView_mm.y,
            first.encodedSpace.fieldOfView_mm.z,
        )
    except Exception:
        logging.info("Incoming metadata was not a standard MRD header: %s", metadata)


def _threshold_outputs_for_series(
    images,
    allocator,
    output_mode=OUTPUT_MODE_MULTI_SERIES,
):
    ordered_images, geometry = _prepare_input_series(images)
    volume = _images_to_volume(ordered_images)
    mean_value = float(np.mean(volume))
    std_value = float(np.std(volume))
    thresholds = [
        ("THRESH_LOW", mean_value, 1),
        ("THRESH_MID", mean_value + 0.5 * std_value, 2),
        ("THRESH_HIGH", mean_value + std_value, 3),
    ]
    output_mode = _normalise_output_mode(output_mode, DEFAULT_OUTPUT_MODE)
    if output_mode == OUTPUT_MODE_SINGLE_SERIES:
        thresholds = [
            threshold
            for threshold in thresholds
            if threshold[0] == DIAGNOSTIC_SINGLE_SERIES_ROLE
        ]

    output_images = []
    if output_mode == OUTPUT_MODE_SINGLE_SERIES:
        logging.info(
            "Creating diagnostic single-series threshold output for input series=%s "
            "role=%s slices=%d mean=%.6g std=%.6g",
            geometry["series_index"],
            DIAGNOSTIC_SINGLE_SERIES_ROLE,
            len(ordered_images),
            mean_value,
            std_value,
        )
    else:
        logging.info(
            "Creating threshold and downsampled outputs for input series=%s "
            "slices=%d mean=%.6g std=%.6g",
            geometry["series_index"],
            len(ordered_images),
            mean_value,
            std_value,
        )
    for role, threshold, label_value in thresholds:
        label_volume = np.where(volume > threshold, label_value, 0).astype(np.uint16)
        output_series_index = allocator.allocate(role)
        output_images.extend(
            _volume_to_mrd_images(
                label_volume,
                ordered_images,
                role,
                output_series_index,
                geometry,
            )
        )

    if output_mode == OUTPUT_MODE_SINGLE_SERIES:
        return output_images

    downsampled_volume, downsampled_geometry = _downsample_volume_and_geometry(
        volume,
        geometry,
    )
    output_series_index = allocator.allocate(DOWNSAMPLED_ROLE)
    output_images.extend(
        _volume_to_mrd_images(
            downsampled_volume,
            ordered_images,
            DOWNSAMPLED_ROLE,
            output_series_index,
            downsampled_geometry,
        )
    )
    return output_images


def _prepare_input_series(images):
    if not images:
        raise ValueError("Cannot process an empty image series")

    sorted_images = _sort_slices_by_physical_position(images)
    first_header = sorted_images[0].getHead()
    matrix = np.array(first_header.matrix_size[:], dtype=float)
    fov = np.array(first_header.field_of_view[:], dtype=float)

    if matrix.shape[0] != 3 or fov.shape[0] != 3:
        raise ValueError("MRD image header matrix_size and field_of_view must have 3 elements")
    if matrix[0] <= 0 or matrix[1] <= 0:
        raise ValueError(f"Invalid in-plane matrix size {matrix}")
    if fov[0] <= 0 or fov[1] <= 0:
        raise ValueError(f"Invalid in-plane field_of_view {fov}")

    header_slice_count = int(matrix[2])
    if header_slice_count > 1 and header_slice_count != len(sorted_images):
        raise ValueError(
            f"Header matrix slice count {header_slice_count} does not match "
            f"drained image count {len(sorted_images)}"
        )
    matrix[2] = len(sorted_images)
    if matrix[2] <= 0:
        raise ValueError("Invalid slice count 0")

    measured_spacing = _estimate_slice_spacing(sorted_images)
    header_spacing = float(fov[2]) if fov[2] > 0 else measured_spacing
    if measured_spacing > 0:
        if header_spacing > 0 and abs(measured_spacing - header_spacing) > max(0.01, 0.05 * header_spacing):
            logging.info(
                "Measured slice spacing differs from header thickness: "
                "series_index=%s header=%.6g measured=%.6g; using measured spacing",
                _get_image_series_index(sorted_images[0]),
                header_spacing,
                measured_spacing,
            )
        slice_spacing = measured_spacing
    else:
        if header_spacing <= 0:
            raise ValueError("Slice spacing is unavailable from both positions and field_of_view")
        slice_spacing = header_spacing

    fov[2] = slice_spacing * len(sorted_images)
    voxel_size = fov / matrix
    if np.any(voxel_size <= 0):
        raise ValueError(f"Invalid computed voxel size {voxel_size}")

    geometry = {
        "series_index": _get_image_series_index(sorted_images[0]),
        "matrix": matrix.astype(int).tolist(),
        "fov": fov.tolist(),
        "voxel_size": voxel_size.tolist(),
        "slice_spacing": float(slice_spacing),
        "read_dir": np.array(first_header.read_dir[:], dtype=float).tolist(),
        "phase_dir": np.array(first_header.phase_dir[:], dtype=float).tolist(),
        "slice_dir": np.array(first_header.slice_dir[:], dtype=float).tolist(),
        "first_position": np.array(first_header.position[:], dtype=float).tolist(),
    }
    _log_json_event("OPENRECONI2I_GEOMETRY", geometry)
    return sorted_images, geometry


def _sort_slices_by_physical_position(images):
    def slice_key(image):
        header = image.getHead()
        position = np.array(header.position[:], dtype=float)
        slice_dir = np.array(header.slice_dir[:], dtype=float)
        norm = np.linalg.norm(slice_dir)
        if norm <= 0:
            return float(getattr(header, "slice", 0))
        return float(np.dot(position, slice_dir / norm))

    return sorted(images, key=slice_key)


def _estimate_slice_spacing(images):
    if len(images) < 2:
        header_spacing = float(images[0].getHead().field_of_view[2])
        return header_spacing if header_spacing > 0 else 1.0

    positions = []
    first_slice_dir = np.array(images[0].getHead().slice_dir[:], dtype=float)
    norm = np.linalg.norm(first_slice_dir)
    if norm <= 0:
        return float(images[0].getHead().field_of_view[2])
    unit_slice_dir = first_slice_dir / norm
    for image in images:
        positions.append(float(np.dot(np.array(image.getHead().position[:], dtype=float), unit_slice_dir)))
    diffs = np.diff(sorted(positions))
    diffs = np.abs(diffs[np.abs(diffs) > 1e-6])
    if len(diffs) == 0:
        return float(images[0].getHead().field_of_view[2])
    return float(np.median(diffs))


def _images_to_volume(images):
    slices = []
    reference_shape = None
    for index, image in enumerate(images):
        data = np.asarray(image.data)
        data = np.abs(data) if np.iscomplexobj(data) else data
        squeezed = np.squeeze(data)
        if squeezed.ndim != 2:
            raise ValueError(
                f"Image {index} in series {_get_image_series_index(image)} "
                f"does not reduce to one 2D slice; shape={data.shape}"
            )
        if reference_shape is None:
            reference_shape = squeezed.shape
        elif squeezed.shape != reference_shape:
            raise ValueError(
                f"Image {index} shape {squeezed.shape} does not match first slice {reference_shape}"
            )
        slices.append(squeezed.astype(np.float32, copy=False))
    return np.stack(slices, axis=0)


def _downsample_volume_and_geometry(volume, geometry):
    source_shape = tuple(int(size) for size in volume.shape)
    target_shape = tuple(_downsampled_size(size) for size in source_shape)
    source_slice_coordinates = _resample_center_coordinates(source_shape[0], target_shape[0])
    downsampled_volume = _resize_volume_linear(volume, target_shape).astype(np.float32, copy=False)

    output_geometry = copy.deepcopy(geometry)
    output_matrix = [
        int(target_shape[2]),
        int(target_shape[1]),
        int(target_shape[0]),
    ]
    output_fov = list(output_geometry["fov"])
    output_slice_spacing = float(output_fov[2]) / float(target_shape[0])
    output_geometry["matrix"] = output_matrix
    output_geometry["slice_spacing"] = output_slice_spacing
    output_geometry["voxel_size"] = [
        float(output_fov[0]) / float(output_matrix[0]),
        float(output_fov[1]) / float(output_matrix[1]),
        output_slice_spacing,
    ]
    output_geometry["source_slice_spacing"] = float(geometry["slice_spacing"])
    output_geometry["source_slice_coordinates"] = [
        float(value) for value in source_slice_coordinates
    ]

    _log_json_event(
        "OPENRECONI2I_DOWNSAMPLE_GEOMETRY",
        {
            "input_shape_zyx": list(source_shape),
            "output_shape_zyx": list(target_shape),
            "input_voxel_size": geometry["voxel_size"],
            "output_voxel_size": output_geometry["voxel_size"],
            "source_slice_coordinates": output_geometry["source_slice_coordinates"],
        },
    )
    return downsampled_volume, output_geometry


def _downsampled_size(size):
    size = int(size)
    if size <= 1:
        return 1
    return max(1, size // DOWNSAMPLE_FACTOR)


def _resize_volume_linear(volume, target_shape):
    resized = np.asarray(volume, dtype=np.float32)
    if resized.ndim != 3:
        raise ValueError(
            f"Expected a 3D volume for downsampling, got shape={resized.shape}"
        )
    for axis, target_size in enumerate(target_shape):
        resized = _resize_axis_linear(resized, axis, int(target_size))
    return resized


def _resize_axis_linear(data, axis, target_size):
    source_size = int(data.shape[axis])
    if target_size <= 0:
        raise ValueError(f"Invalid target size {target_size} for axis {axis}")
    if source_size == target_size:
        return data

    coordinates = _resample_center_coordinates(source_size, target_size)
    lower = np.floor(coordinates).astype(np.int64)
    upper = np.clip(lower + 1, 0, source_size - 1)
    weight = (coordinates - lower).astype(np.float32)

    lower_values = np.take(data, lower, axis=axis)
    upper_values = np.take(data, upper, axis=axis)
    weight_shape = [1] * data.ndim
    weight_shape[axis] = target_size
    weight = weight.reshape(weight_shape)
    return lower_values * (1.0 - weight) + upper_values * weight


def _resample_center_coordinates(source_size, target_size):
    source_size = int(source_size)
    target_size = int(target_size)
    if source_size <= 0 or target_size <= 0:
        raise ValueError(
            f"Cannot resample with source_size={source_size} target_size={target_size}"
        )
    if source_size == 1:
        return np.zeros(target_size, dtype=np.float32)
    coordinates = (np.arange(target_size, dtype=np.float32) + 0.5) * (
        float(source_size) / float(target_size)
    ) - 0.5
    return np.clip(coordinates, 0.0, float(source_size - 1))


def _volume_to_mrd_images(image_volume, source_images, role, output_series_index, geometry):
    output_images = []
    image_volume = np.asarray(image_volume)
    if image_volume.ndim != 3:
        raise ValueError(
            f"Output volume for role {role} must be 3D, got shape={image_volume.shape}"
        )
    source_meta = ismrmrd.Meta.deserialize(source_images[0].attribute_string)
    output_identity = _build_output_identity(source_meta, role, output_series_index)
    output_slice_count, output_rows, output_cols = image_volume.shape
    source_slice_coordinates = geometry.get("source_slice_coordinates")
    has_resampled_slice_coordinates = source_slice_coordinates is not None
    if not has_resampled_slice_coordinates:
        source_slice_coordinates = list(range(output_slice_count))
    if len(source_slice_coordinates) != output_slice_count:
        raise ValueError(
            f"Output role {role} has {output_slice_count} slices but "
            f"{len(source_slice_coordinates)} source coordinates"
        )

    for slice_index, source_coordinate in enumerate(source_slice_coordinates):
        source_image = source_images[
            int(np.clip(round(float(source_coordinate)), 0, len(source_images) - 1))
        ]
        output_image = ismrmrd.Image.from_array(image_volume[slice_index], transpose=False)
        header = copy.deepcopy(source_image.getHead())
        header.data_type = output_image.data_type
        header.image_series_index = int(output_series_index)
        header.image_index = slice_index + 1
        header.slice = slice_index
        header.contrast = 0
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        _set_header_sequence_field(
            header,
            "matrix_size",
            [int(output_cols), int(output_rows), 1],
        )
        _set_header_sequence_field(
            header,
            "field_of_view",
            [
                float(geometry["fov"][0]),
                float(geometry["fov"][1]),
                float(geometry["slice_spacing"]),
            ],
        )
        if has_resampled_slice_coordinates:
            _set_header_sequence_field(
                header,
                "position",
                _position_for_source_slice_coordinate(geometry, source_coordinate),
            )
        output_image.setHead(header)

        tmp_meta = _copy_meta(ismrmrd.Meta.deserialize(source_image.attribute_string))
        _strip_source_parent_refs(tmp_meta)
        _stamp_output_meta(tmp_meta, output_identity, role, slice_index)
        _patch_meta_ice_minihead(tmp_meta, output_identity, role, slice_index)
        output_image.attribute_string = tmp_meta.serialize()
        output_images.append(output_image)

    return output_images


def _position_for_source_slice_coordinate(geometry, source_coordinate):
    first_position = np.array(geometry["first_position"], dtype=float)
    slice_dir = np.array(geometry["slice_dir"], dtype=float)
    source_slice_spacing = float(
        geometry.get("source_slice_spacing", geometry["slice_spacing"])
    )
    norm = np.linalg.norm(slice_dir)
    if norm <= 0:
        return first_position.tolist()
    return (
        first_position
        + float(source_coordinate) * source_slice_spacing * (slice_dir / norm)
    ).tolist()


def _set_header_sequence_field(image_header, field_name, values):
    values = list(values)
    current_value = getattr(image_header, field_name)
    try:
        current_value[:] = values
    except Exception:
        setattr(image_header, field_name, tuple(values))


def _restamp_passthrough_images(images, role, output_series_index):
    restamped_images = []
    image_list = list(images)
    source_meta = ismrmrd.Meta.deserialize(image_list[0].attribute_string)
    output_identity = _build_output_identity(source_meta, role, output_series_index)

    for slice_index, source_image in enumerate(image_list):
        output_image = _clone_mrd_image(source_image)
        header = copy.deepcopy(output_image.getHead())
        header.image_series_index = int(output_series_index)
        header.image_index = slice_index + 1
        header.slice = slice_index
        header.contrast = 0
        output_image.setHead(header)

        tmp_meta = _copy_meta(ismrmrd.Meta.deserialize(source_image.attribute_string))
        _strip_source_parent_refs(tmp_meta)
        _stamp_output_meta(tmp_meta, output_identity, role, slice_index)
        _patch_meta_ice_minihead(tmp_meta, output_identity, role, slice_index)
        output_image.attribute_string = tmp_meta.serialize()
        restamped_images.append(output_image)

    return restamped_images


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
        allocation = {"role": role, "index": candidate, "ordinal": len(self.allocations) + 1}
        self.allocations.append(allocation)
        _log_json_event(
            "OPENRECONI2I_DERIVED_SERIES_ALLOCATION",
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
    observed_indices = {_get_image_series_index(image) for image in _as_image_list(images)}
    registry = [_series_contract_entry(image, source="input") for image in _as_image_list(images)]
    allocator = ConnectionSeriesAllocator(
        observed_indices=observed_indices,
        reserved_indices=RESERVED_SCANNER_SERIES_INDICES,
    )
    _log_json_event(
        INPUT_SERIES_REGISTRY_EVENT,
        {
            "observed_indices": sorted(observed_indices),
            "reserved_indices": sorted(RESERVED_SCANNER_SERIES_INDICES),
            "series": registry,
        },
    )
    return allocator


def _build_output_identity(source_meta, role, output_series_index):
    role = _first_non_empty_text(role).upper() or "DERIVED"
    source_minihead = _decode_ice_minihead(source_meta)
    source_type_token = _source_image_type_value4_token(source_meta, source_minihead)
    source_series = _first_non_empty_text(
        _get_meta_text(source_meta, "SeriesDescription"),
        _get_meta_text(source_meta, "SequenceDescription"),
        _extract_minihead_string_value(source_minihead, "SequenceDescription"),
        "openreconi2i",
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
    suffix = role.lower()
    series_description = f"{source_series}_{suffix}"
    grouping = f"{source_grouping}_{suffix}"
    uid_seed = json.dumps(
        {
            "source_series_instance_uid": source_series_instance_uid,
            "role": role,
            "output_series_index": int(output_series_index),
            "series_description": series_description,
            "grouping": grouping,
        },
        sort_keys=True,
    )
    if source_series_instance_uid:
        derived_uuid = uuid.uuid5(uuid.NAMESPACE_OID, uid_seed)
    else:
        derived_uuid = uuid.uuid4()
    return {
        "role": role,
        "series_description": series_description,
        "sequence_description": series_description,
        "protocol_name": series_description,
        "grouping": grouping,
        "series_instance_uid": f"2.25.{derived_uuid.int}",
        "image_type": f"DERIVED\\PRIMARY\\M\\{role}",
        "source_type_token": source_type_token,
    }


def _stamp_output_meta(meta_obj, output_identity, role, slice_index):
    sop_instance_uid = _sop_instance_uid_for_slice(output_identity, slice_index)
    meta_obj["DataRole"] = "Segmentation" if role.startswith("THRESH_") else "Image"
    meta_obj["ImageProcessingHistory"] = ["PYTHON", "OPENRECONI2IEXAMPLE", role]
    meta_obj["SeriesDescription"] = output_identity["series_description"]
    meta_obj["SequenceDescription"] = output_identity["sequence_description"]
    meta_obj["ProtocolName"] = output_identity["protocol_name"]
    meta_obj["SeriesNumberRangeNameUID"] = output_identity["grouping"]
    meta_obj["SeriesInstanceUID"] = output_identity["series_instance_uid"]
    meta_obj["SOPInstanceUID"] = sop_instance_uid
    meta_obj["ImageType"] = output_identity["image_type"]
    meta_obj["DicomImageType"] = output_identity["image_type"]
    meta_obj["ImageTypeValue3"] = "M"
    meta_obj["ImageTypeValue4"] = role
    meta_obj["ComplexImageComponent"] = "MAGNITUDE"
    meta_obj["ImageComments"] = role
    meta_obj["ImageComment"] = role
    meta_obj["Keep_image_geometry"] = 1
    if "SequenceDescriptionAdditional" in meta_obj:
        try:
            del meta_obj["SequenceDescriptionAdditional"]
        except Exception:
            meta_obj["SequenceDescriptionAdditional"] = ""
    for key, value in _slice_number_fields(slice_index):
        meta_obj[key] = str(int(value))


def _patch_meta_ice_minihead(meta_obj, output_identity, role, slice_index):
    minihead_text = _decode_ice_minihead(meta_obj)
    if not minihead_text:
        return

    changed = False
    sop_instance_uid = _sop_instance_uid_for_slice(output_identity, slice_index)
    for param_name, param_value in (
        ("SOPInstanceUID", sop_instance_uid),
        ("SeriesNumberRangeNameUID", output_identity["grouping"]),
        ("ProtocolName", output_identity["protocol_name"]),
        ("SequenceDescription", output_identity["sequence_description"]),
        ("SeriesDescription", output_identity["series_description"]),
        ("ImageType", output_identity["image_type"]),
        ("DicomImageType", output_identity["image_type"]),
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        minihead_text, did_change = _replace_or_append_minihead_string_param_in_map(
            minihead_text,
            "DICOM",
            param_name,
            param_value,
        )
        changed = changed or did_change

    minihead_text, did_change = _replace_or_append_minihead_string_param_in_map(
        minihead_text,
        "CONTROL",
        "SeriesInstanceUID",
        output_identity["series_instance_uid"],
    )
    changed = changed or did_change

    minihead_text, did_change = _replace_or_append_minihead_array_token(
        minihead_text,
        "ImageTypeValue4",
        output_identity.get("source_type_token"),
        role,
    )
    changed = changed or did_change

    for param_name, param_value in _slice_number_fields(slice_index):
        target_map = (
            "CONTROL"
            if param_name in {"AnatomicalSliceNo", "ChronSliceNo", "IsmrmrdSliceNo"}
            else "DICOM"
        )
        minihead_text, did_change = _replace_or_append_minihead_long_param_in_map(
            minihead_text,
            target_map,
            param_name,
            param_value,
        )
        changed = changed or did_change

    if role.upper() in {
        token.upper()
        for token in _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4")
    }:
        _delete_meta_key(meta_obj, "ImageTypeValue4")

    if changed:
        meta_obj["IceMiniHead"] = _encode_ice_minihead(minihead_text)


def _slice_number_fields(slice_index):
    return (
        ("Actual3DImagePartNumber", slice_index),
        ("AnatomicalPartitionNo", slice_index),
        ("AnatomicalSliceNo", slice_index),
        ("ChronSliceNo", slice_index),
        ("NumberInSeries", slice_index + 1),
        ("ProtocolSliceNumber", slice_index),
        ("SliceNo", slice_index),
        ("IsmrmrdSliceNo", slice_index),
    )


def _series_contract_entry(image, source="output"):
    try:
        meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
    except Exception:
        meta_obj = ismrmrd.Meta()
    minihead_text = _decode_ice_minihead(meta_obj)
    meta_uid = _get_meta_text(meta_obj, "SeriesInstanceUID")
    minihead_uid = _extract_minihead_string_value(minihead_text, "SeriesInstanceUID")
    meta_grouping = _get_meta_text(meta_obj, "SeriesNumberRangeNameUID")
    minihead_grouping = _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID")
    meta_protocol_name = _get_meta_text(meta_obj, "ProtocolName")
    minihead_protocol_name = _extract_minihead_string_value(minihead_text, "ProtocolName")
    meta_sop_uid = _get_meta_text(meta_obj, "SOPInstanceUID")
    minihead_sop_uid = _extract_minihead_string_value(minihead_text, "SOPInstanceUID")
    minihead_image_type_value4_tokens = _minihead_image_type_value4_tokens(
        minihead_text
    )
    role = _first_non_empty_text(
        _get_meta_text(meta_obj, "ImageTypeValue4"),
        _image_type_value4_from_text(_get_meta_text(meta_obj, "DicomImageType")),
        _image_type_value4_from_text(_get_meta_text(meta_obj, "ImageType")),
        _image_type_value4_from_text(
            _extract_minihead_string_value(minihead_text, "ImageType")
        ),
        _first_non_reserved_image_type_token(minihead_image_type_value4_tokens),
        _get_meta_text(meta_obj, "ImageComments"),
        _get_meta_text(meta_obj, "ImageComment"),
        _get_meta_text(meta_obj, "DataRole"),
    ).upper() or "UNKNOWN"
    return {
        "source": source,
        "role": role,
        "image_series_index": _get_image_series_index(image),
        "series_instance_uid": _first_non_empty_text(meta_uid, minihead_uid),
        "meta_series_instance_uid": meta_uid,
        "minihead_series_instance_uid": minihead_uid,
        "sop_instance_uid": _first_non_empty_text(meta_sop_uid, minihead_sop_uid),
        "meta_sop_instance_uid": meta_sop_uid,
        "minihead_sop_instance_uid": minihead_sop_uid,
        "series_grouping": _first_non_empty_text(meta_grouping, minihead_grouping),
        "meta_series_grouping": meta_grouping,
        "minihead_series_grouping": minihead_grouping,
        "protocol_name": _first_non_empty_text(meta_protocol_name, minihead_protocol_name),
        "meta_protocol_name": meta_protocol_name,
        "minihead_protocol_name": minihead_protocol_name,
        "sequence_description": _first_non_empty_text(
            _get_meta_text(meta_obj, "SequenceDescription"),
            _extract_minihead_string_value(minihead_text, "SequenceDescription"),
        ),
        "series_description": _get_meta_text(meta_obj, "SeriesDescription"),
        "minihead_image_type_value4_tokens": minihead_image_type_value4_tokens,
    }


def _image_sop_instance_uids(image):
    try:
        meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
    except Exception:
        meta_obj = ismrmrd.Meta()
    minihead_text = _decode_ice_minihead(meta_obj)
    return (
        _get_meta_text(meta_obj, "SOPInstanceUID"),
        _extract_minihead_string_value(minihead_text, "SOPInstanceUID"),
    )


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
        grouped[key]["count"] += 1
    return list(grouped.values())


def _log_and_validate_output_series_contract(output_images, input_images, context):
    output_summary = _series_contract_summary(output_images, source="output")
    input_summary = _series_contract_summary(input_images, source="input")
    payload = {
        "context": context,
        "input_series": input_summary,
        "output_series": output_summary,
        "reserved_indices": sorted(RESERVED_SCANNER_SERIES_INDICES),
    }
    _log_json_event(OUTPUT_SERIES_CONTRACT_EVENT, payload)
    _validate_output_series_contract(output_summary, input_summary)
    _validate_output_image_instance_contract(output_images, input_images)


def _validate_output_series_contract(output_summary, input_summary):
    errors = []
    roles_by_index = defaultdict(set)
    derived_series_by_uid = defaultdict(set)
    input_series_indices = {
        int(entry["image_series_index"])
        for entry in input_summary
        if entry.get("image_series_index") is not None
    }
    input_uids = {
        _first_non_empty_text(entry.get("series_instance_uid"))
        for entry in input_summary
        if _first_non_empty_text(entry.get("series_instance_uid"))
    }
    input_has_minihead_identity = any(
        _first_non_empty_text(entry.get("minihead_series_instance_uid"))
        or _first_non_empty_text(entry.get("minihead_series_grouping"))
        or _first_non_empty_text(entry.get("minihead_protocol_name"))
        or entry.get("minihead_image_type_value4_tokens")
        for entry in input_summary
    )

    for entry in output_summary:
        role = _first_non_empty_text(entry.get("role")).upper()
        series_index = int(entry.get("image_series_index", 0))
        uid = _first_non_empty_text(entry.get("series_instance_uid"))
        meta_uid = _first_non_empty_text(entry.get("meta_series_instance_uid"))
        minihead_uid = _first_non_empty_text(entry.get("minihead_series_instance_uid"))
        meta_grouping = _first_non_empty_text(entry.get("meta_series_grouping"))
        minihead_grouping = _first_non_empty_text(entry.get("minihead_series_grouping"))
        meta_protocol = _first_non_empty_text(entry.get("meta_protocol_name"))
        minihead_protocol = _first_non_empty_text(entry.get("minihead_protocol_name"))
        minihead_type_tokens = {
            _first_non_empty_text(token).upper()
            for token in entry.get("minihead_image_type_value4_tokens", [])
            if _first_non_empty_text(token)
        }

        roles_by_index[series_index].add(role)
        if role in DERIVED_ROLES:
            if series_index in input_series_indices:
                errors.append(f"derived role {role} reuses input image_series_index {series_index}")
            if series_index in RESERVED_SCANNER_SERIES_INDICES:
                errors.append(f"derived role {role} uses reserved scanner series index {series_index}")
            if uid in input_uids:
                errors.append(f"derived role {role} reuses input SeriesInstanceUID {uid}")
            if uid:
                derived_series_by_uid[uid].add((role, series_index, meta_grouping or "N/A"))
            if not meta_uid:
                errors.append(f"derived role {role} is missing Meta SeriesInstanceUID")
            if input_has_minihead_identity and not minihead_uid:
                errors.append(f"derived role {role} is missing IceMiniHead SeriesInstanceUID")
            if meta_uid and minihead_uid and meta_uid != minihead_uid:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead SeriesInstanceUID mismatch: "
                    f"{meta_uid} != {minihead_uid}"
                )
            if not meta_grouping:
                errors.append(f"derived role {role} is missing Meta SeriesNumberRangeNameUID")
            if input_has_minihead_identity and not minihead_grouping:
                errors.append(f"derived role {role} is missing IceMiniHead SeriesNumberRangeNameUID")
            if meta_grouping and minihead_grouping and meta_grouping != minihead_grouping:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead SeriesNumberRangeNameUID mismatch: "
                    f"{meta_grouping} != {minihead_grouping}"
                )
            if not meta_protocol:
                errors.append(f"derived role {role} is missing Meta ProtocolName")
            if input_has_minihead_identity and not minihead_protocol:
                errors.append(f"derived role {role} is missing IceMiniHead ProtocolName")
            if meta_protocol and minihead_protocol and meta_protocol != minihead_protocol:
                errors.append(
                    f"derived role {role} has Meta/IceMiniHead ProtocolName mismatch: "
                    f"{meta_protocol} != {minihead_protocol}"
                )
            if input_has_minihead_identity and not minihead_type_tokens:
                errors.append(f"derived role {role} is missing IceMiniHead ImageTypeValue4")
            if (
                input_has_minihead_identity
                and minihead_type_tokens
                and role not in minihead_type_tokens
            ):
                errors.append(
                    f"derived role {role} is missing from IceMiniHead ImageTypeValue4 "
                    f"tokens {sorted(minihead_type_tokens)}"
                )

    for series_index, roles in sorted(roles_by_index.items()):
        derived_roles = sorted(role for role in roles if role in DERIVED_ROLES)
        if len(roles) > 1 and derived_roles:
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
        raise ValueError(
            "Invalid openreconi2iexample output series contract before send: "
            + "; ".join(errors)
        )


def _validate_output_image_instance_contract(output_images, input_images):
    errors = []
    input_sop_uids = set()
    for image in _as_image_list(input_images):
        meta_sop, minihead_sop = _image_sop_instance_uids(image)
        input_sop_uids.update(
            uid for uid in (meta_sop, minihead_sop) if _first_non_empty_text(uid)
        )

    seen_output_sops = {}
    input_has_minihead_sop = any(
        _image_sop_instance_uids(image)[1] for image in _as_image_list(input_images)
    )

    for index, image in enumerate(_as_image_list(output_images)):
        entry = _series_contract_entry(image)
        role = _first_non_empty_text(entry.get("role")).upper()
        if role not in DERIVED_ROLES:
            continue

        meta_sop, minihead_sop = _image_sop_instance_uids(image)
        sop = _first_non_empty_text(meta_sop, minihead_sop)
        if not meta_sop:
            errors.append(f"image {index} role {role} is missing Meta SOPInstanceUID")
        if input_has_minihead_sop and not minihead_sop:
            errors.append(f"image {index} role {role} is missing IceMiniHead SOPInstanceUID")
        if meta_sop and minihead_sop and meta_sop != minihead_sop:
            errors.append(
                f"image {index} role {role} has Meta/IceMiniHead SOPInstanceUID mismatch: "
                f"{meta_sop} != {minihead_sop}"
            )
        if sop and sop in input_sop_uids:
            errors.append(f"image {index} role {role} reuses input SOPInstanceUID {sop}")
        if sop in seen_output_sops:
            errors.append(
                f"image {index} role {role} reuses output SOPInstanceUID {sop} "
                f"from image {seen_output_sops[sop]}"
            )
        elif sop:
            seen_output_sops[sop] = index

    if errors:
        raise ValueError(
            "Invalid openreconi2iexample per-image identity contract before send: "
            + "; ".join(errors[:20])
        )


def _send_images_by_series(connection, images, context):
    images = _as_image_list(images)
    if not images:
        logging.info("No images to send for %s", context)
        return

    batch = []
    batch_series = None
    delayed_series = set()
    sent_series_count = 0

    def flush():
        nonlocal batch, batch_series, sent_series_count
        if not batch:
            return
        if sent_series_count > 0 and batch_series not in delayed_series:
            delay_seconds = _series_drain_delay_seconds()
            if delay_seconds > 0:
                logging.info(
                    "Waiting %.3f seconds before sending series_index=%s "
                    "to let the scanner drain prior output",
                    delay_seconds,
                    batch_series,
                )
                time.sleep(delay_seconds)
            delayed_series.add(batch_series)
        for start in range(0, len(batch), SEND_IMAGE_CHUNK_SIZE):
            chunk = batch[start:start + SEND_IMAGE_CHUNK_SIZE]
            logging.info(
                "Sending %s: series_index=%s chunk=%d-%d/%d image_count=%d",
                context,
                batch_series,
                start + 1,
                start + len(chunk),
                len(batch),
                len(chunk),
            )
            connection.send_image(chunk)
        sent_series_count += 1
        batch = []
        batch_series = None

    for image in images:
        series_index = _get_image_series_index(image)
        if batch and series_index != batch_series:
            flush()
        if not batch:
            batch_series = series_index
        batch.append(image)
    flush()


def _wait_for_downstream_drain_before_close(sent_output_image_count):
    delay_seconds = _close_drain_delay_seconds(sent_output_image_count)
    if delay_seconds <= 0:
        return
    logging.info(
        "Waiting %.3f seconds before MRD close so scanner-side DICOM output can drain "
        "sent_output_image_count=%d",
        delay_seconds,
        sent_output_image_count,
    )
    time.sleep(delay_seconds)


def _series_drain_delay_seconds():
    return _env_float(
        SEND_SERIES_DRAIN_SECONDS_ENV,
        SEND_SERIES_DRAIN_SECONDS,
        minimum=0.0,
    )


def _close_drain_delay_seconds(sent_output_image_count):
    image_count = max(0, int(sent_output_image_count or 0))
    max_delay = _env_float(
        CLOSE_DRAIN_SECONDS_MAX_ENV,
        CLOSE_DRAIN_SECONDS_MAX,
        minimum=0.0,
    )
    default_delay = min(
        max_delay,
        image_count * CLOSE_DRAIN_SECONDS_PER_IMAGE,
    )
    return _env_float(
        CLOSE_DRAIN_SECONDS_ENV,
        default_delay,
        minimum=0.0,
    )


def _env_float(name, default, minimum=None):
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return float(default)
    try:
        value = float(raw_value)
    except ValueError:
        logging.warning(
            "Ignoring invalid %s=%r; expected a numeric value",
            name,
            raw_value,
        )
        return float(default)
    if minimum is not None and value < minimum:
        logging.warning(
            "Ignoring invalid %s=%r; expected value >= %.3f",
            name,
            raw_value,
            minimum,
        )
        return float(default)
    return value


def _clone_mrd_image(image):
    image_copy = ismrmrd.Image.from_array(np.array(image.data, copy=True), transpose=False)
    image_copy.setHead(copy.deepcopy(image.getHead()))
    image_copy.attribute_string = image.attribute_string
    return image_copy


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
    for key in list(meta_obj.keys()):
        if str(key).startswith("CONTROL."):
            try:
                del meta_obj[key]
            except Exception:
                pass


def _as_image_list(images):
    if images is None:
        return []
    if isinstance(images, ismrmrd.Image):
        return [images]
    return list(images)


def _get_image_series_index(image):
    try:
        return int(image.image_series_index)
    except Exception:
        return int(image.getHead().image_series_index)


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


def _source_image_type_value4_token(meta_obj, minihead_text):
    return _first_non_empty_text(
        _get_meta_text(meta_obj, "ImageTypeValue4"),
        _first_non_reserved_image_type_token(
            _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4")
        ),
        _extract_minihead_string_value(minihead_text, "ImageTypeValue4"),
        _image_type_value4_from_text(_get_meta_text(meta_obj, "DicomImageType")),
        _image_type_value4_from_text(_get_meta_text(meta_obj, "ImageType")),
    )


def _first_non_reserved_image_type_token(tokens):
    reserved_tokens = {"NORM", "DIS2D", "DIS3D", "FM3_2", "FIL"}
    for token in tokens or []:
        text = _first_non_empty_text(token)
        if text and text.upper() not in reserved_tokens:
            return text
    return ""


def _image_type_value4_from_text(value):
    text = _first_non_empty_text(value)
    if not text or "\\" not in text:
        return ""
    parts = [part.strip() for part in text.split("\\") if part.strip()]
    if len(parts) >= 4:
        return parts[3]
    return ""


def _sop_instance_uid_for_slice(output_identity, slice_index):
    # Deterministic per-slice SOPs keep outputs reproducible without reusing input instances.
    seed = json.dumps(
        {
            "series_instance_uid": output_identity["series_instance_uid"],
            "role": output_identity["role"],
            "slice_index": int(slice_index),
        },
        sort_keys=True,
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_OID, seed).int}"


def _get_meta_text(meta_obj, key):
    try:
        return _first_non_empty_text(meta_obj.get(key))
    except Exception:
        return ""


def _decode_ice_minihead(meta_obj):
    encoded = _get_meta_text(meta_obj, "IceMiniHead")
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        logging.warning("Could not decode IceMiniHead", exc_info=True)
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _extract_minihead_string_value(minihead_text, name):
    if not minihead_text:
        return ""
    pattern = re.compile(rf'<Param(?:String|Long)\."{re.escape(name)}">\s*\{{\s*"?([^"}}]*)"?\s*\}}')
    match = pattern.search(minihead_text)
    if not match:
        return ""
    return match.group(1).strip().strip('"')


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

    return [
        token.strip()
        for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_match.group(0))
    ]


def _minihead_image_type_value4_tokens(minihead_text):
    tokens = [
        _image_type_value4_from_text(
            _extract_minihead_string_value(minihead_text, "ImageType")
        ),
        _image_type_value4_from_text(
            _extract_minihead_string_value(minihead_text, "DicomImageType")
        ),
    ]
    tokens.extend(_extract_minihead_array_tokens(minihead_text, "ImageTypeValue4"))
    tokens.append(_extract_minihead_string_value(minihead_text, "ImageTypeValue4"))
    return _unique_non_empty_tokens(tokens)


def _unique_non_empty_tokens(tokens):
    unique_tokens = []
    seen = set()
    for token in tokens or []:
        text = _first_non_empty_text(token)
        if not text:
            continue
        key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        unique_tokens.append(text)
    return unique_tokens


def _delete_meta_key(meta_obj, key):
    try:
        if key in meta_obj:
            del meta_obj[key]
            return True
        return False
    except Exception:
        pass

    try:
        meta_obj[key] = ""
        return True
    except Exception:
        return False


def _sanitize_minihead_param_value(value):
    text = _first_non_empty_text(value)
    if not text:
        return ""
    return text.replace("\r", " ").replace("\n", " ")


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_minihead_param_value(value)
    if not minihead_text or not value:
        return minihead_text, False
    escaped_value = value.replace('"', '\\"')
    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
    )
    match = pattern.search(minihead_text)
    replacement_value = f'"{escaped_value}"'
    if match:
        replacement = f"{match.group(1)}{replacement_value}{match.group(2)}"
        new_text = minihead_text[:match.start()] + replacement + minihead_text[match.end():]
        return new_text, new_text != minihead_text

    appended_param = f'\n<ParamString."{name}">\t{{ {replacement_value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _find_minihead_param_map_span(minihead_text, map_name):
    if not minihead_text:
        return None

    tag_match = re.search(
        rf'<ParamMap\."{re.escape(map_name)}">\s*\{{',
        minihead_text,
    )
    if not tag_match:
        return None

    open_brace = minihead_text.find("{", tag_match.start(), tag_match.end())
    if open_brace < 0:
        return None

    close_brace = _find_matching_minihead_brace(minihead_text, open_brace)
    if close_brace is None:
        return None

    return (tag_match.start(), close_brace + 1, open_brace + 1, close_brace)


def _find_matching_minihead_brace(text, open_brace):
    depth = 0
    in_quote = False
    for index in range(open_brace, len(text)):
        char = text[index]
        if in_quote:
            if char == '"' and not _is_escaped_quote(text, index):
                in_quote = False
            continue
        if char == '"':
            in_quote = True
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return index
    return None


def _is_escaped_quote(text, quote_index):
    backslash_count = 0
    index = quote_index - 1
    while index >= 0 and text[index] == "\\":
        backslash_count += 1
        index -= 1
    return backslash_count % 2 == 1


def _replace_or_append_minihead_string_param_in_map(
    minihead_text,
    map_name,
    name,
    value,
):
    value = _sanitize_minihead_param_value(value)
    if not minihead_text or not value:
        return minihead_text, False

    span = _find_minihead_param_map_span(minihead_text, map_name)
    if span is None:
        logging.warning(
            'ParamMap.%s not found in source IceMiniHead; falling back to root-level %s',
            map_name,
            name,
        )
        return _replace_or_append_minihead_string_param(minihead_text, name, value)

    escaped_value = value.replace('"', '\\"')
    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
    )
    match = pattern.search(minihead_text, span[2], span[3])
    replacement_value = f'"{escaped_value}"'
    if match:
        replacement = f"{match.group(1)}{replacement_value}{match.group(2)}"
        new_text = minihead_text[:match.start()] + replacement + minihead_text[match.end():]
        return new_text, new_text != minihead_text

    return _insert_minihead_param_line_before_scope_close(
        minihead_text,
        span[3],
        f'<ParamString."{name}">\t{{ {replacement_value} }}',
    )


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
    tokens = [
        token.strip()
        for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_text)
    ]
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
        replacement_source = _first_non_reserved_image_type_token(tokens)

    if not replacement_source:
        target_token = _sanitize_minihead_param_value(target_token)
        escaped_target = target_token.replace('"', '\\"')
        token_matches = list(re.finditer(r'\{\s*"[^"]+"\s*\}', block_text))
        if not token_matches:
            return minihead_text, False

        last_token = token_matches[-1]
        replaced_block = (
            block_text[:last_token.end()]
            + f'{{ "{escaped_target}" }}'
            + block_text[last_token.end():]
        )

        target_size = len(tokens) + 1
        replaced_block = re.sub(
            r'(<DefaultSize>\s*)(\d+)',
            lambda match: (
                f"{match.group(1)}{target_size}"
                if int(match.group(2)) < target_size
                else match.group(0)
            ),
            replaced_block,
            count=1,
        )
        return (
            minihead_text[:block_match.start()]
            + replaced_block
            + minihead_text[block_match.end():],
            True,
        )

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
        minihead_text[:block_match.start()]
        + replaced_block
        + minihead_text[block_match.end():],
        True,
    )


def _replace_or_append_minihead_array_token(
    minihead_text,
    name,
    source_token,
    target_token,
):
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
    escaped_target = target_token.replace('"', '\\"')
    appended_param = (
        f'\n<ParamArray."{name}">\t{{\n'
        "  <DefaultSize> 1\n"
        "  <MaxSize> 2147483647\n"
        '  <Default> <ParamString."">{ }\n'
        f'  {{ "{escaped_target}" }}\n'
        "}\n"
    )
    return current_text.rstrip() + appended_param, True


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    value = int(value)
    pattern = re.compile(rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})')
    match = pattern.search(minihead_text)
    if match:
        current_value = (match.group(2) or "").strip()
        if current_value == str(value):
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_append_minihead_long_param_in_map(
    minihead_text,
    map_name,
    name,
    value,
):
    value = int(value)
    span = _find_minihead_param_map_span(minihead_text, map_name)
    if span is None:
        logging.warning(
            'ParamMap.%s not found in source IceMiniHead; falling back to root-level %s',
            map_name,
            name,
        )
        return _replace_or_append_minihead_long_param(minihead_text, name, value)

    pattern = re.compile(rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})')
    match = pattern.search(minihead_text, span[2], span[3])
    if match:
        current_value = (match.group(2) or "").strip()
        if current_value == str(value):
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    return _insert_minihead_param_line_before_scope_close(
        minihead_text,
        span[3],
        f'<ParamLong."{name}">\t{{ {value} }}',
    )


def _insert_minihead_param_line_before_scope_close(minihead_text, closing_brace, line):
    line_start = minihead_text.rfind("\n", 0, closing_brace) + 1
    closing_indent = minihead_text[line_start:closing_brace]
    if closing_indent.strip():
        closing_indent = ""
    param_indent = closing_indent + "  "
    insertion = f"{param_indent}{line}\n"
    return (
        minihead_text[:line_start]
        + insertion
        + minihead_text[line_start:],
        True,
    )


def _config_boolean(config, key, default=False):
    candidates = []
    if isinstance(config, dict):
        candidates.append(config.get(key))
        parameters = config.get("parameters")
        if isinstance(parameters, dict):
            candidates.append(parameters.get(key))
    else:
        try:
            candidates.append(getattr(config, key))
            parameters = getattr(config, "parameters", None)
            if isinstance(parameters, dict):
                candidates.append(parameters.get(key))
        except Exception:
            pass

    for value in candidates:
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    return default


def _config_text(config, key, default=""):
    candidates = []
    if isinstance(config, dict):
        candidates.append(config.get(key))
        parameters = config.get("parameters")
        if isinstance(parameters, dict):
            candidates.append(parameters.get(key))
    else:
        try:
            candidates.append(getattr(config, key))
            parameters = getattr(config, "parameters", None)
            if isinstance(parameters, dict):
                candidates.append(parameters.get(key))
        except Exception:
            pass

    for value in candidates:
        text = _first_non_empty_text(value)
        if text:
            return text
    return default


def _configured_output_mode(config):
    configured = _first_non_empty_text(
        os.environ.get(OUTPUT_MODE_ENV),
        _config_text(config, "outputmode"),
        _config_text(config, "output_mode"),
        DEFAULT_OUTPUT_MODE,
    )
    return _normalise_output_mode(configured, DEFAULT_OUTPUT_MODE)


def _normalise_output_mode(value, default):
    text = _first_non_empty_text(value).strip().lower().replace("-", "_")
    aliases = {
        OUTPUT_MODE_SINGLE_SERIES: OUTPUT_MODE_SINGLE_SERIES,
        "single": OUTPUT_MODE_SINGLE_SERIES,
        "single_threshold": OUTPUT_MODE_SINGLE_SERIES,
        "diagnostic_single": OUTPUT_MODE_SINGLE_SERIES,
        OUTPUT_MODE_MULTI_SERIES: OUTPUT_MODE_MULTI_SERIES,
        "multi": OUTPUT_MODE_MULTI_SERIES,
        "legacy": OUTPUT_MODE_MULTI_SERIES,
    }
    if text in aliases:
        return aliases[text]

    logging.warning(
        "Ignoring invalid output mode %r; using %s",
        value,
        default,
    )
    return default


def _send_original_enabled(config):
    if not _config_boolean(config, "sendoriginal", False):
        return False
    if _env_boolean(ALLOW_SENDORIGINAL_ENV, False):
        return True
    logging.warning(
        "Ignoring sendoriginal=True because %s is not enabled; this prevents stale "
        "scanner protocols from expanding diagnostic output volume",
        ALLOW_SENDORIGINAL_ENV,
    )
    return False


def _env_boolean(name, default=False):
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return default
    text = str(raw_value).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    logging.warning(
        "Ignoring invalid %s=%r; expected boolean true/false",
        name,
        raw_value,
    )
    return default


def _log_json_event(event_name, payload):
    try:
        logging.info("%s %s", event_name, json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        logging.info("%s %s", event_name, payload)
