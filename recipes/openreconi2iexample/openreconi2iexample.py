import base64
import copy
import json
import logging
import re
import traceback
import uuid
from collections import defaultdict

import constants
import ismrmrd
import numpy as np


DEBUG_FOLDER = "/tmp/share/debug"
RESERVED_SCANNER_SERIES_INDICES = {0, 999, 1000, 1001}
DERIVED_ROLES = {"THRESH_LOW", "THRESH_MID", "THRESH_HIGH", "ORIGINAL"}
OUTPUT_SERIES_CONTRACT_EVENT = "OPENRECONI2I_OUTPUT_SERIES_CONTRACT"
INPUT_SERIES_REGISTRY_EVENT = "OPENRECONI2I_INPUT_SERIES_REGISTRY"
SEND_IMAGE_CHUNK_SIZE = 96


def process(connection, config, metadata):
    logging.info("Config:\n%s", config)
    _log_metadata_summary(metadata)

    all_images = []
    processable_images_by_series = defaultdict(list)
    passthrough_images = []
    waveform_count = 0
    acquisition_count = 0

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

        derived_series_allocator = _build_connection_series_allocator(all_images)
        output_images = []

        for series_index in sorted(processable_images_by_series):
            input_group = processable_images_by_series[series_index]
            output_images.extend(
                _threshold_outputs_for_series(input_group, derived_series_allocator)
            )

            if _config_boolean(config, "sendoriginal", False):
                original_index = derived_series_allocator.allocate("ORIGINAL")
                output_images.extend(
                    _restamp_passthrough_images(input_group, "ORIGINAL", original_index)
                )

        if passthrough_images:
            logging.info(
                "Returning %d unsupported/non-magnitude images with original series identity",
                len(passthrough_images),
            )
            output_images.extend(passthrough_images)

        _log_and_validate_output_series_contract(output_images, all_images, "before_send")
        _send_images_by_series(connection, output_images, "validated openreconi2i output")

    except Exception:
        error_text = traceback.format_exc()
        logging.error(error_text)
        connection.send_logging(constants.MRD_LOGGING_ERROR, error_text)
    finally:
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


def _threshold_outputs_for_series(images, allocator):
    ordered_images, geometry = _prepare_input_series(images)
    volume = _images_to_volume(ordered_images)
    mean_value = float(np.mean(volume))
    std_value = float(np.std(volume))
    thresholds = [
        ("THRESH_LOW", mean_value, 1),
        ("THRESH_MID", mean_value + 0.5 * std_value, 2),
        ("THRESH_HIGH", mean_value + std_value, 3),
    ]

    output_images = []
    logging.info(
        "Creating threshold outputs for input series=%s slices=%d mean=%.6g std=%.6g",
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


def _volume_to_mrd_images(label_volume, source_images, role, output_series_index, geometry):
    output_images = []
    source_meta = ismrmrd.Meta.deserialize(source_images[0].attribute_string)
    output_identity = _build_output_identity(source_meta, role, output_series_index)

    for slice_index, source_image in enumerate(source_images):
        output_image = ismrmrd.Image.from_array(label_volume[slice_index], transpose=False)
        header = copy.deepcopy(source_image.getHead())
        header.image_series_index = int(output_series_index)
        header.image_index = slice_index + 1
        header.slice = slice_index
        header.contrast = 0
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.field_of_view[2] = float(geometry["slice_spacing"])
        output_image.setHead(header)

        tmp_meta = _copy_meta(ismrmrd.Meta.deserialize(source_image.attribute_string))
        _strip_source_parent_refs(tmp_meta)
        _stamp_output_meta(tmp_meta, output_identity, role, slice_index)
        _patch_meta_ice_minihead(tmp_meta, output_identity, role, slice_index)
        output_image.attribute_string = tmp_meta.serialize()
        output_images.append(output_image)

    return output_images


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
    }


def _stamp_output_meta(meta_obj, output_identity, role, slice_index):
    meta_obj["DataRole"] = "Segmentation" if role.startswith("THRESH_") else "Image"
    meta_obj["ImageProcessingHistory"] = ["PYTHON", "OPENRECONI2IEXAMPLE", role]
    meta_obj["SeriesDescription"] = output_identity["series_description"]
    meta_obj["SequenceDescription"] = output_identity["sequence_description"]
    meta_obj["ProtocolName"] = output_identity["protocol_name"]
    meta_obj["SeriesNumberRangeNameUID"] = output_identity["grouping"]
    meta_obj["SeriesInstanceUID"] = output_identity["series_instance_uid"]
    meta_obj["ImageType"] = output_identity["image_type"]
    meta_obj["DicomImageType"] = output_identity["image_type"]
    meta_obj["ImageTypeValue3"] = "M"
    meta_obj["ImageTypeValue4"] = role
    meta_obj["ComplexImageComponent"] = "MAGNITUDE"
    meta_obj["ImageComments"] = role
    meta_obj["ImageComment"] = role
    meta_obj["Keep_image_geometry"] = 1
    for key, value in _slice_number_fields(slice_index):
        meta_obj[key] = str(int(value))


def _patch_meta_ice_minihead(meta_obj, output_identity, role, slice_index):
    minihead_text = _decode_ice_minihead(meta_obj)
    if not minihead_text:
        return

    changed = False
    for param_name, param_value in (
        ("SeriesInstanceUID", output_identity["series_instance_uid"]),
        ("SeriesNumberRangeNameUID", output_identity["grouping"]),
        ("ProtocolName", output_identity["protocol_name"]),
        ("SequenceDescription", output_identity["sequence_description"]),
        ("SeriesDescription", output_identity["series_description"]),
        ("ImageType", output_identity["image_type"]),
        ("ImageTypeValue3", "M"),
        ("ImageTypeValue4", role),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        minihead_text, did_change = _replace_or_append_minihead_string_param(
            minihead_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

    for param_name, param_value in _slice_number_fields(slice_index):
        minihead_text, did_change = _replace_or_append_minihead_long_param(
            minihead_text,
            param_name,
            param_value,
        )
        changed = changed or did_change

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
    role = _first_non_empty_text(
        _get_meta_text(meta_obj, "ImageTypeValue4"),
        _extract_minihead_string_value(minihead_text, "ImageTypeValue4"),
        _get_meta_text(meta_obj, "DataRole"),
    ).upper() or "UNKNOWN"
    return {
        "source": source,
        "role": role,
        "image_series_index": _get_image_series_index(image),
        "series_instance_uid": _first_non_empty_text(meta_uid, minihead_uid),
        "meta_series_instance_uid": meta_uid,
        "minihead_series_instance_uid": minihead_uid,
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


def _send_images_by_series(connection, images, context):
    images = _as_image_list(images)
    if not images:
        logging.info("No images to send for %s", context)
        return

    batch = []
    batch_series = None

    def flush():
        nonlocal batch, batch_series
        if not batch:
            return
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


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _first_non_empty_text(value)
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


def _log_json_event(event_name, payload):
    try:
        logging.info("%s %s", event_name, json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        logging.info("%s %s", event_name, payload)
