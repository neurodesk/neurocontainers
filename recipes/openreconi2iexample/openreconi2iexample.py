"""Minimal configurable OpenRecon image-to-image example."""
import base64
import json
import logging
import re
import traceback
import uuid

import constants
import ismrmrd
import numpy as np


INVERT_SERIES_INDEX = 99
ORIGINAL_SERIES_INDEX = 100
SEGMENT_SERIES_INDEX = 101
UPSAMPLED_SERIES_INDEX = 102
MIP_SERIES_INDEX = 103
INVERT_SERIES_NAME = "openrecon_invert"
ORIGINAL_SERIES_NAME = "openrecon_original"
SEGMENT_SERIES_NAME = "openrecon_segment"
UPSAMPLED_SERIES_NAME = "openrecon_upsampled"
MIP_SERIES_NAME = "openrecon_mip"
SEGMENTATION_LUT = "MicroDeltaHotMetal.pal"
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
SCANNER_PARTITION_INDEX = 0
SLICE_POSITION_TOLERANCE_MM = 1e-4


def process(connection, config, metadata):
    logging.info("Config: %s", config)
    input_images = []
    magnitude_images = []

    try:
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                continue

            input_images.append(item)
            if item.image_type in (ismrmrd.IMTYPE_MAGNITUDE, 0):
                magnitude_images.append(item)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        send_original = _config_bool(config, "sendoriginal", default=False)
        send_invert = _config_bool_any(config, ("invert", "sendinvert"), default=False)
        send_upsampled = _config_bool_any(
            config,
            ("upsampled", "sendupsampled", "sendinterpolated"),
            default=False,
        )
        send_segment = _config_bool_any(
            config,
            ("segment", "sendsegment", "sendthreshold"),
            default=False,
        )
        use_segmentation_colormap = _config_bool_any(
            config,
            ("segmentationcolormap", "sendsegmentationcolormap"),
            default=False,
        )
        send_mip = _config_bool_any(
            config,
            ("mip", "sendmip", "sendthresholdmip"),
            default=False,
        )
        output_images = []
        inverted_images = []
        if send_invert:
            inverted_images = _invert_images(magnitude_images)
            output_images.extend(inverted_images)
        segment_images = []
        if send_segment:
            segment_images = _segment_images(
                magnitude_images,
                use_colormap=use_segmentation_colormap,
            )
            output_images.extend(segment_images)
        upsampled_images = []
        if send_upsampled:
            upsampled_images = _upsampled_images(magnitude_images)
            output_images.extend(upsampled_images)
        mip_images = []
        if send_mip:
            mip_images = _mip_image(magnitude_images)
            output_images.extend(mip_images)
        original_images = []
        if send_original:
            original_images = _restamp_originals(input_images)
            output_images.extend(original_images)

        logging.info(
            "Configured outputs: original=%s invert=%s upsampled=%s segment=%s "
            "segmentationcolormap=%s mip=%s",
            send_original,
            send_invert,
            send_upsampled,
            send_segment,
            use_segmentation_colormap,
            send_mip,
        )
        logging.info(
            "Sending %d original image(s), %d inverted image(s), "
            "%d upsampled image(s), %d segmentation image(s), and %d MIP image(s)",
            len(original_images),
            len(inverted_images),
            len(upsampled_images),
            len(segment_images),
            len(mip_images),
        )
        if not output_images:
            logging.info("No output options enabled; closing without output")
            return

        _validate_output_images(output_images, input_images)
        _log_output_images(output_images)
        connection.send_image(output_images)

    except Exception:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())
    finally:
        connection.send_close()


def _invert_images(images):
    if not images:
        return []

    data_min = min(float(np.min(np.asarray(image.data))) for image in images)
    data_max = max(float(np.max(np.asarray(image.data))) for image in images)
    window_width = max(data_max - data_min, 1.0)
    window_center = data_min + window_width / 2.0

    series_identity = _build_output_series_identity(
        images[0],
        INVERT_SERIES_INDEX,
        "inverted",
        INVERT_SERIES_NAME,
    )
    outputs = []
    for output_index, source_image in enumerate(images):
        source_data = np.asarray(source_image.data)
        inverted = data_min + data_max - source_data.astype(np.float32)
        if np.issubdtype(source_data.dtype, np.integer):
            inverted = np.rint(inverted).astype(source_data.dtype)
        else:
            inverted = inverted.astype(source_data.dtype)

        output = ismrmrd.Image.from_array(inverted, transpose=False)
        header = source_image.getHead()
        header.data_type = output.data_type
        output.setHead(header)
        _stamp_output_image(
            output,
            source_image,
            INVERT_SERIES_INDEX,
            output_index,
            series_identity["series_name"],
            "Image",
            INVERT_SERIES_NAME,
            ["PYTHON", "OPENRECON_INVERT"],
            {
                "WindowCenter": str(window_center),
                "WindowWidth": str(window_width),
            },
            series_identity=series_identity,
        )
        outputs.append(output)

    return outputs


def _segment_images(images, use_colormap=False):
    if not images:
        return []

    threshold = _bright_foreground_threshold(images)
    series_identity = _build_output_series_identity(
        images[0],
        SEGMENT_SERIES_INDEX,
        "segment",
        SEGMENT_SERIES_NAME,
    )
    outputs = []
    for output_index, source_image in enumerate(images):
        source_data = np.asarray(source_image.data)
        foreground = source_data.astype(np.float32) >= threshold
        segmentation = _largest_connected_component_per_plane(foreground).astype(
            np.uint16
        )

        output = ismrmrd.Image.from_array(segmentation, transpose=False)
        header = source_image.getHead()
        header.data_type = output.data_type
        output.setHead(header)
        extra_meta = {
            "WindowCenter": "0.5",
            "WindowWidth": "1",
        }
        if use_colormap:
            extra_meta["LUTFileName"] = SEGMENTATION_LUT
        _stamp_output_image(
            output,
            source_image,
            SEGMENT_SERIES_INDEX,
            output_index,
            series_identity["series_name"],
            "Segmentation",
            SEGMENT_SERIES_NAME,
            ["PYTHON", "OPENRECON_SEGMENT"],
            extra_meta,
            series_identity=series_identity,
        )
        outputs.append(output)

    logging.info(
        "Created %d foreground segmentation image(s) with threshold %.6g "
        "and segmentationcolormap=%s",
        len(outputs),
        threshold,
        use_colormap,
    )
    return outputs


def _bright_foreground_threshold(images):
    values = []
    for image in images:
        data = np.asarray(image.data, dtype=np.float32)
        values.append(data[np.isfinite(data)])
    finite_values = np.concatenate(values) if values else np.asarray([], dtype=np.float32)
    if finite_values.size == 0:
        return np.inf

    data_min = float(np.min(finite_values))
    data_max = float(np.max(finite_values))
    if data_max <= data_min:
        return np.inf

    background = float(np.percentile(finite_values, 50))
    upper = float(np.percentile(finite_values, 95))
    if upper <= background:
        upper = data_max

    threshold = background + 0.5 * (upper - background)
    if threshold >= data_max:
        threshold = background + 0.5 * (data_max - background)
    return threshold


def _largest_connected_component_per_plane(mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.ndim < 2:
        return _largest_connected_component_2d(mask.reshape(1, -1)).reshape(mask.shape)

    result = np.zeros(mask.shape, dtype=bool)
    planes = mask.reshape((-1,) + mask.shape[-2:])
    result_planes = result.reshape((-1,) + mask.shape[-2:])
    for index, plane in enumerate(planes):
        result_planes[index] = _largest_connected_component_2d(plane)
    return result


def _largest_connected_component_2d(mask):
    mask = np.asarray(mask, dtype=bool)
    height, width = mask.shape
    foreground = mask.ravel()
    foreground_indices = np.flatnonzero(foreground)
    if foreground_indices.size == 0:
        return np.zeros(mask.shape, dtype=bool)

    visited = np.zeros(foreground.size, dtype=bool)
    best_component = []
    for seed in foreground_indices:
        if visited[seed]:
            continue

        component = []
        stack = [int(seed)]
        visited[seed] = True
        while stack:
            current = stack.pop()
            component.append(current)
            row, col = divmod(current, width)
            neighbors = []
            if row > 0:
                neighbors.append(current - width)
            if row + 1 < height:
                neighbors.append(current + width)
            if col > 0:
                neighbors.append(current - 1)
            if col + 1 < width:
                neighbors.append(current + 1)
            for neighbor in neighbors:
                if foreground[neighbor] and not visited[neighbor]:
                    visited[neighbor] = True
                    stack.append(neighbor)

        if len(component) > len(best_component):
            best_component = component

    result = np.zeros(foreground.size, dtype=bool)
    result[best_component] = True
    return result.reshape(mask.shape)


def _mip_image(images):
    if not images:
        return []

    stack = np.stack([np.asarray(image.data) for image in images])
    # Stack shape is [image, cha, z, y, x]. Collapse source slices and any
    # per-image z dimension to one projected output slice.
    mip = stack.max(axis=(0, 2))
    mip = mip[:, np.newaxis, :, :]
    mip = _cast_like(mip, np.asarray(images[0].data))

    output = ismrmrd.Image.from_array(mip, transpose=False)
    header = images[0].getHead()
    header.data_type = output.data_type
    output.setHead(header)
    series_identity = _build_output_series_identity(
        images[0],
        MIP_SERIES_INDEX,
        "mip",
        MIP_SERIES_NAME,
    )
    _stamp_output_image(
        output,
        images[0],
        MIP_SERIES_INDEX,
        0,
        series_identity["series_name"],
        "Image",
        MIP_SERIES_NAME,
        ["PYTHON", "OPENRECON_MIP"],
        series_identity=series_identity,
    )
    logging.info(
        "Created maximum intensity projection from %d image(s)",
        len(images),
    )
    return [output]


def _upsampled_images(images):
    if not images:
        return []

    images, slice_axis = _ordered_upsample_sources(images)
    series_identity = _build_output_series_identity(
        images[0],
        UPSAMPLED_SERIES_INDEX,
        "upsampled",
        UPSAMPLED_SERIES_NAME,
    )
    fallback_half_step = _interpolated_half_step(images)
    output_slice_count = 2 * len(images)
    source_slice_count_hint = _source_slice_count_hint(images)
    logging.info(
        "upsampled: input_slices=%d output_slices=%d source_slice_count_hint=%s",
        len(images),
        output_slice_count,
        source_slice_count_hint if source_slice_count_hint is not None else "unknown",
    )
    if (
        source_slice_count_hint is not None
        and output_slice_count > source_slice_count_hint
    ):
        logging.warning(
            "upsampled output_slices=%d exceeds source slice_count hint=%d; "
            "using explicit output geometry",
            output_slice_count,
            source_slice_count_hint,
        )
    origin = _header_position(images[0].getHead())
    last_pair_half_step = None
    outputs = []
    for index, image in enumerate(images):
        current_position = _project_position_on_slice_axis(
            _header_position(image.getHead()),
            origin,
            slice_axis,
        )
        if index + 1 < len(images):
            next_image = images[index + 1]
            next_position = _project_position_on_slice_axis(
                _header_position(next_image.getHead()),
                origin,
                slice_axis,
            )
            local_half_step = 0.5 * (next_position - current_position)
            last_pair_half_step = local_half_step
            midpoint = 0.5 * (
                np.asarray(image.data, dtype=np.float32)
                + np.asarray(next_image.data, dtype=np.float32)
            )
            midpoint_position = current_position + local_half_step
        else:
            local_half_step = (
                last_pair_half_step
                if last_pair_half_step is not None
                else _half_step_on_slice_axis(fallback_half_step, slice_axis)
            )
            midpoint = np.asarray(image.data)
            midpoint_position = current_position + local_half_step

        slice_thickness = float(np.linalg.norm(local_half_step))
        outputs.append(
            _make_upsampled_image(
                image,
                np.asarray(image.data),
                2 * index,
                current_position,
                local_half_step,
                output_slice_count,
                slice_axis=slice_axis,
                slice_thickness=slice_thickness,
                series_identity=series_identity,
            )
        )

        outputs.append(
            _make_upsampled_image(
                image,
                _cast_like(midpoint, np.asarray(image.data)),
                2 * index + 1,
                midpoint_position,
                local_half_step,
                output_slice_count,
                slice_axis=slice_axis,
                slice_thickness=slice_thickness,
                series_identity=series_identity,
            )
        )

    logging.info(
        "Created %d upsampled image(s) from %d source image(s)",
        len(outputs),
        len(images),
    )
    _validate_unique_projected_positions(outputs, slice_axis, "upsampled output")
    return outputs


def _interpolated_images(images):
    return _upsampled_images(images)


def _make_upsampled_image(
    source_image,
    data,
    output_index,
    position,
    half_step,
    output_slice_count=None,
    slice_axis=None,
    slice_thickness=None,
    series_identity=None,
):
    output_slice_count = _require_output_slice_count(output_slice_count)
    output = ismrmrd.Image.from_array(data, transpose=False)
    header = source_image.getHead()
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = output_index + 1
    header.slice = output_index

    fov = [float(value) for value in header.field_of_view]
    if slice_axis is None:
        slice_axis = _normalize_vector(half_step)
    if slice_axis is None:
        slice_axis = _infer_slice_axis([header])

    step_length = (
        float(slice_thickness)
        if slice_thickness is not None
        else float(np.linalg.norm(half_step))
    )
    if step_length > 0:
        fov[2] = step_length
    _set_header_sequence_field(header, "field_of_view", fov)
    _set_header_sequence_field(
        header,
        "position",
        [float(value) for value in position],
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )

    output.setHead(header)
    geometry_meta = _explicit_header_geometry_meta(header)
    extra_meta = {
        "Keep_image_geometry": str(int(0)),
        "partition_count": str(int(1)),
        "slice_count": str(int(output_slice_count)),
    }
    extra_meta.update(geometry_meta)
    _stamp_output_image(
        output,
        source_image,
        UPSAMPLED_SERIES_INDEX,
        output_index,
        (
            series_identity["series_name"]
            if series_identity
            else _derived_series_name(
                source_image,
                "upsampled",
                UPSAMPLED_SERIES_NAME,
            )
        ),
        "Image",
        UPSAMPLED_SERIES_NAME,
        ["PYTHON", "OPENRECON_UPSAMPLED"],
        # The source MiniHead can carry the original SLC bounds; explicit
        # geometry lets the host accept the larger upsampled slice range.
        extra_meta=extra_meta,
        patch_minihead=False,
        series_identity=series_identity,
    )
    return output


def _make_interpolated_image(
    source_image,
    data,
    output_index,
    position,
    half_step,
    output_slice_count=None,
):
    output_slice_count = _require_output_slice_count(output_slice_count)
    return _make_upsampled_image(
        source_image,
        data,
        output_index,
        position,
        half_step,
        output_slice_count,
    )


def _restamp_originals(images):
    if not images:
        return []

    series_identity = _build_output_series_identity(
        images[0],
        ORIGINAL_SERIES_INDEX,
        "original",
        ORIGINAL_SERIES_NAME,
    )
    outputs = []
    for output_index, source_image in enumerate(images):
        output = ismrmrd.Image.from_array(
            np.asarray(source_image.data).copy(),
            transpose=False,
        )
        header = source_image.getHead()
        header.data_type = output.data_type
        output.setHead(header)
        _stamp_output_image(
            output,
            source_image,
            ORIGINAL_SERIES_INDEX,
            output_index,
            series_identity["series_name"],
            "Image",
            ORIGINAL_SERIES_NAME,
            ["PYTHON", "OPENRECON_ORIGINAL_COPY"],
            series_identity=series_identity,
        )
        outputs.append(output)
    return outputs


def _stamp_output_image(
    output,
    source_image,
    series_index,
    output_index,
    series_name,
    data_role,
    image_type_token,
    history,
    extra_meta=None,
    patch_minihead=True,
    series_identity=None,
):
    header = output.getHead()
    header.image_series_index = series_index
    header.image_index = output_index + 1
    header.slice = output_index
    header.contrast = 0
    if output.data_type in (ismrmrd.DATATYPE_CXFLOAT, ismrmrd.DATATYPE_CXDOUBLE):
        header.image_type = ismrmrd.IMTYPE_COMPLEX
    else:
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    output.setHead(header)
    output.image_series_index = series_index
    output.attribute_string = _output_meta(
        source_image,
        series_index,
        output_index,
        series_name,
        data_role,
        image_type_token,
        history,
        extra_meta or {},
        patch_minihead,
        series_identity,
    ).serialize()
    return output


def _output_meta(
    source_image,
    series_index,
    output_index,
    series_name,
    data_role,
    image_type_token,
    history,
    extra_meta,
    patch_minihead,
    series_identity,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    if series_identity is None:
        series_identity = _build_output_series_identity_from_name(
            source_image,
            series_index,
            series_name,
        )
    series_name = series_identity["series_name"]
    series_uid = series_identity["series_uid"]
    series_grouping = series_identity["series_grouping"]
    sop_uid = _derived_instance_uid(
        source_image,
        series_index,
        series_name,
        output_index,
        series_uid,
    )
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"
    meta["DataRole"] = data_role
    meta["ImageProcessingHistory"] = history
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    meta["ImageTypeValue3"] = "M"
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    _set_meta_field(meta, "SequenceDescriptionAdditional", "openrecon")
    _set_meta_scalar(meta, "Keep_image_geometry", 1)
    _set_output_position_meta(meta, output_index)
    for key, value in extra_meta.items():
        if value is not None:
            meta[key] = value

    if not patch_minihead:
        if "IceMiniHead" in meta:
            del meta["IceMiniHead"]
        return meta

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    if minihead:
        patched_minihead, changed = _patch_ice_minihead(
            minihead,
            series_name,
            series_grouping,
            series_uid,
            sop_uid,
            image_type,
            image_type_token,
            output_index,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta[key]


def _set_output_position_meta(meta, output_index):
    for key in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        _set_meta_scalar(meta, key, SCANNER_PARTITION_INDEX)
    for key, value in (
        ("AnatomicalSliceNo", output_index),
        ("ChronSliceNo", output_index),
        ("NumberInSeries", output_index + 1),
        ("ProtocolSliceNumber", output_index),
        ("SliceNo", output_index),
        ("IsmrmrdSliceNo", output_index),
    ):
        _set_meta_scalar(meta, key, value)


def _meta_from_image(image):
    if not image.attribute_string:
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _derived_series_name(source_image, suffix, fallback_base=INVERT_SERIES_NAME):
    source_name = _source_series_name(source_image)
    if source_name:
        return f"{source_name}-{suffix}"
    return f"{fallback_base}-{suffix}"


def _build_output_series_identity(
    anchor_image,
    series_index,
    suffix,
    fallback_base,
):
    return _build_output_series_identity_from_name(
        anchor_image,
        series_index,
        _derived_series_name(anchor_image, suffix, fallback_base),
    )


def _build_output_series_identity_from_name(anchor_image, series_index, series_name):
    return {
        "series_name": series_name,
        "series_grouping": _derived_series_grouping(series_name, series_index),
        "series_uid": _derived_series_uid(anchor_image, series_index, series_name),
    }


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_identity_text(series_name)}_{series_index}"


def _derived_series_uid(source_image, series_index, series_name):
    meta = _meta_from_image(source_image)
    base_uid = _meta_text(meta, "SeriesInstanceUID")
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    if not base_uid and minihead:
        base_uid = _minihead_string_value(minihead, "SeriesInstanceUID")

    seed = "|".join(
        [
            "openreconi2iexample",
            base_uid or _source_series_name(source_image) or "source",
            str(series_index),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_instance_uid(
    source_image,
    series_index,
    series_name,
    output_index,
    series_uid=None,
):
    meta = _meta_from_image(source_image)
    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    source_instance_uid = _meta_text(meta, "SOPInstanceUID")
    if not source_instance_uid and minihead:
        source_instance_uid = _minihead_string_value(minihead, "SOPInstanceUID")
    if series_uid is None:
        series_uid = _derived_series_uid(source_image, series_index, series_name)

    seed = "|".join(
        [
            "openreconi2iexample-instance",
            series_uid,
            source_instance_uid or str(output_index),
            str(output_index),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _source_series_name(source_image):
    meta = _meta_from_image(source_image)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _meta_text(meta, key)
        if value:
            return value

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    if minihead:
        for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
            value = _minihead_string_value(minihead, key)
            if value:
                return value

    return ""


def _meta_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    text = str(value or "").strip()
    return "" if text.upper() == "N/A" else text


def _decode_ice_minihead(value):
    if not value:
        return ""
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _minihead_string_value(minihead, key):
    match = re.search(
        rf'<ParamString\."{re.escape(key)}">\s*{{\s*"([^"]*)"',
        minihead,
    )
    return match.group(1).strip() if match else ""


def _minihead_long_value(minihead, key):
    match = re.search(
        rf'<ParamLong\."{re.escape(key)}">\s*{{\s*(-?\d*)\s*}}',
        minihead,
    )
    value = match.group(1).strip() if match else ""
    return int(value) if value not in {"", "-"} else None


def _minihead_array_tokens(minihead, key):
    block_match = re.search(
        rf'<ParamArray\."{re.escape(key)}">\s*{{.*?^\s*}}',
        minihead,
        flags=re.DOTALL | re.MULTILINE,
    )
    if not block_match:
        return []
    return [
        token.strip()
        for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_match.group(0))
    ]


def _patch_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    sop_uid,
    image_type,
    image_type_token,
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
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            name,
            value,
        )
        changed = changed or did_change
    current_text, did_change = _replace_or_append_minihead_array_token(
        current_text,
        "ImageTypeValue4",
        image_type_token,
    )
    changed = changed or did_change
    for name in ("Actual3DImagePartNumber", "AnatomicalPartitionNo"):
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            name,
            SCANNER_PARTITION_INDEX,
        )
        changed = changed or did_change
    for name, value in (
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


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_identity_text(value)
    if not value:
        return minihead_text, False

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

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    if value is None:
        return minihead_text, False

    value = int(value)
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})'
    )
    matches = list(pattern.finditer(minihead_text))
    if matches:
        if all((match.group(2) or "").strip() == str(value) for match in matches):
            return minihead_text, False
        return (
            pattern.sub(
                lambda match: f"{match.group(1)}{value}{match.group(3)}",
                minihead_text,
            ),
            True,
        )

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_append_minihead_array_token(minihead_text, name, target_token):
    target_token = _sanitize_identity_text(target_token)
    if not target_token:
        return minihead_text, False

    block_pattern = re.compile(
        rf'(<ParamArray\."{re.escape(name)}">\s*\{{)(.*?)(^\s*\}})',
        flags=re.DOTALL | re.MULTILINE,
    )
    block_match = block_pattern.search(minihead_text)
    if not block_match:
        appended_param = (
            f'\n<ParamArray."{name}">\t{{\n\t{{ "{target_token}" }}\n}}\n'
        )
        return minihead_text.rstrip() + appended_param, True

    block_text = block_match.group(0)
    tokens = _minihead_array_tokens(block_text, name)
    if tokens == [target_token]:
        return minihead_text, False

    token_pattern = re.compile(r'\{\s*"[^"]*"\s*\}')
    token_matches = list(token_pattern.finditer(block_text))
    if not token_matches:
        replacement_block = (
            block_text.rstrip()[:-1] + f'\n\t{{ "{target_token}" }}\n}}'
        )
    else:
        first_token = token_matches[0]
        last_token = token_matches[-1]
        replacement_block = (
            block_text[:first_token.start()]
            + f'{{ "{target_token}" }}'
            + block_text[last_token.end():]
        )
    return (
        minihead_text[:block_match.start()]
        + replacement_block
        + minihead_text[block_match.end():],
        True,
    )


def _sanitize_identity_text(value):
    return (
        str(value or "")
        .strip()
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _set_meta_field(meta, key, value):
    meta[key] = value


def _set_meta_scalar(meta, key, value):
    meta[key] = str(int(value))


def _validate_output_images(output_images, input_images):
    errors = []
    seen_image_keys = {}
    seen_storage_keys = {}
    seen_sop_uids = {}
    source_slice_count = len(input_images)
    input_identity = _identity_values(input_images)
    input_has_minihead = any(_image_minihead(image) for image in input_images)
    series_identity = {}
    series_by_uid = {}

    for index, image in enumerate(output_images):
        header = image.getHead()
        image_key = (
            int(image.image_series_index),
            int(header.slice),
            int(header.image_index),
        )
        if image_key in seen_image_keys:
            errors.append(
                f"image {index} duplicates output image key {image_key} "
                f"from image {seen_image_keys[image_key]}"
            )
        else:
            seen_image_keys[image_key] = index

        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
        if input_has_minihead and not minihead and keep_image_geometry != 0:
            errors.append(f"image {index} is missing IceMiniHead")

        identity = {
            "series_description": _meta_text(meta, "SeriesDescription"),
            "sequence_description": _meta_text(meta, "SequenceDescription"),
            "protocol_name": _meta_text(meta, "ProtocolName"),
            "series_grouping": _meta_text(meta, "SeriesNumberRangeNameUID"),
            "series_uid": _meta_text(meta, "SeriesInstanceUID"),
            "sop_uid": _meta_text(meta, "SOPInstanceUID"),
            "minihead_sequence_description": _minihead_string_value(
                minihead, "SequenceDescription"
            ),
            "minihead_protocol_name": _minihead_string_value(minihead, "ProtocolName"),
            "minihead_series_grouping": _minihead_string_value(
                minihead, "SeriesNumberRangeNameUID"
            ),
            "minihead_series_uid": _minihead_string_value(minihead, "SeriesInstanceUID"),
            "minihead_sop_uid": _minihead_string_value(minihead, "SOPInstanceUID"),
        }
        _validate_identity_fields(index, identity, input_identity, errors)
        _validate_storage_fields(
            index,
            image,
            meta,
            minihead,
            input_identity,
            seen_storage_keys,
            seen_sop_uids,
            _series_slice_limit(int(image.image_series_index), source_slice_count),
            errors,
        )

        series_key = int(image.image_series_index)
        comparable_identity = (
            identity["sequence_description"],
            identity["protocol_name"],
            identity["series_grouping"],
            identity["series_uid"],
        )
        previous_identity = series_identity.setdefault(series_key, comparable_identity)
        if previous_identity != comparable_identity:
            errors.append(
                f"image {index} in image_series_index {series_key} has "
                f"inconsistent identity values: {comparable_identity} != "
                f"{previous_identity}"
            )
        series_uid = identity["series_uid"]
        if series_uid:
            previous_series = series_by_uid.setdefault(series_uid, series_key)
            if previous_series != series_key:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by "
                    f"image_series_index {previous_series} and {series_key}"
                )

    if errors:
        raise ValueError(
            "Invalid openreconi2iexample output series contract before send: "
            + "; ".join(errors)
        )


def _validate_identity_fields(index, identity, input_identity, errors):
    required = (
        "series_description",
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "sop_uid",
    )
    for key in required:
        if not identity[key]:
            errors.append(f"image {index} is missing Meta {key}")

    for meta_key, minihead_key in (
        ("sequence_description", "minihead_sequence_description"),
        ("protocol_name", "minihead_protocol_name"),
        ("series_grouping", "minihead_series_grouping"),
        ("series_uid", "minihead_series_uid"),
        ("sop_uid", "minihead_sop_uid"),
    ):
        meta_value = identity[meta_key]
        minihead_value = identity[minihead_key]
        if minihead_value and meta_value != minihead_value:
            errors.append(
                f"image {index} has Meta/IceMiniHead {meta_key} mismatch: "
                f"{meta_value} != {minihead_value}"
            )

    for key in (
        "sequence_description",
        "protocol_name",
        "series_grouping",
        "series_uid",
        "minihead_sequence_description",
        "minihead_protocol_name",
        "minihead_series_grouping",
        "minihead_series_uid",
        "sop_uid",
        "minihead_sop_uid",
    ):
        value = identity[key]
        if value and value in input_identity:
            errors.append(f"image {index} reuses input identity {key}={value}")


def _validate_storage_fields(
    index,
    image,
    meta,
    minihead,
    input_identity,
    seen_storage_keys,
    seen_sop_uids,
    series_slice_limit,
    errors,
):
    header = image.getHead()
    header_slice = int(header.slice)
    if series_slice_limit and not 0 <= header_slice < series_slice_limit:
        errors.append(
            f"image {index} has slice {header_slice} outside source image "
            f"bounds [0..{series_slice_limit})"
        )

    expected_position_fields = {
        "Actual3DImagePartNumber": SCANNER_PARTITION_INDEX,
        "AnatomicalPartitionNo": SCANNER_PARTITION_INDEX,
        "AnatomicalSliceNo": header_slice,
        "ChronSliceNo": header_slice,
        "NumberInSeries": int(header.image_index),
        "ProtocolSliceNumber": header_slice,
        "SliceNo": header_slice,
        "IsmrmrdSliceNo": header_slice,
    }

    for field, expected in expected_position_fields.items():
        meta_value = _meta_int(meta, field)
        if meta_value is None:
            errors.append(f"image {index} is missing Meta {field}")
        elif meta_value != expected:
            errors.append(
                f"image {index} has Meta {field}={meta_value}, expected {expected}"
            )
        if field == "IsmrmrdSliceNo":
            continue
        minihead_value = _minihead_long_value(minihead, field)
        if minihead and minihead_value is None:
            errors.append(f"image {index} is missing IceMiniHead {field}")
        elif minihead_value is not None and minihead_value != expected:
            errors.append(
                f"image {index} has IceMiniHead {field}={minihead_value}, "
                f"expected {expected}"
            )

    keep_image_geometry = _meta_int(meta, "Keep_image_geometry")
    if keep_image_geometry is None:
        errors.append(f"image {index} is missing Meta Keep_image_geometry")
    elif keep_image_geometry == 0:
        partition_count = _meta_int(meta, "partition_count")
        if partition_count is None:
            errors.append(f"image {index} is missing Meta partition_count")
        elif partition_count < 1:
            errors.append(
                f"image {index} has Meta partition_count={partition_count}, "
                "expected at least 1"
            )

        slice_count = _meta_int(meta, "slice_count")
        if slice_count is None:
            errors.append(f"image {index} is missing Meta slice_count")
        elif series_slice_limit and slice_count != series_slice_limit:
            errors.append(
                f"image {index} has Meta slice_count={slice_count}, "
                f"expected {series_slice_limit}"
            )
        elif header_slice >= slice_count:
            errors.append(
                f"image {index} has slice {header_slice} outside explicit "
                f"slice_count {slice_count}"
            )

    sop_uid = _meta_text(meta, "SOPInstanceUID")
    minihead_sop_uid = _minihead_string_value(minihead, "SOPInstanceUID")
    if sop_uid:
        previous_index = seen_sop_uids.setdefault(sop_uid, index)
        if previous_index != index:
            errors.append(
                f"SOPInstanceUID {sop_uid} is shared by output images "
                f"{previous_index} and {index}"
            )
    if minihead_sop_uid:
        previous_index = seen_sop_uids.setdefault(minihead_sop_uid, index)
        if previous_index != index:
            errors.append(
                f"IceMiniHead SOPInstanceUID {minihead_sop_uid} is shared by "
                f"output images {previous_index} and {index}"
            )
    image_type_value4 = _meta_text(meta, "ImageTypeValue4")
    minihead_image_type_value4 = _minihead_array_tokens(minihead, "ImageTypeValue4")
    if not image_type_value4:
        errors.append(f"image {index} is missing Meta ImageTypeValue4")
    if minihead and image_type_value4 not in minihead_image_type_value4:
        errors.append(
            f"image {index} has IceMiniHead ImageTypeValue4 "
            f"{minihead_image_type_value4}, expected {image_type_value4}"
        )

    storage_key = (
        _meta_text(meta, "SeriesInstanceUID"),
        _meta_int(meta, "SliceNo"),
        _meta_int(meta, "ChronSliceNo"),
        _meta_int(meta, "NumberInSeries"),
    )
    previous_index = seen_storage_keys.setdefault(storage_key, index)
    if previous_index != index:
        errors.append(
            f"image {index} duplicates scanner storage key {storage_key} "
            f"from image {previous_index}"
        )
    for field in ("SOPInstanceUID",):
        value = _meta_text(meta, field)
        if value and value in input_identity:
            errors.append(f"image {index} reuses input storage identity {field}={value}")


def _series_slice_limit(series_index, source_slice_count):
    if source_slice_count <= 0:
        return 0
    if series_index == UPSAMPLED_SERIES_INDEX:
        return 2 * source_slice_count
    if series_index == MIP_SERIES_INDEX:
        return 1
    return source_slice_count


def _identity_values(images):
    values = set()
    for image in images:
        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        for key in (
            "SeriesDescription",
            "SequenceDescription",
            "ProtocolName",
            "SeriesNumberRangeNameUID",
            "SeriesInstanceUID",
            "SOPInstanceUID",
        ):
            value = _meta_text(meta, key)
            if value:
                values.add(value)
            minihead_value = _minihead_string_value(minihead, key)
            if minihead_value:
                values.add(minihead_value)
    return values


def _meta_int(meta, key):
    text = _meta_text(meta, key)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _image_minihead(image):
    return _decode_ice_minihead(_meta_text(_meta_from_image(image), "IceMiniHead"))


def _ordered_upsample_sources(images):
    images = list(images)
    if len(images) < 2:
        return images, _infer_slice_axis([image.getHead() for image in images])

    headers = [image.getHead() for image in images]
    slice_axis = _infer_slice_axis(headers)
    projections = np.asarray(
        [_projected_position(image, slice_axis) for image in images],
        dtype=float,
    )
    duplicate_positions = _duplicate_projected_position_count(projections)
    median_spacing = _median_projected_spacing(projections)
    increasing = _is_projected_order_monotonic(projections, increasing=True)
    decreasing = _is_projected_order_monotonic(projections, increasing=False)
    logging.info(
        "upsampled source geometry: count=%d axis=%s projected_range=[%.6f, %.6f] "
        "median_spacing=%.6f duplicate_positions=%d receive_order_monotonic=%s",
        len(images),
        _format_vector(slice_axis),
        float(np.min(projections)),
        float(np.max(projections)),
        median_spacing,
        duplicate_positions,
        increasing or decreasing,
    )
    if duplicate_positions:
        logging.warning(
            "upsampled source geometry has %d duplicate projected slice position(s)",
            duplicate_positions,
        )
    if increasing:
        return images, slice_axis

    sort_indices = sorted(
        range(len(images)),
        key=lambda index: (
            round(float(projections[index]), 4),
            int(headers[index].slice),
            int(headers[index].image_index),
            index,
        ),
    )
    order_reason = (
        "decreasing in physical position"
        if decreasing
        else "not monotonic in physical position"
    )
    logging.warning(
        "upsampled source slices are %s; "
        "sorting by projected position before interpolation: first mappings %s",
        order_reason,
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:24])
        ),
    )
    if len(sort_indices) > 24:
        logging.warning(
            "upsampled source sort mapping omitted %d additional slice(s)",
            len(sort_indices) - 24,
        )
    return [images[index] for index in sort_indices], slice_axis


def _validate_unique_projected_positions(images, slice_axis, label):
    if len(images) < 2:
        return

    projections = np.asarray(
        [_projected_position(image, slice_axis) for image in images],
        dtype=float,
    )
    sort_indices = sorted(range(len(images)), key=lambda index: projections[index])
    duplicates = []
    for previous_index, current_index in zip(sort_indices, sort_indices[1:]):
        if (
            abs(projections[current_index] - projections[previous_index])
            <= SLICE_POSITION_TOLERANCE_MM
        ):
            duplicates.append((previous_index, current_index, projections[current_index]))

    if duplicates:
        details = ", ".join(
            f"{previous}/{current}@{projection:.6f}"
            for previous, current, projection in duplicates[:8]
        )
        if len(duplicates) > 8:
            details += f", ... {len(duplicates) - 8} more"
        raise ValueError(
            f"{label} has duplicate projected slice position(s): {details}"
        )

    sorted_projections = projections[sort_indices]
    sorted_diffs = np.diff(sorted_projections)
    current_increasing = _is_projected_order_monotonic(
        projections,
        increasing=True,
    )
    current_decreasing = _is_projected_order_monotonic(
        projections,
        increasing=False,
    )
    logging.info(
        "%s geometry: count=%d projected_range=[%.6f, %.6f] min_spacing=%.6f "
        "output_order_monotonic=%s",
        label,
        len(images),
        float(np.min(projections)),
        float(np.max(projections)),
        float(np.min(np.abs(sorted_diffs))) if sorted_diffs.size else 0.0,
        current_increasing or current_decreasing,
    )
    if not (current_increasing or current_decreasing):
        logging.warning("%s positions are not monotonic in output order", label)


def _duplicate_projected_position_count(projections):
    if len(projections) < 2:
        return 0
    sorted_diffs = np.diff(np.sort(np.asarray(projections, dtype=float)))
    return int(np.sum(np.abs(sorted_diffs) <= SLICE_POSITION_TOLERANCE_MM))


def _median_projected_spacing(projections):
    if len(projections) < 2:
        return 0.0
    sorted_diffs = np.diff(np.sort(np.asarray(projections, dtype=float)))
    nonzero_diffs = np.abs(
        sorted_diffs[np.abs(sorted_diffs) > SLICE_POSITION_TOLERANCE_MM]
    )
    if nonzero_diffs.size == 0:
        return 0.0
    return float(np.median(nonzero_diffs))


def _is_projected_order_monotonic(projections, increasing):
    if len(projections) < 2:
        return True
    diffs = np.diff(np.asarray(projections, dtype=float))
    if increasing:
        return bool(np.all(diffs > SLICE_POSITION_TOLERANCE_MM))
    return bool(np.all(diffs < -SLICE_POSITION_TOLERANCE_MM))


def _log_output_images(output_images):
    for index, image in enumerate(output_images):
        header = image.getHead()
        meta = _meta_from_image(image)
        minihead = _image_minihead(image)
        logging.info(
            "OPENRECONI2I_OUTPUT index=%d series=%d image_index=%d slice=%d "
            "position=%s fov=%s meta_slice_pos=%s keep_geometry=%s "
            "partition_count=%s slice_count=%s name=%s series_uid=%s sop_uid=%s "
            "minihead_slice=%s minihead_chron_slice=%s minihead_sop_uid=%s",
            index,
            int(image.image_series_index),
            int(header.image_index),
            int(header.slice),
            _format_vector(_header_vector(header, "position")),
            _format_vector(_header_vector(header, "field_of_view")),
            _meta_vector_text(meta, "SlicePosLightMarker"),
            _meta_text(meta, "Keep_image_geometry"),
            _meta_text(meta, "partition_count"),
            _meta_text(meta, "slice_count"),
            _meta_text(meta, "SeriesDescription"),
            _meta_text(meta, "SeriesInstanceUID"),
            _meta_text(meta, "SOPInstanceUID"),
            _minihead_long_value(minihead, "SliceNo"),
            _minihead_long_value(minihead, "ChronSliceNo"),
            _minihead_string_value(minihead, "SOPInstanceUID"),
        )


def _explicit_header_geometry_meta(header):
    # The MRD ImageHeader is the geometry contract; these Meta copies are kept
    # aligned for scanner-side consumers and log/debug inspection.
    return {
        "ImageRowDir": _meta_vector(_header_vector(header, "read_dir")),
        "ImageColumnDir": _meta_vector(_header_vector(header, "phase_dir")),
        "ImageSliceNormDir": _meta_vector(_header_vector(header, "slice_dir")),
        "SlicePosLightMarker": _meta_vector(_header_vector(header, "position")),
    }


def _infer_slice_axis(headers):
    for header in headers:
        axis = _normalize_vector(_header_vector(header, "slice_dir"))
        if axis is not None:
            return axis

    if len(headers) > 1:
        axis = _normalize_vector(
            _header_vector(headers[-1], "position")
            - _header_vector(headers[0], "position")
        )
        if axis is not None:
            return axis

    return np.array([0.0, 0.0, 1.0], dtype=float)


def _projected_position(image, slice_axis):
    return float(np.dot(_header_position(image.getHead()), slice_axis))


def _project_position_on_slice_axis(position, origin, slice_axis):
    position = np.asarray(position, dtype=float)
    origin = np.asarray(origin, dtype=float)
    offset = position - origin
    return origin + float(np.dot(offset, slice_axis)) * slice_axis


def _half_step_on_slice_axis(half_step, slice_axis):
    half_step = np.asarray(half_step, dtype=float)
    signed_length = float(np.dot(half_step, slice_axis))
    if abs(signed_length) <= SLICE_POSITION_TOLERANCE_MM:
        signed_length = float(np.linalg.norm(half_step))
    return signed_length * slice_axis


def _header_vector(header, field_name):
    try:
        values = np.asarray(getattr(header, field_name), dtype=float)
    except Exception:
        return np.zeros(3, dtype=float)
    if values.size < 3:
        padded = np.zeros(3, dtype=float)
        padded[: values.size] = values
        return padded
    return values[:3]


def _normalize_vector(vector):
    vector = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-8:
        return None
    return vector / norm


def _format_vector(vector):
    return "[" + ", ".join(f"{float(value):.6f}" for value in vector) + "]"


def _meta_vector(values):
    return [f"{float(value):.18f}" for value in values]


def _meta_vector_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(str(item) for item in value) + "]"
    if value is None:
        return ""
    return str(value)


def _interpolated_half_step(images):
    if len(images) > 1:
        first = _header_position(images[0].getHead())
        second = _header_position(images[1].getHead())
        step = 0.5 * (second - first)
        if float(np.linalg.norm(step)) > 0:
            return step

    header = images[0].getHead()
    slice_dir = np.asarray(header.slice_dir, dtype=float)
    norm = float(np.linalg.norm(slice_dir))
    if norm == 0:
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    else:
        slice_dir = slice_dir / norm

    spacing = float(header.field_of_view[2]) if header.field_of_view[2] else 1.0
    return 0.5 * spacing * slice_dir


def _header_position(header):
    return np.asarray(header.position, dtype=float)


def _source_slice_count_hint(images):
    meta_keys = ("slice_count", "SliceCount", "NumberOfSlices", "NoOfSlices")
    minihead_keys = meta_keys + ("ImagesInAcquisition", "sSliceArray.lSize")
    for image in images:
        meta = _meta_from_image(image)
        for key in meta_keys:
            value = _meta_int(meta, key)
            if value is not None and value > 0:
                return value

        minihead = _image_minihead(image)
        for key in minihead_keys:
            value = _minihead_long_value(minihead, key)
            if value is not None and value > 0:
                return value

    return None


def _require_output_slice_count(output_slice_count):
    if output_slice_count is None:
        raise ValueError("output_slice_count is required for explicit output geometry")
    try:
        output_slice_count = int(output_slice_count)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "output_slice_count must be a positive integer for explicit output geometry"
        ) from exc
    if output_slice_count < 1:
        raise ValueError(
            "output_slice_count must be a positive integer for explicit output geometry"
        )
    return output_slice_count


def _set_header_sequence_field(image_header, field_name, values):
    values = list(values)
    current_value = getattr(image_header, field_name)
    try:
        current_value[:] = values
    except Exception:
        setattr(image_header, field_name, tuple(values))


def _cast_like(data, reference):
    if np.issubdtype(reference.dtype, np.integer):
        info = np.iinfo(reference.dtype)
        return np.clip(np.rint(data), info.min, info.max).astype(reference.dtype)
    return data.astype(reference.dtype)


def _config_bool(config, key, default=False):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return default

    raw = None
    if isinstance(config, dict):
        raw = config.get(key)
        parameters = config.get("parameters")
        if raw is None and isinstance(parameters, dict):
            raw = parameters.get(key)

    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)

    text = str(raw).strip().lower()
    if text in {"true", "1", "yes", "on"}:
        return True
    if text in {"false", "0", "no", "off"}:
        return False
    return default


def _config_bool_any(config, keys, default=False):
    for key in keys:
        value = _config_bool(config, key, default=None)
        if value is not None:
            return value
    return default
