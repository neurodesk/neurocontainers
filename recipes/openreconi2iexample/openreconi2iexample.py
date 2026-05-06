"""Minimal OpenRecon image-to-image inversion example."""
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
THRESHOLD_MIP_SERIES_INDEX = 101
INTERPOLATED_SERIES_INDEX = 102
INVERT_SERIES_NAME = "openrecon_invert"
ORIGINAL_SERIES_NAME = "openrecon_original"
THRESHOLD_MIP_SERIES_NAME = "openrecon_threshold_mip"
INTERPOLATED_SERIES_NAME = "openrecon_interpolated"


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

        send_original = _config_bool(config, "sendoriginal", default=True)
        send_threshold_mip = _config_bool(config, "sendthresholdmip", default=False)
        send_interpolated = _config_bool(config, "sendinterpolated", default=False)
        output_images = []
        output_images.extend(_invert_images(magnitude_images))
        if send_threshold_mip:
            output_images.extend(_threshold_mip_image(magnitude_images))
        if send_interpolated:
            output_images.extend(_interpolated_images(magnitude_images))
        if send_original:
            output_images.extend(_restamp_originals(input_images))

        interpolated_count = len(magnitude_images) * 2 if send_interpolated else 0
        logging.info(
            "Sending %d inverted image(s), %d threshold MIP image(s), "
            "%d interpolated image(s), and %d original image(s)",
            len(magnitude_images),
            1 if send_threshold_mip and magnitude_images else 0,
            interpolated_count,
            len(input_images) if send_original else 0,
        )
        _validate_output_images(output_images, input_images)
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
            _derived_series_name(source_image, "inverted"),
            "Image",
            INVERT_SERIES_NAME,
            ["PYTHON", "OPENRECON_INVERT"],
            {
                "WindowCenter": str(window_center),
                "WindowWidth": str(window_width),
            },
        )
        outputs.append(output)

    return outputs


def _threshold_mip_image(images):
    if not images:
        return []

    stack = np.stack([np.asarray(image.data) for image in images])
    threshold = float(np.mean(stack) + 0.5 * np.std(stack))
    segmentation = stack > threshold

    # Stack shape is [image, cha, z, y, x]. Collapse source slices and any
    # per-image z dimension to one projected output slice.
    mip = segmentation.max(axis=(0, 2)).astype(np.uint16)
    mip = mip[:, np.newaxis, :, :]

    output = ismrmrd.Image.from_array(mip, transpose=False)
    header = images[0].getHead()
    header.data_type = output.data_type
    output.setHead(header)
    _stamp_output_image(
        output,
        images[0],
        THRESHOLD_MIP_SERIES_INDEX,
        0,
        _derived_series_name(images[0], "mip", THRESHOLD_MIP_SERIES_NAME),
        "Segmentation",
        THRESHOLD_MIP_SERIES_NAME,
        ["PYTHON", "OPENRECON_THRESHOLD_MIP"],
        {
            "WindowCenter": "0.5",
            "WindowWidth": "1",
        },
    )
    logging.info(
        "Created threshold MIP segmentation with threshold %.6g from %d image(s)",
        threshold,
        len(images),
    )
    return [output]


def _interpolated_images(images):
    if not images:
        return []

    half_step = _interpolated_half_step(images)
    outputs = []
    for index, image in enumerate(images):
        outputs.append(
            _make_interpolated_image(
                image,
                np.asarray(image.data),
                2 * index,
                _header_position(image.getHead()),
                half_step,
            )
        )

        if index + 1 < len(images):
            next_image = images[index + 1]
            next_position = _header_position(next_image.getHead())
            midpoint = 0.5 * (
                np.asarray(image.data, dtype=np.float32)
                + np.asarray(next_image.data, dtype=np.float32)
            )
            midpoint_position = 0.5 * (_header_position(image.getHead()) + next_position)
        else:
            midpoint = np.asarray(image.data)
            midpoint_position = _header_position(image.getHead()) + half_step

        outputs.append(
            _make_interpolated_image(
                image,
                _cast_like(midpoint, np.asarray(image.data)),
                2 * index + 1,
                midpoint_position,
                half_step,
            )
        )

    logging.info(
        "Created %d interpolated image(s) from %d source image(s)",
        len(outputs),
        len(images),
    )
    return outputs


def _make_interpolated_image(source_image, data, output_index, position, half_step):
    output = ismrmrd.Image.from_array(data, transpose=False)
    header = source_image.getHead()
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_index = output_index + 1
    header.slice = output_index

    fov = [float(value) for value in header.field_of_view]
    step_length = float(np.linalg.norm(half_step))
    if step_length > 0:
        fov[2] = step_length
    _set_header_sequence_field(header, "field_of_view", fov)
    _set_header_sequence_field(header, "position", [float(value) for value in position])

    output.setHead(header)
    _stamp_output_image(
        output,
        source_image,
        INTERPOLATED_SERIES_INDEX,
        output_index,
        _derived_series_name(source_image, "upsampled", INTERPOLATED_SERIES_NAME),
        "Image",
        INTERPOLATED_SERIES_NAME,
        ["PYTHON", "OPENRECON_INTERPOLATED"],
    )
    return output


def _restamp_originals(images):
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
            _derived_series_name(source_image, "original", ORIGINAL_SERIES_NAME),
            "Image",
            ORIGINAL_SERIES_NAME,
            ["PYTHON", "OPENRECON_ORIGINAL_COPY"],
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
):
    header = output.getHead()
    header.image_index = output_index + 1
    header.slice = output_index
    output.setHead(header)
    output.image_series_index = series_index
    output.attribute_string = _output_meta(
        source_image,
        series_index,
        series_name,
        data_role,
        image_type_token,
        history,
        extra_meta or {},
    ).serialize()
    return output


def _output_meta(
    source_image,
    series_index,
    series_name,
    data_role,
    image_type_token,
    history,
    extra_meta,
):
    meta = _meta_from_image(source_image)
    series_uid = _derived_series_uid(source_image, series_index, series_name)
    series_grouping = _derived_series_grouping(series_name, series_index)
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"
    meta["DataRole"] = data_role
    meta["ImageProcessingHistory"] = history
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["SeriesInstanceUID"] = series_uid
    meta["SeriesNumberRangeNameUID"] = series_grouping
    _set_meta_field(meta, "SequenceDescriptionAdditional", "openrecon")
    meta["Keep_image_geometry"] = 1
    for key, value in extra_meta.items():
        meta[key] = value

    minihead = _decode_ice_minihead(_meta_text(meta, "IceMiniHead"))
    if minihead:
        patched_minihead, changed = _patch_ice_minihead(
            minihead,
            series_name,
            series_grouping,
            series_uid,
            image_type,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)
    return meta


def _meta_from_image(image):
    if not image.attribute_string:
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _derived_series_name(source_image, suffix, fallback_base=INVERT_SERIES_NAME):
    source_name = _source_series_name(source_image)
    if source_name:
        return f"{source_name}-{suffix}"
    return f"{fallback_base}-{suffix}"


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


def _patch_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    image_type,
):
    current_text = minihead_text
    changed = False
    for name, value in (
        ("SeriesDescription", series_name),
        ("SequenceDescription", series_name),
        ("ProtocolName", series_name),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
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
    return current_text, changed


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_identity_text(value)
    if not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    match = pattern.search(minihead_text)
    if match:
        if match.group(2) == value:
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return (
            minihead_text[:match.start()] + replacement + minihead_text[match.end():],
            True,
        )

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


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


def _validate_output_images(output_images, input_images):
    errors = []
    seen_image_keys = {}
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
        if input_has_minihead and not minihead:
            errors.append(f"image {index} is missing IceMiniHead")

        identity = {
            "series_description": _meta_text(meta, "SeriesDescription"),
            "sequence_description": _meta_text(meta, "SequenceDescription"),
            "protocol_name": _meta_text(meta, "ProtocolName"),
            "series_grouping": _meta_text(meta, "SeriesNumberRangeNameUID"),
            "series_uid": _meta_text(meta, "SeriesInstanceUID"),
            "minihead_sequence_description": _minihead_string_value(
                minihead, "SequenceDescription"
            ),
            "minihead_protocol_name": _minihead_string_value(minihead, "ProtocolName"),
            "minihead_series_grouping": _minihead_string_value(
                minihead, "SeriesNumberRangeNameUID"
            ),
            "minihead_series_uid": _minihead_string_value(minihead, "SeriesInstanceUID"),
        }
        _validate_identity_fields(index, identity, input_identity, errors)

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
                f"image_series_index {series_key} has inconsistent identity values"
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
    )
    for key in required:
        if not identity[key]:
            errors.append(f"image {index} is missing Meta {key}")

    for meta_key, minihead_key in (
        ("sequence_description", "minihead_sequence_description"),
        ("protocol_name", "minihead_protocol_name"),
        ("series_grouping", "minihead_series_grouping"),
        ("series_uid", "minihead_series_uid"),
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
    ):
        value = identity[key]
        if value and value in input_identity:
            errors.append(f"image {index} reuses input identity {key}={value}")


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
        ):
            value = _meta_text(meta, key)
            if value:
                values.add(value)
            minihead_value = _minihead_string_value(minihead, key)
            if minihead_value:
                values.add(minihead_value)
    return values


def _image_minihead(image):
    return _decode_ice_minihead(_meta_text(_meta_from_image(image), "IceMiniHead"))


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
