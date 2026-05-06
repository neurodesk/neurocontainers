"""Minimal OpenRecon image-to-image inversion example."""
import base64
import json
import logging
import re
import traceback

import constants
import ismrmrd
import numpy as np


INVERT_SERIES_INDEX = 99
ORIGINAL_SERIES_INDEX = 100
THRESHOLD_MIP_SERIES_INDEX = 101
INTERPOLATED_SERIES_INDEX = 102
INVERT_SERIES_NAME = "openrecon_invert"
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
    for source_image in images:
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
        output.image_series_index = INVERT_SERIES_INDEX
        output.attribute_string = _invert_meta(source_image, window_center, window_width).serialize()
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
    header.slice = 0
    output.setHead(header)
    output.image_series_index = THRESHOLD_MIP_SERIES_INDEX
    output.attribute_string = _threshold_mip_meta(images[0]).serialize()
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
    output.image_series_index = INTERPOLATED_SERIES_INDEX
    output.attribute_string = _interpolated_meta(source_image).serialize()
    return output


def _restamp_originals(images):
    outputs = []
    for image in images:
        meta = _meta_from_image(image)
        meta["Keep_image_geometry"] = 1
        image.attribute_string = meta.serialize()
        image.image_series_index = ORIGINAL_SERIES_INDEX
        outputs.append(image)
    return outputs


def _invert_meta(source_image, window_center, window_width):
    meta = _meta_from_image(source_image)
    series_name = _derived_series_name(source_image, "inverted")
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "OPENRECON_INVERT"]
    meta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{INVERT_SERIES_NAME}"
    meta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{INVERT_SERIES_NAME}"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    _clear_meta_field(meta, "SequenceDescriptionAdditional")
    meta["WindowCenter"] = str(window_center)
    meta["WindowWidth"] = str(window_width)
    meta["Keep_image_geometry"] = 1
    return meta


def _interpolated_meta(source_image):
    meta = _meta_from_image(source_image)
    series_name = _derived_series_name(source_image, "upsampled", INTERPOLATED_SERIES_NAME)
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "OPENRECON_INTERPOLATED"]
    meta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{INTERPOLATED_SERIES_NAME}"
    meta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{INTERPOLATED_SERIES_NAME}"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    _clear_meta_field(meta, "SequenceDescriptionAdditional")
    meta["Keep_image_geometry"] = 1
    return meta


def _threshold_mip_meta(source_image):
    meta = _meta_from_image(source_image)
    series_name = _derived_series_name(source_image, "mip", THRESHOLD_MIP_SERIES_NAME)
    meta["DataRole"] = "Segmentation"
    meta["ImageProcessingHistory"] = ["PYTHON", "OPENRECON_THRESHOLD_MIP"]
    meta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{THRESHOLD_MIP_SERIES_NAME}"
    meta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{THRESHOLD_MIP_SERIES_NAME}"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    _clear_meta_field(meta, "SequenceDescriptionAdditional")
    meta["WindowCenter"] = "0.5"
    meta["WindowWidth"] = "1"
    meta["Keep_image_geometry"] = 1
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


def _minihead_string_value(minihead, key):
    match = re.search(
        rf'<ParamString\."{re.escape(key)}">\s*{{\s*"([^"]*)"',
        minihead,
    )
    return match.group(1).strip() if match else ""


def _clear_meta_field(meta, key):
    meta[key] = ""


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
