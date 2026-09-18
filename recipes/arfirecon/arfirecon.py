"""OpenRecon ARFI magnitude and phase output from reconstructed MRD images."""

import base64
import json
import logging
import math
import re
import traceback
import uuid

import ismrmrd
import numpy as np

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


RECIPE_NAME = "arfirecon"
PHASE_WRAP = 4096.0
SCANNER_DISPLAY_MIN = 0
SCANNER_DISPLAY_MAX = 4095

MAGNITUDE_SERIES_INDEX = 101
PHASE_SERIES_INDEX = 102
ARFI_PHASE_SERIES_INDEX = 103
WITH_FUS_PHASE_STD_SERIES_INDEX = 104
NO_FUS_PHASE_STD_SERIES_INDEX = 105

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
SCANNER_WRITE_UNSAFE_META_KEYS = {
    "ImageTypeValue3",
}


def process(connection, config, metadata):
    logging.info("Config: %s", config)
    input_images = []

    try:
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image):
                input_images.append(item)
            else:
                logging.info("Ignoring unsupported MRD message %s", type(item).__name__)

        if not input_images:
            logging.warning("No image messages received; closing without output")
            return

        settings = _settings_from_config(config)
        result = compute_image_frames(input_images, settings)
        output_images = build_output_images(result, settings, input_images)

        logging.info(
            "ARFI reconstruction: slices=%d frames=%d outputs=%d",
            result["slice_count"],
            result["frame_count"],
            len(output_images),
        )

        _validate_output_images(output_images, input_images)
        _send_images_by_series(connection, output_images)

    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def compute_image_frames(input_images, settings=None):
    settings = dict(settings or {})
    magnitude_images, phase_images = _split_magnitude_phase_images(input_images)
    if not magnitude_images:
        raise ValueError("ARFI reconstruction requires magnitude image messages")
    if not phase_images:
        raise ValueError("ARFI reconstruction requires phase image messages")

    magnitude_groups = _group_frames_by_slice(magnitude_images)
    phase_groups = _group_frames_by_slice(phase_images)
    if len(magnitude_groups) != len(phase_groups):
        raise ValueError(
            "Magnitude and phase inputs contain different numbers of slices: "
            f"{len(magnitude_groups)} and {len(phase_groups)}"
        )

    frame_count = len(magnitude_groups[0])
    for slice_index, (magnitude_group, phase_group) in enumerate(
        zip(magnitude_groups, phase_groups)
    ):
        if len(magnitude_group) != frame_count or len(phase_group) != frame_count:
            raise ValueError(
                "All ARFI slices must contain matching magnitude and phase "
                f"frames; expected {frame_count}, slice {slice_index} has "
                f"{len(magnitude_group)} magnitude and {len(phase_group)} phase"
            )

    phase_wrap = _setting_float(settings, "phasewrap", default=PHASE_WRAP)
    slice_magnitude = []
    slice_phase = []
    for magnitude_group, phase_group in zip(magnitude_groups, phase_groups):
        magnitude_stack = np.stack(
            [_image_volume_data(image) for image in magnitude_group],
            axis=0,
        )
        phase_stack = np.stack(
            [_image_volume_data(image) for image in phase_group],
            axis=0,
        )
        magnitude_stack = np.nan_to_num(
            magnitude_stack.astype(np.float32),
            copy=False,
        )
        phase_stack = np.nan_to_num(phase_stack.astype(np.float32), copy=False)
        slice_magnitude.append(magnitude_stack)
        slice_phase.append(
            np.angle(np.exp(1j * _phase_to_radians(phase_stack, phase_wrap)))
        )

    magnitude = np.concatenate(slice_magnitude, axis=1).astype(np.float32)
    phase = np.concatenate(slice_phase, axis=1).astype(np.float32)
    if magnitude.shape != phase.shape:
        raise ValueError(
            "Magnitude and phase frame volumes have different shapes: "
            f"{magnitude.shape} and {phase.shape}"
        )

    arfi_scheme = settings.get("arfischemes", "singlefreq")
    arfi_block_length = int(settings.get("arfiblocklength", 25))
    send_phase_standard_deviation = bool(
        settings.get("sendphasestandarddeviation", False)
    )
    arfi_phase_difference = None
    with_fus_phase_standard_deviation = None
    no_fus_phase_standard_deviation = None
    if arfi_scheme == "singlefreq":
        no_fus_indices, with_fus_indices = _single_frequency_frame_groups(
            phase.shape[0],
            arfi_block_length,
        )
        arfi_phase_difference = _single_frequency_phase_difference(
            magnitude,
            phase,
            arfi_block_length,
        )
    elif arfi_scheme == "multiplefreq":
        no_fus_indices, with_fus_indices = _multiple_frequency_frame_groups(
            phase.shape[0]
        )
        arfi_phase_difference = _multiple_frequency_phase_difference(
            magnitude,
            phase,
        )

    if send_phase_standard_deviation and arfi_phase_difference is not None:
        (
            no_fus_phase_standard_deviation,
            with_fus_phase_standard_deviation,
        ) = _phase_standard_deviations(
            magnitude,
            phase,
            no_fus_indices,
            with_fus_indices,
        )

    return {
        "frame_count": frame_count,
        "slice_count": int(phase.shape[1]),
        "magnitude_frame_anchor_images": [
            [group[frame_index] for group in magnitude_groups]
            for frame_index in range(frame_count)
        ],
        "phase_frame_anchor_images": [
            [group[frame_index] for group in phase_groups]
            for frame_index in range(frame_count)
        ],
        "magnitude_images": magnitude_images,
        "phase_images": phase_images,
        "magnitude": magnitude,
        "phase": phase,
        "arfi_phase_difference": arfi_phase_difference,
        "with_fus_phase_standard_deviation": (
            with_fus_phase_standard_deviation
        ),
        "no_fus_phase_standard_deviation": no_fus_phase_standard_deviation,
    }


def build_output_images(result, settings=None, input_images=None):
    settings = dict(settings or {})
    input_images = input_images or result["magnitude_images"] + result["phase_images"]
    series_indices = _allocate_output_series_indices(input_images)
    source_name = (
        _source_series_name(result["magnitude_frame_anchor_images"][0][0])
        or RECIPE_NAME
    )
    arfi_scheme = settings.get("arfischemes", "singlefreq")
    arfi_block_length = int(settings.get("arfiblocklength", 25))
    slice_count = int(result["slice_count"])
    frame_count = int(result["frame_count"])

    outputs = []
    for frame_index in range(frame_count):
        slice_anchors = result["magnitude_frame_anchor_images"][frame_index]
        prepared_display = _prepare_display_volume(
            result["magnitude"][frame_index],
            "ARFIMAGNITUDE",
            "a.u.",
        )
        for slice_index in range(slice_count):
            anchor = slice_anchors[min(slice_index, len(slice_anchors) - 1)]
            outputs.append(_map_to_mrd_image(
                result["magnitude"][frame_index], anchor,
                slice_anchors,
                series_indices["magnitude"],
                f"{source_name} Magnitude",
                "ARFIMAGNITUDE",
                "ARFIMagnitude",
                "a.u.",
                frame_index=frame_index,
                slice_index=slice_index,
                slice_count=slice_count,
                image_index=frame_index * slice_count + slice_index + 1,
                images_in_series=frame_count * slice_count,
                arfi_scheme=arfi_scheme,
                arfi_block_length=arfi_block_length,
                prepared_display=prepared_display,
            ))

    for frame_index in range(frame_count):
        slice_anchors = result["phase_frame_anchor_images"][frame_index]
        prepared_display = _prepare_display_volume(
            result["phase"][frame_index],
            "ARFIPHASE",
            "degrees",
        )
        for slice_index in range(slice_count):
            anchor = slice_anchors[min(slice_index, len(slice_anchors) - 1)]
            outputs.append(_map_to_mrd_image(
                result["phase"][frame_index], anchor,
                slice_anchors,
                series_indices["phase"],
                f"{source_name} Phase",
                "ARFIPHASE",
                "ARFIPhase",
                "degrees",
                frame_index=frame_index,
                slice_index=slice_index,
                slice_count=slice_count,
                image_index=frame_index * slice_count + slice_index + 1,
                images_in_series=frame_count * slice_count,
                arfi_scheme=arfi_scheme,
                arfi_block_length=arfi_block_length,
                prepared_display=prepared_display,
            ))

    if result["arfi_phase_difference"] is not None:
        slice_anchors = result["phase_frame_anchor_images"][0]
        prepared_display = _prepare_display_volume(
            result["arfi_phase_difference"],
            "ARFIPHASE",
            "degrees",
        )
        for slice_index in range(slice_count):
            anchor = slice_anchors[min(slice_index, len(slice_anchors) - 1)]
            outputs.append(_map_to_mrd_image(
                result["arfi_phase_difference"], anchor,
                slice_anchors,
                series_indices["arfi_phase"],
                f"{source_name} ARFI phase",
                "ARFIPHASE",
                "ARFIPhaseDifference",
                "degrees",
                frame_index=0,
                slice_index=slice_index,
                slice_count=slice_count,
                image_index=slice_index + 1,
                images_in_series=slice_count,
                arfi_scheme=arfi_scheme,
                arfi_block_length=arfi_block_length,
                prepared_display=prepared_display,
            ))

    standard_deviation_outputs = (
        (
            "with_fus_phase_standard_deviation",
            "with_fus_phase_std",
            f"{source_name} withFUS phase standard deviation",
            "ARFIWithFUSPhaseStandardDeviation",
        ),
        (
            "no_fus_phase_standard_deviation",
            "no_fus_phase_std",
            f"{source_name} noFUS phase standard deviation",
            "ARFINoFUSPhaseStandardDeviation",
        ),
    )
    for result_key, series_key, series_name, map_role in (
        standard_deviation_outputs
    ):
        standard_deviation = result.get(result_key)
        if standard_deviation is None:
            continue
        slice_anchors = result["phase_frame_anchor_images"][0]
        prepared_display = _prepare_display_volume(
            standard_deviation,
            "ARFIPHASESTD",
            "degrees",
        )
        for slice_index in range(slice_count):
            anchor = slice_anchors[min(slice_index, len(slice_anchors) - 1)]
            outputs.append(_map_to_mrd_image(
                standard_deviation,
                anchor,
                slice_anchors,
                series_indices[series_key],
                series_name,
                "ARFIPHASESTD",
                map_role,
                "degrees",
                frame_index=None,
                slice_index=slice_index,
                slice_count=slice_count,
                image_index=slice_index + 1,
                images_in_series=slice_count,
                arfi_scheme=arfi_scheme,
                arfi_block_length=arfi_block_length,
                prepared_display=prepared_display,
            ))

    return outputs


def _phase_to_radians(phase_stack, phase_wrap):
    if phase_wrap <= 0:
        return phase_stack.astype(np.float32)
    return phase_stack.astype(np.float32) * (2.0 * math.pi / phase_wrap) - math.pi


def _single_frequency_phase_difference(
    magnitude_frames,
    phase_frames,
    block_length,
):
    no_fus_indices, with_fus_indices = _single_frequency_frame_groups(
        phase_frames.shape[0],
        block_length,
    )
    return _complex_phase_difference(
        magnitude_frames,
        phase_frames,
        no_fus_indices,
        with_fus_indices,
        f"Single Freq block_length={block_length}",
    )


def _single_frequency_frame_groups(frame_count, block_length):
    if block_length <= 0:
        raise ValueError(
            "ARFI Block Length must be greater than zero for Single Freq"
        )

    frame_count = int(frame_count)
    group_indices = np.arange(frame_count) // int(block_length)
    no_fus_indices = np.flatnonzero(group_indices % 2 == 0)
    with_fus_indices = np.flatnonzero(group_indices % 2 == 1)
    if with_fus_indices.size == 0:
        raise ValueError(
            "Single Freq ARFI requires at least two sequential blocks; "
            f"got {frame_count} frames with block length {block_length}"
        )

    return no_fus_indices, with_fus_indices


def _multiple_frequency_frame_groups(frame_count):
    frame_numbers = np.arange(1, int(frame_count) + 1, dtype=np.int64)
    my_temp = np.floor(
        (
            np.sqrt(((frame_numbers - 1) // 2) * 8 + 1)
            + 1
        )
        / 2
    ).astype(np.int64)
    with_fus = frame_numbers > my_temp * my_temp
    return np.flatnonzero(~with_fus), np.flatnonzero(with_fus)


def _multiple_frequency_phase_difference(magnitude_frames, phase_frames):
    no_fus_indices, with_fus_indices = _multiple_frequency_frame_groups(
        phase_frames.shape[0]
    )
    if no_fus_indices.size == 0 or with_fus_indices.size == 0:
        raise ValueError(
            "Multiple Freq ARFI requires at least one noFUS and one withFUS "
            f"frame; got {phase_frames.shape[0]} frame(s)"
        )
    return _complex_phase_difference(
        magnitude_frames,
        phase_frames,
        no_fus_indices,
        with_fus_indices,
        "Multiple Freq",
    )


def _complex_phase_difference(
    magnitude_frames,
    phase_frames,
    no_fus_indices,
    with_fus_indices,
    scheme_description,
):
    complex_frames = np.asarray(magnitude_frames, dtype=np.float32) * np.exp(
        1j * np.asarray(phase_frames, dtype=np.float32)
    )
    no_fus_average_frame = np.mean(complex_frames[no_fus_indices], axis=0)
    with_fus_average_frame = np.mean(complex_frames[with_fus_indices], axis=0)
    logging.info(
        "%s ARFI complex averages: noFUS_frames=%d withFUS_frames=%d",
        scheme_description,
        no_fus_indices.size,
        with_fus_indices.size,
    )
    return np.angle(
        with_fus_average_frame * np.conj(no_fus_average_frame)
    ).astype(np.float32)


def _phase_standard_deviations(
    magnitude_frames,
    phase_frames,
    no_fus_indices,
    with_fus_indices,
):
    complex_frames = np.asarray(magnitude_frames, dtype=np.float32) * np.exp(
        1j * np.asarray(phase_frames, dtype=np.float32)
    )
    return (
        _phase_standard_deviation(complex_frames, no_fus_indices),
        _phase_standard_deviation(complex_frames, with_fus_indices),
    )


def _phase_standard_deviation(complex_frames, frame_indices):
    selected_frames = complex_frames[frame_indices]
    mean_frame = np.mean(selected_frames, axis=0)
    residual_phase = np.angle(
        selected_frames * np.conj(mean_frame)[np.newaxis, ...]
    )
    if selected_frames.shape[0] <= 1:
        return np.zeros(mean_frame.shape, dtype=np.float32)
    return np.std(residual_phase, axis=0, ddof=1).astype(np.float32)


def _split_magnitude_phase_images(images):
    magnitude_images = []
    phase_images = []
    for image in images:
        if _is_phase_image(image):
            phase_images.append(image)
        elif _is_magnitude_image(image):
            magnitude_images.append(image)
        else:
            logging.info(
                "Ignoring non-magnitude/non-phase image_type=%s",
                image.image_type,
            )
    if not phase_images:
        fallback_magnitude, fallback_phase = _fallback_split_magnitude_phase_series(
            magnitude_images
        )
        if fallback_phase:
            logging.warning(
                "No explicit phase MRD image_type was present; treating the first "
                "of two equal-length image series as magnitude and the second as phase"
            )
            return fallback_magnitude, fallback_phase
    return magnitude_images, phase_images


def _fallback_split_magnitude_phase_series(images):
    series_groups = []
    group_index_by_key = {}
    for image in images:
        key = _source_series_key(image)
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(series_groups)
            group_index_by_key[key] = group_index
            series_groups.append([])
        series_groups[group_index].append(image)

    if len(series_groups) != 2 or len(series_groups[0]) != len(series_groups[1]):
        return images, []

    series_groups = sorted(series_groups, key=_series_group_sort_key)
    return series_groups[0], series_groups[1]


def _source_series_key(image):
    meta = _meta_from_image(image)
    return (
        int(image.getHead().image_series_index),
        _meta_text(meta, "SeriesInstanceUID"),
        _meta_text(meta, "SeriesNumberRangeNameUID"),
        _meta_text(meta, "SequenceDescription"),
        _meta_text(meta, "ProtocolName"),
    )


def _series_group_sort_key(group):
    first_image = group[0]
    meta = _meta_from_image(first_image)
    series_number = _dicom_json_numeric_value(meta, "00200011")
    if series_number is None:
        series_number = int(first_image.getHead().image_series_index)
    return (series_number, int(first_image.getHead().image_series_index))


def _is_magnitude_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "MAGNITUDE" in text or re.search(r"(^|\\|\s)M($|\\|\s)", text) is not None


def _is_phase_image(image):
    image_type = int(getattr(image, "image_type", 0))
    if image_type == int(getattr(ismrmrd, "IMTYPE_PHASE", 2)):
        return True
    meta = _meta_from_image(image)
    text = " ".join(
        [
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()
    return "PHASE" in text or re.search(r"(^|\\|\s)P($|\\|\s)", text) is not None


def _group_frames_by_slice(images):
    indexed = list(enumerate(images))
    if indexed and all(_is_volume_frame_image(image) for _order, image in indexed):
        logging.info(
            "Grouped %d volume-frame image(s) into one ARFI frame group",
            len(indexed),
        )
        return [[image for _order, image in sorted(indexed, key=_frame_sort_key)]]

    candidates = []
    for name, key_func in (
        ("position", _position_group_key),
        ("slice", _slice_group_key),
    ):
        groups = _groups_from_key(indexed, key_func)
        if groups and all(group for group in groups):
            candidates.append((name, groups))

    if not candidates:
        raise ValueError("ARFI phase reconstruction could not group frames by slice")

    def score(candidate):
        name, groups = candidate
        group_sizes = {len(group) for group in groups}
        informative_geometry = (
            name != "chunk"
            and len(groups) > 1
            and len(group_sizes) == 1
        )
        return (
            informative_geometry,
            name != "chunk",
            len(groups),
            -max(group_sizes),
        )

    selected_name, selected_groups = max(candidates, key=score)
    logging.info(
        "Grouped %d image(s) into %d ARFI slice group(s) using %s",
        len(images),
        len(selected_groups),
        selected_name,
    )
    return [
        [image for _order, image in sorted(group, key=_frame_sort_key)]
        for group in sorted(selected_groups, key=_slice_sort_key)
    ]


def _groups_from_key(indexed_images, key_func):
    groups = []
    group_index_by_key = {}
    for item in indexed_images:
        key = key_func(item[1])
        group_index = group_index_by_key.get(key)
        if group_index is None:
            group_index = len(groups)
            group_index_by_key[key] = group_index
            groups.append([])
        groups[group_index].append(item)
    return groups


def _position_group_key(image):
    header = image.getHead()
    position = tuple(round(float(value), 3) for value in header.position)
    slice_dir = tuple(round(float(value), 3) for value in header.slice_dir)
    return position, slice_dir


def _slice_group_key(image):
    return int(image.getHead().slice)


def _frame_sort_key(indexed_image):
    order, image = indexed_image
    header = image.getHead()
    image_index = int(getattr(header, "image_index", 0))
    return (
        image_index if image_index > 0 else order + 1,
        int(getattr(header, "contrast", 0)),
        int(getattr(header, "phase", 0)),
        int(getattr(header, "repetition", 0)),
        int(getattr(header, "set", 0)),
        order,
    )


def _slice_sort_key(group):
    first_image = group[0][1]
    header = first_image.getHead()
    axis = _normalize_vector(header.slice_dir)
    if axis is None:
        axis = np.asarray((0.0, 0.0, 1.0))
    position = np.asarray(header.position, dtype=float)
    return (float(np.dot(position, axis)), group[0][0])


def _is_volume_frame_image(image):
    data = np.asarray(image.data)
    return data.ndim == 4 and data.shape[0] == 1 and data.shape[1] > 1


def _image_volume_data(image):
    data = np.asarray(image.data)
    if data.ndim == 4 and data.shape[0] == 1:
        return data[0]
    data = np.squeeze(data)
    if data.ndim == 2:
        return data[np.newaxis, :, :]
    if data.ndim != 3:
        raise ValueError(
            "ARFI input images must be single-channel 2D frames or "
            "single-channel 3D volume frames; "
            f"got data shape {np.asarray(image.data).shape}"
        )
    return data


def _format_display_number(value):
    number = float(value)
    if number == 0.0:
        return "0"
    if number.is_integer():
        return str(int(number))
    return f"{number:.12g}"


def _scale_volume_to_display_range(volume, units):
    values = np.asarray(volume, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        display = np.zeros(values.shape, dtype=np.uint16)
        return display, {
            "input_min": 0.0,
            "input_max": 0.0,
            "scale": 1.0,
            "display_min": SCANNER_DISPLAY_MIN,
            "display_max": SCANNER_DISPLAY_MIN,
            "rescale_slope": 1.0,
            "rescale_intercept": 0.0,
            "formula": f"{units} = display",
        }

    input_min = float(np.min(finite))
    input_max = float(np.max(finite))
    input_range = input_max - input_min
    if input_range <= 0.0 or not np.isfinite(input_range):
        display = np.zeros(values.shape, dtype=np.uint16)
        intercept_text = _format_display_number(input_min)
        return display, {
            "input_min": input_min,
            "input_max": input_max,
            "scale": 1.0,
            "display_min": SCANNER_DISPLAY_MIN,
            "display_max": SCANNER_DISPLAY_MIN,
            "rescale_slope": 1.0,
            "rescale_intercept": input_min,
            "formula": f"{units} = display + {intercept_text}",
        }

    display_range = float(SCANNER_DISPLAY_MAX - SCANNER_DISPLAY_MIN)
    scale = display_range / input_range
    rescale_slope = input_range / display_range
    cleaned = np.nan_to_num(
        values,
        nan=input_min,
        posinf=input_max,
        neginf=input_min,
    )
    display = np.rint((cleaned - input_min) * scale + SCANNER_DISPLAY_MIN)
    display = np.clip(display, SCANNER_DISPLAY_MIN, SCANNER_DISPLAY_MAX)
    display = display.astype(np.uint16, copy=False)
    slope_text = _format_display_number(rescale_slope)
    intercept_text = _format_display_number(input_min)
    return display, {
        "input_min": input_min,
        "input_max": input_max,
        "scale": scale,
        "display_min": int(np.min(display)) if display.size else SCANNER_DISPLAY_MIN,
        "display_max": int(np.max(display)) if display.size else SCANNER_DISPLAY_MIN,
        "rescale_slope": rescale_slope,
        "rescale_intercept": input_min,
        "formula": f"{units} = display * {slope_text} + {intercept_text}",
    }


def _prepare_display_volume(volume, image_type_token, units):
    volume = np.asarray(volume)
    if volume.ndim != 3:
        raise ValueError(f"Output map volume must be 3D, got shape {volume.shape}")

    if image_type_token.startswith("ARFIPHASE"):
        physical_volume = np.degrees(volume)
        conversion_comment = "phase converted from radians to degrees; "
    else:
        physical_volume = volume
        conversion_comment = ""

    output_data, display_meta = _scale_volume_to_display_range(
        physical_volume,
        units,
    )
    display_meta["comment"] = (
        conversion_comment
        + f"scanner display uint16 {SCANNER_DISPLAY_MIN}-{SCANNER_DISPLAY_MAX}; "
        + display_meta["formula"]
    )
    if conversion_comment:
        display_meta["conversion"] = "radians to degrees"
    return physical_volume, output_data, display_meta


def _map_to_mrd_image(
    volume,
    anchor_image,
    slice_anchor_images,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    frame_index=None,
    slice_index=0,
    slice_count=None,
    image_index=None,
    images_in_series=None,
    arfi_scheme="singlefreq",
    arfi_block_length=25,
    window_center=None,
    window_width=None,
    prepared_display=None,
):
    volume = np.asarray(volume)
    if volume.ndim != 3:
        raise ValueError(f"Output map volume must be 3D, got shape {volume.shape}")
    slice_count = int(slice_count if slice_count is not None else volume.shape[0])
    slice_index = int(slice_index)
    if not 0 <= slice_index < volume.shape[0]:
        raise ValueError(
            f"Output slice index {slice_index} is outside volume shape {volume.shape}"
        )
    if prepared_display is None:
        prepared_display = _prepare_display_volume(
            volume,
            image_type_token,
            units,
        )
    physical_volume, output_volume, display_meta = prepared_display
    output = ismrmrd.Image.from_array(
        output_volume[slice_index],
        transpose=False,
    )

    header = anchor_image.getHead()
    header.data_type = output.data_type
    header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
    header.image_series_index = int(series_index)
    header.image_index = int(image_index if image_index is not None else slice_index + 1)
    header.slice = slice_index
    header.contrast = 0
    output_header = output.getHead()
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in output_header.matrix_size],
    )

    slice_axis = _infer_slice_axis(slice_anchor_images)
    _set_header_sequence_field(
        header,
        "position",
        _output_slice_position(
            slice_anchor_images,
            slice_axis,
            slice_index,
            slice_count,
        ),
    )
    _set_header_sequence_field(
        header,
        "slice_dir",
        [float(value) for value in slice_axis],
    )
    output.setHead(header)
    output.image_series_index = int(series_index)

    # DICOM applies RescaleSlope/RescaleIntercept before WindowCenter/WindowWidth,
    # so window values must use physical units rather than normalized pixels.
    center, width = _window_center_width(physical_volume)
    if window_center is not None:
        center = window_center
    if window_width is not None:
        width = window_width
    logging.info(
        "ARFI output scaling: series=%d type=%s frame=%s slice=%d units=%s "
        "physical=[%s,%s] display=[%d,%d] slope=%s intercept=%s "
        "window=[%.6g,%.6g]",
        series_index,
        image_type_token,
        "-" if frame_index is None else str(frame_index + 1),
        slice_index + 1,
        units,
        _format_display_number(display_meta["input_min"]),
        _format_display_number(display_meta["input_max"]),
        display_meta["display_min"],
        display_meta["display_max"],
        _format_display_number(display_meta["rescale_slope"]),
        _format_display_number(display_meta["rescale_intercept"]),
        center,
        width,
    )
    output.attribute_string = _output_meta(
        anchor_image,
        header,
        series_index,
        series_name,
        image_type_token,
        map_role,
        units,
        slice_count,
        slice_index,
        int(image_index if image_index is not None else slice_index + 1),
        int(images_in_series if images_in_series is not None else slice_count),
        center,
        width,
        frame_index,
        arfi_scheme,
        arfi_block_length,
        display_meta,
    ).serialize()
    return output


def _output_meta(
    source_image,
    header,
    series_index,
    series_name,
    image_type_token,
    map_role,
    units,
    slice_count,
    slice_index,
    image_index,
    images_in_series,
    window_center,
    window_width,
    frame_index=None,
    arfi_scheme="singlefreq",
    arfi_block_length=25,
    display_meta=None,
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    _strip_scanner_write_unsafe_meta(meta)
    if "IceMiniHead" in meta:
        del meta["IceMiniHead"]

    series_uid = _derived_series_uid(source_image, series_index, series_name)
    sop_uid = _derived_instance_uid(
        source_image,
        series_uid,
        series_index,
        series_name,
        image_index,
    )
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "ARFIRECON"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    image_comment = series_name
    if display_meta is not None:
        scale_text = _format_display_number(display_meta["scale"])
        image_comment = f"{series_name}; {display_meta['comment']}"
        meta["ARFIDisplayScale"] = scale_text
        meta["ARFIDisplayFormula"] = display_meta["formula"]
        meta["ARFIDisplayInputMin"] = _format_display_number(
            display_meta["input_min"]
        )
        meta["ARFIDisplayInputMax"] = _format_display_number(
            display_meta["input_max"]
        )
        meta["ARFIDisplayMin"] = str(display_meta["display_min"])
        meta["ARFIDisplayMax"] = str(display_meta["display_max"])
        # The DICOM writer maps these MRD image attributes to
        # (0028,1053) Rescale Slope and (0028,1052) Rescale Intercept.
        meta["RescaleSlope"] = _format_display_number(
            display_meta["rescale_slope"]
        )
        meta["RescaleIntercept"] = _format_display_number(
            display_meta["rescale_intercept"]
        )
        if "conversion" in display_meta:
            meta["ARFIDisplayConversion"] = display_meta["conversion"]
    meta["ImageComments"] = image_comment
    meta["ImageComment"] = image_comment
    meta["SeriesNumberRangeNameUID"] = _derived_series_grouping(
        series_name,
        series_index,
    )
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SequenceDescriptionAdditional"] = "openrecon"
    # The output pixels retain the source row/column ordering. Ask ICE to keep
    # the matching source geometry so coronal images are not reversed in the
    # Head-Feet direction when the derived series is written to DICOM.
    meta["Keep_image_geometry"] = "1"
    meta["partition_count"] = "1"
    meta["slice_count"] = str(int(slice_count))
    meta["NumberOfSlices"] = str(int(slice_count))
    meta["ImagesInAcquisition"] = str(int(images_in_series))
    meta["NumberInSeries"] = str(int(image_index))
    meta["SliceNo"] = str(int(slice_index))
    meta["AnatomicalSliceNo"] = str(int(slice_index))
    meta["ChronSliceNo"] = str(int(slice_index))
    meta["ProtocolSliceNumber"] = str(int(slice_index))
    meta["Actual3DImagePartNumber"] = "0"
    meta["AnatomicalPartitionNo"] = "0"
    meta["ARFIOutput"] = map_role
    meta["ARFIUnits"] = units
    if frame_index is not None:
        meta["ARFIFrameIndex"] = str(int(frame_index + 1))
    meta["ARFIScheme"] = str(arfi_scheme)
    meta["ARFIBlockLength"] = str(int(arfi_block_length))
    meta["WindowCenter"] = f"{float(window_center):.6g}"
    meta["WindowWidth"] = f"{float(window_width):.6g}"
    meta.update(_header_geometry_meta(header))
    _strip_scanner_write_unsafe_meta(meta)
    return meta


def _header_geometry_meta(header):
    return {
        "ImageRowDir": [f"{float(value):.18f}" for value in header.read_dir],
        "ImageColumnDir": [f"{float(value):.18f}" for value in header.phase_dir],
        "ImageSliceNormDir": [f"{float(value):.18f}" for value in header.slice_dir],
        "SlicePosLightMarker": [f"{float(value):.18f}" for value in header.position],
    }


def _allocate_output_series_indices(input_images):
    used = {int(image.getHead().image_series_index) for image in input_images}

    def reserve(preferred):
        series_index = int(preferred)
        while series_index in used:
            series_index += 1
        used.add(series_index)
        return series_index

    return {
        "magnitude": reserve(MAGNITUDE_SERIES_INDEX),
        "phase": reserve(PHASE_SERIES_INDEX),
        "arfi_phase": reserve(ARFI_PHASE_SERIES_INDEX),
        "with_fus_phase_std": reserve(WITH_FUS_PHASE_STD_SERIES_INDEX),
        "no_fus_phase_std": reserve(NO_FUS_PHASE_STD_SERIES_INDEX),
    }


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_identity_text(series_name)}_{int(series_index)}"


def _derived_series_uid(source_image, series_index, series_name):
    seed = "|".join(
        [
            RECIPE_NAME,
            _source_series_uid(source_image)
            or _source_series_name(source_image)
            or "source",
            str(int(series_index)),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_instance_uid(
    source_image,
    series_uid,
    series_index,
    series_name,
    image_index,
):
    seed = "|".join(
        [
            f"{RECIPE_NAME}-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
            str(int(image_index)),
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
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _minihead_string_value(minihead, key)
        if value:
            return value
    return ""


def _source_series_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SeriesInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SeriesInstanceUID",
    )


def _source_sop_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SOPInstanceUID") or _minihead_string_value(
        _decode_ice_minihead(_meta_text(meta, "IceMiniHead")),
        "SOPInstanceUID",
    )


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(
            key.startswith(prefix)
            for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES
        ):
            del meta[key]


def _strip_scanner_write_unsafe_meta(meta):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta:
            del meta[key]


def _validate_output_images(output_images, input_images):
    errors = []
    input_series_indices = {
        int(image.getHead().image_series_index)
        for image in input_images
    }
    input_series_uids = {
        _source_series_uid(image)
        for image in input_images
        if _source_series_uid(image)
    }
    seen_series_uids = {}
    seen_sop_uids = {}

    for index, image in enumerate(output_images):
        header = image.getHead()
        series_index = int(header.image_series_index)
        meta = _meta_from_image(image)
        series_uid = _meta_text(meta, "SeriesInstanceUID")
        sop_uid = _meta_text(meta, "SOPInstanceUID")
        keep_geometry = _meta_int(meta, "Keep_image_geometry")
        image_type_token = _meta_text(meta, "ImageTypeValue4")

        if series_index in input_series_indices:
            errors.append(
                f"image {index} reuses input image_series_index {series_index}"
            )
        if keep_geometry != 1:
            errors.append(
                f"image {index} has Keep_image_geometry={keep_geometry}, expected 1"
            )
        if _meta_text(meta, "IceMiniHead"):
            errors.append(f"image {index} keeps source IceMiniHead on derived output")
        if _meta_text(meta, "ImageTypeValue3"):
            errors.append(f"image {index} keeps unsafe ImageTypeValue3")
        if not series_uid:
            errors.append(f"image {index} is missing SeriesInstanceUID")
        if series_uid in input_series_uids:
            errors.append(f"image {index} reuses input SeriesInstanceUID {series_uid}")
        if not sop_uid:
            errors.append(f"image {index} is missing SOPInstanceUID")
        if series_uid:
            previous = seen_series_uids.setdefault(series_uid, series_index)
            if previous != series_index:
                errors.append(
                    f"SeriesInstanceUID {series_uid} is shared by series "
                    f"{previous} and {series_index}"
                )
        if sop_uid:
            previous = seen_sop_uids.setdefault(sop_uid, index)
            if previous != index:
                errors.append(
                    f"image {index} duplicates SOPInstanceUID from image {previous}"
                )
        if int(header.image_index) < 1:
            errors.append(
                f"image {index} has image_index {header.image_index}, expected >= 1"
            )
        slice_count = _meta_int(meta, "NumberOfSlices")
        slice_index = int(header.slice)
        if slice_count < 1 or not 0 <= slice_index < slice_count:
            errors.append(
                f"image {index} has slice {slice_index} outside "
                f"NumberOfSlices={slice_count}"
            )
        if _meta_int(meta, "SliceNo") != slice_index:
            errors.append(
                f"image {index} SliceNo does not match header slice {slice_index}"
            )
        data = np.asarray(image.data)
        if data.dtype != np.uint16:
            errors.append(f"image {index} {image_type_token} data is not uint16")
        if data.ndim != 4 or data.shape[1] != 1:
            errors.append(
                f"image {index} {image_type_token} is not a single-slice 2D image"
            )
        data_min = int(np.min(data)) if data.size else SCANNER_DISPLAY_MIN
        data_max = int(np.max(data)) if data.size else SCANNER_DISPLAY_MIN
        if data_min < SCANNER_DISPLAY_MIN or data_max > SCANNER_DISPLAY_MAX:
            errors.append(f"image {index} {image_type_token} data is outside 0..4095")
        formula = _meta_text(meta, "ARFIDisplayFormula")
        if not formula or formula not in _meta_text(meta, "ImageComments"):
            errors.append(
                f"image {index} {image_type_token} ImageComments is missing "
                "its inverse formula"
            )
        display_min = _meta_int(meta, "ARFIDisplayMin")
        display_max = _meta_int(meta, "ARFIDisplayMax")
        if data_min < display_min:
            errors.append(
                f"image {index} {image_type_token} is below its display minimum"
            )
        if data_max > display_max:
            errors.append(
                f"image {index} {image_type_token} exceeds its display maximum"
            )
        for key in (
            "ARFIDisplayInputMin",
            "ARFIDisplayInputMax",
            "RescaleSlope",
            "RescaleIntercept",
        ):
            if not _meta_text(meta, key):
                errors.append(
                    f"image {index} {image_type_token} output is missing {key}"
                )
        if image_type_token.startswith("ARFIPHASE"):
            comments = _meta_text(meta, "ImageComments")
            if "radians to degrees" not in comments:
                errors.append(
                    f"image {index} phase ImageComments is missing its conversion"
                )

    if errors:
        raise ValueError(
            "Invalid arfirecon output series contract before send: "
            + "; ".join(errors)
        )


def _send_images_by_series(connection, images):
    batches = []
    batch_by_series = {}
    for image in images:
        series_index = int(image.getHead().image_series_index)
        if series_index not in batch_by_series:
            batch_by_series[series_index] = []
            batches.append(batch_by_series[series_index])
        batch_by_series[series_index].append(image)
    for batch in batches:
        connection.send_image(batch)


def _settings_from_config(config):
    return {
        "arfischemes": _config_choice(
            config,
            "arfischemes",
            choices={"singlefreq", "multiplefreq"},
            default="singlefreq",
        ),
        "arfiblocklength": _config_int(
            config,
            "arfiblocklength",
            default=25,
            minimum=0,
            maximum=100,
        ),
        "sendphasestandarddeviation": _config_bool(
            config,
            "sendphasestandarddeviation",
            default=False,
        ),
        "phasewrap": _config_float(config, "phasewrap", default=PHASE_WRAP),
    }


def _config_parameters(config):
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    if not isinstance(config, dict):
        return {}
    parameters = config.get("parameters", config)
    return parameters if isinstance(parameters, dict) else {}


def _config_float(config, key, default=0.0):
    value = _config_parameters(config).get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _config_int(config, key, default=0, minimum=None, maximum=None):
    value = _config_parameters(config).get(key, default)
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = int(default)
    if minimum is not None and value < minimum:
        return int(default)
    if maximum is not None and value > maximum:
        return int(default)
    return value


def _config_bool(config, key, default=False):
    value = _config_parameters(config).get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off", ""}:
            return False
    return bool(default)


def _config_choice(config, key, choices, default):
    value = str(_config_parameters(config).get(key, default)).strip().lower()
    return value if value in choices else default


def _setting_float(settings, key, default=0.0):
    try:
        return float(settings.get(key, default))
    except (TypeError, ValueError):
        return float(default)


def _infer_slice_axis(images):
    if images:
        axis = _normalize_vector(images[0].getHead().slice_dir)
        if axis is not None:
            return axis
    if len(images) > 1:
        positions = [
            np.asarray(image.getHead().position, dtype=float)
            for image in images
        ]
        delta = positions[-1] - positions[0]
        axis = _normalize_vector(delta)
        if axis is not None:
            return axis
    return np.asarray((0.0, 0.0, 1.0), dtype=float)


def _normalize_vector(values):
    vector = np.asarray(values, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        return None
    return vector / norm


def _output_slice_position(images, slice_axis, slice_index, slice_count):
    if len(images) > slice_index:
        return [
            float(value)
            for value in images[slice_index].getHead().position
        ]

    first_image = images[0]
    position = np.asarray(first_image.getHead().position, dtype=float)
    spacing = _source_slice_spacing(first_image, slice_count)
    return [
        float(value)
        for value in position + np.asarray(slice_axis, dtype=float) * spacing * slice_index
    ]


def _source_slice_spacing(image, slice_count):
    meta = _meta_from_image(image)
    for key in ("SpacingBetweenSlices", "SliceThickness"):
        try:
            spacing = abs(float(_meta_text(meta, key)))
        except (TypeError, ValueError):
            spacing = 0.0
        if spacing > 0.0:
            return spacing

    for tag in ("00180088", "00180050"):
        spacing = _dicom_json_numeric_value(meta, tag)
        if spacing is not None and abs(float(spacing)) > 0.0:
            return abs(float(spacing))

    fov_z = abs(float(image.getHead().field_of_view[2]))
    if slice_count > 1 and fov_z > 0.0:
        return fov_z / float(slice_count)
    return fov_z


def _output_fov_z(slice_anchor_images, slice_axis, output_slice_count):
    source_fov_z = float(slice_anchor_images[0].getHead().field_of_view[2])
    if len(slice_anchor_images) <= 1:
        return source_fov_z

    projections = [
        float(np.dot(np.asarray(image.getHead().position, dtype=float), slice_axis))
        for image in slice_anchor_images
    ]
    projections = sorted(projections)
    spacings = np.diff(projections)
    spacings = spacings[np.abs(spacings) > 1e-6]
    if spacings.size:
        return float(np.median(np.abs(spacings)) * output_slice_count)
    return float(source_fov_z * output_slice_count)


def _window_center_width(data):
    values = np.asarray(data, dtype=np.float32)
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 1.0
    data_min = float(np.min(finite))
    data_max = float(np.max(finite))
    width = data_max - data_min
    if width <= 0:
        width = 1.0
    return data_min + width / 2.0, width


def _set_header_sequence_field(header, field_name, values):
    target = getattr(header, field_name)
    for index, value in enumerate(values):
        target[index] = value


def _meta_from_image(image):
    if not getattr(image, "attribute_string", ""):
        return ismrmrd.Meta()
    return ismrmrd.Meta.deserialize(image.attribute_string)


def _meta_text(meta, key):
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    text = str(value or "").strip()
    return "" if text.upper() == "N/A" else text


def _meta_int(meta, key):
    value = _meta_text(meta, key)
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _dicom_json_numeric_value(meta, tag):
    text = _meta_text(meta, "DicomJson")
    if not text:
        return None
    try:
        dicom_json = json.loads(base64.b64decode(text).decode("utf-8"))
    except Exception:
        return None
    values = dicom_json.get(tag, {}).get("Value", [])
    if not values:
        return None
    try:
        return float(values[0])
    except (TypeError, ValueError):
        return None


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


def _sanitize_identity_text(value):
    return (
        str(value or "")
        .strip()
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )
