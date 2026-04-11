import ismrmrd
import os
import itertools
import logging
import traceback
import numpy as np
import numpy.fft as fft
import xml.dom.minidom
import base64
import ctypes
import re
import mrdhelper
import constants
from time import perf_counter
import nibabel as nib
import subprocess


# Folder for debug output files
debugFolder = "/tmp/share/debug"
mmSegmentInputPath = "/opt/input.nii.gz"
mmSegmentOutputPath = "/opt/input_dseg.nii.gz"


def process(connection, config, metadata):
    logging.info("Config: \n%s", config)

    # Metadata should be MRD formatted header, but may be a string
    # if it failed conversion earlier
    try:
        # Disabled due to incompatibility between PyXB and Python 3.8:
        # https://github.com/pabigot/pyxb/issues/123
        # # logging.info("Metadata: \n%s", metadata.toxml('utf-8'))

        logging.info("Incoming dataset contains %d encodings", len(metadata.encoding))
        logging.info(
            "First encoding is of type '%s', with a matrix size of (%s x %s x %s) and a field of view of (%s x %s x %s)mm^3",
            metadata.encoding[0].trajectory,
            metadata.encoding[0].encodedSpace.matrixSize.x,
            metadata.encoding[0].encodedSpace.matrixSize.y,
            metadata.encoding[0].encodedSpace.matrixSize.z,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.x,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.y,
            metadata.encoding[0].encodedSpace.fieldOfView_mm.z,
        )

    except:
        logging.info("Improperly formatted metadata: \n%s", metadata)

    # Continuously parse incoming data parsed from MRD messages
    acqGroup = []
    imgGroup = []
    currentImageVolumeKey = None
    waveformGroup = []
    try:
        for item in connection:
            # ----------------------------------------------------------
            # Raw k-space data messages
            # ----------------------------------------------------------
            if isinstance(item, ismrmrd.Acquisition):
                # Accumulate all imaging readouts in a group
                if (
                    not item.is_flag_set(ismrmrd.ACQ_IS_NOISE_MEASUREMENT)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_PHASECORR_DATA)
                    and not item.is_flag_set(ismrmrd.ACQ_IS_NAVIGATION_DATA)
                ):
                    acqGroup.append(item)

                # When this criteria is met, run process_raw() on the accumulated
                # data, which returns images that are sent back to the client.
                if item.is_flag_set(ismrmrd.ACQ_LAST_IN_SLICE):
                    logging.info("Processing a group of k-space data")
                    # image = process_raw(acqGroup, connection, config, metadata)
                    connection.send_image(image)
                    acqGroup = []

            # ----------------------------------------------------------
            # Image data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Image):
                itemVolumeKey = _build_image_volume_key(item)
                if imgGroup and currentImageVolumeKey is not None and itemVolumeKey != currentImageVolumeKey:
                    logging.info(
                        "Processing a group of images because volume key changed from %s to %s",
                        currentImageVolumeKey,
                        itemVolumeKey,
                    )
                    image = process_image(imgGroup, connection, config, metadata)
                    connection.send_image(image)
                    imgGroup = []
                    currentImageVolumeKey = None

                # Only process magnitude Water images -- send phase images and non-Water images back without modification
                tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                dicomImageType = _extract_dicom_image_type_values(tmpMeta)
                imageTypeValue3 = _get_dicom_image_type_value(tmpMeta, 2)
                imageTypeValue4 = _get_dicom_image_type_value(tmpMeta, 3)
                
                # Debug: Print various metadata fields to identify where Water information is stored
                logging.info("=== Image Metadata Debug ===")
                logging.info(f"SequenceDescription: {tmpMeta.get('SequenceDescription', 'N/A')}")
                logging.info(f"SeriesDescription: {tmpMeta.get('SeriesDescription', 'N/A')}")
                logging.info(f"ImageComments: {tmpMeta.get('ImageComments', 'N/A')}")
                logging.info(f"ImageType: {tmpMeta.get('ImageType', 'N/A')}")
                logging.info(f"DicomImageType: {tmpMeta.get('DicomImageType', 'N/A')}")
                logging.info(f"ImageTypeValue3: {tmpMeta.get('ImageTypeValue3', 'N/A')}")
                logging.info(f"ImageTypeValue4: {tmpMeta.get('ImageTypeValue4', 'N/A')}")
                logging.info(f"SequenceDescriptionAdditional: {tmpMeta.get('SequenceDescriptionAdditional', 'N/A')}")
                logging.info(f"All metadata keys: {list(tmpMeta.keys())}")
                logging.info("===========================")
                
                # Check metadata for Water/Fat and DIXON information
                isDixonScan = (imageTypeValue3 == "DIXON") or (imageTypeValue4 in {"WATER", "FAT", "IN_PHASE", "OUT_PHASE"})
                isWaterImage = imageTypeValue4 == "WATER"
                
                # Check ISMRMRD image type for magnitude vs phase/complex (data representation).
                # image_type=0 is not standard, but some generators leave it unset.
                isMagnitude = item.image_type == ismrmrd.IMTYPE_MAGNITUDE
                if item.image_type == 0:
                    logging.warning(
                        "Received non-standard MRD image_type=0 for volume key %s; treating it as magnitude",
                        itemVolumeKey,
                    )
                    isMagnitude = True
                
                # Process magnitude images, but for DIXON scans only process WATER images
                shouldProcess = isMagnitude and (not isDixonScan or isWaterImage)
                logging.info(
                    "Resolved image typing: volume_key=%s, dicom_type=%s, value3=%s, value4=%s, image_type=%s, should_process=%s",
                    itemVolumeKey,
                    dicomImageType,
                    imageTypeValue3 or "N/A",
                    imageTypeValue4 or "N/A",
                    item.image_type,
                    shouldProcess,
                )
                
                if shouldProcess:
                    currentImageVolumeKey = itemVolumeKey
                    imgGroup.append(item)
                else:
                    tmpMeta["Keep_image_geometry"] = 1
                    item.attribute_string = tmpMeta.serialize()

                    connection.send_image(item)
                    continue

            # ----------------------------------------------------------
            # Waveform data messages
            # ----------------------------------------------------------
            elif isinstance(item, ismrmrd.Waveform):
                waveformGroup.append(item)

            elif item is None:
                break

            else:
                logging.error("Unsupported data type %s", type(item).__name__)

        if len(imgGroup) > 0:
            logging.info("Processing a group of images (untriggered)")
            image = process_image(imgGroup, connection, config, metadata)
            connection.send_image(image)
            imgGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        connection.send_close()

# from https://github.com/benoitberanger/openrecon-template/blob/main/app/i2i-save-original-images.py
def compute_nifti_affine(image_header, voxel_size):
    # MRD stores geometry in DICOM/LPS (x=Left, y=Posterior, z=Superior).
    # NIfTI uses RAS (x=Right, y=Anterior, z=Superior).
    # Convert by negating x and y components.
    lps_to_ras = np.array([-1, -1, 1], dtype=float)

    position  = np.array(image_header.position)  * lps_to_ras
    read_dir  = np.array(image_header.read_dir)   * lps_to_ras
    phase_dir = np.array(image_header.phase_dir)  * lps_to_ras
    slice_dir = np.array(image_header.slice_dir)  * lps_to_ras

    # Construct rotation-scaling matrix
    rotation_scaling_matrix = np.column_stack([
        voxel_size[0] * read_dir,
        voxel_size[1] * phase_dir,
        voxel_size[2] * slice_dir,
    ])

    # Construct affine matrix
    affine = np.eye(4)
    affine[:3, :3] = rotation_scaling_matrix
    affine[:3,  3] = position

    return affine


def _get_first_sequence_param(metadata, name):
    try:
        seq = metadata.sequenceParameters
        values = getattr(seq, name)
        if values is None:
            return None
        if isinstance(values, (list, tuple)):
            if len(values) == 0:
                return None
            return float(values[0])
        return float(values)
    except Exception:
        return None


def _format_acq_time_from_stamp(stamp):
    try:
        total_seconds = float(stamp) * 2.5e-3
    except Exception:
        return None

    if total_seconds < 0:
        return None

    hours = int(total_seconds // 3600) % 24
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds - (hours * 3600 + minutes * 60)
    return f"{hours:02d}{minutes:02d}{seconds:06.3f}"


def _get_first_meta_float(meta_obj, keys):
    for key in keys:
        try:
            value = meta_obj.get(key)
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                if len(value) == 0:
                    continue
                return float(value[0])
            return float(value)
        except Exception:
            continue
    return None


def _meta_text_values(value):
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        raw_values = value
    else:
        raw_values = str(value).split("\\")

    values = []
    for raw_value in raw_values:
        text = str(raw_value).strip()
        if text:
            values.append(text.upper())
    return values


def _extract_dicom_image_type_values(meta_obj):
    dicom_values = _meta_text_values(meta_obj.get("DicomImageType"))
    if dicom_values:
        return dicom_values

    image_type_values = _meta_text_values(meta_obj.get("ImageType"))
    if not image_type_values:
        return []

    # If ImageType already contains 3+ backslash-separated components
    # (e.g. "DERIVED\PRIMARY\DIXON\OPP_PHASE"), it is a complete DICOM
    # image type string and should be used directly without padding.
    if len(image_type_values) >= 3:
        return image_type_values

    value3 = _meta_text_values(meta_obj.get("ImageTypeValue3"))
    prefix = ["", "", value3[0] if value3 else ""]
    return prefix + image_type_values


def _get_dicom_image_type_value(meta_obj, index):
    values = _extract_dicom_image_type_values(meta_obj)
    if len(values) > index:
        return values[index]
    return ""


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


def _strip_dixon_series_suffix(series_name):
    series_name = _first_non_empty_text(series_name)
    if not series_name:
        return ""

    # Siemens DIXON child reconstructions can carry suffixes like _W_1 or
    # _F_1. Keep derived images in the parent DIXON series by removing only
    # that final image-type token before the trailing series number.
    return re.sub(
        r"_(?:W|WATER|F|FAT|IN|IN_PHASE|OPP|OPP_PHASE|OUT|OUT_PHASE)(_\d+)$",
        r"\1",
        series_name,
        flags=re.IGNORECASE,
    )


def _format_dixon_image_label(image_type_value):
    label_map = {
        "WATER": "Water",
        "FAT": "Fat",
        "IN_PHASE": "In_Phase",
        "OUT_PHASE": "Opposed_Phase",
        "OPP_PHASE": "Opposed_Phase",
        "OPPOSED_PHASE": "Opposed_Phase",
    }
    image_type_value = _first_non_empty_text(image_type_value).upper()
    if image_type_value in label_map:
        return label_map[image_type_value]
    if image_type_value:
        return image_type_value.title().replace(" ", "_")
    return ""


def _build_segmentation_image_label(source_image_type_value):
    source_label = _format_dixon_image_label(source_image_type_value)
    if source_label:
        return f"Musclemap_Segmentation_{source_label}"
    return "Musclemap_Segmentation"


def _decode_ice_minihead(meta_obj):
    try:
        encoded = meta_obj.get("IceMiniHead")
        if encoded is None:
            return ""
        if isinstance(encoded, (list, tuple)):
            encoded = encoded[0] if encoded else None
        if not encoded:
            return ""
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _extract_minihead_string_value(minihead_text, name):
    if not minihead_text:
        return ""

    try:
        return _first_non_empty_text(mrdhelper.extract_minihead_string_param(minihead_text, name))
    except Exception:
        pass

    match = re.search(
        rf'<ParamString\."{re.escape(name)}">\s*\{{\s*"([^"]*)"\s*\}}',
        minihead_text,
    )
    if match:
        return match.group(1).strip()
    return ""


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

    return [token.strip() for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_match.group(0))]


def _replace_minihead_string_param(minihead_text, name, value):
    if not minihead_text or not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    match = pattern.search(minihead_text)
    if not match:
        return minihead_text, False

    replacement = f"{match.group(1)}{value}{match.group(3)}"
    return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True


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
    tokens = [token.strip() for token in re.findall(r'\{\s*"([^"]+)"\s*\}', block_text)]
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
        reserved_tokens = {"NORM", "DIS2D", "DIS3D"}
        for token in tokens:
            if token.upper() not in reserved_tokens:
                replacement_source = token
                break

    if not replacement_source:
        return minihead_text, False

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
        minihead_text[:block_match.start()] + replaced_block + minihead_text[block_match.end():],
        True,
    )


def _patch_ice_minihead(
    minihead_text,
    parent_sequence,
    parent_grouping,
    source_type_token,
    target_type_token,
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text

    for param_name, param_value in (
        ("SequenceDescription", parent_sequence),
        ("SeriesNumberRangeNameUID", parent_grouping),
        ("ImageType", f"DERIVED\\PRIMARY\\M\\{target_type_token}"),
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_minihead_string_param(current_text, param_name, param_value)
        changed = changed or did_change

    current_text, did_change = _replace_minihead_array_token(
        current_text,
        "ImageTypeValue4",
        source_type_token,
        target_type_token,
    )
    changed = changed or did_change

    return current_text, changed


def _build_image_volume_key(image):
    return (
        int(getattr(image, "image_series_index", 0)),
        int(getattr(image, "average", 0)),
        int(getattr(image, "contrast", 0)),
        int(getattr(image, "phase", 0)),
        int(getattr(image, "repetition", 0)),
        int(getattr(image, "set", 0)),
    )


def _header_vector(image_header, field_name):
    try:
        return np.asarray(getattr(image_header, field_name), dtype=float)
    except Exception:
        return np.zeros(3, dtype=float)


def _normalize_vector(vector):
    vector = np.asarray(vector, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm < 1e-8:
        return None
    return vector / norm


def _format_vector(vector):
    return "[" + ", ".join(f"{float(value):.3f}" for value in vector) + "]"


def _infer_slice_axis(image_headers):
    for image_header in image_headers:
        axis = _normalize_vector(_header_vector(image_header, "slice_dir"))
        if axis is not None:
            return axis

    if len(image_headers) > 1:
        axis = _normalize_vector(
            _header_vector(image_headers[-1], "position")
            - _header_vector(image_headers[0], "position")
        )
        if axis is not None:
            return axis

    return np.array([0.0, 0.0, 1.0], dtype=float)


def _build_slice_geometry_records(image_headers, input_indices=None, slice_axis=None):
    if input_indices is None:
        input_indices = list(range(len(image_headers)))
    if slice_axis is None:
        slice_axis = _infer_slice_axis(image_headers)

    records = []
    for local_index, image_header in enumerate(image_headers):
        position = _header_vector(image_header, "position")
        slice_dir = _normalize_vector(_header_vector(image_header, "slice_dir"))
        slice_dir_dot_axis = float(np.dot(slice_dir, slice_axis)) if slice_dir is not None else 0.0
        records.append(
            {
                "local_index": local_index,
                "input_index": int(input_indices[local_index]),
                "image_index": int(getattr(image_header, "image_index", 0)),
                "slice": int(getattr(image_header, "slice", 0)),
                "phase": int(getattr(image_header, "phase", 0)),
                "position": position,
                "projected_position": float(np.dot(position, slice_axis)),
                "slice_dir_dot_axis": slice_dir_dot_axis,
            }
        )

    return slice_axis, records


def _estimate_slice_spacing(image_headers, slice_axis=None):
    if len(image_headers) < 2:
        return None

    slice_axis, records = _build_slice_geometry_records(image_headers, slice_axis=slice_axis)
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    sorted_diffs = np.diff(np.sort(projected_positions))
    nonzero_diffs = np.abs(sorted_diffs[np.abs(sorted_diffs) > 1e-4])
    if nonzero_diffs.size == 0:
        return None
    return float(np.median(nonzero_diffs))


def _slice_sort_indices(image_headers):
    slice_axis, records = _build_slice_geometry_records(image_headers)
    sorted_records = sorted(
        records,
        key=lambda record: (
            round(record["projected_position"], 4),
            record["slice"],
            record["image_index"],
            record["input_index"],
        ),
    )
    return [record["input_index"] for record in sorted_records], slice_axis, records


def _log_slice_geometry(label, image_headers, input_indices=None, slice_axis=None, max_records=12):
    slice_axis, records = _build_slice_geometry_records(
        image_headers,
        input_indices=input_indices,
        slice_axis=slice_axis,
    )
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    slice_dir_alignment = np.array(
        [record["slice_dir_dot_axis"] for record in records],
        dtype=float,
    )
    min_slice_dir_alignment = float(np.min(slice_dir_alignment)) if slice_dir_alignment.size else 1.0

    if len(projected_positions) > 1:
        current_diffs = np.diff(projected_positions)
        image_indices = np.array([record["image_index"] for record in records], dtype=int)
        slice_indices = np.array([record["slice"] for record in records], dtype=int)
        sorted_diffs = np.diff(np.sort(projected_positions))
        nonzero_sorted_diffs = np.abs(sorted_diffs[np.abs(sorted_diffs) > 1e-4])
        median_spacing = float(np.median(nonzero_sorted_diffs)) if nonzero_sorted_diffs.size else 0.0
        max_spacing = float(np.max(nonzero_sorted_diffs)) if nonzero_sorted_diffs.size else 0.0
        duplicate_positions = int(np.sum(np.abs(sorted_diffs) <= 1e-4))
        large_gap_count = int(
            np.sum(nonzero_sorted_diffs > (1.5 * median_spacing))
        ) if median_spacing > 0 else 0
        monotonic_increasing = bool(np.all(current_diffs >= -1e-4))
        monotonic_decreasing = bool(np.all(current_diffs <= 1e-4))
        image_index_increasing = bool(np.all(np.diff(image_indices) >= 0))
        slice_index_increasing = bool(np.all(np.diff(slice_indices) >= 0))
    else:
        median_spacing = 0.0
        max_spacing = 0.0
        duplicate_positions = 0
        large_gap_count = 0
        monotonic_increasing = True
        monotonic_decreasing = True
        image_index_increasing = True
        slice_index_increasing = True

    logging.info(
        "%s slice geometry: count=%d axis=%s projected_range=[%.3f, %.3f] "
        "median_spacing=%.3f max_spacing=%.3f duplicates=%d large_gaps=%d "
        "order_inc=%s order_dec=%s image_index_inc=%s slice_index_inc=%s min_slice_dir_dot_axis=%.6f",
        label,
        len(records),
        _format_vector(slice_axis),
        float(np.min(projected_positions)) if projected_positions.size else 0.0,
        float(np.max(projected_positions)) if projected_positions.size else 0.0,
        median_spacing,
        max_spacing,
        duplicate_positions,
        large_gap_count,
        monotonic_increasing,
        monotonic_decreasing,
        image_index_increasing,
        slice_index_increasing,
        min_slice_dir_alignment,
    )

    if not monotonic_increasing:
        logging.warning(
            "%s slice positions are not increasing along slice_dir in their current order",
            label,
        )
    if duplicate_positions > 0:
        logging.warning(
            "%s has %d duplicate projected slice position(s)",
            label,
            duplicate_positions,
        )
    if large_gap_count > 0:
        logging.warning(
            "%s has %d slice spacing gap(s) larger than 1.5x the median spacing",
            label,
            large_gap_count,
        )
    if min_slice_dir_alignment < 0.99:
        logging.warning(
            "%s has slice_dir vectors that are not aligned with the inferred slice axis",
            label,
        )

    if len(records) <= max_records:
        sample_records = records
        omitted_count = 0
    else:
        head_count = max_records // 2
        tail_count = max_records - head_count
        sample_records = records[:head_count] + records[-tail_count:]
        omitted_count = len(records) - len(sample_records)

    for record in sample_records:
        logging.info(
            "%s slice sample: local=%d input=%d image_index=%d slice=%d phase=%d "
            "proj=%.3f pos=%s slice_dir_dot_axis=%.6f",
            label,
            record["local_index"],
            record["input_index"],
            record["image_index"],
            record["slice"],
            record["phase"],
            record["projected_position"],
            _format_vector(record["position"]),
            record["slice_dir_dot_axis"],
        )
    if omitted_count > 0:
        logging.info("%s slice sample omitted %d middle slice(s)", label, omitted_count)

    return slice_axis, records


def _log_slice_sort_mapping(sort_indices, max_records=24):
    identity = list(range(len(sort_indices)))
    if sort_indices == identity:
        logging.info("Segmentation input slice order already matches physical slice order")
        return

    logging.warning(
        "Reordering segmentation input slices by physical position: first mappings %s",
        ", ".join(
            f"out{output_index}->in{input_index}"
            for output_index, input_index in enumerate(sort_indices[:max_records])
        ),
    )
    if len(sort_indices) > max_records:
        logging.warning(
            "Reordering mapping omitted %d additional slice(s)",
            len(sort_indices) - max_records,
        )


def _log_affine_slice_consistency(image_headers, voxel_size):
    if len(image_headers) < 2:
        return

    slice_axis, records = _build_slice_geometry_records(image_headers)
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    expected_positions = projected_positions[0] + np.arange(len(projected_positions)) * float(voxel_size[2])
    residuals = projected_positions - expected_positions
    max_abs_residual = float(np.max(np.abs(residuals))) if residuals.size else 0.0
    logging.info(
        "NIfTI affine slice consistency: z_spacing=%.6f residual_range=[%.6f, %.6f] max_abs_residual=%.6f",
        float(voxel_size[2]),
        float(np.min(residuals)),
        float(np.max(residuals)),
        max_abs_residual,
    )
    if max_abs_residual > max(0.25, 0.25 * float(voxel_size[2])):
        logging.warning(
            "Input slice positions are not well described by a single linear NIfTI z-spacing; "
            "segmentation output will still reuse original per-slice MRD positions"
        )


def _header_to_log_dict(image_header):
    return {
        "version": getattr(image_header, "version", None),
        "flags": getattr(image_header, "flags", None),
        "measurement_uid": getattr(image_header, "measurement_uid", None),
        "matrix_size": list(getattr(image_header, "matrix_size", [])),
        "field_of_view": list(getattr(image_header, "field_of_view", [])),
        "channels": getattr(image_header, "channels", None),
        "position": list(getattr(image_header, "position", [])),
        "read_dir": list(getattr(image_header, "read_dir", [])),
        "phase_dir": list(getattr(image_header, "phase_dir", [])),
        "slice_dir": list(getattr(image_header, "slice_dir", [])),
        "patient_table_position": list(getattr(image_header, "patient_table_position", [])),
        "average": getattr(image_header, "average", None),
        "slice": getattr(image_header, "slice", None),
        "contrast": getattr(image_header, "contrast", None),
        "phase": getattr(image_header, "phase", None),
        "repetition": getattr(image_header, "repetition", None),
        "set": getattr(image_header, "set", None),
        "acquisition_time_stamp": getattr(image_header, "acquisition_time_stamp", None),
        "physiology_time_stamp": list(getattr(image_header, "physiology_time_stamp", [])),
        "image_type": getattr(image_header, "image_type", None),
        "image_index": getattr(image_header, "image_index", None),
        "image_series_index": getattr(image_header, "image_series_index", None),
        "user_int": list(getattr(image_header, "user_int", [])),
        "user_float": list(getattr(image_header, "user_float", [])),
        "data_type": getattr(image_header, "data_type", None),
    }


def _parse_mm_segment_chunksize(value):
    if isinstance(value, str):
        text = value.strip().lower()
        if text == "auto":
            return "auto"
        try:
            return int(text)
        except ValueError:
            return value

    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _extract_mm_segment_chunk_size(output_text):
    if not output_text:
        return None

    patterns = (
        r"Using chunk size:\s*(\d+)",
        r"estimated=(\d+)\s*slice",
    )
    for pattern in patterns:
        match = re.search(pattern, output_text, flags=re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                return None
    return None


def _log_mm_segment_output(output_text, log_fn=logging.info):
    if not output_text:
        return

    for line in output_text.splitlines():
        line = line.strip()
        if line:
            log_fn("mm_segment | %s", line)


def _is_likely_mm_segment_oom(exc, output_text):
    if exc.returncode in (-9, 9, 137):
        return True

    message = f"{exc}\n{output_text or ''}".lower()
    oom_tokens = (
        "out of memory",
        "cuda error: out of memory",
        "cannot allocate memory",
        "can't allocate memory",
        "bad alloc",
        "sigkill",
    )
    return any(token in message for token in oom_tokens)


def _build_mm_segment_retry_chunks(chunksize, image_depth, output_text):
    if image_depth < 1:
        return []

    parsed_chunksize = _parse_mm_segment_chunksize(chunksize)
    attempted_chunk = _extract_mm_segment_chunk_size(output_text)
    if attempted_chunk is None and isinstance(parsed_chunksize, int):
        attempted_chunk = parsed_chunksize
    if attempted_chunk is None:
        attempted_chunk = min(int(image_depth), 32)

    attempted_chunk = max(1, min(int(image_depth), int(attempted_chunk)))
    candidates = []
    next_chunk = attempted_chunk
    while next_chunk > 1:
        next_chunk = max(1, next_chunk // 2)
        if next_chunk not in candidates:
            candidates.append(next_chunk)
        if next_chunk == 1:
            break
    return candidates


def _run_mm_segment(bodyregion, chunksize, spatialoverlap, image_depth):
    attempt_chunks = [chunksize]
    seen_chunks = set()

    while attempt_chunks:
        current_chunksize = attempt_chunks.pop(0)
        chunk_label = str(current_chunksize)
        if chunk_label in seen_chunks:
            continue
        seen_chunks.add(chunk_label)

        mm_segment_cmd = [
            "mm_segment",
            "-i", mmSegmentInputPath,
            "-r", bodyregion,
            "-c", chunk_label,
            "-s", str(spatialoverlap),
            "-g", "Y",
        ]

        if os.path.exists(mmSegmentOutputPath):
            os.remove(mmSegmentOutputPath)

        logging.info("Running command: %s", " ".join(mm_segment_cmd))

        try:
            mm_segment_result = subprocess.run(
                mm_segment_cmd,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            combined_output = "\n".join(
                part for part in (exc.stdout, exc.stderr) if part
            )
            _log_mm_segment_output(combined_output, logging.warning)

            if not _is_likely_mm_segment_oom(exc, combined_output):
                raise

            retry_chunks = [
                candidate
                for candidate in _build_mm_segment_retry_chunks(
                    current_chunksize,
                    image_depth,
                    combined_output,
                )
                if str(candidate) not in seen_chunks
            ]
            if not retry_chunks:
                raise

            logging.warning(
                "mm_segment exited with return code %s, likely due to memory pressure. "
                "Retrying with smaller chunk size(s): %s",
                exc.returncode,
                ", ".join(str(candidate) for candidate in retry_chunks),
            )
            attempt_chunks = retry_chunks + attempt_chunks
            continue

        combined_output = "\n".join(
            part for part in (mm_segment_result.stdout, mm_segment_result.stderr) if part
        )
        _log_mm_segment_output(combined_output)

        if not os.path.exists(mmSegmentOutputPath):
            raise FileNotFoundError(
                f"mm_segment finished without creating the expected output: {mmSegmentOutputPath}"
            )
        return mmSegmentOutputPath

    raise RuntimeError("mm_segment did not complete successfully")


def process_image(imgGroup, connection, config, metadata):
    if len(imgGroup) == 0:
        return []

    only_save_original = mrdhelper.get_json_config_param(config, 'onlysaveoriginal', default=False, type='bool')
    if isinstance(only_save_original, str):
        only_save_original = only_save_original.strip().lower() in ("1", "true", "yes", "on")
    else:
        only_save_original = bool(only_save_original)
    logging.info("onlysaveoriginal resolved to %s", only_save_original)

    if only_save_original:
        logging.info("Skipping segmentation and forwarding only original images due to onlysaveoriginal=True")
        imagesOut = []
        for idx, image in enumerate(imgGroup):
            tmpImg = image
            tmpMeta = ismrmrd.Meta.deserialize(tmpImg.attribute_string)
            tmpMeta["Keep_image_geometry"] = 1
            tmpImg.attribute_string = tmpMeta.serialize()
            logging.info(
                "Original-only header [%d/%d]: %s",
                idx + 1,
                len(imgGroup),
                _header_to_log_dict(tmpImg.getHead()),
            )
            imagesOut.append(tmpImg)
        return imagesOut

    # Determine the source image type (e.g. "Water", "Fat", "In_Phase", …)
    # from the first image's metadata so the segmentation output is named accordingly.
    _first_meta = ismrmrd.Meta.deserialize(imgGroup[0].attribute_string)
    _source_type_value = _get_dicom_image_type_value(_first_meta, 3)  # value4
    source_image_label = _build_segmentation_image_label(_source_type_value)
    source_image_type_value4 = source_image_label
    source_dicom_image_type_value4 = source_image_label.upper()
    source_volume_key = _build_image_volume_key(imgGroup[0])
    raw_source_series_description = _get_meta_text(_first_meta, "SeriesDescription")
    source_series_description = _strip_dixon_series_suffix(raw_source_series_description)
    source_minihead = _decode_ice_minihead(_first_meta)
    raw_source_parent_sequence = _first_non_empty_text(
        _get_meta_text(_first_meta, "SequenceDescription"),
        _extract_minihead_string_value(source_minihead, "SequenceDescription"),
    )
    source_parent_sequence = _strip_dixon_series_suffix(raw_source_parent_sequence)
    raw_source_parent_grouping = _first_non_empty_text(
        _extract_minihead_string_value(source_minihead, "SeriesNumberRangeNameUID"),
        source_parent_sequence,
    )
    source_parent_grouping = _strip_dixon_series_suffix(raw_source_parent_grouping)
    if not source_parent_sequence:
        source_parent_sequence = source_series_description
    if not source_parent_grouping:
        source_parent_grouping = source_parent_sequence
    logging.info("Source image type for segmentation naming: %s -> %s", _source_type_value, source_image_label)
    logging.info(
        "Source segmentation parent identity: volume_key=%s series_description=%s -> %s sequence=%s -> %s grouping=%s -> %s",
        source_volume_key,
        raw_source_series_description or "N/A",
        source_series_description or "N/A",
        raw_source_parent_sequence or "N/A",
        source_parent_sequence or "N/A",
        raw_source_parent_grouping or "N/A",
        source_parent_grouping or "N/A",
    )

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")


    # Note: The MRD Image class stores data as [cha z y x]

    unsorted_head = [img.getHead() for img in imgGroup]
    slice_sort_indices, slice_axis, _ = _slice_sort_indices(unsorted_head)
    _log_slice_geometry(
        "Incoming segmentation source",
        unsorted_head,
        slice_axis=slice_axis,
    )
    _log_slice_sort_mapping(slice_sort_indices)

    ordered_img_group = [imgGroup[index] for index in slice_sort_indices]

    # Extract image data into a 5D array of size [img cha z y x]
    data = np.stack([img.data for img in ordered_img_group])
    head = [unsorted_head[index] for index in slice_sort_indices]
    meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in ordered_img_group]

    _log_slice_geometry(
        "Sorted segmentation source",
        head,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
    )

    matrix = np.array(head[0].matrix_size[:])

    #adjust the matrix size to the full 3D volume, it should be as many slices as in length(imgGroup)
    if matrix[2] != len(ordered_img_group):
        matrix[2] = len(ordered_img_group)    

    fov = np.array(head[0].field_of_view[:])

    #we also need to adjust fov z to be slice thickness * number of slices
    slice_thickness = fov[2]
    measured_slice_spacing = _estimate_slice_spacing(head, slice_axis=slice_axis)
    if measured_slice_spacing is not None:
        if abs(float(measured_slice_spacing) - float(slice_thickness)) > 0.05:
            logging.warning(
                "MRD slice thickness %.6f differs from measured slice spacing %.6f; "
                "using measured spacing for NIfTI affine",
                float(slice_thickness),
                float(measured_slice_spacing),
            )
        fov[2] = measured_slice_spacing * len(ordered_img_group)
    else:
        fov[2] = slice_thickness * len(ordered_img_group)

    voxelsize = fov/matrix
    _log_affine_slice_consistency(head, voxelsize)

    print("matrix:")
    print(matrix)
    print("fov:")
    print(fov)
    print("voxelsize:") 
    print(voxelsize)

    crop_size = data.shape

    print("shape before transpose:")
    print(data.shape)

    # Reformat data to [y x img cha z], i.e. [row col slice ...]
    data = data.transpose((3, 4, 0, 1, 2))

    print("shape after initial transpose:")
    print(data.shape)

    # convert data to nifti using nibabel
    affine = compute_nifti_affine(head[0], voxelsize)
    print("affine matrix:")
    print(affine)

    data = np.squeeze(data)
    print("shape before saving nifti and running mm_segment:")
    print(data.shape)

    # Transpose from [y, x, z] to NIfTI [x, y, z].
    if data.ndim == 2:
        data_nifti = np.asarray(data.T[:, :, None])
    else:
        data_nifti = np.asarray(data.transpose((1, 0, 2)))

    # Store segmentation input as int16 for mm_segment compatibility.
    i16 = np.iinfo(np.int16)
    data_min = np.min(data_nifti)
    data_max = np.max(data_nifti)
    if data_min < i16.min or data_max > i16.max:
        raise ValueError(f"Input image values [{data_min}, {data_max}] exceed int16 range")
    data_nifti = data_nifti.astype(np.int16, copy=False)
    new_img = nib.nifti1.Nifti1Image(data_nifti, affine)

    # Set scanner-like header fields explicitly (nib defaults are not suitable here).
    header = new_img.header
    header.set_data_dtype(np.int16)
    header.set_dim_info(freq=1, phase=0, slice=2)
    header.set_xyzt_units(xyz='mm', t='sec')

    te = _get_first_sequence_param(metadata, "TE")
    if te is None:
        te = _get_first_meta_float(meta[0], ["EchoTime", "TE"])
    tr = _get_first_sequence_param(metadata, "TR")
    if te is not None and te < 1.0:
        te = te * 1000.0  # seconds -> ms
    if tr is None:
        tr = 0.00368
    elif tr > 1.0:
        tr = tr / 1000.0  # ms -> sec
    header["pixdim"][4] = float(tr)
    header["pixdim"][5] = 0.0
    header["pixdim"][6] = 0.0
    header["pixdim"][7] = 0.0

    acq_time_str = _format_acq_time_from_stamp(head[0].acquisition_time_stamp)
    phase_num = int(getattr(head[0], "phase", 0)) + 1
    desc_parts = []
    if te is not None:
        desc_parts.append(f"TE={te:g}")
    if acq_time_str:
        desc_parts.append(f"Time={acq_time_str}")
    desc_parts.append(f"phase={phase_num}")
    header["descrip"] = ";".join(desc_parts)
    header["aux_file"] = "Not for diagnostic use"
    header.set_slope_inter(1.0, 0.0)

    new_img.set_qform(affine, code=1)
    new_img.set_sform(affine, code=1)

    nib.save(new_img, mmSegmentInputPath)

    # Extract UI parameters from JSON config
    bodyregion = mrdhelper.get_json_config_param(config, 'bodyregion', default='wholebody', type='str')
    chunksize = mrdhelper.get_json_config_param(config, 'chunksize', default='auto', type='str')
    spatialoverlap = mrdhelper.get_json_config_param(config, 'spatialoverlap', default=50, type='int')
    
    logging.info(f"mm_segment parameters: bodyregion={bodyregion}, chunksize={chunksize}, spatialoverlap={spatialoverlap}")
    
    DEBUG=False
    if DEBUG:
        logging.info("DEBUG mode: Skipping actual mm_segment execution and creating dummy output.")
        subprocess.run(f"cp /buildhostdirectory/input_dseg.nii.gz {mmSegmentOutputPath}", shell=True, check=True)
    else:
        _run_mm_segment(
            bodyregion=bodyregion,
            chunksize=chunksize,
            spatialoverlap=spatialoverlap,
            image_depth=int(data_nifti.shape[2]),
        )

    img = nib.load(mmSegmentOutputPath)

    # Keep on-disk label values to avoid float conversion/rescaling artifacts.
    data = np.asarray(img.dataobj)

    unique_preview = np.unique(data)
    logging.info(
        "Loaded segmented labels: dtype=%s, range=[%s, %s], unique_count=%d",
        data.dtype,
        np.min(data),
        np.max(data),
        unique_preview.size,
    )
    logging.info("Segmented labels preview (first 50): %s", unique_preview[:50])

    if np.issubdtype(data.dtype, np.floating):
        rounded = np.rint(data)
        if not np.allclose(data, rounded, atol=1e-3):
            raise ValueError("Segmented labels are not integer-valued; refusing to cast.")
        data = rounded

    data = data.astype(np.int64, copy=False)

    # Expected label coding ends in 0/1/2 (except background 0).
    bad_labels = np.unique(data[(data != 0) & ((data % 10) > 2)])
    if bad_labels.size > 0:
        raise ValueError(f"Unexpected labels detected (first 20): {bad_labels[:20].tolist()}")

    print("maximum value in segmented data:")
    print(np.max(data))

    # Reformat data
    print("shape after loading with nibabel")
    print(data.shape)

    if data.ndim == 2:
        data = data[:, :, None]

    # Bring [x, y, z] back to [y, x, z].
    if data.ndim >= 3:
        data = data.transpose((1, 0, 2))

    data = data[..., None, None]
    data = data.transpose((0, 1, 4, 3, 2))

    print("shape after applying transpose")
    print(data.shape)

    # compare size of data to crop_size, if not identical do a center crop
    print("crop_size:")
    print(crop_size)

    print("data shape before crop:")
    print(data.shape)
    
    # crop_size is [img, cha, z, y, x]
    # data is [y, x, 1, 1, img]
    if data.shape[0] != crop_size[3] or data.shape[1] != crop_size[4]:
        crop_y = int((data.shape[0] - crop_size[3]) / 2)
        crop_x = int((data.shape[1] - crop_size[4]) / 2)
        data = data[crop_y : crop_y + crop_size[3], crop_x : crop_x + crop_size[4], ...]

    print("data shape after crop:")
    print(data.shape)

    label_transform = mrdhelper.get_json_config_param(config, 'labeltransform', default=True, type='bool')
    if isinstance(label_transform, str):
        label_transform = label_transform.strip().lower() in ("1", "true", "yes", "on")
    else:
        label_transform = bool(label_transform)
    logging.info("labeltransform resolved to %s", label_transform)

    if label_transform:
        logging.info("Applying label transformation: 3 * (label_in // 10) + (label_in % 10)")
        data = 3 * (data // 10) + (data % 10)
        logging.info(f"Label transformation complete. New data range: [{data.min()}, {data.max()}]")


    print("maximum value in segmented data before sending out:")
    maxVal =  np.max(data)      
    print(maxVal)

    currentSeries = 0

    # Re-slice back into 2D images
    imagesOut = [None] * data.shape[-1]

    print("data.shape before creating output images:")
    print(data.shape)

    print("checking data type of data:")
    print(data.dtype)

    # check if data type is int16_t and if not convert it
    if data.dtype != np.int16:
        i16 = np.iinfo(np.int16)
        data_min = int(np.min(data))
        data_max = int(np.max(data))
        if data_min < i16.min or data_max > i16.max:
            raise ValueError(f"Segmented labels [{data_min}, {data_max}] exceed int16 range")
        logging.info(f"Converting segmented data from {data.dtype} to int16")
        data = data.astype(np.int16, copy=False)

    print("checking data type of final data:")
    print(data.dtype)

    print("header length - should be as many as images:")
    print(len(head))

    if data.shape[-1] != len(head):
        raise ValueError(
            f"Segmented slice count ({data.shape[-1]}) does not match source header count ({len(head)})"
        )

    _, output_slice_records = _log_slice_geometry(
        "Segmentation output header order",
        head,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
    )

    for iImg in range(data.shape[-1]):
        # Create new MRD instance for the segmented image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
        imagesOut[iImg] = ismrmrd.Image.from_array(data[..., iImg].transpose((3, 2, 0, 1)), transpose=False)

        # Create a copy of the original fixed header and update the data_type
        # (we changed it to int16 from all other types)
        oldHeader = head[iImg]
        source_record = output_slice_records[iImg]
        source_image_index = int(getattr(oldHeader, "image_index", 0))
        source_slice_index = int(getattr(oldHeader, "slice", 0))
        oldHeader.data_type = imagesOut[iImg].data_type
        oldHeader.image_index = iImg + 1
        oldHeader.slice = iImg

        print(f"Image {iImg}: data_type = {imagesOut[iImg].data_type}")

        # Supported ISMRMRD Data Types:
        #     ISMRMRD_USHORT   = 1, /**< corresponds to uint16_t */
        #     ISMRMRD_SHORT    = 2, /**< corresponds to int16_t */
        #     ISMRMRD_FLOAT    = 5, /**< corresponds to float */
        #     ISMRMRD_CXFLOAT  = 7, /**< corresponds to complex float */

        # NOT SUPPORTED:
        # ISMRMRD_UINT     = 3, /**< corresponds to uint32_t */
        # ISMRMRD_INT      = 4, /**< corresponds to int32_t */
        # ISMRMRD_DOUBLE   = 6, /**< corresponds to double */
        # ISMRMRD_CXDOUBLE = 8  /**< corresponds to complex double */

        # check if datatype is supported and if not show an error and stop:
        if imagesOut[iImg].data_type not in [ismrmrd.DATATYPE_USHORT, ismrmrd.DATATYPE_SHORT, ismrmrd.DATATYPE_FLOAT, ismrmrd.DATATYPE_CXFLOAT]:
            logging.error(f"Unsupported data type {imagesOut[iImg].data_type} in output image {iImg}. Supported types are: uint16, int16, float32, complex float32.")
            raise ValueError(f"Unsupported data type {imagesOut[iImg].data_type} in output image {iImg}. Supported types are: uint16, int16, float32, complex float32.")

        # Set the image_type to match the data_type for complex data
        if (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXFLOAT) or (imagesOut[iImg].data_type == ismrmrd.DATATYPE_CXDOUBLE):
            oldHeader.image_type = ismrmrd.IMTYPE_COMPLEX
        else:
            oldHeader.image_type = ismrmrd.IMTYPE_MAGNITUDE

        # Increment series number when flag detected (i.e. follow ICE logic for splitting series)
        if mrdhelper.get_meta_value(meta[iImg], "IceMiniHead") is not None:
            if (
                mrdhelper.extract_minihead_bool_param(
                    base64.b64decode(meta[iImg]["IceMiniHead"]).decode("utf-8"), "BIsSeriesEnd"
                )
                is True
            ):
                currentSeries += 1

        imagesOut[iImg].setHead(oldHeader)

        # Create a copy of the original ISMRMRD Meta attributes and update
        tmpMeta = meta[iImg]

        tmpMeta["DataRole"] = "Image"
        tmpMeta["ImageProcessingHistory"] = ["OPENRECON", "MUSCLEMAP"]
        tmpMeta["WindowCenter"] = str((maxVal + 1) / 2)
        tmpMeta["WindowWidth"] = str((maxVal + 1))
        tmpMeta["ImageType"] = source_image_type_value4
        tmpMeta["ImageTypeValue3"] = "M"
        tmpMeta["ImageTypeValue4"] = source_image_type_value4
        tmpMeta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{source_dicom_image_type_value4}"
        tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
        tmpMeta["ImageComments"] = source_image_label
        tmpMeta["MuscleMapSourceInputIndex"] = str(source_record["input_index"])
        tmpMeta["MuscleMapSourceImageIndex"] = str(source_image_index)
        tmpMeta["MuscleMapSourceSlice"] = str(source_slice_index)
        tmpMeta["MuscleMapSourceProjectedPosition"] = f"{source_record['projected_position']:.6f}"
        if source_series_description:
            tmpMeta["SeriesDescription"] = source_series_description
        if source_parent_sequence:
            tmpMeta["SequenceDescription"] = source_parent_sequence
        if "SequenceDescriptionAdditional" in tmpMeta:
            try:
                del tmpMeta["SequenceDescriptionAdditional"]
            except Exception:
                tmpMeta["SequenceDescriptionAdditional"] = ""

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                source_parent_sequence,
                source_parent_grouping,
                _source_type_value,
                source_image_type_value4,
            )
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)
            else:
                logging.warning(
                    "IceMiniHead was present but not updated for segmentation output slice %d",
                    iImg,
                )
        tmpMeta["Keep_image_geometry"] = 1

        # Add image orientation directions to MetaAttributes if not already present
        if tmpMeta.get("ImageRowDir") is None:
            tmpMeta["ImageRowDir"] = [
                "{:.18f}".format(oldHeader.read_dir[0]),
                "{:.18f}".format(oldHeader.read_dir[1]),
                "{:.18f}".format(oldHeader.read_dir[2]),
            ]

        if tmpMeta.get("ImageColumnDir") is None:
            tmpMeta["ImageColumnDir"] = [
                "{:.18f}".format(oldHeader.phase_dir[0]),
                "{:.18f}".format(oldHeader.phase_dir[1]),
                "{:.18f}".format(oldHeader.phase_dir[2]),
            ]

        metaXml = tmpMeta.serialize()
        # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        # logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

        if (
            data.shape[-1] <= 12
            or iImg < 6
            or iImg >= data.shape[-1] - 6
        ):
            logging.info(
                "Segmentation output mapping: output=%d source_input=%d "
                "image_index=%d->%d slice=%d->%d proj=%.3f pos=%s nonzero_voxels=%d",
                iImg,
                source_record["input_index"],
                source_image_index,
                oldHeader.image_index,
                source_slice_index,
                oldHeader.slice,
                source_record["projected_position"],
                _format_vector(source_record["position"]),
                int(np.count_nonzero(data[..., iImg])),
            )

        if iImg == 0:
            final_meta = ismrmrd.Meta.deserialize(metaXml)
            final_header = imagesOut[iImg].getHead()
            final_minihead = _decode_ice_minihead(final_meta)
            logging.info(
                "Final segmentation identity: measurement_uid=%s source_volume_key=%s header=%s meta=%s minihead=%s",
                getattr(final_header, "measurement_uid", None),
                source_volume_key,
                {
                    "image_series_index": getattr(final_header, "image_series_index", None),
                    "image_index": getattr(final_header, "image_index", None),
                    "slice": getattr(final_header, "slice", None),
                    "contrast": getattr(final_header, "contrast", None),
                    "image_type": getattr(final_header, "image_type", None),
                    "position": list(getattr(final_header, "position", [])),
                    "slice_dir": list(getattr(final_header, "slice_dir", [])),
                },
                {
                    "SeriesDescription": final_meta.get("SeriesDescription", "N/A"),
                    "SequenceDescription": final_meta.get("SequenceDescription", "N/A"),
                    "SequenceDescriptionAdditional": final_meta.get("SequenceDescriptionAdditional", "N/A"),
                    "ImageType": final_meta.get("ImageType", "N/A"),
                    "ImageTypeValue3": final_meta.get("ImageTypeValue3", "N/A"),
                    "ImageTypeValue4": final_meta.get("ImageTypeValue4", "N/A"),
                    "DicomImageType": final_meta.get("DicomImageType", "N/A"),
                    "ComplexImageComponent": final_meta.get("ComplexImageComponent", "N/A"),
                    "ImageComments": final_meta.get("ImageComments", "N/A"),
                    "MuscleMapSourceInputIndex": final_meta.get("MuscleMapSourceInputIndex", "N/A"),
                    "MuscleMapSourceImageIndex": final_meta.get("MuscleMapSourceImageIndex", "N/A"),
                    "MuscleMapSourceSlice": final_meta.get("MuscleMapSourceSlice", "N/A"),
                    "MuscleMapSourceProjectedPosition": final_meta.get("MuscleMapSourceProjectedPosition", "N/A"),
                    "IceMiniHeadPresent": "IceMiniHead" in final_meta,
                },
                {
                    "SequenceDescription": _extract_minihead_string_value(final_minihead, "SequenceDescription") or "N/A",
                    "SeriesNumberRangeNameUID": _extract_minihead_string_value(final_minihead, "SeriesNumberRangeNameUID") or "N/A",
                    "ImageType": _extract_minihead_string_value(final_minihead, "ImageType") or "N/A",
                    "ImageTypeValue3": _extract_minihead_string_value(final_minihead, "ImageTypeValue3") or "N/A",
                    "ComplexImageComponent": _extract_minihead_string_value(final_minihead, "ComplexImageComponent") or "N/A",
                    "ImageTypeValue4Tokens": _extract_minihead_array_tokens(final_minihead, "ImageTypeValue4"),
                },
            )
            logging.info("=== Final metadata dump for first segmentation output image ===")
            for key in final_meta.keys():
                logging.info("  META [%s] = %s", key, final_meta[key])
            logging.info("=== End final metadata dump ===")


     # Send a copy of original (unmodified) images back too if selected
    opre_sendoriginal = mrdhelper.get_json_config_param(config, 'sendoriginal', default=True, type='bool')
    if opre_sendoriginal:
        stack = traceback.extract_stack()
        if stack[-2].name == 'process_raw':
            logging.warning('sendOriginal is true, but input was raw data, so no original images to return!')
        else:
            logging.info('Sending a copy of original unmodified images due to sendOriginal set to True')
            # In reverse order so that they'll be in correct order as we insert them to the front of the list
            for i, image in enumerate(reversed(imgGroup)):
                # Create a copy to not modify the original inputs
                tmpImg = image

                # Change the series_index to have a different series
                old_series_index = tmpImg.image_series_index
                tmpImg.image_series_index = 99

                # Ensure Keep_image_geometry is set to not reverse image orientation
                tmpMeta = ismrmrd.Meta.deserialize(tmpImg.attribute_string)
                tmpMeta['Keep_image_geometry'] = 1
                tmpImg.attribute_string = tmpMeta.serialize()

                logging.info(
                    "Send-original header [%d/%d] (series %s -> %s): %s",
                    i + 1,
                    len(imgGroup),
                    old_series_index,
                    tmpImg.image_series_index,
                    _header_to_log_dict(tmpImg.getHead()),
                )

                imagesOut.insert(0, tmpImg)

    return imagesOut
