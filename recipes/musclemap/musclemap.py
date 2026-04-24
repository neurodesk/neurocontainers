import ismrmrd
import csv
import copy
import glob
import os
import itertools
import json
import logging
import shutil
import traceback
import numpy as np
import numpy.fft as fft
import xml.dom.minidom
import base64
import ctypes
import re
import uuid
import mrdhelper
import constants
from time import perf_counter
import nibabel as nib
import subprocess


# Folder for debug output files
debugFolder = "/tmp/share/debug"
mmSegmentInputPath = "/opt/input.nii.gz"
mmSegmentOutputPath = "/opt/input_dseg.nii.gz"
mmMetricsSegmentationPath = "/tmp/musclemap_input_dseg_metrics.nii.gz"
mmMetricsOutputDir = "/tmp/musclemap_metrics"
mmMetricsMethod = "average"
muscleMapDisplayLabel = "Musclemap"
muscleMapImageTypeToken = "MUSCLEMAP"


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
    inputVolumeSummaries = {}
    sentImagesForSummary = []
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
                    _send_images_with_summary(connection, image, "raw_group", sentImagesForSummary, log_now=False)
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
                    _send_images_with_summary(
                        connection,
                        image,
                        "processed_volume_key_change",
                        sentImagesForSummary,
                        log_now=False,
                    )
                    imgGroup = []
                    currentImageVolumeKey = None

                # Only process magnitude Water images -- send phase images and non-Water images back without modification
                tmpMeta = ismrmrd.Meta.deserialize(item.attribute_string)
                dicomImageType = _extract_dicom_image_type_values(tmpMeta)
                imageTypeValue3 = _get_dicom_image_type_value(tmpMeta, 2)
                imageTypeValue4 = _get_dicom_image_type_value(tmpMeta, 3)
                
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
                _record_input_volume_summary(
                    inputVolumeSummaries,
                    item,
                    tmpMeta,
                    dicomImageType,
                    imageTypeValue3,
                    imageTypeValue4,
                    shouldProcess,
                )
                
                if shouldProcess:
                    currentImageVolumeKey = itemVolumeKey
                    imgGroup.append(item)
                else:
                    tmpMeta["Keep_image_geometry"] = 1
                    item.attribute_string = tmpMeta.serialize()

                    _send_images_with_summary(
                        connection,
                        item,
                        "passthrough_not_processed",
                        sentImagesForSummary,
                        log_now=False,
                    )
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
            _send_images_with_summary(
                connection,
                image,
                "processed_untriggered",
                sentImagesForSummary,
                log_now=False,
            )
            imgGroup = []

    except Exception as e:
        logging.error(traceback.format_exc())
        connection.send_logging(constants.MRD_LOGGING_ERROR, traceback.format_exc())

    finally:
        _log_input_volume_summaries(inputVolumeSummaries)
        _log_send_batch_summary(sentImagesForSummary, "connection_total")
        connection.send_close()

# from https://github.com/benoitberanger/openrecon-template/blob/main/app/i2i-save-original-images.py
def compute_nifti_affine(image_header, voxel_size, slice_axis=None):
    # MRD stores geometry in DICOM/LPS (x=Left, y=Posterior, z=Superior).
    # NIfTI uses RAS (x=Right, y=Anterior, z=Superior).
    # Convert by negating x and y components.
    lps_to_ras = np.array([-1, -1, 1], dtype=float)

    position  = np.array(image_header.position)  * lps_to_ras
    read_dir  = np.array(image_header.read_dir)   * lps_to_ras
    phase_dir = np.array(image_header.phase_dir)  * lps_to_ras
    raw_slice_dir = np.array(image_header.slice_dir, dtype=float)
    if np.linalg.norm(raw_slice_dir) < 1e-8 and slice_axis is not None:
        raw_slice_dir = np.asarray(slice_axis, dtype=float)
    slice_dir = raw_slice_dir * lps_to_ras

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


def _get_first_meta_int(meta_obj, keys):
    for key in keys:
        try:
            value = meta_obj.get(key)
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                if len(value) == 0:
                    continue
                return int(value[0])
            return int(value)
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


def _as_config_bool(value):
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _strip_dixon_series_suffix(series_name):
    series_name = _first_non_empty_text(series_name)
    if not series_name:
        return ""

    # Siemens DIXON child reconstructions can carry suffixes like _W_1 or
    # _F_1, and scanner product names may also end directly in _W, _opp, etc.
    # Keep derived images in the parent DIXON group by removing only that final
    # image-type token while preserving a trailing series number when present.
    return re.sub(
        r"_(?:W|WATER|F|FAT|IN|IN_PHASE|OPP|OPP_PHASE|OUT|OUT_PHASE)(_\d+)?$",
        lambda match: match.group(1) or "",
        series_name,
        flags=re.IGNORECASE,
    )


def _resolve_source_series_identity(meta_obj, minihead_text):
    raw_series_description = _get_meta_text(meta_obj, "SeriesDescription")
    series_description = _strip_dixon_series_suffix(raw_series_description)

    raw_parent_sequence = _first_non_empty_text(
        _get_meta_text(meta_obj, "SequenceDescription"),
        _extract_minihead_string_value(minihead_text, "SequenceDescription"),
    )
    parent_sequence = _strip_dixon_series_suffix(raw_parent_sequence)

    raw_parent_grouping = _first_non_empty_text(
        _get_meta_text(meta_obj, "SeriesNumberRangeNameUID"),
        _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID"),
        parent_sequence,
    )
    parent_grouping = _strip_dixon_series_suffix(raw_parent_grouping)

    if not parent_sequence:
        parent_sequence = series_description
    if not parent_grouping:
        parent_grouping = parent_sequence

    return {
        "raw_series_description": raw_series_description,
        "series_description": series_description,
        "raw_parent_sequence": raw_parent_sequence,
        "parent_sequence": parent_sequence,
        "raw_parent_grouping": raw_parent_grouping,
        "parent_grouping": parent_grouping,
        "series_instance_uid": _first_non_empty_text(
            _get_meta_text(meta_obj, "SeriesInstanceUID"),
            _extract_minihead_string_value(minihead_text, "SeriesInstanceUID"),
        ),
    }


def _build_derived_series_instance_uid(
    source_series_instance_uid,
    derived_kind,
    derived_series_index,
    series_grouping,
    series_description,
):
    stable_source_uid = _first_non_empty_text(source_series_instance_uid)
    if stable_source_uid:
        seed_text = json.dumps(
            {
                "source_series_instance_uid": stable_source_uid,
                "derived_kind": _first_non_empty_text(derived_kind),
                "derived_series_index": int(derived_series_index) if derived_series_index is not None else None,
                "series_grouping": _first_non_empty_text(series_grouping),
                "series_description": _first_non_empty_text(series_description),
            },
            sort_keys=True,
        )
        derived_uuid = uuid.uuid5(uuid.NAMESPACE_OID, seed_text)
    else:
        derived_uuid = uuid.uuid4()

    return f"2.25.{derived_uuid.int}"


def _build_derived_series_identity(
    source_identity,
    *,
    series_description,
    sequence_description=None,
    grouping_suffix=None,
    series_grouping=None,
    derived_series_index=None,
    derived_kind="derived",
):
    series_description = _first_non_empty_text(series_description)
    sequence_description = _first_non_empty_text(sequence_description, series_description)
    grouping_token = _first_non_empty_text(grouping_suffix, series_description, sequence_description)

    if not series_grouping:
        if source_identity.get("parent_grouping") and grouping_token:
            series_grouping = f"{source_identity['parent_grouping']}_{grouping_token}"
        else:
            series_grouping = grouping_token or source_identity.get("parent_grouping", "")

    return {
        "series_description": series_description,
        "sequence_description": sequence_description,
        "grouping": series_grouping,
        "series_instance_uid": _build_derived_series_instance_uid(
            source_series_instance_uid=source_identity.get("series_instance_uid"),
            derived_kind=derived_kind,
            derived_series_index=derived_series_index,
            series_grouping=series_grouping,
            series_description=series_description,
        ),
    }


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
    # NONE is the standard placeholder DICOM emits for non-tissue-typed scans;
    # treat it like an empty value so the segmentation series is not named
    # "Musclemap_Segmentation_None".
    if image_type_value in ("", "NONE"):
        return ""
    return image_type_value.title().replace(" ", "_")


def _build_segmentation_image_label(source_image_type_value):
    return muscleMapDisplayLabel


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


def _sanitize_minihead_param_value(value):
    text = _first_non_empty_text(value)
    if not text:
        return ""
    return (
        text.replace("\\", "/")
        .replace('"', "'")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_minihead_param_value(value)
    if not minihead_text or not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*")([^"]*)("\s*\}})'
    )
    match = pattern.search(minihead_text)
    if match:
        if match.group(2) == value:
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamString."{name}">\t{{ "{value}" }}\n'
    return minihead_text.rstrip() + appended_param, True


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


def _replace_or_append_minihead_array_token(minihead_text, name, source_token, target_token):
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
    appended_param = f'\n<ParamArray."{name}">\t{{\n\t{{ "{target_token}" }}\n}}\n'
    return current_text.rstrip() + appended_param, True


def _replace_or_append_minihead_long_param(minihead_text, name, value):
    if not minihead_text or value is None:
        return minihead_text, False

    value = int(value)
    pattern = re.compile(
        rf'(<ParamLong\."{re.escape(name)}">\s*\{{\s*)(-?\d*)?(\s*\}})'
    )
    match = pattern.search(minihead_text)
    if match:
        current_value = (match.group(2) or "").strip()
        if current_value == str(value):
            return minihead_text, False
        replacement = f"{match.group(1)}{value}{match.group(3)}"
        return minihead_text[:match.start()] + replacement + minihead_text[match.end():], True

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _patch_ice_minihead(
    minihead_text,
    parent_sequence,
    parent_grouping,
    series_instance_uid,
    source_type_token,
    target_type_token,
    metrics_text=None,
    target_display_token=None,
    target_image_type_value3="M",
):
    if not minihead_text:
        return minihead_text, False

    changed = False
    current_text = minihead_text
    target_display_token = target_display_token or target_type_token

    for param_name, param_value in (
        ("SequenceDescription", parent_sequence),
        ("SeriesNumberRangeNameUID", parent_grouping),
        ("SeriesInstanceUID", series_instance_uid),
        ("ImageType", f"DERIVED\\PRIMARY\\{target_image_type_value3}\\{target_type_token}"),
        ("ImageTypeValue3", "M"),
        ("ComplexImageComponent", "MAGNITUDE"),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(current_text, param_name, param_value)
        changed = changed or did_change

    current_text, did_change = _replace_or_append_minihead_array_token(
        current_text,
        "ImageTypeValue4",
        source_type_token,
        target_display_token,
    )
    changed = changed or did_change

    if metrics_text:
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            "MuscleMapMetrics",
            metrics_text,
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


def _set_header_sequence_field(image_header, field_name, values):
    values = list(values)
    current_value = getattr(image_header, field_name)
    try:
        current_value[:] = values
    except Exception:
        setattr(image_header, field_name, tuple(values))


def _copy_meta(meta_obj):
    try:
        return ismrmrd.Meta.deserialize(meta_obj.serialize())
    except Exception:
        return copy.deepcopy(meta_obj)


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


def _json_log_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _log_json_event(event_name, payload, level=logging.info):
    level("%s %s", event_name, json.dumps(payload, sort_keys=True, default=_json_log_default))


def _sample_indices(count, edge_count=6):
    if count <= 0:
        return []
    if count <= edge_count * 2:
        return list(range(count))
    return list(range(edge_count)) + list(range(count - edge_count, count))


def _meta_identity(meta_obj):
    return {
        "SequenceDescription": _get_meta_text(meta_obj, "SequenceDescription") or "N/A",
        "SeriesDescription": _get_meta_text(meta_obj, "SeriesDescription") or "N/A",
        "SeriesNumberRangeNameUID": _get_meta_text(meta_obj, "SeriesNumberRangeNameUID") or "N/A",
        "SeriesInstanceUID": _get_meta_text(meta_obj, "SeriesInstanceUID") or "N/A",
        "ImageType": _get_meta_text(meta_obj, "ImageType") or "N/A",
        "ImageTypeValue3": _get_meta_text(meta_obj, "ImageTypeValue3") or "N/A",
        "ImageTypeValue4": _get_meta_text(meta_obj, "ImageTypeValue4") or "N/A",
        "DicomImageType": _get_meta_text(meta_obj, "DicomImageType") or "N/A",
        "ComplexImageComponent": _get_meta_text(meta_obj, "ComplexImageComponent") or "N/A",
        "ImageComment": _get_meta_text(meta_obj, "ImageComment") or "N/A",
        "ImageComments": _get_meta_text(meta_obj, "ImageComments") or "N/A",
        "Keep_image_geometry": _get_meta_text(meta_obj, "Keep_image_geometry") or "N/A",
        "contrast_count": _get_meta_text(meta_obj, "contrast_count") or "N/A",
        "partition_count": _get_meta_text(meta_obj, "partition_count") or "N/A",
    }


def _minihead_identity(minihead_text):
    return {
        "SequenceDescription": _extract_minihead_string_value(minihead_text, "SequenceDescription") or "N/A",
        "SeriesNumberRangeNameUID": _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID") or "N/A",
        "SeriesInstanceUID": _extract_minihead_string_value(minihead_text, "SeriesInstanceUID") or "N/A",
        "ImageType": _extract_minihead_string_value(minihead_text, "ImageType") or "N/A",
        "ImageTypeValue3": _extract_minihead_string_value(minihead_text, "ImageTypeValue3") or "N/A",
        "ImageTypeValue4": _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4"),
        "ComplexImageComponent": _extract_minihead_string_value(minihead_text, "ComplexImageComponent") or "N/A",
    }


def _extract_minihead_long_value(minihead_text, name):
    if not minihead_text:
        return None

    match = re.search(
        rf'<ParamLong\."{re.escape(name)}">\s*\{{\s*(-?\d+)\s*\}}',
        minihead_text,
    )
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def _minihead_slice_identity(minihead_text):
    return {
        "Actual3DImagePartNumber": _extract_minihead_long_value(minihead_text, "Actual3DImagePartNumber"),
        "AnatomicalPartitionNo": _extract_minihead_long_value(minihead_text, "AnatomicalPartitionNo"),
        "ChronSliceNo": _extract_minihead_long_value(minihead_text, "ChronSliceNo"),
    }


def _image_identity(image, meta_obj=None):
    header = image.getHead()
    if meta_obj is None:
        meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
    minihead_text = _decode_ice_minihead(meta_obj)
    return {
        "header": {
            "measurement_uid": int(getattr(header, "measurement_uid", 0)),
            "image_series_index": int(getattr(header, "image_series_index", 0)),
            "image_index": int(getattr(header, "image_index", 0)),
            "slice": int(getattr(header, "slice", 0)),
            "average": int(getattr(header, "average", 0)),
            "contrast": int(getattr(header, "contrast", 0)),
            "phase": int(getattr(header, "phase", 0)),
            "repetition": int(getattr(header, "repetition", 0)),
            "set": int(getattr(header, "set", 0)),
            "position": list(getattr(header, "position", [])),
            "slice_dir": list(getattr(header, "slice_dir", [])),
            "image_type": int(getattr(header, "image_type", 0)),
        },
        "meta": _meta_identity(meta_obj),
        "minihead": _minihead_identity(minihead_text),
    }


def _flatten_identity(prefix, value):
    if isinstance(value, dict):
        flattened = {}
        for key, sub_value in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            flattened.update(_flatten_identity(child_prefix, sub_value))
        return flattened
    return {prefix: value}


def _identity_changed_fields(before, after):
    before_flat = _flatten_identity("", before)
    after_flat = _flatten_identity("", after)
    fields = sorted(set(before_flat) | set(after_flat))
    return [
        field
        for field in fields
        if json.dumps(before_flat.get(field), sort_keys=True, default=_json_log_default)
        != json.dumps(after_flat.get(field), sort_keys=True, default=_json_log_default)
    ]


def _new_input_volume_summary_entry(image, meta_obj, dicom_type, value3, value4, should_process):
    header = image.getHead()
    minihead_text = _decode_ice_minihead(meta_obj)
    return {
        "volume_key": list(_build_image_volume_key(image)),
        "count": 0,
        "should_process": bool(should_process),
        "dicom_type": dicom_type,
        "resolved_ImageTypeValue3": value3 or "N/A",
        "resolved_ImageTypeValue4": value4 or "N/A",
        "measurement_uid": int(getattr(header, "measurement_uid", 0)),
        "image_series_index": int(getattr(header, "image_series_index", 0)),
        "average": int(getattr(header, "average", 0)),
        "contrast": int(getattr(header, "contrast", 0)),
        "phase": int(getattr(header, "phase", 0)),
        "repetition": int(getattr(header, "repetition", 0)),
        "set": int(getattr(header, "set", 0)),
        "meta": _meta_identity(meta_obj),
        "minihead": _minihead_identity(minihead_text),
        "_slice_samples": [],
    }


def _record_input_volume_summary(summary_map, image, meta_obj, dicom_type, value3, value4, should_process):
    volume_key = _build_image_volume_key(image)
    entry = summary_map.get(volume_key)
    if entry is None:
        entry = _new_input_volume_summary_entry(
            image,
            meta_obj,
            dicom_type,
            value3,
            value4,
            should_process,
        )
        summary_map[volume_key] = entry

    entry["count"] += 1
    entry["_slice_samples"].append(_image_identity(image, meta_obj))


def _log_input_volume_summaries(summary_map):
    for entry in summary_map.values():
        payload = dict(entry)
        all_samples = payload.pop("_slice_samples", [])
        sample_indices = sorted(
            set(
                _sample_indices(len(all_samples), edge_count=3)
                + ([len(all_samples) // 2] if all_samples else [])
            )
        )
        payload["slice_samples"] = [
            {
                "sample_index": sample_index,
                "identity": all_samples[sample_index],
            }
            for sample_index in sample_indices
        ]
        _log_json_event("INPUT_VOLUME_SUMMARY", payload)


def _log_slice_order_checkpoint(
    checkpoint_name,
    image_headers,
    metas=None,
    input_indices=None,
    slice_axis=None,
    extra=None,
):
    slice_axis, records = _build_slice_geometry_records(
        image_headers,
        input_indices=input_indices,
        slice_axis=slice_axis,
    )
    projected_positions = np.array(
        [record["projected_position"] for record in records],
        dtype=float,
    )
    if projected_positions.size > 1:
        current_diffs = np.diff(projected_positions)
        sorted_diffs = np.diff(np.sort(projected_positions))
        nonzero_sorted_diffs = np.abs(sorted_diffs[np.abs(sorted_diffs) > 1e-4])
        median_spacing = float(np.median(nonzero_sorted_diffs)) if nonzero_sorted_diffs.size else 0.0
        max_spacing = float(np.max(nonzero_sorted_diffs)) if nonzero_sorted_diffs.size else 0.0
        duplicate_positions = int(np.sum(np.abs(sorted_diffs) <= 1e-4))
        large_gap_count = int(
            np.sum(nonzero_sorted_diffs > (1.5 * median_spacing))
        ) if median_spacing > 0 else 0
        monotonic_increasing = bool(np.all(current_diffs >= -1e-4))
    else:
        median_spacing = 0.0
        max_spacing = 0.0
        duplicate_positions = 0
        large_gap_count = 0
        monotonic_increasing = True

    sample_records = []
    sample_indices = _sample_indices(len(records), edge_count=6)
    for sample_index in sample_indices:
        record = records[sample_index]
        minihead_text = ""
        source_slice = record["slice"]
        output_slice = int(getattr(image_headers[sample_index], "slice", 0))
        if metas is not None and sample_index < len(metas):
            sample_meta = metas[sample_index]
            minihead_text = _decode_ice_minihead(sample_meta)
            meta_source_slice = _get_first_meta_int(sample_meta, ["MuscleMapSourceSlice"])
            if meta_source_slice is not None:
                source_slice = meta_source_slice
        sample_records.append(
            {
                "local_index": record["local_index"],
                "input_index": record["input_index"],
                "source_slice": source_slice,
                "output_slice": output_slice,
                "image_index": record["image_index"],
                "projected_position": record["projected_position"],
                "position": record["position"],
                "slice_dir": _header_vector(image_headers[sample_index], "slice_dir"),
                "minihead": _minihead_slice_identity(minihead_text),
            }
        )

    payload = {
        "checkpoint": checkpoint_name,
        "count": len(records),
        "axis": slice_axis,
        "projected_range": [
            float(np.min(projected_positions)) if projected_positions.size else 0.0,
            float(np.max(projected_positions)) if projected_positions.size else 0.0,
        ],
        "median_spacing": median_spacing,
        "max_spacing": max_spacing,
        "duplicates": duplicate_positions,
        "large_gaps": large_gap_count,
        "order_inc": monotonic_increasing,
        "samples": sample_records,
    }
    if extra:
        payload.update(extra)
    _log_json_event("SLICE_ORDER_CHECKPOINT", payload)


def _group_count_key(value):
    text = _first_non_empty_text(value)
    return text if text else "N/A"


def _increment_group(group_map, key):
    group_key = _group_count_key(key)
    group_map[group_key] = group_map.get(group_key, 0) + 1


def _log_send_batch_summary(images, context):
    if images is None:
        images = []
    elif not isinstance(images, (list, tuple)):
        images = [images]

    groups = {
        "image_series_index": {},
        "SequenceDescription": {},
        "SeriesNumberRangeNameUID": {},
        "ImageTypeValue4": {},
    }
    for image in images:
        header = image.getHead()
        meta_obj = ismrmrd.Meta.deserialize(image.attribute_string)
        minihead_text = _decode_ice_minihead(meta_obj)
        _increment_group(groups["image_series_index"], int(getattr(header, "image_series_index", 0)))
        _increment_group(
            groups["SequenceDescription"],
            _first_non_empty_text(
                _get_meta_text(meta_obj, "SequenceDescription"),
                _extract_minihead_string_value(minihead_text, "SequenceDescription"),
            ),
        )
        _increment_group(
            groups["SeriesNumberRangeNameUID"],
            _first_non_empty_text(
                _get_meta_text(meta_obj, "SeriesNumberRangeNameUID"),
                _extract_minihead_string_value(minihead_text, "SeriesNumberRangeNameUID"),
            ),
        )
        image_type_value4 = _first_non_empty_text(
            _get_meta_text(meta_obj, "ImageTypeValue4"),
            _get_dicom_image_type_value(meta_obj, 3),
            _extract_minihead_array_tokens(minihead_text, "ImageTypeValue4"),
        )
        _increment_group(groups["ImageTypeValue4"], image_type_value4)

    _log_json_event(
        "SEND_BATCH_SUMMARY",
        {
            "context": context,
            "total_images": len(images),
            "groups": groups,
        },
    )


def _as_image_list(images):
    if images is None:
        return []
    if isinstance(images, (list, tuple)):
        return list(images)
    return [images]


def _send_images_with_summary(connection, images, context, sent_images=None, log_now=True):
    image_list = _as_image_list(images)
    if sent_images is not None:
        sent_images.extend(image_list)
    if log_now:
        _log_send_batch_summary(image_list, context)
    connection.send_image(images)


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


def _log_mm_extract_metrics_output(output_text, log_fn=logging.info):
    if not output_text:
        return

    for line in output_text.splitlines():
        line = line.strip()
        if line:
            log_fn("mm_extract_metrics | %s", line)


def _read_metrics_csv(csv_path):
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = [field.strip() for field in (reader.fieldnames or []) if field]
        rows = []
        for row in reader:
            cleaned_row = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned_row[key.strip()] = "" if value is None else str(value).strip()
            if any(value for value in cleaned_row.values()):
                rows.append(cleaned_row)
    return fieldnames, rows


def _run_mm_extract_metrics(method, region, components, segmentation_path, input_image_path, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    metrics_cmd = [
        "python",
        "/opt/MuscleMap/scripts/mm_extract_metrics.py",
        "-m", method,
        "-s", segmentation_path,
        "-i", input_image_path,
        "-o", output_dir,
    ]
    if region:
        metrics_cmd.extend(["-r", region])
    if method in ("kmeans", "gmm") and components is not None:
        metrics_cmd.extend(["-c", str(components)])

    logging.info("Running command: %s", " ".join(metrics_cmd))
    metrics_result = subprocess.run(
        metrics_cmd,
        check=True,
        capture_output=True,
        text=True,
        cwd=output_dir,
    )
    combined_output = "\n".join(
        part for part in (metrics_result.stdout, metrics_result.stderr) if part
    )
    _log_mm_extract_metrics_output(combined_output)

    csv_candidates = sorted(glob.glob(os.path.join(output_dir, "*_results.csv")))
    if not csv_candidates:
        raise FileNotFoundError(f"mm_extract_metrics finished without creating a results CSV in {output_dir}")

    csv_path = max(csv_candidates, key=os.path.getmtime)
    fieldnames, rows = _read_metrics_csv(csv_path)
    if not rows:
        raise ValueError(f"mm_extract_metrics results CSV was empty: {csv_path}")

    logging.info(
        "Loaded MuscleMap metrics CSV %s with %d row(s) and columns=%s",
        csv_path,
        len(rows),
        fieldnames,
    )
    return {
        "method": method,
        "region": region,
        "components": components,
        "csv_path": csv_path,
        "fieldnames": fieldnames,
        "rows": rows,
    }


def _metrics_rows(metrics_result):
    if not metrics_result:
        return []
    return metrics_result.get("rows") or []


def _metrics_fieldnames(metrics_result):
    if not metrics_result:
        return []
    fieldnames = metrics_result.get("fieldnames") or []
    if fieldnames:
        return fieldnames
    rows = _metrics_rows(metrics_result)
    if rows:
        return list(rows[0].keys())
    return []


def _find_metrics_label_field(fieldnames):
    lower_map = {field.lower().strip(): field for field in fieldnames}
    for candidate in ("label", "label_name", "name", "muscle", "structure", "anatomy", "region"):
        if candidate in lower_map:
            return lower_map[candidate]
    for field in fieldnames:
        lowered = field.lower()
        if "label" in lowered or "muscle" in lowered or "anatomy" in lowered:
            return field
    return fieldnames[0] if fieldnames else ""


def _select_metrics_summary_fields(fieldnames, label_field, max_fields=3):
    preferred_tokens = (
        "csa",
        "fat",
        "fraction",
        "ff",
        "volume",
        "area",
        "mean",
        "average",
        "slice",
        "count",
    )
    selected = []
    for token in preferred_tokens:
        for field in fieldnames:
            if field == label_field or field in selected:
                continue
            if token in field.lower():
                selected.append(field)
                if len(selected) >= max_fields:
                    return selected
    for field in fieldnames:
        if field != label_field and field not in selected:
            selected.append(field)
            if len(selected) >= max_fields:
                return selected
    return selected


def _format_metric_value(value, max_chars=24):
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    try:
        numeric_value = float(text)
        if np.isfinite(numeric_value):
            text = f"{numeric_value:.4g}"
    except Exception:
        pass
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _format_metrics_comment(metrics_result, max_chars=1000):
    rows = _metrics_rows(metrics_result)
    fieldnames = _metrics_fieldnames(metrics_result)
    if not rows:
        return ""

    label_field = _find_metrics_label_field(fieldnames)
    summary_fields = _select_metrics_summary_fields(fieldnames, label_field)
    payload = {
        "v": 1,
        "method": metrics_result.get("method", mmMetricsMethod),
        "region": metrics_result.get("region", ""),
        "labels": metrics_result.get("label_scale", ""),
        "rows": len(rows),
        "csv": os.path.basename(metrics_result.get("csv_path", "")),
        "sample": [],
    }

    for row in rows[:5]:
        entry = {}
        if label_field:
            entry[label_field] = _format_metric_value(row.get(label_field), 32)
        for field in summary_fields:
            entry[field] = _format_metric_value(row.get(field))
        payload["sample"].append(entry)

    while True:
        text = "MMMETRICS " + json.dumps(payload, separators=(",", ":"))
        if len(text) <= max_chars:
            return text
        if payload["sample"]:
            payload["sample"].pop()
            continue
        payload.pop("csv", None)
        text = "MMMETRICS " + json.dumps(payload, separators=(",", ":"))
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 3] + "..."


def _format_metrics_minihead_value(metrics_result, max_chars=3500):
    rows = _metrics_rows(metrics_result)
    fieldnames = _metrics_fieldnames(metrics_result)
    if not rows:
        return ""

    label_field = _find_metrics_label_field(fieldnames)
    summary_fields = _select_metrics_summary_fields(fieldnames, label_field)
    parts = [
        "MuscleMapMetrics v=1",
        f"method={metrics_result.get('method', mmMetricsMethod)}",
        f"region={metrics_result.get('region', '')}",
        f"labels={metrics_result.get('label_scale', '')}",
        f"rows={len(rows)}",
    ]

    for row in rows:
        label = _format_metric_value(row.get(label_field), 32) if label_field else ""
        values = []
        for field in summary_fields:
            value = _format_metric_value(row.get(field))
            if value:
                values.append(f"{field}={value}")
        row_text = label
        if values:
            row_text = f"{row_text}: " + ", ".join(values) if row_text else ", ".join(values)
        if row_text:
            parts.append(row_text)
        text = "; ".join(parts)
        if len(text) > max_chars:
            parts.pop()
            parts.append("truncated=true")
            break

    text = "; ".join(parts)
    if len(text) > max_chars:
        marker = "truncated=true"
        base_parts = parts[:-1] if parts and parts[-1] == marker else parts
        base_text = "; ".join(base_parts)
        suffix = f"; {marker}"
        prefix = base_text[: max(0, max_chars - len(suffix))]
        boundary = prefix.rfind("; ")
        if boundary > 0:
            prefix = prefix[:boundary]
        text = f"{prefix.rstrip('; ')}{suffix}" if prefix else marker

    return _sanitize_minihead_param_value(text[:max_chars])


def _join_image_comment(prefix, metrics_comment, max_chars=1200):
    prefix = _first_non_empty_text(prefix)
    metrics_comment = _first_non_empty_text(metrics_comment)
    if prefix and metrics_comment:
        text = f"{prefix} | {metrics_comment}"
    else:
        text = prefix or metrics_comment
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _save_metrics_segmentation_nifti(segmentation_data, reference_img, output_path):
    labels = np.asarray(segmentation_data)
    if labels.ndim == 5 and labels.shape[2] == 1 and labels.shape[3] == 1:
        labels = labels[:, :, 0, 0, :]
    elif labels.ndim == 3:
        pass
    elif labels.ndim == 2:
        labels = labels[:, :, None]
    else:
        labels = np.squeeze(labels)
        if labels.ndim == 2:
            labels = labels[:, :, None]
        if labels.ndim != 3:
            raise ValueError(f"Cannot convert segmentation data with shape {labels.shape} to NIfTI")

    labels_nifti = labels.transpose((1, 0, 2))
    i16 = np.iinfo(np.int16)
    labels_min = int(np.min(labels_nifti))
    labels_max = int(np.max(labels_nifti))
    if labels_min < i16.min or labels_max > i16.max:
        raise ValueError(f"Metrics segmentation labels [{labels_min}, {labels_max}] exceed int16 range")

    header = reference_img.header.copy()
    header.set_data_dtype(np.int16)
    metrics_img = nib.Nifti1Image(labels_nifti.astype(np.int16, copy=False), reference_img.affine, header=header)
    try:
        metrics_img.set_qform(reference_img.get_qform(), code=int(reference_img.header["qform_code"]))
        metrics_img.set_sform(reference_img.get_sform(), code=int(reference_img.header["sform_code"]))
    except Exception:
        metrics_img.set_qform(reference_img.affine, code=1)
        metrics_img.set_sform(reference_img.affine, code=1)
    metrics_img.header.set_slope_inter(1.0, 0.0)
    nib.save(metrics_img, output_path)
    return output_path


def _pil_text_size(draw, text, font):
    bbox = draw.textbbox((0, 0), str(text), font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _truncate_text_to_width(draw, text, font, max_width):
    text = _format_metric_value(text, 128)
    if _pil_text_size(draw, text, font)[0] <= max_width:
        return text
    if max_width <= 0:
        return ""
    suffix = "..."
    low = 0
    high = len(text)
    while low < high:
        mid = (low + high + 1) // 2
        candidate = text[:mid] + suffix
        if _pil_text_size(draw, candidate, font)[0] <= max_width:
            low = mid
        else:
            high = mid - 1
    return text[:low] + suffix if low > 0 else suffix


def _chunk_list(values, chunk_size):
    chunk_size = max(1, int(chunk_size))
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def _build_metrics_column_groups(fieldnames, available_width):
    if not fieldnames:
        return []

    label_field = _find_metrics_label_field(fieldnames)
    min_column_width = 96
    if label_field and len(fieldnames) > 1:
        other_fields = [field for field in fieldnames if field != label_field]
        max_other_columns = max(1, int((available_width - 160) // min_column_width))
        return [[label_field] + group for group in _chunk_list(other_fields, max_other_columns)]

    max_columns = max(1, int(available_width // min_column_width))
    return _chunk_list(fieldnames, max_columns)


def _orient_metrics_report_page(page_array):
    # The scanner viewer currently displays the burned-in metrics page mirrored
    # left/right and rotated; transpose the synthetic raster before wrapping it
    # as DICOM/MRD so the visible page reads correctly.
    return np.ascontiguousarray(np.rot90(np.fliplr(page_array), 1))


def _render_metrics_report_pages(metrics_result, width, height):
    rows = _metrics_rows(metrics_result)
    fieldnames = _metrics_fieldnames(metrics_result)
    if not rows or not fieldnames:
        return []

    from PIL import Image, ImageDraw, ImageFont

    width = max(int(width), 512)
    height = max(int(height), 512)
    margin = 18
    image_probe = Image.new("L", (width, height), 0)
    draw_probe = ImageDraw.Draw(image_probe)
    font = ImageFont.load_default()
    line_height = max(12, _pil_text_size(draw_probe, "Ag", font)[1] + 6)
    title_line_count = 4
    table_top = margin + title_line_count * line_height + 8
    footer_height = line_height + 6
    available_width = width - (2 * margin)
    available_height = max(line_height * 3, height - table_top - margin - footer_height)
    rows_per_page = max(1, int(available_height // line_height) - 1)
    row_groups = _chunk_list(rows, rows_per_page)
    column_groups = _build_metrics_column_groups(fieldnames, available_width)
    total_pages = max(1, len(row_groups) * len(column_groups))
    pages = []
    page_number = 0

    for row_group_index, row_group in enumerate(row_groups):
        row_start = row_group_index * rows_per_page
        for column_group_index, column_group in enumerate(column_groups):
            page_number += 1
            image = Image.new("L", (width, height), 0)
            draw = ImageDraw.Draw(image)

            title_lines = [
                "MuscleMap Metrics",
                f"Method: {metrics_result.get('method', mmMetricsMethod)}    Region: {metrics_result.get('region', '')}    Rows: {len(rows)}",
                f"CSV: {os.path.basename(metrics_result.get('csv_path', ''))}",
                f"Page {page_number}/{total_pages}    Rows {row_start + 1}-{row_start + len(row_group)}    Column group {column_group_index + 1}/{len(column_groups)}",
            ]
            y = margin
            for title_line in title_lines:
                draw.text((margin, y), _truncate_text_to_width(draw, title_line, font, available_width), fill=255, font=font)
                y += line_height

            column_width = max(1, int(available_width // len(column_group)))
            y = table_top
            x = margin
            for field in column_group:
                draw.text((x, y), _truncate_text_to_width(draw, field, font, column_width - 6), fill=255, font=font)
                x += column_width
            y += line_height
            draw.line((margin, y - 2, width - margin, y - 2), fill=120)

            for row in row_group:
                x = margin
                for field in column_group:
                    draw.text(
                        (x, y),
                        _truncate_text_to_width(draw, row.get(field, ""), font, column_width - 6),
                        fill=220,
                        font=font,
                    )
                    x += column_width
                y += line_height

            page_array = np.asarray(image, dtype=np.uint16) * 16
            page_array = _orient_metrics_report_page(page_array)
            pages.append(page_array)

    return pages


def _build_metrics_report_images(
    metrics_result,
    source_headers,
    source_meta,
    metrics_series_index,
    source_series_identity,
    source_type_token,
    metrics_comment,
    metrics_minihead_text,
    include_minihead_metrics,
    report_spacing,
):
    rows = _metrics_rows(metrics_result)
    if not rows or not source_headers:
        return []

    base_header = copy.deepcopy(source_headers[0])
    base_meta = _copy_meta(source_meta[0]) if source_meta else ismrmrd.Meta()
    source_matrix = np.array(base_header.matrix_size[:], dtype=float)
    source_fov = np.array(base_header.field_of_view[:], dtype=float)
    source_width = int(source_matrix[0]) if source_matrix.size > 0 and source_matrix[0] > 0 else 512
    source_height = int(source_matrix[1]) if source_matrix.size > 1 and source_matrix[1] > 0 else 512
    report_width = max(source_width, 768)
    report_height = max(source_height, 768)

    try:
        report_pages = _render_metrics_report_pages(metrics_result, report_width, report_height)
    except Exception:
        logging.warning("Failed to render MuscleMap metrics report images:\n%s", traceback.format_exc())
        return []

    if not report_pages:
        return []

    voxel_x = float(source_fov[0] / source_matrix[0]) if source_matrix.size > 0 and source_matrix[0] > 0 else 1.0
    voxel_y = float(source_fov[1] / source_matrix[1]) if source_matrix.size > 1 and source_matrix[1] > 0 else 1.0
    report_spacing = float(report_spacing) if report_spacing and float(report_spacing) > 0 else 1.0
    slice_dir = _normalize_vector(_header_vector(base_header, "slice_dir"))
    if slice_dir is None:
        slice_dir = np.array([0.0, 0.0, 1.0], dtype=float)
    base_position = _header_vector(base_header, "position")
    report_series_description = (
        f"{source_series_identity['series_description']}_MuscleMap_Metrics"
        if source_series_identity["series_description"]
        else "MuscleMap_Metrics"
    )
    report_identity = _build_derived_series_identity(
        source_series_identity,
        series_description=report_series_description,
        sequence_description=report_series_description,
        grouping_suffix="MuscleMap_Metrics",
        derived_series_index=metrics_series_index,
        derived_kind="metrics",
    )
    report_parent_grouping = report_identity["grouping"]
    report_series_instance_uid = report_identity["series_instance_uid"]

    report_images = []
    for page_index, page_array in enumerate(report_pages):
        page_height = int(page_array.shape[0]) if page_array.ndim >= 1 else report_height
        page_width = int(page_array.shape[1]) if page_array.ndim >= 2 else report_width
        report_image = ismrmrd.Image.from_array(page_array.astype(np.uint16, copy=False), transpose=False)
        report_header = copy.deepcopy(base_header)
        report_header.data_type = report_image.data_type
        _set_header_sequence_field(report_header, "matrix_size", [page_width, page_height, 1])
        _set_header_sequence_field(
            report_header,
            "field_of_view",
            [voxel_x * page_width, voxel_y * page_height, report_spacing],
        )
        _set_header_sequence_field(report_header, "slice_dir", [float(value) for value in slice_dir])
        _set_header_sequence_field(
            report_header,
            "position",
            [float(value) for value in (base_position + page_index * report_spacing * slice_dir)],
        )
        report_header.image_index = page_index + 1
        report_header.slice = page_index
        report_header.image_series_index = metrics_series_index
        report_header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        report_image.setHead(report_header)

        report_meta = _copy_meta(base_meta)
        report_meta["DataRole"] = "Image"
        report_meta["ImageProcessingHistory"] = ["OPENRECON", "MUSCLEMAP", "METRICS"]
        report_meta["WindowCenter"] = "2040"
        report_meta["WindowWidth"] = "4080"
        report_meta["SeriesDescription"] = report_identity["series_description"]
        report_meta["SequenceDescription"] = report_identity["sequence_description"]
        report_meta["SeriesNumberRangeNameUID"] = report_parent_grouping
        report_meta["SeriesInstanceUID"] = report_series_instance_uid
        report_meta["ImageType"] = "METRICS"
        report_meta["ImageTypeValue3"] = "M"
        report_meta["ImageTypeValue4"] = "METRICS"
        report_meta["DicomImageType"] = "DERIVED\\SECONDARY\\M\\METRICS"
        report_meta["ComplexImageComponent"] = "MAGNITUDE"
        report_meta["ImageComments"] = metrics_comment
        report_meta["Keep_image_geometry"] = 1
        report_meta["ImageRowDir"] = [
            "{:.18f}".format(report_header.read_dir[0]),
            "{:.18f}".format(report_header.read_dir[1]),
            "{:.18f}".format(report_header.read_dir[2]),
        ]
        report_meta["ImageColumnDir"] = [
            "{:.18f}".format(report_header.phase_dir[0]),
            "{:.18f}".format(report_header.phase_dir[1]),
            "{:.18f}".format(report_header.phase_dir[2]),
        ]

        minihead_text = _decode_ice_minihead(report_meta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                report_identity["sequence_description"],
                report_parent_grouping,
                report_series_instance_uid,
                source_type_token,
                "METRICS",
                metrics_text=metrics_minihead_text if include_minihead_metrics else None,
            )
            if minihead_changed:
                report_meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)

        report_image.attribute_string = report_meta.serialize()
        report_images.append(report_image)

    logging.info(
        "Created %d MuscleMap metrics report image(s) in image_series_index=%d",
        len(report_images),
        metrics_series_index,
    )
    return report_images


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
            # Pin cwd so mm_segment's output path resolution is independent of
            # the container WORKDIR. Upstream writes "<input>_dseg.nii.gz"
            # relative to cwd when the input is supplied; without this the
            # segmentation would land in whatever directory the server was
            # launched from (e.g. /opt/spinalcordtoolbox-6.5/).
            mm_segment_result = subprocess.run(
                mm_segment_cmd,
                check=True,
                capture_output=True,
                text=True,
                cwd=os.path.dirname(mmSegmentOutputPath) or "/opt",
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

    segmentation_colormap = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'segmentationcolormap', default=False, type='bool')
    )
    logging.info("segmentationcolormap resolved to %s", segmentation_colormap)
    compute_metrics_flag = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'computemetrics', default=True, type='bool')
    )
    metrics_burn_series = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'metricsburnseries', default=True, type='bool')
    )
    metrics_in_comments = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'metricsincomments', default=True, type='bool')
    )
    metrics_in_minihead = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'metricsinminihead', default=True, type='bool')
    )
    # Treat the master "computemetrics" toggle as advisory: if the operator has
    # ticked any specific output (burned series, ImageComments, IceMiniHead),
    # we must compute the metrics regardless. This avoids the foot-gun where
    # the master checkbox is unchecked on the UI but an output is requested,
    # which would otherwise silently produce no metrics.
    compute_metrics = compute_metrics_flag or metrics_burn_series or metrics_in_comments or metrics_in_minihead
    logging.info(
        "MuscleMap metrics config: computemetrics=%s (flag=%s) burnseries=%s comments=%s minihead=%s method=%s",
        compute_metrics,
        compute_metrics_flag,
        metrics_burn_series,
        metrics_in_comments,
        metrics_in_minihead,
        mmMetricsMethod,
    )

    # Determine the source image type (e.g. "Water", "Fat", "In_Phase", …)
    # from the first image's metadata so the segmentation output is named accordingly.
    _first_meta = ismrmrd.Meta.deserialize(imgGroup[0].attribute_string)
    _source_type_value = _get_dicom_image_type_value(_first_meta, 3)  # value4
    source_image_label = muscleMapDisplayLabel
    source_image_type_value4 = muscleMapDisplayLabel
    source_dicom_image_type_value4 = muscleMapImageTypeToken
    source_volume_key = _build_image_volume_key(imgGroup[0])
    source_minihead = _decode_ice_minihead(_first_meta)
    if not source_minihead:
        logging.info(
            "Source images carry no IceMiniHead; minihead patching and "
            "metricsinminihead injection will be no-ops for this volume"
        )
    source_series_identity = _resolve_source_series_identity(_first_meta, source_minihead)
    logging.info("Source image type for segmentation naming: %s -> %s", _source_type_value, source_image_label)
    logging.info(
        "Source segmentation parent identity: volume_key=%s series_description=%s -> %s sequence=%s -> %s grouping=%s -> %s source_series_uid=%s",
        source_volume_key,
        source_series_identity["raw_series_description"] or "N/A",
        source_series_identity["series_description"] or "N/A",
        source_series_identity["raw_parent_sequence"] or "N/A",
        source_series_identity["parent_sequence"] or "N/A",
        source_series_identity["raw_parent_grouping"] or "N/A",
        source_series_identity["parent_grouping"] or "N/A",
        source_series_identity["series_instance_uid"] or "N/A",
    )

    # Create folder, if necessary
    if not os.path.exists(debugFolder):
        os.makedirs(debugFolder)
        logging.debug("Created folder " + debugFolder + " for debug output files")


    # Note: The MRD Image class stores data as [cha z y x]

    unsorted_head = [img.getHead() for img in imgGroup]
    unsorted_meta = [ismrmrd.Meta.deserialize(img.attribute_string) for img in imgGroup]
    slice_sort_indices, slice_axis, _ = _slice_sort_indices(unsorted_head)
    _log_slice_order_checkpoint(
        "incoming",
        unsorted_head,
        metas=unsorted_meta,
        slice_axis=slice_axis,
    )
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
    meta = [unsorted_meta[index] for index in slice_sort_indices]

    _log_slice_order_checkpoint(
        "sorted_for_nifti",
        head,
        metas=meta,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
    )
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
    affine = compute_nifti_affine(head[0], voxelsize, slice_axis=slice_axis)
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
    _log_slice_order_checkpoint(
        "nifti_written",
        head,
        metas=meta,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
        extra={
            "nifti_path": mmSegmentInputPath,
            "nifti_shape": list(data_nifti.shape),
            "nifti_dtype": str(data_nifti.dtype),
        },
    )

    # Extract UI parameters from JSON config
    bodyregion = mrdhelper.get_json_config_param(config, 'bodyregion', default='wholebody', type='str')
    metrics_region = mrdhelper.get_json_config_param(config, 'metricsregion', default='', type='str')
    metrics_region = _first_non_empty_text(metrics_region) or bodyregion
    chunksize = mrdhelper.get_json_config_param(config, 'chunksize', default='auto', type='str')
    spatialoverlap = mrdhelper.get_json_config_param(config, 'spatialoverlap', default=50, type='int')
    
    logging.info(
        "mm_segment parameters: bodyregion=%s, chunksize=%s, spatialoverlap=%s, metrics_region=%s",
        bodyregion,
        chunksize,
        spatialoverlap,
        metrics_region,
    )
    
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
    _log_slice_order_checkpoint(
        "segmentation_loaded",
        head,
        metas=meta,
        input_indices=slice_sort_indices,
        slice_axis=slice_axis,
        extra={
            "segmentation_path": mmSegmentOutputPath,
            "segmentation_shape": list(img.shape),
            "segmentation_dtype": str(img.get_data_dtype()),
        },
    )

    metrics_result = None
    metrics_comment = ""
    metrics_minihead_text = ""

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

    label_transform = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'labeltransform', default=True, type='bool')
    )
    logging.info("labeltransform resolved to %s", label_transform)

    if label_transform:
        logging.info("Applying label transformation: 3 * (label_in // 10) + (label_in % 10)")
        data = 3 * (data // 10) + (data % 10)
        logging.info(f"Label transformation complete. New data range: [{data.min()}, {data.max()}]")

    if compute_metrics and (metrics_burn_series or metrics_in_comments or metrics_in_minihead):
        try:
            metrics_segmentation_path = _save_metrics_segmentation_nifti(
                data,
                new_img,
                mmMetricsSegmentationPath,
            )
            metrics_region_for_run = "" if label_transform else metrics_region
            if label_transform and metrics_region:
                logging.info(
                    "Skipping MuscleMap metrics region label-name mapping for transformed labels; "
                    "metrics labels will match the returned overlay values"
                )
            logging.info(
                "Running MuscleMap metrics on %s labels saved to %s",
                "transformed" if label_transform else "untransformed",
                metrics_segmentation_path,
            )
            metrics_result = _run_mm_extract_metrics(
                method=mmMetricsMethod,
                region=metrics_region_for_run,
                components=None,
                segmentation_path=metrics_segmentation_path,
                input_image_path=mmSegmentInputPath,
                output_dir=mmMetricsOutputDir,
            )
            metrics_result["region"] = metrics_region
            metrics_result["label_scale"] = "transformed" if label_transform else "untransformed"
            metrics_comment = _format_metrics_comment(metrics_result)
            metrics_minihead_text = _format_metrics_minihead_value(metrics_result)
        except subprocess.CalledProcessError as exc:
            combined_output = "\n".join(
                part for part in (exc.stdout, exc.stderr) if part
            )
            _log_mm_extract_metrics_output(combined_output, logging.warning)
            logging.warning(
                "MuscleMap metrics extraction failed with return code %s; "
                "segmentation output will still be returned",
                exc.returncode,
            )
            metrics_result = None
        except Exception:
            logging.warning(
                "MuscleMap metrics extraction failed; segmentation output will still be returned:\n%s",
                traceback.format_exc(),
            )
            metrics_result = None
    elif compute_metrics:
        logging.info("MuscleMap metrics computation is enabled but no metrics output path is enabled; skipping")
    else:
        logging.info("MuscleMap metrics computation disabled")


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
    source_series_indices = [
        int(getattr(image_header, "image_series_index", 0))
        for image_header in head
    ]
    known_series_floor = max(source_series_indices, default=0)
    for meta_obj in meta:
        contrast_count = _get_first_meta_int(meta_obj, ["contrast_count", "ContrastCount"])
        if contrast_count is not None:
            known_series_floor = max(known_series_floor, contrast_count)
    segmentation_series_index = known_series_floor + 1
    if segmentation_series_index == 99:
        segmentation_series_index += 1
    segmentation_identity = _build_derived_series_identity(
        source_series_identity,
        series_description=muscleMapDisplayLabel,
        sequence_description=muscleMapDisplayLabel,
        grouping_suffix=muscleMapDisplayLabel,
        derived_series_index=segmentation_series_index,
        derived_kind="segmentation",
    )
    logging.info(
        "Using image_series_index=%d for MuscleMap segmentation "
        "(source series indices=%s, known_series_floor=%d, derived_grouping=%s, derived_series_uid=%s)",
        segmentation_series_index,
        sorted(set(source_series_indices)),
        known_series_floor,
        segmentation_identity["grouping"],
        segmentation_identity["series_instance_uid"],
    )

    for iImg in range(data.shape[-1]):
        # Create new MRD instance for the segmented image
        # Transpose from convenience shape of [y x z cha] to MRD Image shape of [cha z y x]
        # from_array() should be called with 'transpose=False' to avoid warnings, and when called
        # with this option, can take input as: [cha z y x], [z y x], or [y x]
        # imagesOut[iImg] = ismrmrd.Image.from_array(data[...,iImg].transpose((3, 2, 0, 1)), transpose=False)
        imagesOut[iImg] = ismrmrd.Image.from_array(data[..., iImg].transpose((3, 2, 0, 1)), transpose=False)

        source_record = output_slice_records[iImg]
        source_image_index = int(getattr(head[iImg], "image_index", 0))
        source_slice_index = int(getattr(head[iImg], "slice", 0))

        # Create an independent copy of the source fixed header. Some
        # pyismrmrd versions expose getHead() as a live ctypes-backed object,
        # so mutating it can corrupt sendoriginal images.
        oldHeader = copy.deepcopy(head[iImg])
        oldHeader.data_type = imagesOut[iImg].data_type
        _set_header_sequence_field(
            oldHeader,
            "matrix_size",
            [
                int(oldHeader.matrix_size[0]),
                int(oldHeader.matrix_size[1]),
                1,
            ],
        )
        _set_header_sequence_field(
            oldHeader,
            "field_of_view",
            [
                float(oldHeader.field_of_view[0]),
                float(oldHeader.field_of_view[1]),
                float(voxelsize[2]),
            ],
        )
        if np.linalg.norm(_header_vector(oldHeader, "slice_dir")) < 1e-6:
            _set_header_sequence_field(
                oldHeader,
                "slice_dir",
                [float(value) for value in slice_axis],
            )
        oldHeader.image_index = source_image_index
        oldHeader.slice = source_slice_index
        oldHeader.contrast = segmentation_series_index - 1
        oldHeader.image_series_index = segmentation_series_index

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
        tmpMeta = _copy_meta(meta[iImg])

        tmpMeta["DataRole"] = "Image"
        tmpMeta["ImageProcessingHistory"] = ["OPENRECON", "MUSCLEMAP"]
        tmpMeta["WindowCenter"] = str((maxVal + 1) / 2)
        tmpMeta["WindowWidth"] = str((maxVal + 1))
        tmpMeta["SeriesDescription"] = segmentation_identity["series_description"]
        tmpMeta["SequenceDescription"] = segmentation_identity["sequence_description"]
        tmpMeta["SeriesNumberRangeNameUID"] = segmentation_identity["grouping"]
        tmpMeta["SeriesInstanceUID"] = segmentation_identity["series_instance_uid"]
        tmpMeta["ImageType"] = f"DERIVED\\PRIMARY\\M\\{muscleMapImageTypeToken}"
        tmpMeta["ImageTypeValue3"] = "M"
        tmpMeta["ImageTypeValue4"] = muscleMapImageTypeToken
        tmpMeta["DicomImageType"] = f"DERIVED\\PRIMARY\\M\\{muscleMapImageTypeToken}"
        tmpMeta["ComplexImageComponent"] = "MAGNITUDE"
        tmpMeta["partition_count"] = str(data.shape[-1])
        if metrics_in_comments and metrics_comment:
            image_comment = _join_image_comment(source_image_label, metrics_comment)
        else:
            image_comment = source_image_label
        tmpMeta["ImageComments"] = image_comment
        tmpMeta["ImageComment"] = image_comment
        tmpMeta["MuscleMapSourceInputIndex"] = str(source_record["input_index"])
        tmpMeta["MuscleMapSourceImageIndex"] = str(source_image_index)
        tmpMeta["MuscleMapSourceSlice"] = str(source_slice_index)
        tmpMeta["MuscleMapSourceProjectedPosition"] = f"{source_record['projected_position']:.6f}"
        if "SequenceDescriptionAdditional" in tmpMeta:
            try:
                del tmpMeta["SequenceDescriptionAdditional"]
            except Exception:
                tmpMeta["SequenceDescriptionAdditional"] = ""

        minihead_text = _decode_ice_minihead(tmpMeta)
        if minihead_text:
            patched_minihead_text, minihead_changed = _patch_ice_minihead(
                minihead_text,
                segmentation_identity["sequence_description"],
                segmentation_identity["grouping"],
                segmentation_identity["series_instance_uid"],
                _source_type_value,
                muscleMapImageTypeToken,
                metrics_text=metrics_minihead_text if metrics_in_minihead and metrics_minihead_text else None,
                target_display_token=muscleMapImageTypeToken,
                target_image_type_value3="M",
            )
            if minihead_changed:
                minihead_text = patched_minihead_text
            else:
                logging.warning(
                    "IceMiniHead was present but not updated for segmentation output slice %d",
                    iImg,
                )
            for long_param_name, long_param_value in (
                ("Actual3DImagePartNumber", source_slice_index),
                ("AnatomicalPartitionNo", source_slice_index),
                ("ChronSliceNo", source_slice_index),
                ("NumberInSeries", iImg + 1),
            ):
                minihead_text, did_change = _replace_or_append_minihead_long_param(
                    minihead_text,
                    long_param_name,
                    long_param_value,
                )
                minihead_changed = minihead_changed or did_change
            if minihead_changed:
                tmpMeta["IceMiniHead"] = _encode_ice_minihead(minihead_text)
        tmpMeta["Keep_image_geometry"] = 1

        tmpMeta["ImageRowDir"] = [
            "{:.18f}".format(oldHeader.read_dir[0]),
            "{:.18f}".format(oldHeader.read_dir[1]),
            "{:.18f}".format(oldHeader.read_dir[2]),
        ]

        tmpMeta["ImageColumnDir"] = [
            "{:.18f}".format(oldHeader.phase_dir[0]),
            "{:.18f}".format(oldHeader.phase_dir[1]),
            "{:.18f}".format(oldHeader.phase_dir[2]),
        ]

        if segmentation_colormap:
            tmpMeta["LUTFileName"] = "MicroDeltaHotMetal.pal"

        metaXml = tmpMeta.serialize()
        # logging.debug("Image MetaAttributes: %s", xml.dom.minidom.parseString(metaXml).toprettyxml())
        # logging.debug("Image data has %d elements", imagesOut[iImg].data.size)

        imagesOut[iImg].attribute_string = metaXml

        if iImg in set(_sample_indices(data.shape[-1], edge_count=6)):
            final_meta = ismrmrd.Meta.deserialize(metaXml)
            final_header = imagesOut[iImg].getHead()
            final_minihead = _decode_ice_minihead(final_meta)
            _log_json_event(
                "MUSCLEMAP_OUTPUT_IDENTITY",
                {
                    "source_volume_key": list(source_volume_key),
                    "output_series_index": int(segmentation_series_index),
                    "display_label": source_image_label,
                    "parent_grouping": source_series_identity["parent_grouping"],
                    "series_grouping": segmentation_identity["grouping"],
                    "source": {
                        "input_index": source_record["input_index"],
                        "image_index": source_image_index,
                        "slice": source_slice_index,
                        "projected_position": source_record["projected_position"],
                    },
                    "header": {
                        "image_series_index": int(getattr(final_header, "image_series_index", 0)),
                        "image_index": int(getattr(final_header, "image_index", 0)),
                        "slice": int(getattr(final_header, "slice", 0)),
                        "position": list(getattr(final_header, "position", [])),
                    },
                    "meta": _meta_identity(final_meta),
                    "minihead": _minihead_identity(final_minihead),
                    "minihead_slice": _minihead_slice_identity(final_minihead),
                },
            )

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

    segmentation_output_metas = [ismrmrd.Meta.deserialize(image.attribute_string) for image in imagesOut]
    segmentation_source_input_indices = []
    for index, meta_obj in enumerate(segmentation_output_metas):
        source_input_index = _get_first_meta_int(meta_obj, ["MuscleMapSourceInputIndex"])
        segmentation_source_input_indices.append(source_input_index if source_input_index is not None else index)
    _log_slice_order_checkpoint(
        "output_before_send",
        [image.getHead() for image in imagesOut],
        metas=segmentation_output_metas,
        input_indices=segmentation_source_input_indices,
        slice_axis=slice_axis,
        extra={
            "output_series_index": segmentation_series_index,
            "display_label": source_image_label,
            "parent_grouping": source_series_identity["parent_grouping"],
            "series_grouping": segmentation_identity["grouping"],
        },
    )

    if metrics_burn_series and _metrics_rows(metrics_result):
        metrics_series_index = segmentation_series_index + 1
        if metrics_series_index == 99:
            metrics_series_index += 1
        report_images = _build_metrics_report_images(
            metrics_result=metrics_result,
            source_headers=head,
            source_meta=meta,
            metrics_series_index=metrics_series_index,
            source_series_identity=source_series_identity,
            source_type_token=_source_type_value,
            metrics_comment=metrics_comment or _format_metrics_comment(metrics_result),
            metrics_minihead_text=metrics_minihead_text,
            include_minihead_metrics=metrics_in_minihead,
            report_spacing=float(voxelsize[2]) if len(voxelsize) > 2 else 1.0,
        )
        imagesOut.extend(report_images)
    elif metrics_burn_series and compute_metrics:
        logging.info("MuscleMap metrics report series requested but no metrics rows are available")

    # Send a copy of original (unmodified) images back too if selected
    opre_sendoriginal = _as_config_bool(
        mrdhelper.get_json_config_param(config, 'sendoriginal', default=True, type='bool')
    )
    if opre_sendoriginal:
        stack = traceback.extract_stack()
        if stack[-2].name == 'process_raw':
            logging.warning('sendOriginal is true, but input was raw data, so no original images to return!')
        else:
            logging.info('Sending a copy of original unmodified images due to sendOriginal set to True')
            # In reverse order so that they'll be in correct order as we insert them to the front of the list
            original_sample_indices = set(_sample_indices(len(imgGroup), edge_count=6))
            allowed_original_changes = {"meta.Keep_image_geometry"}
            for i, image in enumerate(reversed(imgGroup)):
                source_index = len(imgGroup) - 1 - i
                before_identity = _image_identity(image)

                # Create a copy to not modify the original inputs.
                tmpImg = ismrmrd.Image.from_array(np.array(image.data, copy=True), transpose=False)
                tmpHeader = copy.deepcopy(image.getHead())
                tmpImg.setHead(tmpHeader)

                # Ensure Keep_image_geometry is set to not reverse image orientation
                tmpMeta = _copy_meta(ismrmrd.Meta.deserialize(image.attribute_string))
                tmpMeta['Keep_image_geometry'] = 1
                tmpImg.attribute_string = tmpMeta.serialize()

                after_identity = _image_identity(tmpImg, tmpMeta)
                changed_fields = _identity_changed_fields(before_identity, after_identity)
                unexpected_changed_fields = [
                    field
                    for field in changed_fields
                    if field not in allowed_original_changes
                ]
                if source_index in original_sample_indices or unexpected_changed_fields:
                    _log_json_event(
                        "ORIGINAL_PASSTHROUGH_IDENTITY",
                        {
                            "slice_sample": source_index,
                            "before": before_identity,
                            "after": after_identity,
                            "changed_fields": changed_fields,
                        },
                        level=logging.warning if unexpected_changed_fields else logging.info,
                    )
                if unexpected_changed_fields:
                    raise ValueError(
                        "Original passthrough changed naming/grouping field(s): "
                        + ", ".join(unexpected_changed_fields)
                    )

                imagesOut.insert(0, tmpImg)

    _log_send_batch_summary(imagesOut, "process_image_return")
    return imagesOut
