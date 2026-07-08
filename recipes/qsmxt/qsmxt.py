"""OpenRecon bridge for QSMxT v9.

The OpenRecon side receives reconstructed MRD image messages. QSMxT v9 is a
BIDS-native Rust binary, so this bridge writes a temporary BIDS MEGRE dataset,
runs ``qsmxt run``, and converts selected derivatives back to MRD images.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import traceback
import uuid

import ismrmrd
import nibabel as nib
import numpy as np

try:
    import constants
except ImportError:
    class constants:
        MRD_LOGGING_ERROR = 3


RECIPE_NAME = "qsmxt"
DEFAULT_QSMXT_BINARY = "/opt/qsmxt/qsmxt"
OPENRECON_WORK_ROOT = Path("/tmp/share/qsmxt_openrecon")
QSMXT_DERIVATIVE_ROOT = Path("derivatives/qsmxt.rs")
DEFAULT_ECHO_TIME_MS = 20.0
DEFAULT_ECHO_SPACING_MS = 5.0
DEFAULT_FIELD_STRENGTH_T = 3.0
DEFAULT_B0_DIR = (0.0, 0.0, 1.0)
ORIGINAL_SERIES_START = 100
OUTPUT_SERIES_START = 180
SCANNER_PARTITION_INDEX = 0
SCANNER_DISPLAY_MIN = 0
SCANNER_DISPLAY_MAX = 4096
SCANNER_DISPLAY_CENTER = 2048
SCANNER_DISPLAY_SCALE_FACTORS = (
    1000000.0,
    100000.0,
    10000.0,
    1000.0,
    100.0,
    10.0,
    1.0,
    0.1,
    0.01,
    0.001,
)

QSMXT_OUTPUTS = {
    "qsm": {
        "suffix": "Chimap",
        "token": "QSMXT_CHIMAP",
        "series": "QSMxT QSM",
        "units": "ppm",
    },
    "mask": {
        "suffix": "mask",
        "token": "QSMXT_MASK",
        "series": "QSMxT mask",
        "units": "binary",
    },
    "magnitude": {
        "suffix": "magnitude",
        "token": "QSMXT_MAGNITUDE",
        "series": "QSMxT magnitude",
        "units": "a.u.",
    },
    "swi": {
        "suffix": "swi",
        "token": "QSMXT_SWI",
        "series": "QSMxT SWI",
        "units": "a.u.",
    },
    "t2star": {
        "suffix": "T2starmap",
        "token": "QSMXT_T2STAR",
        "series": "QSMxT T2star",
        "units": "s",
    },
    "r2star": {
        "suffix": "R2starmap",
        "token": "QSMXT_R2STAR",
        "series": "QSMxT R2star",
        "units": "Hz",
    },
}

SCANNER_WRITE_UNSAFE_META_KEYS = {
    "ImageTypeValue3",
}
ORIGINAL_STORAGE_FIELDS = (
    "Actual3DImagePartNumber",
    "Actual3DImaPartNumber",
    "AnatomicalPartitionNo",
    "AnatomicalSliceNo",
    "ChronSliceNo",
    "NumberInSeries",
    "ProtocolSliceNumber",
    "SliceNo",
    "IsmrmrdSliceNo",
)
MINIHEAD_ORIGINAL_STORAGE_FIELD_SPECS = (
    ("Actual3DImagePartNumber", "Actual3DImagePartNumber"),
    ("Actual3DImaPartNumber", "Actual3DImagePartNumber"),
    ("AnatomicalPartitionNo", "AnatomicalPartitionNo"),
    ("AnatomicalSliceNo", "AnatomicalSliceNo"),
    ("ChronSliceNo", "ChronSliceNo"),
    ("NumberInSeries", "NumberInSeries"),
    ("ProtocolSliceNumber", "ProtocolSliceNumber"),
    ("SliceNo", "SliceNo"),
    ("IsmrmrdSliceNo", "IsmrmrdSliceNo"),
)
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


def process(connection, config, metadata):
    logging.info("QSMxT OpenRecon config: %s", config)
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

        settings = _settings_from_config(config, metadata)
        OPENRECON_WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="run_",
            dir=str(OPENRECON_WORK_ROOT),
        ) as work_dir_name:
            work_dir = Path(work_dir_name)
            bids_dir = work_dir / "bids"
            output_dir = work_dir / "output"

            conversion = write_bids_dataset(input_images, metadata, bids_dir, settings)
            _run_qsmxt(bids_dir, output_dir, settings)
            output_specs = _find_qsmxt_outputs(output_dir, conversion, settings)
            output_images = _build_output_images(output_specs, conversion, input_images)

        if settings["send_original"]:
            original_images = _build_original_passthrough_images(
                input_images,
                reserved_images=output_images,
            )
            logging.info(
                "Sending %d restamped original image(s) before QSMxT outputs",
                len(original_images),
            )
            _send_images_by_series(connection, original_images)

        if output_images:
            _validate_output_images(output_images, input_images)
            _send_images_by_series(connection, output_images)
        else:
            logging.warning("QSMxT completed but no requested output files were found")

    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def write_bids_dataset(input_images, metadata, bids_dir, settings):
    series_groups = _group_images_by_series(input_images)
    magnitude_group, phase_group = _select_magnitude_phase_groups(series_groups)
    magnitude_echo_groups = _echo_image_groups(magnitude_group["images"])
    phase_echo_groups = _echo_image_groups(phase_group["images"])

    n_echoes = min(len(magnitude_echo_groups), len(phase_echo_groups))
    if n_echoes == 0:
        raise ValueError("QSMxT requires at least one magnitude and one phase image")
    if len(magnitude_echo_groups) != len(phase_echo_groups):
        logging.warning(
            "Magnitude/phase echo group count mismatch; using first %d pair(s): "
            "%d magnitude, %d phase",
            n_echoes,
            len(magnitude_echo_groups),
            len(phase_echo_groups),
        )

    if settings["max_echoes"] > 0:
        n_echoes = min(n_echoes, settings["max_echoes"])
    magnitude_echo_groups = magnitude_echo_groups[:n_echoes]
    phase_echo_groups = phase_echo_groups[:n_echoes]

    logging.info(
        "Derived QSMxT echo groups: magnitude=%d group size(s)=%s, "
        "phase=%d group size(s)=%s, selected=%d",
        len(magnitude_echo_groups),
        [len(group) for group in magnitude_echo_groups],
        len(phase_echo_groups),
        [len(group) for group in phase_echo_groups],
        n_echoes,
    )

    echo_times = _echo_times_seconds(
        n_echoes,
        [group[0] for group in phase_echo_groups],
        metadata,
        settings,
    )
    field_strength = settings["field_strength_t"]
    logging.info(
        "Resolved QSMxT acquisition parameters: echo_times_ms=%s, "
        "field_strength_t=%.6g (%s), phase_wrap=%.6g",
        [round(value * 1000.0, 6) for value in echo_times],
        field_strength,
        settings["field_strength_source"],
        settings["phase_wrap"],
    )

    anat_dir = bids_dir / "sub-01" / "anat"
    anat_dir.mkdir(parents=True, exist_ok=True)

    acquisition_label = _sanitize_bids_label(
        _source_series_name(magnitude_echo_groups[0][0]) or "openrecon"
    )
    if not acquisition_label:
        acquisition_label = "openrecon"

    phase_paths = []
    magnitude_paths = []
    resolved_b0_dir = None
    resolved_b0_dir_source = None
    for echo_index in range(n_echoes):
        echo_number = echo_index + 1
        basename = f"sub-01_acq-{acquisition_label}_echo-{echo_number}"
        mag_path = anat_dir / f"{basename}_part-mag_MEGRE.nii.gz"
        phase_path = anat_dir / f"{basename}_part-phase_MEGRE.nii.gz"

        mag_volume, affine = _images_to_nifti_volume(
            magnitude_echo_groups[echo_index],
            kind="magnitude",
        )
        phase_volume, phase_affine = _images_to_nifti_volume(
            phase_echo_groups[echo_index],
            kind="phase",
        )
        phase_volume = _phase_to_qsmxt_counts(phase_volume, settings["phase_wrap"])
        if mag_volume.shape != phase_volume.shape:
            raise ValueError(
                "Magnitude and phase echo volumes have different shapes for "
                f"echo {echo_number}: {mag_volume.shape} vs {phase_volume.shape}"
            )
        b0_dir, b0_dir_source = _b0_dir_for_affine(phase_affine, settings)
        if resolved_b0_dir is None:
            resolved_b0_dir = b0_dir
            resolved_b0_dir_source = b0_dir_source
            logging.info(
                "Resolved QSMxT B0 direction from %s: %s",
                b0_dir_source,
                ",".join(f"{value:.6g}" for value in b0_dir),
            )

        nib.save(nib.Nifti1Image(mag_volume.astype(np.float32), affine), mag_path)
        nib.save(nib.Nifti1Image(phase_volume.astype(np.float32), phase_affine), phase_path)

        sidecar = {
            "EchoTime": echo_times[echo_index],
            "MagneticFieldStrength": field_strength,
            "B0_dir": list(b0_dir),
            "Modality": "MR",
            "ProtocolName": _metadata_protocol_name(metadata),
            "QSMxTOpenReconSource": "ISMRMRD image stream",
            "QSMxTRawPhaseScale": settings["phase_wrap"],
        }
        _write_json(_nifti_sidecar_path(mag_path), dict(sidecar, ImageType=["ORIGINAL", "PRIMARY", "M"]))
        _write_json(_nifti_sidecar_path(phase_path), dict(sidecar, ImageType=["ORIGINAL", "PRIMARY", "P"]))

        magnitude_paths.append(mag_path)
        phase_paths.append(phase_path)

    _write_json(
        bids_dir / "dataset_description.json",
        {
            "Name": "QSMxT OpenRecon transient BIDS dataset",
            "BIDSVersion": "1.9.0",
            "DatasetType": "raw",
            "GeneratedBy": [{"Name": "qsmxt-openrecon", "Version": "1"}],
        },
    )

    logging.info(
        "Wrote QSMxT BIDS input: %d echo pair(s), mag=%s phase=%s",
        n_echoes,
        magnitude_paths[0],
        phase_paths[0],
    )
    return {
        "bids_dir": bids_dir,
        "anat_dir": anat_dir,
        "subject": "01",
        "acquisition_label": acquisition_label,
        "basename": f"sub-01_acq-{acquisition_label}",
        "n_echoes": n_echoes,
        "magnitude_images": [image for group in magnitude_echo_groups for image in group],
        "phase_images": [image for group in phase_echo_groups for image in group],
        "anchor_image": magnitude_echo_groups[0][0],
        "magnitude_paths": magnitude_paths,
        "phase_paths": phase_paths,
        "field_strength_t": field_strength,
        "b0_dir": resolved_b0_dir or DEFAULT_B0_DIR,
        "b0_dir_source": resolved_b0_dir_source or "nifti_affine",
    }


def _run_qsmxt(bids_dir, output_dir, settings):
    binary = (
        settings["qsmxt_binary"]
        or os.environ.get("QSMXT_BINARY")
        or DEFAULT_QSMXT_BINARY
    )
    cmd = [
        binary,
        "run",
        str(bids_dir),
        str(output_dir),
        "--force",
        "--n-procs",
        "24",
    ]

    _append_optional_arg(cmd, "--qsm-algorithm", settings["qsm_algorithm"])
    _append_optional_arg(cmd, "--unwrapping-algorithm", settings["unwrapping_algorithm"])
    _append_optional_arg(cmd, "--bf-algorithm", settings["bf_algorithm"])
    _append_optional_arg(cmd, "--mask-preset", settings["mask_preset"])
    _append_optional_arg(cmd, "--qsm-reference", settings["qsm_reference"])
    # Auto-enable the generation flags for any derivative the user selected in
    # sendoutputs; otherwise the map is requested but never produced by QSMxT.
    selected = _selected_output_ids(settings["send_outputs"])
    do_swi = settings["do_swi"] or "swi" in selected
    do_t2starmap = settings["do_t2starmap"] or "t2star" in selected
    do_r2starmap = settings["do_r2starmap"] or "r2star" in selected
    if settings["no_qsm"]:
        cmd.append("--no-qsm")
    if do_swi:
        cmd.append("--do-swi")
    if do_t2starmap:
        cmd.append("--do-t2starmap")
    if do_r2starmap:
        cmd.append("--do-r2starmap")
    if not settings["inhomogeneity_correction"]:
        cmd.append("--no-inhomogeneity-correction")

    logging.info("Running QSMxT: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
        cwd=str(bids_dir),
    )
    if result.stdout:
        logging.info("QSMxT stdout:\n%s", result.stdout)
    if result.stderr:
        logging.info("QSMxT stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"qsmxt failed with exit code {result.returncode}")


def _find_qsmxt_outputs(output_dir, conversion, settings):
    selected = _selected_output_ids(settings["send_outputs"])
    derivative_anat_dir = (
        output_dir
        / QSMXT_DERIVATIVE_ROOT
        / f"sub-{conversion['subject']}"
        / "anat"
    )
    specs = []
    for output_id in selected:
        spec = QSMXT_OUTPUTS[output_id]
        candidates = sorted(derivative_anat_dir.glob(f"*_{spec['suffix']}.nii*"))
        if not candidates:
            logging.info(
                "Requested QSMxT output %s not found in %s",
                output_id,
                derivative_anat_dir,
            )
            continue
        specs.append((output_id, spec, candidates[0]))
    return specs


def _build_output_images(output_specs, conversion, input_images):
    used_series = {int(image.getHead().image_series_index) for image in input_images}
    output_images = []
    for index, (output_id, spec, nifti_path) in enumerate(output_specs):
        series_index = _reserve_series_index(used_series, OUTPUT_SERIES_START + index)
        output_images.extend(
            _nifti_to_mrd_images(
                nifti_path,
                conversion["anchor_image"],
                series_index,
                f"{spec['series']}",
                spec["token"],
                output_id,
                spec["units"],
            )
        )
    return output_images


def _build_original_passthrough_images(input_images, reserved_images=None):
    used_series = {int(image.getHead().image_series_index) for image in input_images}
    for image in reserved_images or []:
        used_series.add(int(image.getHead().image_series_index))

    output_images = []
    for group_index, group_images in enumerate(_passthrough_source_groups(input_images)):
        series_index = _reserve_series_index(
            used_series,
            ORIGINAL_SERIES_START + group_index,
        )
        series_name = _original_passthrough_series_name(group_images[0])
        identity = {
            "series_index": series_index,
            "series_name": series_name,
            "series_grouping": _derived_series_grouping(series_name, series_index),
            "series_uid": _derived_series_uid(
                group_images[0],
                series_index,
                series_name,
            ),
        }
        partition_count = _source_partition_count_hint(group_images)
        slice_count = _source_slice_count_hint(group_images)

        logging.info(
            "Restamping %d original image(s) from %s as image_series_index=%d "
            "with partition_count=%d slice_count=%d",
            len(group_images),
            _source_series_name(group_images[0]) or group_index,
            series_index,
            partition_count,
            slice_count,
        )
        for output_index, image in enumerate(group_images):
            output_images.append(
                _stamp_original_passthrough_image(
                    image,
                    identity,
                    output_index,
                    len(group_images),
                    partition_count,
                    slice_count,
                )
            )

    _validate_original_passthrough_images(output_images, input_images)
    return output_images


def _passthrough_source_groups(images):
    groups = []
    by_key = {}
    for image in images:
        key = _passthrough_source_group_key(image)
        group = by_key.get(key)
        if group is None:
            group = []
            by_key[key] = group
            groups.append(group)
        group.append(image)
    return groups


def _passthrough_source_group_key(image):
    # Group originals by contrast identity (series description + image type +
    # shape) rather than the raw scanner series index. Siemens 3D sequences
    # often split one logical contrast into several concatenations that arrive
    # as separate image_series_index values while sharing a SeriesDescription;
    # keying on the series index would emit one passthrough series per
    # concatenation (e.g. magnitude split across two half-slice series). Merging
    # by contrast identity keeps all slices of a contrast in a single series.
    header = image.getHead()
    return (
        _source_series_name(image),
        _classify_series([image]),
        int(getattr(header, "image_type", 0)),
        tuple(int(value) for value in np.asarray(image.data).shape),
    )


def _original_passthrough_series_name(source_image):
    source_name = _source_series_name(source_image) or RECIPE_NAME
    return f"{source_name}_original"


def _stamp_original_passthrough_image(
    source_image,
    identity,
    output_index,
    image_count,
    partition_count,
    slice_count,
):
    output_image = _clone_mrd_image(source_image)
    storage_fields = _original_storage_fields(
        source_image,
        output_index,
        partition_count,
        slice_count,
    )
    header = copy.deepcopy(output_image.getHead())
    header.image_series_index = int(identity["series_index"])
    header.image_index = output_index + 1
    header.slice = int(storage_fields["SliceNo"])
    output_image.setHead(header)
    output_image.image_series_index = int(identity["series_index"])
    output_image.attribute_string = _original_passthrough_meta(
        source_image,
        header,
        identity,
        output_index,
        image_count,
        partition_count,
        slice_count,
        storage_fields,
    ).serialize()
    return output_image


def _clone_mrd_image(image):
    output = ismrmrd.Image.from_array(np.array(image.data, copy=True), transpose=False)
    output.setHead(copy.deepcopy(image.getHead()))
    output.attribute_string = image.attribute_string
    return output


def _original_passthrough_meta(
    source_image,
    header,
    identity,
    output_index,
    image_count,
    partition_count,
    slice_count,
    storage_fields,
):
    meta = _copy_meta(_meta_from_image(source_image))
    minihead_text = _decode_ice_minihead(meta)
    _strip_source_parent_refs(meta)

    series_index = int(identity["series_index"])
    series_name = identity["series_name"]
    series_uid = identity["series_uid"]
    sop_uid = _derived_original_instance_uid(
        source_image,
        series_uid,
        series_index,
        series_name,
        output_index,
    )

    meta["DataRole"] = "Image"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesNumberRangeNameUID"] = identity["series_grouping"]
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SequenceDescriptionAdditional"] = "openrecon"
    meta["Keep_image_geometry"] = "1"
    meta["partition_count"] = str(int(max(partition_count, 1)))
    meta["slice_count"] = str(int(max(slice_count, 1)))
    meta["NumberOfSlices"] = str(int(max(slice_count, 1)))
    meta["ImagesInAcquisition"] = str(int(max(image_count, 1)))
    for key in ORIGINAL_STORAGE_FIELDS:
        if key in storage_fields:
            meta[key] = str(int(storage_fields[key]))
    meta.update(_header_geometry_meta(header))

    if minihead_text:
        patched_minihead, changed = _patch_original_ice_minihead(
            minihead_text,
            series_name,
            identity["series_grouping"],
            series_uid,
            sop_uid,
            storage_fields,
            output_index == image_count - 1,
        )
        if changed:
            meta["IceMiniHead"] = _encode_ice_minihead(patched_minihead)

    return meta


def _original_storage_fields(source_image, output_index, partition_count, slice_count):
    slice_index = _source_slice_storage_index(
        source_image,
        output_index,
        partition_count,
        slice_count,
    )
    partition_index = _source_partition_storage_index(
        source_image,
        output_index,
        partition_count,
        slice_index,
    )
    return {
        "Actual3DImagePartNumber": partition_index,
        "Actual3DImaPartNumber": partition_index,
        "AnatomicalPartitionNo": partition_index,
        "AnatomicalSliceNo": slice_index,
        "ChronSliceNo": slice_index,
        "NumberInSeries": output_index + 1,
        "ProtocolSliceNumber": slice_index,
        "SliceNo": slice_index,
        "IsmrmrdSliceNo": slice_index,
    }


def _source_slice_storage_index(source_image, output_index, partition_count, slice_count):
    if partition_count > 1 and slice_count <= 1:
        return SCANNER_PARTITION_INDEX

    source_value = _source_slice_index_hint(source_image)
    if source_value >= 0:
        return source_value
    if 0 <= output_index < max(slice_count, 1):
        return output_index
    return SCANNER_PARTITION_INDEX


def _source_partition_storage_index(
    source_image,
    output_index,
    partition_count,
    slice_index,
):
    if partition_count <= 1:
        return SCANNER_PARTITION_INDEX

    for key in (
        "Actual3DImagePartNumber",
        "Actual3DImaPartNumber",
        "AnatomicalPartitionNo",
    ):
        value = _source_storage_int(source_image, key)
        if value is not None and 0 <= value < partition_count:
            return value

    if 0 <= output_index < partition_count:
        return output_index
    if 0 <= slice_index < partition_count:
        return slice_index
    return SCANNER_PARTITION_INDEX


def _source_partition_count_hint(images):
    for image in images:
        value = _meta_int(_meta_from_image(image), "partition_count")
        if value is not None and value > 0:
            return value
    return 1


def _source_slice_count_hint(images):
    for image in images:
        meta = _meta_from_image(image)
        for key in ("slice_count", "NumberOfSlices"):
            value = _meta_int(meta, key)
            if value is not None and value > 0:
                return value

    inferred = []
    for image in images:
        slice_index = _source_slice_index_hint(image)
        if slice_index >= 0:
            inferred.append(slice_index)

    inferred_count = 0
    if inferred:
        unique_indices = set(inferred)
        if 0 in unique_indices:
            inferred_count = max(unique_indices) + 1
        else:
            inferred_count = max(len(unique_indices), max(unique_indices))

    echo_count = _source_echo_count_hint(images)
    if echo_count > 1 and len(images) % echo_count == 0:
        inferred_count = max(inferred_count, len(images) // echo_count)

    if inferred_count > 0:
        return inferred_count

    for image in images:
        data_shape = np.asarray(image.data).shape
        if len(data_shape) >= 3:
            return max(1, int(data_shape[-3]))
    return 1


def _source_slice_index_hint(image):
    for key in ("SliceNo", "IsmrmrdSliceNo"):
        value = _source_storage_int(image, key)
        if value is not None and value >= 0:
            return value

    header_value = int(getattr(image.getHead(), "slice", -1))
    if header_value >= 0:
        return header_value

    for key in ("AnatomicalSliceNo", "ProtocolSliceNumber", "ChronSliceNo"):
        value = _source_storage_int(image, key)
        if value is not None and value >= 0:
            return value
    return -1


def _source_echo_count_hint(images):
    echo_numbers = {
        value
        for image in images
        for value in [_source_echo_number(image)]
        if value is not None and value >= 0
    }
    return len(echo_numbers)


def _source_storage_int(image, key):
    meta = _meta_from_image(image)
    value = _meta_int(meta, key)
    if value is not None:
        return value
    return _extract_minihead_long_value(_decode_ice_minihead(meta), key)


def _patch_original_ice_minihead(
    minihead_text,
    series_name,
    series_grouping,
    series_uid,
    sop_uid,
    storage_fields,
    is_series_end,
):
    current_text = minihead_text
    changed = False
    for key, value in (
        ("SeriesDescription", series_name),
        ("SequenceDescription", series_name),
        ("ProtocolName", series_name),
        ("SeriesNumberRangeNameUID", series_grouping),
        ("SeriesInstanceUID", series_uid),
        ("SOPInstanceUID", sop_uid),
    ):
        current_text, did_change = _replace_or_append_minihead_string_param(
            current_text,
            key,
            value,
        )
        changed = changed or did_change

    for minihead_key, storage_key in MINIHEAD_ORIGINAL_STORAGE_FIELD_SPECS:
        current_text, did_change = _replace_or_append_minihead_long_param(
            current_text,
            minihead_key,
            storage_fields.get(storage_key),
        )
        changed = changed or did_change

    for key in ("BIsSeriesEnd", "ConcatenationEnd"):
        current_text, did_change = _replace_or_append_minihead_bool_param(
            current_text,
            key,
            is_series_end,
        )
        changed = changed or did_change

    return current_text, changed


def _nifti_to_mrd_images(
    nifti_path,
    anchor_image,
    series_index,
    series_name,
    image_type_token,
    output_id,
    units,
):
    nifti = nib.load(str(nifti_path))
    data_xyz = np.asarray(nifti.get_fdata(dtype=np.float32), dtype=np.float32)
    data_xyz = np.squeeze(data_xyz)
    if data_xyz.ndim == 2:
        data_xyz = data_xyz[:, :, np.newaxis]
    if data_xyz.ndim != 3:
        raise ValueError(f"QSMxT output must be 3D, got {nifti_path}: {data_xyz.shape}")

    data_zyx = np.transpose(data_xyz, (2, 1, 0))
    display_data_zyx, display_meta = _scanner_display_volume(data_zyx, output_id, units)
    display_data_xyz = np.transpose(display_data_zyx, (2, 1, 0))
    slice_count = int(data_zyx.shape[0])
    volume_fov = _nifti_field_of_view(nifti, data_xyz.shape)
    output_images = []

    for slice_index in range(slice_count):
        slice_data = display_data_zyx[slice_index:slice_index + 1]
        output = ismrmrd.Image.from_array(slice_data.astype(np.uint16), transpose=False)

        header = copy.deepcopy(anchor_image.getHead())
        out_header = output.getHead()
        header.data_type = output.data_type
        header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
        header.image_series_index = int(series_index)
        header.image_index = slice_index + 1
        header.slice = slice_index
        header.contrast = 0
        _set_header_sequence_field(
            header,
            "matrix_size",
            [int(value) for value in out_header.matrix_size],
        )
        if volume_fov:
            _set_header_sequence_field(header, "field_of_view", volume_fov)
        _set_header_sequence_field(
            header,
            "position",
            _slice_position_from_affine(nifti, data_xyz.shape, slice_index, anchor_image),
        )

        output.setHead(header)
        output.image_series_index = int(series_index)
        output.attribute_string = _output_meta(
            anchor_image,
            header,
            series_index,
            series_name,
            image_type_token,
            output_id,
            units,
            display_data_xyz,
            nifti_path,
            slice_index,
            slice_count,
            display_meta,
        ).serialize()
        output_images.append(output)

    return output_images


def _nifti_field_of_view(nifti, shape_xyz):
    zooms = nifti.header.get_zooms()[:3]
    if len(zooms) != 3:
        return None
    return [
        float(zooms[0]) * int(shape_xyz[0]),
        float(zooms[1]) * int(shape_xyz[1]),
        float(zooms[2]) * int(shape_xyz[2]),
    ]


def _slice_position_from_affine(nifti, shape_xyz, slice_index, fallback_image):
    try:
        voxel = np.asarray(
            [
                (int(shape_xyz[0]) - 1) / 2.0,
                (int(shape_xyz[1]) - 1) / 2.0,
                float(slice_index),
                1.0,
            ],
            dtype=float,
        )
        position = np.asarray(nifti.affine, dtype=float) @ voxel
        if np.all(np.isfinite(position[:3])):
            return [float(value) for value in position[:3]]
    except Exception:
        pass
    return [float(value) for value in fallback_image.getHead().position]


def _image_to_nifti_volume(image, kind):
    data = np.asarray(image.data)
    if data.ndim == 4:
        if kind == "magnitude":
            if np.iscomplexobj(data):
                vol_zyx = np.sqrt(np.sum(np.abs(data) ** 2, axis=0))
            elif data.shape[0] > 1:
                vol_zyx = np.sqrt(np.sum(data.astype(np.float32) ** 2, axis=0))
            else:
                vol_zyx = data[0]
        else:
            first_channel = data[0]
            vol_zyx = np.angle(first_channel) if np.iscomplexobj(first_channel) else first_channel
    else:
        squeezed = np.squeeze(data)
        if squeezed.ndim == 2:
            vol_zyx = squeezed[np.newaxis, :, :]
        elif squeezed.ndim == 3:
            vol_zyx = squeezed
        else:
            raise ValueError(f"Unsupported MRD image data shape: {data.shape}")

    vol_xyz = np.transpose(np.asarray(vol_zyx, dtype=np.float32), (2, 1, 0))
    return vol_xyz, _affine_from_image(image, vol_xyz.shape)


def _images_to_nifti_volume(images, kind):
    ordered_images = _sorted_stack_images(images)
    if len(ordered_images) == 1:
        return _image_to_nifti_volume(ordered_images[0], kind)

    volumes = []
    for image in ordered_images:
        volume, _ = _image_to_nifti_volume(image, kind)
        volumes.append(volume)

    stacked = np.concatenate(volumes, axis=2)
    return stacked, _affine_from_image_stack(ordered_images, stacked.shape)


def _sorted_stack_images(images):
    return sorted(images, key=_stack_image_sort_key)


def _stack_image_sort_key(image):
    for key in (
        "Actual3DImagePartNumber",
        "Actual3DImaPartNumber",
        "AnatomicalPartitionNo",
        "SliceNo",
        "IsmrmrdSliceNo",
    ):
        value = _source_storage_int(image, key)
        if value is not None and value >= 0:
            return (0, value, int(getattr(image.getHead(), "image_index", 0)))

    header = image.getHead()
    return (
        1,
        int(getattr(header, "slice", 0)),
        int(getattr(header, "image_index", 0)),
    )


def _phase_to_qsmxt_counts(phase_volume, phase_wrap):
    phase = np.asarray(phase_volume, dtype=np.float32)
    if not phase.size:
        return phase
    finite = phase[np.isfinite(phase)]
    if finite.size == 0:
        return phase

    data_min = float(np.min(finite))
    data_max = float(np.max(finite))
    if -np.pi <= data_min and data_max <= np.pi:
        return phase

    try:
        phase_wrap = float(phase_wrap)
    except (TypeError, ValueError):
        phase_wrap = 4096.0
    if phase_wrap <= 0 or abs(phase_wrap - 4096.0) < 1e-6:
        return phase
    logging.info(
        "Rescaling raw phase counts from wrap %.6g to QSMxT 4096-count convention",
        phase_wrap,
    )
    return phase * (4096.0 / phase_wrap)


def _affine_from_image(image, shape_xyz):
    header = image.getHead()
    read_dir = _normalized_or_default(header.read_dir, (1.0, 0.0, 0.0))
    phase_dir = _normalized_or_default(header.phase_dir, (0.0, 1.0, 0.0))
    slice_dir = _normalized_or_default(header.slice_dir, (0.0, 0.0, 1.0))
    fov = np.asarray(header.field_of_view, dtype=float)
    dims = np.asarray(shape_xyz, dtype=float)
    voxel = np.divide(fov, dims, out=np.ones(3, dtype=float), where=dims > 0)
    position = np.asarray(header.position, dtype=float)
    origin = (
        position
        - read_dir * voxel[0] * (shape_xyz[0] - 1) / 2.0
        - phase_dir * voxel[1] * (shape_xyz[1] - 1) / 2.0
        - slice_dir * voxel[2] * (shape_xyz[2] - 1) / 2.0
    )
    affine = np.eye(4, dtype=float)
    affine[:3, 0] = read_dir * voxel[0]
    affine[:3, 1] = phase_dir * voxel[1]
    affine[:3, 2] = slice_dir * voxel[2]
    affine[:3, 3] = origin
    return affine


def _affine_from_image_stack(images, shape_xyz):
    first_header = images[0].getHead()
    read_dir = _normalized_or_default(first_header.read_dir, (1.0, 0.0, 0.0))
    phase_dir = _normalized_or_default(first_header.phase_dir, (0.0, 1.0, 0.0))
    slice_dir = _normalized_or_default(first_header.slice_dir, (0.0, 0.0, 1.0))
    fov = np.asarray(first_header.field_of_view, dtype=float)
    dims = np.asarray(shape_xyz, dtype=float)
    voxel = np.divide(fov, dims, out=np.ones(3, dtype=float), where=dims > 0)

    slice_step = slice_dir * voxel[2]
    positions = [
        np.asarray(image.getHead().position, dtype=float)
        for image in images
    ]
    if len(positions) > 1:
        candidate_step = (positions[-1] - positions[0]) / float(len(positions) - 1)
        if np.linalg.norm(candidate_step) > 0:
            slice_step = candidate_step

    first_position = positions[0]
    origin = (
        first_position
        - read_dir * voxel[0] * (shape_xyz[0] - 1) / 2.0
        - phase_dir * voxel[1] * (shape_xyz[1] - 1) / 2.0
    )
    affine = np.eye(4, dtype=float)
    affine[:3, 0] = read_dir * voxel[0]
    affine[:3, 1] = phase_dir * voxel[1]
    affine[:3, 2] = slice_step
    affine[:3, 3] = origin
    return affine


def _b0_dir_for_affine(affine, settings):
    override = settings.get("b0_dir_override")
    if override is not None:
        return override, "config"
    return _b0_dir_from_affine(affine), "nifti_affine"


def _b0_dir_from_affine(affine):
    basis = np.asarray(affine, dtype=float)[:3, :3]
    normalized_basis = np.zeros((3, 3), dtype=float)
    for index in range(3):
        column = basis[:, index]
        norm = float(np.linalg.norm(column))
        if norm <= 0 or not np.isfinite(norm):
            return DEFAULT_B0_DIR
        normalized_basis[:, index] = column / norm

    try:
        image_b0 = np.linalg.solve(
            normalized_basis,
            np.asarray(DEFAULT_B0_DIR, dtype=float),
        )
    except np.linalg.LinAlgError:
        return DEFAULT_B0_DIR
    image_b0 = _normalized_or_default(image_b0, DEFAULT_B0_DIR)
    image_b0[np.abs(image_b0) < 1e-12] = 0.0
    return tuple(float(value) for value in image_b0)


def _settings_from_config(config, metadata=None):
    params = _config_parameters(config)
    configured_field_strength = _config_float_or_none(params, "fieldstrength")
    metadata_field_strength = _metadata_field_strength(metadata)
    if configured_field_strength is not None:
        field_strength = configured_field_strength
        field_strength_source = "config"
    elif metadata_field_strength is not None:
        field_strength = metadata_field_strength
        field_strength_source = "metadata"
    else:
        field_strength = DEFAULT_FIELD_STRENGTH_T
        field_strength_source = "default"

    configured_b0_dir = _config_vector_or_none(params, "b0dir", length=3)
    if configured_b0_dir is not None:
        b0_dir_source = "config"
    else:
        b0_dir_source = "nifti_affine"

    return {
        "send_original": _config_bool(params, "sendoriginal", False),
        "send_outputs": str(params.get("sendoutputs", "qsm") or "qsm"),
        "max_echoes": _config_int(params, "maxechoes", 0),
        "echo_times_ms": _config_text(params, "echotimesms", ""),
        "echo_time_ms": _config_float(params, "echotimems", DEFAULT_ECHO_TIME_MS),
        "echo_spacing_ms": _config_float(params, "echospacingms", DEFAULT_ECHO_SPACING_MS),
        "field_strength_t": field_strength,
        "field_strength_source": field_strength_source,
        "b0_dir_override": configured_b0_dir,
        "b0_dir_source": b0_dir_source,
        "phase_wrap": _config_float(params, "phasewrap", 4096.0),
        "qsmxt_binary": _config_text(params, "qsmxtbinary", ""),
        "qsm_algorithm": _optional_choice(params, "qsmalgorithm"),
        "unwrapping_algorithm": _optional_choice(params, "unwrappingalgorithm"),
        "bf_algorithm": _optional_choice(params, "bfalgorithm"),
        "mask_preset": _optional_choice(params, "maskpreset"),
        "qsm_reference": _optional_choice(params, "qsmreference"),
        "no_qsm": _config_bool(params, "noqsm", False),
        "do_swi": _config_bool(params, "doswi", False),
        "do_t2starmap": _config_bool(params, "dot2starmap", False),
        "do_r2starmap": _config_bool(params, "dor2starmap", False),
        "inhomogeneity_correction": _config_bool(params, "inhomogeneitycorrection", True),
    }


def _group_images_by_series(images):
    groups = []
    by_key = {}
    for order, image in enumerate(images):
        key = _series_group_key(image)
        group = by_key.get(key)
        if group is None:
            group = {
                "key": key,
                "images": [],
                "order": order,
            }
            by_key[key] = group
            groups.append(group)
        group["images"].append(image)

    for group in groups:
        group["kind"] = _classify_series(group["images"])
        group["name"] = _source_series_name(group["images"][0])
        logging.info(
            "Input series %s classified as %s with %d image(s)",
            group["name"] or group["key"],
            group["kind"],
            len(group["images"]),
        )
    return groups


def _select_magnitude_phase_groups(groups):
    magnitude_groups = [group for group in groups if group["kind"] == "magnitude"]
    phase_groups = [group for group in groups if group["kind"] == "phase"]

    if not phase_groups and len(groups) == 2:
        sorted_groups = sorted(groups, key=_series_signal_range)
        sorted_groups[0]["kind"] = "phase"
        sorted_groups[1]["kind"] = "magnitude"
        phase_groups = [sorted_groups[0]]
        magnitude_groups = [sorted_groups[1]]
        logging.warning(
            "No explicit phase metadata found; using value range fallback: "
            "%s=phase, %s=magnitude",
            sorted_groups[0]["name"],
            sorted_groups[1]["name"],
        )

    if not magnitude_groups:
        raise ValueError("No magnitude image series found in MRD stream")
    if not phase_groups:
        raise ValueError("No phase image series found in MRD stream")
    if len(magnitude_groups) > 1 or len(phase_groups) > 1:
        logging.warning(
            "Multiple magnitude/phase series found; using first pair: %d magnitude, %d phase",
            len(magnitude_groups),
            len(phase_groups),
        )

    return (
        sorted(magnitude_groups, key=lambda group: group["order"])[0],
        sorted(phase_groups, key=lambda group: group["order"])[0],
    )


def _echo_image_groups(images):
    sorted_images = _sorted_series_images(images)
    echo_numbers = [_source_echo_number(image) for image in sorted_images]
    if any(value is not None for value in echo_numbers):
        return _group_images_by_ordered_key(
            sorted_images,
            [
                ("echo", value) if value is not None else ("image", index)
                for index, value in enumerate(echo_numbers)
            ],
        )

    contrasts = [
        int(getattr(image.getHead(), "contrast", 0))
        for image in sorted_images
    ]
    if len(set(contrasts)) > 1:
        return _group_images_by_ordered_key(
            sorted_images,
            [("contrast", value) for value in contrasts],
        )

    return [[image] for image in sorted_images]


def _group_images_by_ordered_key(images, keys):
    groups = []
    by_key = {}
    for image, key in zip(images, keys):
        group = by_key.get(key)
        if group is None:
            group = []
            by_key[key] = group
            groups.append(group)
        group.append(image)
    return groups


def _source_echo_number(image):
    for key in ("EchoNumber", "EchoNo", "EchoIndex"):
        value = _source_storage_int(image, key)
        if value is not None and value >= 0:
            return value
    return None


def _classify_series(images):
    image = images[0]
    image_type = int(getattr(image, "image_type", 0))
    if image_type == int(getattr(ismrmrd, "IMTYPE_PHASE", 2)):
        return "phase"

    meta = _meta_from_image(image)
    text = " ".join(
        [
            _source_series_name(image),
            _meta_text(meta, "ComplexImageComponent"),
            _meta_text(meta, "ImageType"),
            _meta_text(meta, "DicomImageType"),
            _meta_text(meta, "ImageTypeValue3"),
            _meta_text(meta, "ImageTypeValue4"),
        ]
    ).upper()

    if re.search(r"(^|[_\\\s-])(PHASE|PHA|P)($|[_\\\s-])", text) or text.endswith("_PHA"):
        return "phase"
    if re.search(r"(^|[_\\\s-])(MAG|MAGNITUDE|M)($|[_\\\s-])", text):
        return "magnitude"
    if image_type in (0, int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))):
        return "magnitude"
    return "unknown"


def _series_signal_range(group):
    data = np.asarray(group["images"][0].data)
    summary = np.abs(data) if np.iscomplexobj(data) else data
    return float(np.nanmax(summary) - np.nanmin(summary))


def _series_group_key(image):
    meta = _meta_from_image(image)
    return (
        int(image.getHead().image_series_index),
        _meta_text(meta, "SeriesInstanceUID"),
        _source_series_name(image),
    )


def _sorted_series_images(images):
    return sorted(images, key=_image_sort_key)


def _image_sort_key(image):
    header = image.getHead()
    return (
        int(getattr(header, "image_index", 0)),
        int(getattr(header, "contrast", 0)),
        int(getattr(header, "phase", 0)),
        int(getattr(header, "repetition", 0)),
        int(getattr(header, "set", 0)),
    )


def _echo_times_seconds(n_echoes, phase_images, metadata, settings):
    configured = _parse_float_list(settings.get("echo_times_ms", ""))
    if configured:
        values_ms = configured
        source = "config"
    else:
        values_ms = _metadata_echo_times_ms(metadata)
        source = "metadata"
        if not values_ms:
            values_ms = _image_echo_times_ms(phase_images)
            source = "image-meta"

    if values_ms:
        values_ms = list(values_ms)
        while len(values_ms) < n_echoes:
            values_ms.append(values_ms[-1] + settings["echo_spacing_ms"])
        selected_ms = [float(value) for value in values_ms[:n_echoes]]
        logging.info(
            "Resolved QSMxT echo times from %s: %s ms",
            source,
            [round(value, 6) for value in selected_ms],
        )
        return [value / 1000.0 for value in selected_ms]

    logging.warning(
        "No EchoTime found in MRD metadata or config; using %.3f ms + %.3f ms spacing",
        settings["echo_time_ms"],
        settings["echo_spacing_ms"],
    )
    selected_ms = [
        (settings["echo_time_ms"] + index * settings["echo_spacing_ms"]) / 1000.0
        for index in range(n_echoes)
    ]
    logging.info(
        "Resolved QSMxT echo times from fallback: %s ms",
        [round(value * 1000.0, 6) for value in selected_ms],
    )
    return selected_ms


def _metadata_echo_times_ms(metadata):
    sequence = getattr(metadata, "sequenceParameters", None)
    te = getattr(sequence, "TE", None)
    if te is None:
        return []
    if isinstance(te, (list, tuple)):
        values = [float(value) for value in te]
    else:
        values = [float(te)]
    if values and max(values) < 1.0:
        return [value * 1000.0 for value in values]
    return values


def _image_echo_times_ms(images):
    values = []
    for image in images:
        meta = _meta_from_image(image)
        value = _meta_float(meta, "EchoTime")
        if value is None:
            continue
        values.append(value * 1000.0 if value < 1.0 else value)
    return values


def _metadata_field_strength(metadata):
    try:
        value = metadata.acquisitionSystemInformation.systemFieldStrength_T
    except AttributeError:
        return None
    return float(value) if value is not None else None


def _metadata_protocol_name(metadata):
    try:
        value = metadata.measurementInformation.protocolName
    except AttributeError:
        return ""
    return str(value or "")


def _output_meta(
    source_image,
    header,
    series_index,
    series_name,
    image_type_token,
    output_id,
    units,
    output_data,
    nifti_path,
    slice_index,
    slice_count,
    display_meta,
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
        slice_index,
    )
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"
    center, width = _window_center_width(output_data)
    slice_number = str(int(slice_index))
    image_comment = _scanner_display_comment(series_name, display_meta)

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "QSMXT"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = image_comment
    meta["ImageComment"] = image_comment
    meta["SeriesNumberRangeNameUID"] = _derived_series_grouping(series_name, series_index)
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SequenceDescriptionAdditional"] = "openrecon"
    meta["Keep_image_geometry"] = "0"
    meta["partition_count"] = "1"
    slice_count_text = str(int(slice_count))
    meta["slice_count"] = slice_count_text
    meta["NumberOfSlices"] = slice_count_text
    meta["ImagesInAcquisition"] = slice_count_text
    meta["NumberInSeries"] = str(int(slice_index) + 1)
    meta["SliceNo"] = slice_number
    meta["IsmrmrdSliceNo"] = slice_number
    meta["AnatomicalSliceNo"] = slice_number
    meta["ChronSliceNo"] = slice_number
    meta["ProtocolSliceNumber"] = slice_number
    meta["Actual3DImagePartNumber"] = "0"
    meta["Actual3DImaPartNumber"] = "0"
    meta["AnatomicalPartitionNo"] = "0"
    meta["QSMxTOutput"] = output_id
    meta["QSMxTUnits"] = units
    meta["QSMxTSourceFile"] = str(nifti_path)
    meta["QSMxTDisplayScale"] = _format_display_number(display_meta["scale"])
    meta["QSMxTDisplayOffset"] = _format_display_number(display_meta["offset"])
    meta["QSMxTDisplayFormula"] = display_meta["formula"]
    meta["QSMxTDisplayInputMin"] = f"{float(display_meta['input_min']):.6g}"
    meta["QSMxTDisplayInputMax"] = f"{float(display_meta['input_max']):.6g}"
    meta["QSMxTDisplayMin"] = str(int(display_meta["display_min"]))
    meta["QSMxTDisplayMax"] = str(int(display_meta["display_max"]))
    meta["QSMxTDisplayClippedVoxels"] = str(int(display_meta["clipped_voxels"]))
    meta["WindowCenter"] = f"{float(center):.6g}"
    meta["WindowWidth"] = f"{float(width):.6g}"
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


def _validate_output_images(output_images, input_images):
    input_series_indices = {
        int(image.getHead().image_series_index)
        for image in input_images
    }
    errors = []
    series_records = {}
    for index, image in enumerate(output_images):
        header = image.getHead()
        series_index = int(header.image_series_index)
        meta = _meta_from_image(image)
        if series_index in input_series_indices:
            errors.append(f"image {index} reuses input series index {series_index}")
        if _meta_text(meta, "IceMiniHead"):
            errors.append(f"image {index} keeps source IceMiniHead")
        if _meta_text(meta, "ImageTypeValue3"):
            errors.append(f"image {index} keeps unsafe ImageTypeValue3")
        if not _meta_text(meta, "SeriesInstanceUID"):
            errors.append(f"image {index} is missing SeriesInstanceUID")
        if not _meta_text(meta, "SOPInstanceUID"):
            errors.append(f"image {index} is missing SOPInstanceUID")
        if np.asarray(image.data).dtype != np.uint16:
            errors.append(f"image {index} data is not uint16")
        if image.data.size:
            data_min = int(np.min(image.data))
            data_max = int(np.max(image.data))
            if data_min < SCANNER_DISPLAY_MIN or data_max > SCANNER_DISPLAY_MAX:
                errors.append(
                    f"image {index} data range {data_min}..{data_max} is outside "
                    f"{SCANNER_DISPLAY_MIN}..{SCANNER_DISPLAY_MAX}"
                )
        for key in ("QSMxTDisplayScale", "QSMxTDisplayOffset", "QSMxTDisplayFormula"):
            if not _meta_text(meta, key):
                errors.append(f"image {index} is missing {key}")

        slice_index = _meta_int(meta, "SliceNo")
        if slice_index is None:
            errors.append(f"image {index} is missing SliceNo")
        elif int(getattr(header, "slice", 0)) != slice_index:
            errors.append(
                f"image {index} header slice {int(getattr(header, 'slice', 0))} "
                f"does not match SliceNo {slice_index}"
            )

        expected_count = _meta_int(meta, "ImagesInAcquisition")
        number_of_slices = _meta_int(meta, "NumberOfSlices")
        if expected_count is None:
            errors.append(f"image {index} is missing ImagesInAcquisition")
        if number_of_slices is None:
            errors.append(f"image {index} is missing NumberOfSlices")
        if expected_count is not None and number_of_slices is not None:
            if expected_count != number_of_slices:
                errors.append(
                    f"image {index} ImagesInAcquisition={expected_count} "
                    f"does not match NumberOfSlices={number_of_slices}"
                )

        record = series_records.setdefault(
            series_index,
            {"expected": expected_count, "slices": []},
        )
        if record["expected"] is None:
            record["expected"] = expected_count
        elif expected_count is not None and record["expected"] != expected_count:
            errors.append(
                f"series {series_index} mixes image counts "
                f"{record['expected']} and {expected_count}"
            )
        if slice_index is not None:
            record["slices"].append(slice_index)

    for series_index, record in series_records.items():
        expected_count = record["expected"]
        slice_indices = record["slices"]
        if expected_count is not None and len(slice_indices) != expected_count:
            errors.append(
                f"series {series_index} has {len(slice_indices)} image(s), "
                f"expected {expected_count}"
            )
        if len(set(slice_indices)) != len(slice_indices):
            errors.append(f"series {series_index} has duplicate SliceNo values")
    if errors:
        raise ValueError("Invalid qsmxt output image contract: " + "; ".join(errors))


def _validate_original_passthrough_images(output_images, input_images):
    input_series_indices = {
        int(image.getHead().image_series_index)
        for image in input_images
    }
    errors = []
    series_records = {}
    for index, image in enumerate(output_images):
        header = image.getHead()
        series_index = int(header.image_series_index)
        meta = _meta_from_image(image)
        if series_index in input_series_indices:
            errors.append(f"original image {index} reuses input series index {series_index}")
        if _meta_text(meta, "Keep_image_geometry") != "1":
            errors.append(f"original image {index} does not set Keep_image_geometry=1")
        if not _meta_text(meta, "SeriesInstanceUID"):
            errors.append(f"original image {index} is missing SeriesInstanceUID")
        if not _meta_text(meta, "SOPInstanceUID"):
            errors.append(f"original image {index} is missing SOPInstanceUID")

        partition_count = _meta_int(meta, "partition_count") or 1
        slice_count = _meta_int(meta, "slice_count") or 1
        image_count = _meta_int(meta, "ImagesInAcquisition")
        number_of_slices = _meta_int(meta, "NumberOfSlices")
        number_in_series = _meta_int(meta, "NumberInSeries")
        slice_index = _meta_int(meta, "SliceNo")
        if image_count is None:
            errors.append(f"original image {index} is missing ImagesInAcquisition")
        if number_of_slices is None:
            errors.append(f"original image {index} is missing NumberOfSlices")
        elif number_of_slices != slice_count:
            errors.append(
                f"original image {index} NumberOfSlices={number_of_slices} "
                f"does not match slice_count={slice_count}"
            )
        if number_in_series is None:
            errors.append(f"original image {index} is missing NumberInSeries")
        elif image_count is not None and not (1 <= number_in_series <= image_count):
            errors.append(
                f"original image {index} NumberInSeries={number_in_series} "
                f"outside [1..{image_count}]"
            )
        if slice_index is None:
            errors.append(f"original image {index} is missing SliceNo")
        elif not (0 <= slice_index < slice_count):
            errors.append(
                f"original image {index} has SliceNo {slice_index} "
                f"outside [0..{slice_count})"
            )
        elif int(getattr(header, "slice", 0)) != slice_index:
            errors.append(
                f"original image {index} header slice "
                f"{int(getattr(header, 'slice', 0))} does not match SliceNo {slice_index}"
            )

        record = series_records.setdefault(
            series_index,
            {
                "expected": image_count,
                "count": 0,
                "slice_count": slice_count,
                "slices": [],
            },
        )
        record["count"] += 1
        if record["expected"] is None:
            record["expected"] = image_count
        elif image_count is not None and record["expected"] != image_count:
            errors.append(
                f"original series {series_index} mixes image counts "
                f"{record['expected']} and {image_count}"
            )
        if record["slice_count"] != slice_count:
            errors.append(
                f"original series {series_index} mixes slice counts "
                f"{record['slice_count']} and {slice_count}"
            )
        if slice_index is not None:
            record["slices"].append(slice_index)

        if partition_count > 1:
            partition_index = _meta_int(meta, "Actual3DImagePartNumber")
            if partition_index is None or not (0 <= partition_index < partition_count):
                errors.append(
                    "original image "
                    f"{index} has partition {partition_index} outside "
                    f"[0..{partition_count})"
                )

            minihead_text = _decode_ice_minihead(meta)
            if minihead_text:
                for minihead_key, storage_key in (
                    ("Actual3DImagePartNumber", "Actual3DImagePartNumber"),
                    ("Actual3DImaPartNumber", "Actual3DImagePartNumber"),
                    ("AnatomicalPartitionNo", "AnatomicalPartitionNo"),
                    ("SliceNo", "SliceNo"),
                ):
                    minihead_value = _extract_minihead_long_value(
                        minihead_text,
                        minihead_key,
                    )
                    meta_value = _meta_int(meta, storage_key)
                    if minihead_value is not None and minihead_value != meta_value:
                        errors.append(
                            "original image "
                            f"{index} has MiniHead {minihead_key}={minihead_value}, "
                            f"Meta {storage_key}={meta_value}"
                        )
                expected_end = image_count is not None and number_in_series == image_count
                for minihead_key in ("BIsSeriesEnd", "ConcatenationEnd"):
                    minihead_value = _extract_minihead_bool_value(
                        minihead_text,
                        minihead_key,
                    )
                    if minihead_value is not None and minihead_value != expected_end:
                        errors.append(
                            "original image "
                            f"{index} has MiniHead {minihead_key}={minihead_value}, "
                            f"expected {expected_end}"
                        )
    for series_index, record in series_records.items():
        expected = record["expected"]
        if expected is not None and record["count"] != expected:
            errors.append(
                f"original series {series_index} has {record['count']} image(s), "
                f"expected {expected}"
            )
        if record["slices"] and max(record["slices"]) >= record["slice_count"]:
            errors.append(
                f"original series {series_index} has slice index "
                f"{max(record['slices'])} outside slice_count={record['slice_count']}"
            )
    if errors:
        raise ValueError(
            "Invalid qsmxt original passthrough contract: " + "; ".join(errors)
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


def _config_bool(params, key, default=False):
    return _coerce_bool(params.get(key, default), default)


def _config_float(params, key, default=0.0):
    value = params.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _config_float_or_none(params, key):
    if key not in params:
        return None
    value = params.get(key)
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _config_int(params, key, default=0):
    value = params.get(key, default)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _config_text(params, key, default=""):
    value = params.get(key, default)
    return str(value if value is not None else default).strip()


def _optional_choice(params, key):
    value = _config_text(params, key, "")
    return "" if value in {"", "default", "none"} else value


def _config_vector(params, key, default, length):
    values = _parse_float_list(params.get(key, ""))
    if len(values) == length:
        return tuple(float(value) for value in values)
    return tuple(float(value) for value in default)


def _config_vector_or_none(params, key, length):
    if key not in params:
        return None
    values = _parse_float_list(params.get(key, ""))
    if len(values) == length:
        return tuple(float(value) for value in values)
    return None


def _parse_float_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        parts = value
    else:
        parts = re.split(r"[,;\s]+", str(value).strip())
    values = []
    for part in parts:
        if part == "":
            continue
        try:
            values.append(float(part))
        except (TypeError, ValueError):
            return []
    return values


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _append_optional_arg(cmd, flag, value):
    if value:
        cmd.extend([flag, str(value)])


def _selected_output_ids(value):
    text = str(value or "qsm").strip().lower()
    if text == "all":
        return list(QSMXT_OUTPUTS.keys())
    selected = []
    for part in re.split(r"[,;\s]+", text):
        if not part:
            continue
        if part not in QSMXT_OUTPUTS:
            logging.warning("Ignoring unknown QSMxT output selection: %s", part)
            continue
        selected.append(part)
    return selected or ["qsm"]


def _reserve_series_index(used, preferred):
    series_index = int(preferred)
    while series_index in used:
        series_index += 1
    used.add(series_index)
    return series_index


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def _nifti_sidecar_path(path):
    text = str(path)
    if text.endswith(".nii.gz"):
        return Path(text[:-7] + ".json")
    if text.endswith(".nii"):
        return Path(text[:-4] + ".json")
    return Path(text + ".json")


def _sanitize_bids_label(value):
    return re.sub(r"[^A-Za-z0-9]+", "", str(value or ""))[:32]


def _normalized_or_default(values, default):
    vector = np.asarray(values, dtype=float)
    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        return np.asarray(default, dtype=float)
    return vector / norm


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


def _scanner_display_volume(data, output_id, units):
    values = np.asarray(data, dtype=np.float32)
    finite = values[np.isfinite(values)]

    if output_id == "mask" or units == "binary":
        display = np.where(np.nan_to_num(values, nan=0.0) > 0, SCANNER_DISPLAY_MAX, 0)
        display = display.astype(np.uint16, copy=False)
        return display, {
            "scale": float(SCANNER_DISPLAY_MAX),
            "offset": 0.0,
            "units": units,
            "formula": f"{units} = display / {SCANNER_DISPLAY_MAX}",
            "clipped_voxels": 0,
            "input_min": float(np.min(finite)) if finite.size else 0.0,
            "input_max": float(np.max(finite)) if finite.size else 0.0,
            "display_min": int(np.min(display)) if display.size else 0,
            "display_max": int(np.max(display)) if display.size else 0,
        }

    input_min = float(np.min(finite)) if finite.size else 0.0
    input_max = float(np.max(finite)) if finite.size else 0.0
    offset = float(SCANNER_DISPLAY_CENTER) if _scanner_display_needs_offset(
        output_id,
        input_min,
    ) else 0.0
    scale = _scanner_display_scale(input_min, input_max, offset)
    cleaned = np.nan_to_num(values, nan=0.0, posinf=input_max, neginf=input_min)
    scaled = cleaned * scale + offset
    clipped_voxels = int(
        np.count_nonzero(
            (scaled < SCANNER_DISPLAY_MIN) | (scaled > SCANNER_DISPLAY_MAX)
        )
    )
    display = np.clip(np.rint(scaled), SCANNER_DISPLAY_MIN, SCANNER_DISPLAY_MAX)
    display = display.astype(np.uint16, copy=False)
    formula = _scanner_display_formula(units, offset, scale)

    return display, {
        "scale": float(scale),
        "offset": float(offset),
        "units": units,
        "formula": formula,
        "clipped_voxels": clipped_voxels,
        "input_min": input_min,
        "input_max": input_max,
        "display_min": int(np.min(display)) if display.size else 0,
        "display_max": int(np.max(display)) if display.size else 0,
    }


def _scanner_display_needs_offset(output_id, input_min):
    return output_id == "qsm" or input_min < 0.0


def _scanner_display_scale(input_min, input_max, offset):
    limits = []
    if input_max > 0.0:
        limits.append((SCANNER_DISPLAY_MAX - offset) / input_max)
    if input_min < 0.0:
        limits.append((SCANNER_DISPLAY_MIN - offset) / input_min)
    max_scale = min(limits) if limits else SCANNER_DISPLAY_SCALE_FACTORS[0]
    if max_scale <= 0.0 or not np.isfinite(max_scale):
        max_scale = SCANNER_DISPLAY_SCALE_FACTORS[-1]

    for factor in SCANNER_DISPLAY_SCALE_FACTORS:
        if factor <= max_scale + 1e-9:
            return factor
    return SCANNER_DISPLAY_SCALE_FACTORS[-1]


def _scanner_display_formula(units, offset, scale):
    units_text = units or "value"
    scale_text = _format_display_number(scale)
    if offset:
        offset_text = _format_display_number(offset)
        return f"{units_text} = (display - {offset_text}) / {scale_text}"
    return f"{units_text} = display / {scale_text}"


def _scanner_display_comment(series_name, display_meta):
    return (
        f"{series_name}; scanner display uint16 0-{SCANNER_DISPLAY_MAX}; "
        f"{display_meta['formula']}"
    )


def _format_display_number(value):
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6g}"


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


def _meta_float(meta, key):
    value = _meta_text(meta, key)
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _meta_int(meta, key):
    value = _meta_text(meta, key)
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _copy_meta(meta):
    try:
        return ismrmrd.Meta.deserialize(meta.serialize())
    except Exception:
        return copy.deepcopy(meta)


def _decode_ice_minihead(meta):
    try:
        encoded = meta.get("IceMiniHead")
        if isinstance(encoded, (list, tuple)):
            encoded = encoded[0] if encoded else ""
        if not encoded:
            return ""
        return base64.b64decode(str(encoded)).decode("utf-8")
    except Exception:
        return ""


def _encode_ice_minihead(minihead_text):
    return base64.b64encode(minihead_text.encode("utf-8")).decode("ascii")


def _extract_minihead_long_value(minihead_text, name):
    if not minihead_text:
        return None
    match = re.search(
        rf'<ParamLong\."{re.escape(name)}">\s*\{{\s*(-?\d*)\s*\}}',
        minihead_text,
    )
    if not match:
        return None
    value = match.group(1).strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _extract_minihead_bool_value(minihead_text, name):
    if not minihead_text:
        return None
    match = re.search(
        rf'<ParamBool\."{re.escape(name)}">\s*\{{\s*"?([^"}}]*)"?\s*\}}',
        minihead_text,
    )
    if not match:
        return None
    value = match.group(1).strip().lower()
    if value in {"true", "1"}:
        return True
    if value in {"false", "0"}:
        return False
    return None


def _sanitize_minihead_value(value):
    text = str(value if value is not None else "").strip()
    return text.replace('"', "'").replace("\r", " ").replace("\n", " ")


def _replace_or_append_minihead_string_param(minihead_text, name, value):
    value = _sanitize_minihead_value(value)
    if not minihead_text or not value:
        return minihead_text, False

    pattern = re.compile(
        rf'(<ParamString\."{re.escape(name)}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
    )
    match = pattern.search(minihead_text)
    replacement_value = f'"{value}"'
    if match:
        replacement = f"{match.group(1)}{replacement_value}{match.group(2)}"
        if match.group(0) == replacement:
            return minihead_text, False
        return (
            minihead_text[:match.start()]
            + replacement
            + minihead_text[match.end():],
            True,
        )

    appended_param = f'\n<ParamString."{name}">\t{{ {replacement_value} }}\n'
    return minihead_text.rstrip() + appended_param, True


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
        return (
            minihead_text[:match.start()]
            + replacement
            + minihead_text[match.end():],
            True,
        )

    appended_param = f'\n<ParamLong."{name}">\t{{ {value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _replace_or_append_minihead_bool_param(minihead_text, name, value):
    if not minihead_text:
        return minihead_text, False

    value_text = "true" if bool(value) else "false"
    pattern = re.compile(
        rf'(<ParamBool\."{re.escape(name)}">\s*\{{\s*)"?[^"}}]*"?(\s*\}})'
    )
    match = pattern.search(minihead_text)
    replacement_value = f'"{value_text}"'
    if match:
        replacement = f"{match.group(1)}{replacement_value}{match.group(2)}"
        if match.group(0) == replacement:
            return minihead_text, False
        return (
            minihead_text[:match.start()]
            + replacement
            + minihead_text[match.end():],
            True,
        )

    appended_param = f'\n<ParamBool."{name}">\t{{ {replacement_value} }}\n'
    return minihead_text.rstrip() + appended_param, True


def _source_series_name(source_image):
    meta = _meta_from_image(source_image)
    for key in ("SeriesDescription", "SequenceDescription", "ProtocolName"):
        value = _meta_text(meta, key)
        if value:
            return value
    return ""


def _source_series_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SeriesInstanceUID")


def _source_sop_uid(source_image):
    meta = _meta_from_image(source_image)
    return _meta_text(meta, "SOPInstanceUID")


def _strip_source_parent_refs(meta):
    for key in list(meta.keys()):
        if key in SOURCE_PARENT_REFERENCE_META_KEYS:
            del meta[key]
            continue
        if any(key.startswith(prefix) for prefix in SOURCE_PARENT_REFERENCE_META_PREFIXES):
            del meta[key]


def _strip_scanner_write_unsafe_meta(meta):
    for key in SCANNER_WRITE_UNSAFE_META_KEYS:
        if key in meta:
            del meta[key]


def _derived_series_grouping(series_name, series_index):
    return f"{_sanitize_bids_label(series_name) or RECIPE_NAME}_{int(series_index)}"


def _derived_series_uid(source_image, series_index, series_name):
    seed = "|".join(
        [
            RECIPE_NAME,
            _source_series_uid(source_image) or _source_series_name(source_image) or "source",
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
    output_index=0,
):
    seed = "|".join(
        [
            f"{RECIPE_NAME}-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
            str(int(output_index)),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"


def _derived_original_instance_uid(
    source_image,
    series_uid,
    series_index,
    series_name,
    output_index,
):
    header = source_image.getHead()
    seed = "|".join(
        [
            f"{RECIPE_NAME}-original-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
            str(int(output_index)),
            str(int(getattr(header, "image_index", 0))),
            str(int(getattr(header, "slice", 0))),
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"
