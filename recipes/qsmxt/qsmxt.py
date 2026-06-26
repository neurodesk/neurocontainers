"""OpenRecon bridge for QSMxT v9.

The OpenRecon side receives reconstructed MRD image messages. QSMxT v9 is a
BIDS-native Rust binary, so this bridge writes a temporary BIDS MEGRE dataset,
runs ``qsmxt run``, and converts selected derivatives back to MRD images.
"""

from __future__ import annotations

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
OUTPUT_SERIES_START = 180

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
    "minip": {
        "suffix": "minIP",
        "token": "QSMXT_MINIP",
        "series": "QSMxT minIP",
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
            logging.info("Sending %d original image(s) before QSMxT outputs", len(input_images))
            _send_images_by_series(connection, input_images)

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
    magnitude_images = _sorted_series_images(magnitude_group["images"])
    phase_images = _sorted_series_images(phase_group["images"])

    n_echoes = min(len(magnitude_images), len(phase_images))
    if n_echoes == 0:
        raise ValueError("QSMxT requires at least one magnitude and one phase image")
    if len(magnitude_images) != len(phase_images):
        logging.warning(
            "Magnitude/phase frame count mismatch; using first %d pair(s): "
            "%d magnitude, %d phase",
            n_echoes,
            len(magnitude_images),
            len(phase_images),
        )

    if settings["max_echoes"] > 0:
        n_echoes = min(n_echoes, settings["max_echoes"])

    echo_times = _echo_times_seconds(
        n_echoes,
        phase_images[:n_echoes],
        metadata,
        settings,
    )
    field_strength = settings["field_strength_t"]
    b0_dir = settings["b0_dir"]

    anat_dir = bids_dir / "sub-01" / "anat"
    anat_dir.mkdir(parents=True, exist_ok=True)

    acquisition_label = _sanitize_bids_label(
        _source_series_name(magnitude_images[0]) or "openrecon"
    )
    if not acquisition_label:
        acquisition_label = "openrecon"

    phase_paths = []
    magnitude_paths = []
    for echo_index in range(n_echoes):
        echo_number = echo_index + 1
        basename = f"sub-01_acq-{acquisition_label}_echo-{echo_number}"
        mag_path = anat_dir / f"{basename}_part-mag_MEGRE.nii.gz"
        phase_path = anat_dir / f"{basename}_part-phase_MEGRE.nii.gz"

        mag_volume, affine = _image_to_nifti_volume(
            magnitude_images[echo_index],
            kind="magnitude",
        )
        phase_volume, phase_affine = _image_to_nifti_volume(
            phase_images[echo_index],
            kind="phase",
        )
        phase_volume = _phase_to_qsmxt_counts(phase_volume, settings["phase_wrap"])
        if mag_volume.shape != phase_volume.shape:
            raise ValueError(
                "Magnitude and phase echo volumes have different shapes for "
                f"echo {echo_number}: {mag_volume.shape} vs {phase_volume.shape}"
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
        "magnitude_images": magnitude_images[:n_echoes],
        "phase_images": phase_images[:n_echoes],
        "anchor_image": magnitude_images[0],
        "magnitude_paths": magnitude_paths,
        "phase_paths": phase_paths,
        "field_strength_t": field_strength,
        "b0_dir": b0_dir,
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
        str(settings["n_procs"]),
    ]

    _append_optional_arg(cmd, "--qsm-algorithm", settings["qsm_algorithm"])
    _append_optional_arg(cmd, "--unwrapping-algorithm", settings["unwrapping_algorithm"])
    _append_optional_arg(cmd, "--bf-algorithm", settings["bf_algorithm"])
    _append_optional_arg(cmd, "--mask-preset", settings["mask_preset"])
    _append_optional_arg(cmd, "--qsm-reference", settings["qsm_reference"])
    # Auto-enable the generation flags for any derivative the user selected in
    # sendoutputs; otherwise the map is requested but never produced by QSMxT
    # (minIP is a by-product of the SWI pipeline, so it also needs --do-swi).
    selected = _selected_output_ids(settings["send_outputs"])
    do_swi = settings["do_swi"] or "swi" in selected or "minip" in selected
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
        output_images.append(
            _nifti_to_mrd_image(
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


def _nifti_to_mrd_image(
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
    output = ismrmrd.Image.from_array(data_zyx.astype(np.float32), transpose=False)

    header = copy.deepcopy(anchor_image.getHead())
    out_header = output.getHead()
    header.data_type = output.data_type
    header.image_type = int(getattr(ismrmrd, "IMTYPE_MAGNITUDE", 1))
    header.image_series_index = int(series_index)
    header.image_index = 1
    header.slice = 0
    header.contrast = 0
    _set_header_sequence_field(
        header,
        "matrix_size",
        [int(value) for value in out_header.matrix_size],
    )

    zooms = nifti.header.get_zooms()[:3]
    if len(zooms) == 3:
        fov = [
            float(zooms[0]) * data_xyz.shape[0],
            float(zooms[1]) * data_xyz.shape[1],
            float(zooms[2]) * data_xyz.shape[2],
        ]
        _set_header_sequence_field(header, "field_of_view", fov)

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
        data_xyz,
        nifti_path,
    ).serialize()
    return output


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


def _settings_from_config(config, metadata=None):
    params = _config_parameters(config)
    return {
        "send_original": _config_bool(params, "sendoriginal", False),
        "send_outputs": str(params.get("sendoutputs", "qsm") or "qsm"),
        "max_echoes": _config_int(params, "maxechoes", 0),
        "echo_times_ms": _config_text(params, "echotimesms", ""),
        "echo_time_ms": _config_float(params, "echotimems", DEFAULT_ECHO_TIME_MS),
        "echo_spacing_ms": _config_float(params, "echospacingms", DEFAULT_ECHO_SPACING_MS),
        "field_strength_t": _config_float(
            params,
            "fieldstrength",
            _metadata_field_strength(metadata) or DEFAULT_FIELD_STRENGTH_T,
        ),
        "b0_dir": _config_vector(params, "b0dir", DEFAULT_B0_DIR, length=3),
        "phase_wrap": _config_float(params, "phasewrap", 4096.0),
        "qsmxt_binary": _config_text(params, "qsmxtbinary", ""),
        "qsm_algorithm": _optional_choice(params, "qsmalgorithm"),
        "unwrapping_algorithm": _optional_choice(params, "unwrappingalgorithm"),
        "bf_algorithm": _optional_choice(params, "bfalgorithm"),
        "mask_preset": _optional_choice(params, "maskpreset"),
        "qsm_reference": _optional_choice(params, "qsmreference"),
        "n_procs": max(1, _config_int(params, "nprocs", 1)),
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
    else:
        values_ms = _metadata_echo_times_ms(metadata)
        if not values_ms:
            values_ms = _image_echo_times_ms(phase_images)

    if values_ms:
        values_ms = list(values_ms)
        while len(values_ms) < n_echoes:
            values_ms.append(values_ms[-1] + settings["echo_spacing_ms"])
        return [float(value) / 1000.0 for value in values_ms[:n_echoes]]

    logging.warning(
        "No EchoTime found in MRD metadata or config; using %.3f ms + %.3f ms spacing",
        settings["echo_time_ms"],
        settings["echo_spacing_ms"],
    )
    return [
        (settings["echo_time_ms"] + index * settings["echo_spacing_ms"]) / 1000.0
        for index in range(n_echoes)
    ]


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
):
    meta = _meta_from_image(source_image)
    _strip_source_parent_refs(meta)
    _strip_scanner_write_unsafe_meta(meta)
    if "IceMiniHead" in meta:
        del meta["IceMiniHead"]

    series_uid = _derived_series_uid(source_image, series_index, series_name)
    sop_uid = _derived_instance_uid(source_image, series_uid, series_index, series_name)
    image_type = f"DERIVED\\PRIMARY\\M\\{image_type_token}"
    center, width = _window_center_width(output_data)

    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "QSMXT"]
    meta["ImageType"] = image_type
    meta["DicomImageType"] = image_type
    meta["ImageTypeValue4"] = image_type_token
    meta["ComplexImageComponent"] = "MAGNITUDE"
    meta["SeriesDescription"] = series_name
    meta["SequenceDescription"] = series_name
    meta["ProtocolName"] = series_name
    meta["ImageComments"] = series_name
    meta["ImageComment"] = series_name
    meta["SeriesNumberRangeNameUID"] = _derived_series_grouping(series_name, series_index)
    meta["SeriesInstanceUID"] = series_uid
    meta["SOPInstanceUID"] = sop_uid
    meta["SequenceDescriptionAdditional"] = "openrecon"
    meta["Keep_image_geometry"] = "0"
    meta["partition_count"] = "1"
    slice_count = str(int(output_data.shape[2]))
    meta["slice_count"] = slice_count
    meta["NumberOfSlices"] = slice_count
    meta["ImagesInAcquisition"] = slice_count
    meta["NumberInSeries"] = "1"
    meta["SliceNo"] = "0"
    meta["IsmrmrdSliceNo"] = "0"
    meta["AnatomicalSliceNo"] = "0"
    meta["ChronSliceNo"] = "0"
    meta["ProtocolSliceNumber"] = "0"
    meta["Actual3DImagePartNumber"] = "0"
    meta["AnatomicalPartitionNo"] = "0"
    meta["QSMxTOutput"] = output_id
    meta["QSMxTUnits"] = units
    meta["QSMxTSourceFile"] = str(nifti_path)
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
    if errors:
        raise ValueError("Invalid qsmxt output image contract: " + "; ".join(errors))


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


def _derived_instance_uid(source_image, series_uid, series_index, series_name):
    seed = "|".join(
        [
            f"{RECIPE_NAME}-instance",
            series_uid,
            _source_sop_uid(source_image) or "source",
            str(int(series_index)),
            series_name,
        ]
    )
    return f"2.25.{uuid.uuid5(uuid.NAMESPACE_URL, seed).int}"
