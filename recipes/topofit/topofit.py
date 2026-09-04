#!/usr/bin/env python3
"""OpenRecon image adapter and standalone launcher for BrainNet TopoFit."""

from __future__ import annotations

import argparse
import copy
from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import re
import traceback
import uuid

import constants
import ismrmrd
import nibabel as nib
import numpy as np

import mrdhelper
from topofit_core import (
    MODEL_PRESETS,
    RESEARCH_WARNING,
    TopoFitOptions,
    mrd_lps_to_nifti_ras_affine,
    run_topofit_workflow,
)


WORKSPACE = Path(os.environ.get("TOPOFIT_OPENRECON_WORKSPACE", "/tmp/share/topofit"))
SEND_CHUNK_SIZE = 64
DEFAULTS = {
    "sendoriginal": True,
    "tfdevice": "cuda",
    "tfmodel": "t1w_1mm",
    "tfconform": True,
    "tfdebugmock": False,
}
MP2RAGE_IDENTITY_META_KEYS = (
    "SeriesDescription",
    "SequenceDescription",
    "ProtocolName",
    "SequenceName",
    "ImageComments",
    "ImageComment",
    "ImageType",
    "DicomImageType",
    "ImageTypeValue4",
)


def _config_value(config, key: str, default, value_type: str):
    try:
        return mrdhelper.get_json_config_param(
            config, key, default=default, type=value_type
        )
    except Exception:
        if isinstance(config, dict):
            parameters = config.get("parameters", config)
            if isinstance(parameters, dict):
                return parameters.get(key, default)
        return default


def _config_bool(config, key: str, default: bool) -> bool:
    value = _config_value(config, key, default, "bool")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _meta_from_image(image) -> ismrmrd.Meta:
    attribute_string = getattr(image, "attribute_string", "")
    if not attribute_string:
        return ismrmrd.Meta()
    try:
        return ismrmrd.Meta.deserialize(attribute_string)
    except Exception:
        logging.warning("Could not deserialize source MRD metadata; using a new Meta block")
        return ismrmrd.Meta()


def _copy_meta(image) -> ismrmrd.Meta:
    source = _meta_from_image(image)
    try:
        return ismrmrd.Meta.deserialize(source.serialize())
    except Exception:
        output = ismrmrd.Meta()
        for key in source.keys():
            try:
                output[key] = source[key]
            except Exception:
                pass
        return output


def _meta_text(meta: ismrmrd.Meta, key: str) -> str:
    try:
        value = meta.get(key)
    except Exception:
        return ""
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    return str(value or "").strip()


def _normalized_identity_text(value) -> str:
    """Normalize scanner labels so UNI-DEN, UNI_DEN, and UNIDEN compare alike."""

    if isinstance(value, (list, tuple)):
        value = " ".join(str(item) for item in value)
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _image_identity_values(image) -> list[str]:
    meta = _meta_from_image(image)
    values = []
    for key in MP2RAGE_IDENTITY_META_KEYS:
        try:
            value = meta.get(key)
        except Exception:
            continue
        if isinstance(value, (list, tuple)):
            values.extend(str(item) for item in value if item is not None)
        elif value is not None:
            values.append(str(value))
    return values


def _select_anatomical_input_images(images):
    """For an MP2RAGE stream, retain only its denoised uniform contrast."""

    images = list(images)
    identities = [
        [_normalized_identity_text(value) for value in _image_identity_values(image)]
        for image in images
    ]
    if not any("mp2rage" in value for values in identities for value in values):
        return images

    selected = [
        image
        for image, values in zip(images, identities)
        if any("uniden" in value for value in values)
    ]
    if not selected:
        raise ValueError(
            "MP2RAGE input was detected, but no UNI-DEN contrast was received"
        )
    logging.info(
        "MP2RAGE input detected; selected %d UNI-DEN image(s) and ignored %d "
        "other MP2RAGE image(s)",
        len(selected),
        len(images) - len(selected),
    )
    return selected


def _normalized(vector) -> np.ndarray | None:
    value = np.asarray(vector, dtype=float)
    if value.shape != (3,) or not np.all(np.isfinite(value)):
        return None
    norm = float(np.linalg.norm(value))
    if norm < 1e-8:
        return None
    return value / norm


def _meta_vector(meta: ismrmrd.Meta, key: str) -> np.ndarray | None:
    try:
        value = meta.get(key)
    except Exception:
        return None
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("\\", " ").replace(",", " ").split()
    try:
        vector = _normalized(value)
    except (TypeError, ValueError):
        vector = None
    if vector is None:
        logging.warning("Ignoring invalid %s MRD metadata vector: %r", key, value)
    return vector


def _image_directions(image) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return pixel-axis directions, preferring post-reconstruction metadata."""

    header = image.getHead()
    meta = _meta_from_image(image)
    meta_read_dir = _meta_vector(meta, "ImageRowDir")
    meta_phase_dir = _meta_vector(meta, "ImageColumnDir")
    read_dir = meta_read_dir
    phase_dir = meta_phase_dir
    if meta_read_dir is None:
        read_dir = _normalized(header.read_dir)
    if meta_phase_dir is None:
        phase_dir = _normalized(header.phase_dir)
    slice_dir = _meta_vector(meta, "ImageSliceNormDir")
    if (
        slice_dir is None
        and meta_read_dir is not None
        and meta_phase_dir is not None
    ):
        slice_dir = _normalized(np.cross(read_dir, phase_dir))
    if slice_dir is None:
        slice_dir = _normalized(header.slice_dir)
    if read_dir is None or phase_dir is None:
        raise ValueError("MRD image is missing valid in-plane direction vectors")
    if slice_dir is None:
        slice_dir = _normalized(np.cross(read_dir, phase_dir))
    if slice_dir is None:
        raise ValueError("MRD image is missing a valid slice direction vector")
    return read_dir, phase_dir, slice_dir


def _ordered_images(images):
    headers = [image.getHead() for image in images]
    _, _, slice_axis = _image_directions(images[0])
    if slice_axis is None and len(headers) > 1:
        slice_axis = _normalized(
            np.asarray(headers[-1].position) - np.asarray(headers[0].position)
        )
    if slice_axis is None:
        slice_axis = np.asarray([0.0, 0.0, 1.0])

    order = sorted(
        range(len(images)),
        key=lambda index: (
            float(np.dot(np.asarray(headers[index].position, dtype=float), slice_axis)),
            int(getattr(headers[index], "image_index", index)),
        ),
    )
    return [images[index] for index in order], slice_axis


def _slice_spacing(headers, slice_axis: np.ndarray, fallback: float) -> float:
    if len(headers) < 2:
        return fallback
    positions = [np.asarray(header.position, dtype=float) for header in headers]
    if any(
        position.shape != (3,) or not np.all(np.isfinite(position))
        for position in positions
    ):
        raise ValueError("MRD slice positions must contain three finite values")
    locations = sorted(float(np.dot(position, slice_axis)) for position in positions)
    differences = np.diff(locations)
    if np.any(differences <= 1e-4):
        raise ValueError("MRD series contains duplicate or unresolved slice positions")
    spacing = float(np.median(differences))
    if not np.allclose(differences, spacing, rtol=0.05, atol=0.05):
        raise ValueError(
            f"MRD series has irregular slice spacing: {differences.tolist()}"
        )
    return spacing


def _mrd_series_to_nifti(images) -> tuple[list, np.ndarray, np.ndarray]:
    """Validate one MRD image series and return ordered images, data, and affine."""

    if len(images) < 2:
        raise ValueError("TopoFit requires a 3D image series with at least two slices")
    ordered, slice_axis = _ordered_images(images)
    planes = []
    expected_shape = None
    for index, image in enumerate(ordered):
        plane = np.squeeze(np.asarray(image.data))
        if np.iscomplexobj(plane):
            plane = np.abs(plane)
        if plane.ndim != 2:
            raise ValueError(
                f"source image {index} must contain one 2D plane, got shape {plane.shape}"
            )
        if expected_shape is None:
            expected_shape = plane.shape
        elif plane.shape != expected_shape:
            raise ValueError(
                f"source image shapes differ: expected {expected_shape}, got {plane.shape}"
            )
        planes.append(np.asarray(plane, dtype=np.float32))

    header = ordered[0].getHead()
    matrix = np.asarray(header.matrix_size, dtype=float)
    field_of_view = np.asarray(header.field_of_view, dtype=float)
    if matrix.shape != (3,) or field_of_view.shape != (3,):
        raise ValueError("MRD matrix_size and field_of_view must each have three values")
    if (
        matrix[0] <= 0
        or matrix[1] <= 0
        or field_of_view[0] <= 0
        or field_of_view[1] <= 0
    ):
        raise ValueError("MRD in-plane matrix and field of view must be positive")
    data_matrix = (int(expected_shape[1]), int(expected_shape[0]))
    header_matrix = (int(matrix[0]), int(matrix[1]))
    if header_matrix != data_matrix:
        raise ValueError(
            "MRD in-plane matrix does not match its pixel data: "
            f"header={header_matrix} data={data_matrix}"
        )

    fallback_spacing = (
        float(field_of_view[2] / matrix[2])
        if matrix[2] > 1
        else float(field_of_view[2])
    )
    if fallback_spacing <= 0:
        fallback_spacing = 1.0
    spacing = _slice_spacing(
        [image.getHead() for image in ordered], slice_axis, fallback_spacing
    )
    voxel_size = (field_of_view[0] / matrix[0], field_of_view[1] / matrix[1], spacing)
    read_dir, phase_dir, _ = _image_directions(ordered[0])
    affine = mrd_lps_to_nifti_ras_affine(
        header.position,
        read_dir,
        phase_dir,
        slice_axis,
        voxel_size,
        (expected_shape[1], expected_shape[0]),
    )
    volume_yxz = np.stack(planes, axis=2)
    volume_xyz = np.ascontiguousarray(volume_yxz.transpose((1, 0, 2)))
    return ordered, volume_xyz, affine


def _series_description(image, fallback: str) -> str:
    meta = _meta_from_image(image)
    return (
        _meta_text(meta, "SeriesDescription")
        or _meta_text(meta, "SequenceDescription")
        or fallback
    )


def _strip_instance_identity(meta: ismrmrd.Meta) -> None:
    keys = {
        "SOPInstanceUID",
        "MultiFrameSOPInstanceUID",
        "PSMultiFrameSOPInstanceUID",
        "PSSeriesInstanceUID",
    }
    for key in list(meta.keys()):
        if key in keys or key.startswith("ReferencedImageSequence"):
            try:
                del meta[key]
            except Exception:
                pass


def _stamp_output(
    output,
    source,
    series_index: int,
    image_index: int,
    series_uid: str,
    series_description: str,
    processing_history: list[str],
    warning: str = "",
) -> None:
    header = copy.copy(source.getHead())
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_series_index = series_index
    header.image_index = image_index + 1
    output.setHead(header)

    meta = _copy_meta(source)
    _strip_instance_identity(meta)
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = processing_history
    meta["Keep_image_geometry"] = 1
    meta["SeriesInstanceUID"] = series_uid
    meta["SeriesDescription"] = series_description[:64]
    meta["SequenceDescriptionAdditional"] = "TopoFit"
    if warning:
        meta["ImageComments"] = warning
        meta["TopoFitStatus"] = "SURFACE_READY_RESEARCH_ONLY"
        meta["TopoFitPrescriptionStatus"] = "WITHHELD"
        meta["WindowCenter"] = "2048"
        meta["WindowWidth"] = "4096"
    output.attribute_string = meta.serialize()


def _new_series_uid() -> str:
    return f"2.25.{uuid.uuid4().int}"


def _clone_original_series(images, series_index: int) -> list:
    series_uid = _new_series_uid()
    description = f"{_series_description(images[0], 'mprage')}_original"
    output = []
    for index, source in enumerate(images):
        clone = ismrmrd.Image.from_array(np.array(source.data, copy=True), transpose=False)
        _stamp_output(
            clone,
            source,
            series_index,
            index,
            series_uid,
            description,
            ["PYTHON", "TOPOFIT", "ORIGINAL"],
        )
        output.append(clone)
    return output


def _qc_mrd_images(qc_path: Path, source_images, series_index: int) -> list:
    image = nib.load(str(qc_path))
    volume_xyz = np.asarray(image.get_fdata(dtype=np.float32))
    if volume_xyz.shape != (
        source_images[0].data.shape[-1],
        source_images[0].data.shape[-2],
        len(source_images),
    ):
        raise ValueError(
            "TopoFit QC geometry does not match the source MRD series: "
            f"qc={volume_xyz.shape} source_yx={source_images[0].data.shape[-2:]} "
            f"source_z={len(source_images)}"
        )
    volume_yxz = np.rint(volume_xyz.transpose((1, 0, 2))).astype(np.int16)
    series_uid = _new_series_uid()
    description = f"{_series_description(source_images[0], 'mprage')}_topofit_qc"
    output = []
    for index, source in enumerate(source_images):
        plane = np.ascontiguousarray(volume_yxz[:, :, index])
        derived = ismrmrd.Image.from_array(
            plane[np.newaxis, np.newaxis, :, :], transpose=False
        )
        _stamp_output(
            derived,
            source,
            series_index,
            index,
            series_uid,
            description,
            ["PYTHON", "BRAINNET", "TOPOFIT", "SURFACE_QC"],
            RESEARCH_WARNING,
        )
        output.append(derived)
    return output


def _send_images(connection, images, label: str) -> None:
    for start in range(0, len(images), SEND_CHUNK_SIZE):
        chunk = images[start : start + SEND_CHUNK_SIZE]
        logging.info(
            "Sending %s images %d-%d/%d",
            label,
            start + 1,
            start + len(chunk),
            len(images),
        )
        connection.send_image(chunk)


def _options_from_config(config) -> TopoFitOptions:
    options = TopoFitOptions(
        device=str(
            _config_value(config, "tfdevice", DEFAULTS["tfdevice"], "str")
        ).strip().lower(),
        preset=str(
            _config_value(config, "tfmodel", DEFAULTS["tfmodel"], "str")
        ).strip().lower(),
        conform=_config_bool(config, "tfconform", DEFAULTS["tfconform"]),
        mock=_config_bool(config, "tfdebugmock", DEFAULTS["tfdebugmock"]),
    )
    if options.preset not in MODEL_PRESETS:
        raise ValueError(f"unsupported TopoFit model preset: {options.preset}")
    return options


def process(connection, config, metadata):
    """Buffer reconstructed magnitude images, run TopoFit, and return QC series."""

    del metadata
    try:
        options = _options_from_config(config)
        send_original = _config_bool(
            config, "sendoriginal", DEFAULTS["sendoriginal"]
        )
        magnitude_images = []
        skipped = 0
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                skipped += 1
                continue
            if item.image_type not in (ismrmrd.IMTYPE_MAGNITUDE, 0):
                skipped += 1
                continue
            magnitude_images.append(item)

        if not magnitude_images:
            raise ValueError("no magnitude MRD image series was received")
        next_series_index = max(
            int(image.image_series_index) for image in magnitude_images
        ) + 1
        selected_images = _select_anatomical_input_images(magnitude_images)
        image_groups: dict[int, list] = {}
        for image in selected_images:
            image_groups.setdefault(int(image.image_series_index), []).append(image)
        logging.info(
            "TopoFit received %d series and skipped %d non-magnitude/non-image messages",
            len(image_groups),
            skipped,
        )

        pending_output: list[tuple[str, list]] = []
        for source_series_index, images in sorted(image_groups.items()):
            ordered, volume_xyz, affine = _mrd_series_to_nifti(images)
            run_id = f"series-{source_series_index}-{uuid.uuid4().hex}"
            run_dir = WORKSPACE / run_id
            run_dir.mkdir(parents=True, exist_ok=False)
            input_path = run_dir / "input_mprage.nii.gz"
            nifti = nib.Nifti1Image(volume_xyz, affine)
            nifti.header.set_xyzt_units(xyz="mm", t="sec")
            nifti.set_qform(affine, code=1)
            nifti.set_sform(affine, code=1)
            nib.save(nifti, str(input_path))

            if send_original:
                originals = _clone_original_series(ordered, next_series_index)
                pending_output.append(("restamped original", originals))
                next_series_index += 1

            result = run_topofit_workflow(input_path, run_dir, options)
            qc_images = _qc_mrd_images(
                Path(result.qc_image), ordered, next_series_index
            )
            pending_output.append(("TopoFit QC", qc_images))
            next_series_index += 1
            logging.info(
                "TOPOFIT_OPENRECON_RESULT status=%s elapsed_seconds=%.3f "
                "manifest=%s warning=%s",
                result.status,
                result.elapsed_seconds,
                result.manifest,
                result.warning,
            )

        for label, images in pending_output:
            _send_images(connection, images, label)
        connection.send_logging(
            constants.MRD_LOGGING_INFO,
            f"TopoFit completed: {len(image_groups)} series; {RESEARCH_WARNING}",
        )
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the same BrainNet TopoFit workflow used by the OpenRecon adapter "
            "on a NIfTI image."
        )
    )
    parser.add_argument("input", type=Path, help="Input 3D MRI in NIfTI format")
    parser.add_argument("output_dir", type=Path, help="Directory for surfaces and QC")
    parser.add_argument("--device", choices=("cuda", "cpu"), default="cuda")
    parser.add_argument(
        "--model", choices=tuple(MODEL_PRESETS), default="t1w_1mm"
    )
    parser.add_argument(
        "--no-conform",
        dest="conform",
        action="store_false",
        help="Require an already conformed 1 mm RAS input",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Generate geometry-valid test surfaces without running BrainNet",
    )
    parser.set_defaults(conform=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    options = TopoFitOptions(
        device=args.device,
        preset=args.model,
        conform=args.conform,
        mock=args.mock,
    )
    result = run_topofit_workflow(args.input, args.output_dir, options)
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
