#!/usr/bin/env python3
"""OpenRecon image adapter for FLAMeS white matter lesion segmentation.

The segmentation itself is the Neurodesk web app's `flames` command line,
installed unchanged, so the scanner and the browser run the same pipeline.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import subprocess
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path

import constants
import ismrmrd
import mrdhelper
import nibabel as nib
import numpy as np
from scipy import ndimage

WORKSPACE = Path(
    os.environ.get("WHITEMATTERLESIONS_WORKSPACE", "/tmp/share/whitematterlesions")
)
FLAMES = os.environ.get("FLAMES_EXECUTABLE", "/opt/flames/flames")
SEND_CHUNK_SIZE = 64
RESEARCH_WARNING = "RESEARCH ONLY - REVIEW THE LESION MASK BEFORE USE"
PROBABILITY_SCALE = 1000
DEFAULTS = {
    "sendoriginal": True,
    "wmlmodel": "fold0",
    "wmlskullstripped": False,
    "wmlprobability": False,
}
FOLDS = {"fold0": "1", "ensemble": "5"}


@dataclass(frozen=True)
class FlamesResult:
    mask: Path
    probability: Path
    table: Path
    lesion_count: int
    volume_ml: float
    provenance: dict


def run_flames(
    input_path: Path, output_dir: Path, model: str, skull_stripped: bool
) -> FlamesResult:
    command = [
        FLAMES, str(input_path), str(output_dir), "--folds", FOLDS[model], "--offline"
    ]
    if skull_stripped:
        command.append("--skull-stripped")
    logging.info("Running %s", " ".join(command))
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    for line in completed.stderr.splitlines():
        logging.info("flames: %s", line)
    if completed.returncode != 0:
        lines = completed.stderr.strip().splitlines()
        raise RuntimeError(
            f"flames exited with status {completed.returncode}: "
            f"{lines[-1] if lines else 'no message'}"
        )
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    stem = input_path.name.removesuffix(".gz").removesuffix(".nii")
    return FlamesResult(
        mask=output_dir / f"{stem}_lesions.nii",
        probability=output_dir / f"{stem}_lesion_probability.nii",
        table=output_dir / f"{stem}_lesions.tsv",
        lesion_count=int(report["count"]),
        volume_ml=float(report["totalMl"]),
        provenance=report["provenance"],
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


def _config_bool(config, key: str) -> bool:
    value = _config_value(config, key, DEFAULTS[key], "bool")
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _config_model(config) -> str:
    model = str(_config_value(config, "wmlmodel", DEFAULTS["wmlmodel"], "str"))
    model = model.strip().lower()
    if model not in FOLDS:
        raise ValueError(f"wmlmodel must be one of {sorted(FOLDS)}, got {model!r}")
    return model


def _meta_from_image(image) -> ismrmrd.Meta:
    attribute_string = getattr(image, "attribute_string", "")
    if not attribute_string:
        return ismrmrd.Meta()
    try:
        return ismrmrd.Meta.deserialize(attribute_string)
    except Exception:
        logging.warning("Could not deserialize source MRD metadata; using a new Meta block")
        return ismrmrd.Meta()


def _meta_text(meta: ismrmrd.Meta, key: str) -> str:
    value = meta.get(key)
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    return str(value or "").strip()


def _normalized(vector) -> np.ndarray | None:
    value = np.asarray(vector, dtype=float)
    if value.shape != (3,) or not np.all(np.isfinite(value)):
        return None
    norm = float(np.linalg.norm(value))
    if norm < 1e-8:
        return None
    return value / norm


def _meta_vector(meta: ismrmrd.Meta, key: str) -> np.ndarray | None:
    value = meta.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("\\", " ").replace(",", " ").split()
    try:
        return _normalized([float(item) for item in value])
    except (TypeError, ValueError):
        logging.warning("Ignoring invalid %s MRD metadata vector: %r", key, value)
        return None


def _image_directions(image) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return pixel-axis directions, preferring post-reconstruction metadata."""

    header = image.getHead()
    meta = _meta_from_image(image)
    meta_read_dir = _meta_vector(meta, "ImageRowDir")
    meta_phase_dir = _meta_vector(meta, "ImageColumnDir")
    read_dir = meta_read_dir if meta_read_dir is not None else _normalized(header.read_dir)
    phase_dir = meta_phase_dir if meta_phase_dir is not None else _normalized(header.phase_dir)
    if read_dir is None or phase_dir is None:
        raise ValueError("MRD image is missing valid in-plane direction vectors")
    # The slice direction need not equal read x phase: a left-handed series
    # would otherwise be sorted, and returned, in reverse slice order.
    slice_dir = _meta_vector(meta, "ImageSliceNormDir")
    if slice_dir is None and meta_read_dir is not None and meta_phase_dir is not None:
        slice_dir = _normalized(np.cross(read_dir, phase_dir))
    if slice_dir is None:
        slice_dir = _normalized(header.slice_dir)
    if slice_dir is None:
        slice_dir = _normalized(np.cross(read_dir, phase_dir))
    if slice_dir is None:
        raise ValueError("MRD image is missing a valid slice direction vector")
    return read_dir, phase_dir, slice_dir


def mrd_lps_to_nifti_ras_affine(
    position, read_dir, phase_dir, slice_dir, voxel_size, in_plane_shape
) -> np.ndarray:
    """Convert centered MRD slice geometry from LPS to a NIfTI RAS affine."""

    directions = [np.asarray(item, dtype=float) for item in (read_dir, phase_dir, slice_dir)]
    gram = np.column_stack(directions).T @ np.column_stack(directions)
    if not np.allclose(gram, np.eye(3), atol=1e-3):
        raise ValueError("MRD direction vectors must form an orthonormal basis")
    spacing = np.asarray(voxel_size, dtype=float)
    shape = np.asarray(in_plane_shape, dtype=float)
    # ImageHeader.position is the center of the slice, not of voxel (0, 0).
    origin_lps = (
        np.asarray(position, dtype=float)
        - directions[0] * spacing[0] * (shape[0] - 1) / 2.0
        - directions[1] * spacing[1] * (shape[1] - 1) / 2.0
    )
    lps_to_ras = np.diag([-1.0, -1.0, 1.0])
    affine = np.eye(4)
    affine[:3, :3] = np.column_stack(
        [lps_to_ras @ direction * step for direction, step in zip(directions, spacing)]
    )
    affine[:3, 3] = lps_to_ras @ origin_lps
    return affine


def _slice_spacing(headers, slice_axis: np.ndarray) -> float:
    locations = sorted(
        float(np.dot(np.asarray(header.position, dtype=float), slice_axis))
        for header in headers
    )
    differences = np.diff(locations)
    if np.any(differences <= 1e-4):
        raise ValueError("MRD series contains duplicate or unresolved slice positions")
    # The full span, not a median step: rounded positions quantize single steps,
    # and that error would accumulate along a long series.
    spacing = (locations[-1] - locations[0]) / (len(locations) - 1)
    if not np.allclose(differences, spacing, rtol=0.05, atol=0.05):
        raise ValueError(f"MRD series has irregular slice spacing: {differences.tolist()}")
    return spacing


def mrd_series_to_volume(images) -> tuple[list, np.ndarray, np.ndarray]:
    """Order one MRD series by slice position and return it as a NIfTI volume."""

    if len(images) < 2:
        raise ValueError("FLAMeS needs a 3D FLAIR series with at least two slices")
    read_dir, phase_dir, slice_axis = _image_directions(images[0])
    ordered = sorted(
        images,
        key=lambda image: (
            float(np.dot(np.asarray(image.getHead().position, dtype=float), slice_axis)),
            int(image.getHead().image_index),
        ),
    )
    planes = [np.squeeze(np.asarray(image.data)) for image in ordered]
    planes = [np.abs(plane) if np.iscomplexobj(plane) else plane for plane in planes]
    if any(plane.ndim != 2 or plane.shape != planes[0].shape for plane in planes):
        raise ValueError(
            "every source image must hold one 2D plane of the same shape, got "
            f"{sorted({plane.shape for plane in planes})}"
        )
    rows, columns = planes[0].shape
    header = ordered[0].getHead()
    matrix = np.asarray(header.matrix_size, dtype=float)
    field_of_view = np.asarray(header.field_of_view, dtype=float)
    if (int(matrix[0]), int(matrix[1])) != (columns, rows):
        raise ValueError(
            "MRD in-plane matrix does not match its pixel data: "
            f"header={tuple(int(value) for value in matrix[:2])} data={(columns, rows)}"
        )
    voxel_size = (
        field_of_view[0] / matrix[0],
        field_of_view[1] / matrix[1],
        _slice_spacing([image.getHead() for image in ordered], slice_axis),
    )
    affine = mrd_lps_to_nifti_ras_affine(
        header.position, read_dir, phase_dir, slice_axis, voxel_size, (columns, rows)
    )
    # FLAMeS sees one regular grid, so every slice must lie on it.
    tolerance = 0.1 * max(voxel_size)
    lps = np.diag([-1.0, -1.0, 1.0])
    for index, image in enumerate(ordered):
        slice_header = image.getHead()
        expected_center = lps @ (
            affine @ np.array([(columns - 1) / 2, (rows - 1) / 2, index, 1.0])
        )[:3]
        offset = np.linalg.norm(np.asarray(slice_header.position) - expected_center)
        if (
            not np.allclose(slice_header.matrix_size[:2], matrix[:2])
            or not np.allclose(slice_header.field_of_view[:2], field_of_view[:2])
            or not all(
                np.allclose(a, b, atol=1e-3)
                for a, b in zip(_image_directions(image), (read_dir, phase_dir, slice_axis))
            )
            or offset > tolerance
        ):
            raise ValueError(
                f"slice {index} does not lie on the series grid "
                f"(center {offset:.2f} mm from its expected position)"
            )
    volume_xyz = np.ascontiguousarray(
        np.stack(planes, axis=2).transpose((1, 0, 2)).astype(np.float32)
    )
    return ordered, volume_xyz, affine


def lesion_outline(mask_xyz: np.ndarray) -> np.ndarray:
    """In-plane lesion boundary, so the FLAIR signal inside each lesion stays visible."""

    mask = mask_xyz > 0
    interior = ndimage.binary_erosion(mask, structure=np.ones((3, 3, 1), dtype=bool))
    return mask & ~interior


def _series_description(image) -> str:
    meta = _meta_from_image(image)
    return (
        _meta_text(meta, "SeriesDescription")
        or _meta_text(meta, "SequenceDescription")
        or "flair"
    )


def _derived_description(source, suffix: str) -> str:
    return _series_description(source)[: 64 - len(suffix)] + suffix


def _stamp_output(
    output, source, series_index: int, image_index: int, series_uid: str,
    description: str, role: str, comment: str, display_max: int | None = None,
) -> None:
    header = copy.copy(source.getHead())
    header.data_type = output.data_type
    header.image_type = ismrmrd.IMTYPE_MAGNITUDE
    header.image_series_index = series_index
    header.image_index = image_index + 1
    output.setHead(header)

    meta = ismrmrd.Meta.deserialize(_meta_from_image(source).serialize())
    for key in list(meta.keys()):
        if key in {
            "SOPInstanceUID",
            "MultiFrameSOPInstanceUID",
            "PSMultiFrameSOPInstanceUID",
            "PSSeriesInstanceUID",
        } or key.startswith("ReferencedImageSequence"):
            del meta[key]
    meta["DataRole"] = "Image"
    meta["ImageProcessingHistory"] = ["PYTHON", "FLAMES", role]
    meta["Keep_image_geometry"] = 1
    meta["SeriesInstanceUID"] = series_uid
    meta["SeriesDescription"] = description
    meta["SequenceDescriptionAdditional"] = "FLAMeS"
    if comment:
        meta["ImageComment"] = comment
        meta["ImageComments"] = comment
    if display_max is not None:
        # Labels and probabilities are not FLAIR intensities: drop the source
        # calibration and window them from 0 (black) to display_max (white).
        for key in ("RescaleSlope", "RescaleIntercept", "RescaleType"):
            if key in meta.keys():
                del meta[key]
        meta["WindowCenter"] = str((display_max + 1) / 2)
        meta["WindowWidth"] = str(display_max + 1)
    output.attribute_string = meta.serialize()


def volume_to_mrd_series(
    volume_xyz: np.ndarray, sources, series_index: int, suffix: str, role: str,
    comment: str = "", display_max: int | None = None,
) -> list:
    """Return one MRD image per source slice, on the source images' geometry."""

    expected = (sources[0].data.shape[-1], sources[0].data.shape[-2], len(sources))
    if volume_xyz.shape != expected:
        raise ValueError(
            f"FLAMeS output grid {volume_xyz.shape} does not match the source series {expected}"
        )
    series_uid = f"2.25.{uuid.uuid4().int}"
    description = _derived_description(sources[0], suffix)
    output = []
    for index, source in enumerate(sources):
        plane = np.ascontiguousarray(volume_xyz[:, :, index].T)
        image = ismrmrd.Image.from_array(plane[np.newaxis, np.newaxis], transpose=False)
        _stamp_output(
            image, source, series_index, index, series_uid, description, role,
            comment, display_max,
        )
        output.append(image)
    return output


def original_series(ordered, series_index: int) -> list:
    series_uid = f"2.25.{uuid.uuid4().int}"
    description = _derived_description(ordered[0], "_original")
    output = []
    for index, source in enumerate(ordered):
        image = ismrmrd.Image.from_array(np.array(source.data, copy=True), transpose=False)
        _stamp_output(image, source, series_index, index, series_uid, description, "ORIGINAL", "")
        output.append(image)
    return output


def segmentation_series(
    result: FlamesResult, ordered, volume_xyz: np.ndarray, first_series_index: int,
    send_probability: bool,
) -> list[tuple[str, list]]:
    mask = np.asarray(nib.load(str(result.mask)).dataobj) > 0
    summary = (
        f"FLAMeS: {result.lesion_count} lesions, {result.volume_ml:.2f} ml; "
        f"{RESEARCH_WARNING}"
    )
    source_dtype = np.asarray(ordered[0].data).dtype
    if np.iscomplexobj(ordered[0].data):
        source_dtype = np.dtype(np.float32)
    # The brightest source value stays within the scanner's stored bit range.
    overlay = volume_xyz.copy()
    overlay[lesion_outline(mask)] = volume_xyz.max()
    series = [
        ("lesion overlay", volume_to_mrd_series(
            overlay.astype(source_dtype), ordered, first_series_index,
            "_lesion_overlay", "LESION_OVERLAY", summary,
        )),
        ("lesion mask", volume_to_mrd_series(
            mask.astype(np.int16), ordered, first_series_index + 1,
            "_lesions", "LESION_MASK", summary, display_max=1,
        )),
    ]
    if send_probability:
        probability = np.asarray(nib.load(str(result.probability)).dataobj, dtype=np.float32)
        series.append(("lesion probability", volume_to_mrd_series(
            np.rint(np.clip(probability, 0, 1) * PROBABILITY_SCALE).astype(np.int16),
            ordered, first_series_index + 2, "_lesion_probability",
            "LESION_PROBABILITY", f"Lesion probability x{PROBABILITY_SCALE}; {summary}",
            display_max=PROBABILITY_SCALE,
        )))
    return series


def _send_images(connection, images, label: str) -> None:
    for start in range(0, len(images), SEND_CHUNK_SIZE):
        chunk = images[start : start + SEND_CHUNK_SIZE]
        logging.info(
            "Sending %s images %d-%d/%d", label, start + 1, start + len(chunk), len(images)
        )
        connection.send_image(chunk)


def process(connection, config, metadata):
    """Buffer reconstructed FLAIR images, run FLAMeS, and return lesion series."""

    del metadata
    try:
        model = _config_model(config)
        skull_stripped = _config_bool(config, "wmlskullstripped")
        send_original = _config_bool(config, "sendoriginal")
        send_probability = _config_bool(config, "wmlprobability")
        series: dict[int, list] = {}
        skipped = 0
        for item in connection:
            if item is None:
                break
            if isinstance(item, ismrmrd.Image) and item.image_type in (
                ismrmrd.IMTYPE_MAGNITUDE, 0
            ):
                series.setdefault(int(item.image_series_index), []).append(item)
            else:
                skipped += 1
        if not series:
            raise ValueError("no magnitude MRD image series was received")
        logging.info(
            "FLAMeS received %d series and skipped %d other messages", len(series), skipped
        )

        next_series_index = max(series) + 1
        pending: list[tuple[str, list]] = []
        summaries = []
        for source_series_index, images in sorted(series.items()):
            ordered, volume_xyz, affine = mrd_series_to_volume(images)
            run_dir = WORKSPACE / f"series-{source_series_index}-{uuid.uuid4().hex}"
            run_dir.mkdir(parents=True)
            input_path = run_dir / "flair.nii.gz"
            nifti = nib.Nifti1Image(volume_xyz, affine)
            nifti.header.set_xyzt_units(xyz="mm")
            nifti.set_qform(affine, code=1)
            nifti.set_sform(affine, code=1)
            nib.save(nifti, str(input_path))

            if send_original:
                pending.append(("original", original_series(ordered, next_series_index)))
                next_series_index += 1

            result = run_flames(input_path, run_dir / "flames", model, skull_stripped)
            outputs = segmentation_series(
                result, ordered, volume_xyz, next_series_index, send_probability
            )
            pending.extend(outputs)
            next_series_index += len(outputs)
            summaries.append(f"{result.lesion_count} lesions, {result.volume_ml:.2f} ml")
            logging.info(
                "WHITEMATTERLESIONS_RESULT series=%d lesions=%d volume_ml=%.3f "
                "table=%s provenance=%s",
                source_series_index, result.lesion_count, result.volume_ml,
                result.table, json.dumps(result.provenance),
            )

        for label, images in pending:
            _send_images(connection, images, label)
        connection.send_logging(
            constants.MRD_LOGGING_INFO,
            f"FLAMeS completed: {'; '.join(summaries)}. {RESEARCH_WARNING}",
        )
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()
