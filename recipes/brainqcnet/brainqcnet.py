"""OpenRecon image-in/image-out adapter for BrainQCNet T1w artifact detection.

Each reconstructed magnitude volume is written as a one-subject BIDS dataset and
scored by the upstream BIDS app through the ``brainqcnet`` wrapper. The adapter
returns the originals as a new series whose name carries the verdict, and a
one-page report image with the artifact probability per axis.
"""
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
import csv
import json
import logging
from pathlib import Path
import subprocess
import tempfile
import traceback

import ismrmrd
import nibabel as nib
import numpy as np

import constants
import openreconi2iexample as shared

PRED_METHODS = ("percentage", "mean", "median")
# transform_log_to_csv.compute_proba_to_pred: a scan fails above this probability
FAIL_THRESHOLD = 0.5


@dataclass(frozen=True)
class Settings:
    predmethod: str = "percentage"
    sendoriginal: bool = True

    @classmethod
    def parse(cls, config):
        if config in (None, "", "brainqcnet"):
            values = {}
        elif isinstance(config, str):
            values = json.loads(config)
        else:
            values = config
        if not isinstance(values, dict):
            raise ValueError("BrainQCNet configuration must be a JSON object")
        values = values.get("parameters", values)
        if not isinstance(values, dict):
            raise ValueError("BrainQCNet parameters must be a JSON object")
        unknown = set(values) - set(cls.__dataclass_fields__) - {"config"}
        if unknown:
            raise ValueError(f"Unknown BrainQCNet parameters: {sorted(unknown)}")
        result = cls(**{key: value for key, value in values.items() if key in cls.__dataclass_fields__})
        if result.predmethod not in PRED_METHODS:
            raise ValueError(f"predmethod must be one of {PRED_METHODS}")
        if type(result.sendoriginal) is not bool:
            raise ValueError("sendoriginal must be a JSON boolean")
        return result


@dataclass(frozen=True)
class QCResult:
    proba: float
    x_proba: float
    y_proba: float
    z_proba: float
    seconds: float
    method: str

    @property
    def verdict(self):
        return "FAIL" if self.proba > FAIL_THRESHOLD else "PASS"

    def label(self):
        """Series name suffix. The shared helpers seed series and instance UIDs from it, so a
        rerun with another result or method never reuses the UIDs of different pixels."""
        return f"brainqcnet-{self.verdict}-{self.proba:.2f}-{self.method}"

    def comment(self):
        return (f"BrainQCNet {self.verdict}: artifact probability {self.proba:.2f} "
                f"(x {self.x_proba:.2f}, y {self.y_proba:.2f}, z {self.z_proba:.2f}; {self.method})")


@dataclass(frozen=True)
class SourceVolume:
    images: tuple
    voxels: np.ndarray
    affine: np.ndarray

    @classmethod
    def from_images(cls, images):
        """Volume reoriented to the closest canonical (RAS) axes, as dcm2niix does for 3D
        acquisitions, so x, y and z are the sagittal, coronal and axial planes BrainQCNet
        slices along."""
        for image in images:
            _validate_image(image)
        items, axis = shared._ordered_source_geometry_items(
            [(image, image.data[0, 0]) for image in images], "BrainQCNet"
        )
        ordered = tuple(image for image, _ in items)
        first = ordered[0].getHead()
        nx, ny, _ = map(int, first.matrix_size)
        read = np.asarray(first.read_dir, dtype=float)
        phase = np.asarray(first.phase_dir, dtype=float)
        spacing = np.asarray(first.field_of_view, dtype=float) / np.asarray(first.matrix_size)
        positions = np.array([image.getHead().position for image in ordered], dtype=float)
        if len(ordered) > 1:
            dz = float(np.median(np.diff(positions @ axis)))
            if not np.isfinite(dz) or dz <= 0:
                raise ValueError("BrainQCNet requires distinct regularly spaced slices")
        else:
            dz = float(spacing[2])
        expected = positions[0] + np.arange(len(ordered))[:, None] * axis * dz
        if not np.allclose(positions, expected, atol=0.01, rtol=0):
            raise ValueError("BrainQCNet slices must form a regular volume without gaps or in-plane drift")
        for image in ordered:
            header = image.getHead()
            for field in ("matrix_size", "field_of_view", "read_dir", "phase_dir", "slice_dir"):
                if not np.allclose(getattr(header, field), getattr(first, field), atol=1e-5, rtol=1e-5):
                    raise ValueError(f"BrainQCNet source slices have inconsistent {field}")
        affine = np.eye(4)
        affine[:3, 0] = read * spacing[0]
        affine[:3, 1] = -phase * spacing[1]
        affine[:3, 2] = axis * dz
        affine[:3, 3] = positions[0] - read * spacing[0] * (nx - 1) / 2 + phase * spacing[1] * (ny - 1) / 2
        affine[:2, :] *= -1
        voxels = np.stack([plane.T[:, ::-1] for _, plane in items], axis=2).astype(np.float32)
        canonical = nib.as_closest_canonical(nib.Nifti1Image(voxels, affine))
        return cls(ordered, np.asarray(canonical.dataobj, dtype=np.float32), canonical.affine)


def _validate_image(image):
    header = image.getHead()
    data = np.asarray(image.data)
    if header.image_type != ismrmrd.IMTYPE_MAGNITUDE or np.iscomplexobj(data):
        raise ValueError("BrainQCNet accepts reconstructed magnitude images only")
    nx, ny, nz = map(int, header.matrix_size)
    if header.channels != 1 or nz != 1 or data.shape != (1, 1, ny, nx) or min(nx, ny) < 1:
        raise ValueError("BrainQCNet requires single-channel, single-plane images matching matrix_size")
    if not np.isfinite(data).all():
        raise ValueError("BrainQCNet input pixels must be finite")
    fov = np.asarray(header.field_of_view, dtype=float)
    position = np.asarray(header.position, dtype=float)
    axes = np.array([header.read_dir, header.phase_dir, header.slice_dir], dtype=float)
    if not np.isfinite(fov).all() or np.any(fov <= 0) or not np.isfinite(position).all():
        raise ValueError("BrainQCNet requires finite positions and positive field_of_view")
    if not np.isfinite(axes).all() or not np.allclose(axes @ axes.T, np.eye(3), atol=1e-4):
        raise ValueError("BrainQCNet requires orthonormal read, phase and slice directions")
    if not np.allclose(np.cross(axes[0], axes[1]), axes[2], atol=1e-4):
        raise ValueError("BrainQCNet source directions must form a right-handed coordinate system")


def assess(volume, settings):
    with tempfile.TemporaryDirectory(prefix="brainqcnet-", dir="/tmp") as directory:
        work = Path(directory)
        anat = work / "bids" / "sub-scan" / "anat"
        anat.mkdir(parents=True)
        nib.save(nib.Nifti1Image(volume.voxels, volume.affine), anat / "sub-scan_T1w.nii.gz")
        command = ["brainqcnet", str(work / "bids"), str(work / "out"), "participant",
                   "--pred_method", settings.predmethod]
        completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if completed.returncode:
            raise RuntimeError(f"BrainQCNet failed with exit {completed.returncode}: {completed.stdout[-8000:]}")
        results = list((work / "out").glob("sub-scan/*/*/tot_df.csv"))
        if len(results) != 1:
            raise RuntimeError(f"BrainQCNet did not produce exactly one tot_df.csv: {completed.stdout[-8000:]}")
        with results[0].open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        if len(rows) != 1:
            raise RuntimeError("BrainQCNet tot_df.csv must contain one row")
        row = rows[0]
        values = {key: float(row[key]) for key in ("proba", "x_proba", "y_proba", "z_proba", "t")}
        if not all(np.isfinite(value) for value in values.values()) or not 0 <= values["proba"] <= 1:
            raise ValueError(f"BrainQCNet returned an invalid probability: {row}")
        return QCResult(values["proba"], values["x_proba"], values["y_proba"], values["z_proba"],
                        values["t"], settings.predmethod)


def _render_report(result, width, height):
    from PIL import Image, ImageDraw, ImageFont

    page = Image.new("L", (width, height), 0)
    draw = ImageDraw.Draw(page)
    font = ImageFont.load_default()
    margin = max(2, min(24, width // 32))
    line = max(10, min(24, (height - 2 * margin) // 9))
    lines = [
        ("BrainQCNet T1w quality control", 255),
        (f"Verdict: {result.verdict}", 255),
        (f"Artifact probability: {result.proba:.2f} (fail above {FAIL_THRESHOLD})", 220),
        (f"Sagittal (x): {result.x_proba:.2f}", 220),
        (f"Coronal (y): {result.y_proba:.2f}", 220),
        (f"Axial (z): {result.z_proba:.2f}", 220),
        (f"Method: {result.method}", 180),
        ("Research use only; not for diagnosis", 180),
    ]
    for row, (text, fill) in enumerate(lines):
        draw.text((margin, margin + row * line), text, fill=fill, font=font)
    return np.asarray(page, dtype=np.uint16) * 16


def _report(volume, result, series_index):
    header = volume.images[0].getHead()
    # The page carries its own standalone geometry, so it need not match the source matrix.
    width, height = max(int(header.matrix_size[0]), 512), max(int(header.matrix_size[1]), 512)
    page = shared._orient_metrics_report_page_for_scanner(_render_report(result, width, height))
    identity = shared._build_output_series_identity(
        volume.images[0], series_index, f"{result.label()}-report", "source")
    source = shared._metrics_report_source_image(volume.images[0], width, height)
    comment = result.comment()
    extra = {"WindowCenter": "2040", "WindowWidth": "4080", "ImageComments": comment, "ImageComment": comment,
             "BrainQCNetVerdict": result.verdict, "BrainQCNetProbability": f"{result.proba:.2f}"}
    return [shared._pack_explicit_volume(
        [(source, page.reshape(1, 1, height, width))], series_index, identity, "Segmentation",
        identity["series_name"], ["PYTHON", "BRAINQCNET"], extra, "BrainQCNet report")]


def _originals(volume, result, series_index):
    identity = shared._build_output_series_identity(volume.images[0], series_index, result.label(), "source")
    comment = result.comment()
    outputs = []
    for index, source in enumerate(volume.images):
        output = deepcopy(source)
        # The pass-through keeps the source SeriesDescription, which scanner postprocessing relies on.
        shared._stamp_original_image(output, source, series_index, index, identity["series_name"], identity)
        meta = ismrmrd.Meta.deserialize(output.attribute_string)
        meta["SequenceDescriptionAdditional"] = f"BrainQCNet_{result.verdict}"
        meta["ImageComments"] = comment
        meta["BrainQCNetVerdict"] = result.verdict
        meta["BrainQCNetProbability"] = f"{result.proba:.2f}"
        output.attribute_string = meta.serialize()
        outputs.append(output)
    return outputs


def process(connection, config, metadata):
    try:
        settings = Settings.parse(config)
        series = defaultdict(list)
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                raise ValueError("BrainQCNet accepts reconstructed MRD images, not raw acquisitions")
            series[int(item.image_series_index)].append(item)
        groups = [group for images in series.values() for group in shared._source_volume_groups(images)]
        used = set(series)
        available = (index for index in range(1, 65536) if index not in used)
        for group in groups:
            volume = SourceVolume.from_images(group)
            result = assess(volume, settings)
            logging.info("%s (%.1f s)", result.comment(), result.seconds)
            if settings.sendoriginal:
                connection.send_image(_originals(volume, result, next(available)))
            connection.send_image(_report(volume, result, next(available)))
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()
