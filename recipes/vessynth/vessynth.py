from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
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

MODALITIES = ("TOF", "T2star", "HiPCT", "OCT", "fibers")


@dataclass(frozen=True)
class Settings:
    modality: str = "TOF"
    threshold: float = 0.3
    patch_size: int = 128
    step_size: int = 32
    output: str = "binary"
    sendoriginal: bool = True
    gaussian_weights: bool = True

    @classmethod
    def parse(cls, config):
        if config in (None, "", "vessynth"):
            values = {}
        elif isinstance(config, str):
            values = json.loads(config)
        else:
            values = config
        if not isinstance(values, dict):
            raise ValueError("VesSynth configuration must be a JSON object")
        values = values.get("parameters", values)
        if not isinstance(values, dict):
            raise ValueError("VesSynth parameters must be a JSON object")
        parameter_fields = {name.replace("_", ""): name for name in cls.__dataclass_fields__}
        unknown = set(values) - parameter_fields.keys() - {"config"}
        if unknown:
            raise ValueError(f"Unknown VesSynth parameters: {sorted(unknown)}")
        selected = {parameter_fields[key]: value for key, value in values.items() if key in parameter_fields}
        if isinstance(selected.get("threshold"), str):
            selected["threshold"] = float(selected["threshold"])
        if "patch_size" in selected and "step_size" not in selected and type(selected["patch_size"]) is int:
            selected["step_size"] = selected["patch_size"] // 4
        result = cls(**selected)
        if result.modality not in MODALITIES:
            raise ValueError(f"modality must be one of {MODALITIES}")
        if isinstance(result.threshold, bool) or not isinstance(result.threshold, (int, float)):
            raise ValueError("threshold must be a finite number between 0 and 1")
        if not np.isfinite(result.threshold) or not 0 <= result.threshold <= 1:
            raise ValueError("threshold must be a finite number between 0 and 1")
        if type(result.patch_size) is not int or result.patch_size < 32 or result.patch_size % 32:
            raise ValueError("patch_size must be a positive multiple of 32, at least 32")
        if type(result.step_size) is not int or not 1 <= result.step_size <= result.patch_size:
            raise ValueError("step_size must be between 1 and patch_size")
        if result.patch_size % result.step_size:
            raise ValueError("step_size must divide patch_size for upstream probability averaging")
        if result.output not in ("binary", "probability", "both"):
            raise ValueError("output must be binary, probability or both")
        if type(result.sendoriginal) is not bool or type(result.gaussian_weights) is not bool:
            raise ValueError("sendoriginal and gaussian_weights must be JSON booleans")
        return result


@dataclass(frozen=True)
class SourceVolume:
    images: tuple
    voxels: np.ndarray
    affine: np.ndarray

    @classmethod
    def from_images(cls, images):
        for image in images:
            _validate_image(image)
        items, axis = shared._ordered_source_geometry_items(
            [(image, image.data[0, 0]) for image in images], "VesSynth"
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
                raise ValueError("VesSynth requires distinct regularly spaced slices")
        else:
            dz = float(spacing[2])
        expected = positions[0] + np.arange(len(ordered))[:, None] * axis * dz
        if not np.allclose(positions, expected, atol=0.01, rtol=0):
            raise ValueError("VesSynth slices must form a regular volume without gaps or in-plane drift")
        for image in ordered:
            header = image.getHead()
            for field in ("matrix_size", "field_of_view", "read_dir", "phase_dir", "slice_dir"):
                if not np.allclose(getattr(header, field), getattr(first, field), atol=1e-5, rtol=1e-5):
                    raise ValueError(f"VesSynth source slices have inconsistent {field}")
        affine = np.eye(4)
        affine[:3, 0] = read * spacing[0]
        affine[:3, 1] = phase * spacing[1]
        affine[:3, 2] = axis * dz
        affine[:3, 3] = positions[0] - read * spacing[0] * (nx - 1) / 2 - phase * spacing[1] * (ny - 1) / 2
        affine[:2, :] *= -1
        voxels = np.stack([plane.T for _, plane in items], axis=2).astype(np.float32)
        return cls(ordered, voxels, affine)


def _validate_image(image):
    header = image.getHead()
    data = np.asarray(image.data)
    if header.image_type != ismrmrd.IMTYPE_MAGNITUDE or np.iscomplexobj(data):
        raise ValueError("VesSynth accepts reconstructed magnitude images only")
    nx, ny, nz = map(int, header.matrix_size)
    if header.channels != 1 or nz != 1 or data.shape != (1, 1, ny, nx) or min(nx, ny) < 1:
        raise ValueError("VesSynth requires single-channel, single-plane images matching matrix_size")
    if not np.isfinite(data).all():
        raise ValueError("VesSynth input pixels must be finite")
    fov = np.asarray(header.field_of_view, dtype=float)
    position = np.asarray(header.position, dtype=float)
    axes = np.array([header.read_dir, header.phase_dir, header.slice_dir], dtype=float)
    if not np.isfinite(fov).all() or np.any(fov <= 0) or not np.isfinite(position).all():
        raise ValueError("VesSynth requires finite positions and positive field_of_view")
    if not np.isfinite(axes).all() or not np.allclose(axes @ axes.T, np.eye(3), atol=1e-4):
        raise ValueError("VesSynth requires orthonormal read, phase and slice directions")
    if not np.allclose(np.cross(axes[0], axes[1]), axes[2], atol=1e-4):
        raise ValueError("VesSynth source directions must form a right-handed coordinate system")


def infer(volume, settings):
    with tempfile.TemporaryDirectory(prefix="vessynth-", dir="/tmp") as directory:
        work = Path(directory)
        source = work / "source.nii.gz"
        nib.save(nib.Nifti1Image(volume.voxels, volume.affine), source)
        command = ["vessynth", "-i", str(source), "-o", str(work), "-mod", settings.modality,
                   "-t", str(settings.threshold), "--patch_size", str(settings.patch_size),
                   "--step_size", str(settings.step_size)]
        if not settings.gaussian_weights:
            command.append("-nw")
        completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if completed.returncode:
            raise RuntimeError(f"VesSynth inference failed with exit {completed.returncode}: {completed.stdout[-8000:]}")
        candidates = list(work.glob("source_vessels_prob.nii.gz"))
        if len(candidates) != 1:
            raise RuntimeError("VesSynth did not produce exactly one probability volume")
        prediction = nib.load(candidates[0])
        probabilities = np.asarray(prediction.dataobj, dtype=np.float32)
        # Upstream squeezes singleton axes. Restore only axes present in the source.
        squeezed_shape = tuple(size for size in volume.voxels.shape if size != 1)
        if probabilities.shape != volume.voxels.shape:
            if probabilities.shape != squeezed_shape or probabilities.size != volume.voxels.size:
                raise ValueError("VesSynth output shape differs from source geometry")
            probabilities = probabilities.reshape(volume.voxels.shape)
        if not np.allclose(prediction.affine, volume.affine, atol=1e-4, rtol=1e-5):
            raise ValueError("VesSynth output affine differs from source geometry")
        if not np.isfinite(probabilities).all():
            raise ValueError("VesSynth produced non-finite probabilities")
        if np.any(probabilities < -1e-5) or np.any(probabilities > 1 + 1e-5):
            raise ValueError("VesSynth probabilities are outside 0..1")
        return np.clip(probabilities, 0, 1)


def _derived(volume, pixels, series_index, binary, settings):
    suffix = f"vessynth-{settings.modality}-{'binary' if binary else 'probability'}"
    identity = shared._build_output_series_identity(volume.images[0], series_index, suffix, "source")
    outputs = []
    for index, source in enumerate(volume.images):
        plane = pixels[:, :, index].T.astype(np.uint16 if binary else np.float32)
        output = ismrmrd.Image.from_array(plane, transpose=False)
        constructor = output.getHead()
        header = deepcopy(source.getHead())
        header.data_type = constructor.data_type
        header.channels = constructor.channels
        header.matrix_size = constructor.matrix_size
        output.setHead(header)
        extra = {"VesSynthModality": settings.modality, "VesSynthThreshold": str(settings.threshold)}
        if binary:
            shared._stamp_segment_source_geometry_image(
                output, source, series_index, index, identity["series_name"], extra, identity
            )
        else:
            shared._stamp_output_image(
                output, source, series_index, index, identity["series_name"], "Image",
                "DERIVED", ["VesSynthProbability"], extra_meta=extra, series_identity=identity
            )
        outputs.append(output)
    return outputs


def _originals(volume, series_index):
    identity = shared._build_output_series_identity(volume.images[0], series_index, "original", "source")
    outputs = []
    for index, source in enumerate(volume.images):
        output = deepcopy(source)
        shared._stamp_original_image(output, source, series_index, index, identity["series_name"], identity)
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
                raise ValueError("VesSynth accepts reconstructed MRD images, not raw acquisitions")
            series[int(item.image_series_index)].append(item)
        groups = [group for images in series.values() for group in shared._source_volume_groups(images)]
        used = set(series)
        available = (index for index in range(1, 65536) if index not in used)
        for group in groups:
            volume = SourceVolume.from_images(group)
            probabilities = infer(volume, settings)
            if settings.sendoriginal:
                connection.send_image(_originals(volume, next(available)))
            if settings.output in ("binary", "both"):
                binary = (probabilities > settings.threshold).astype(np.uint16)
                connection.send_image(_derived(volume, binary, next(available), True, settings))
            if settings.output in ("probability", "both"):
                connection.send_image(_derived(volume, probabilities, next(available), False, settings))
    except Exception:
        message = traceback.format_exc()
        logging.error(message)
        connection.send_logging(constants.MRD_LOGGING_ERROR, message)
    finally:
        connection.send_close()
