"""Multi-echo GRE B0 mapping for OpenRecon and classic/enhanced MR DICOM."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
import pydicom
from scipy import ndimage

import openreconi2iexample as helpers


VERSION = os.environ.get("B0MAPROMEO_VERSION", "development")
WORK_ROOT = Path("/tmp/share/b0mapromeo")
ROMEO_COMMAND = [
    "/opt/julia/bin/julia",
    "--startup-file=no",
    "--project=/opt/b0mapromeo/julia",
    "/opt/b0mapromeo/romeo.jl",
]


def phase_radians(values: np.ndarray, units: str) -> np.ndarray:
    """Use the acquisition's fixed scale, never the observed pixel range."""
    values = np.asarray(values, dtype=np.float32)
    if not np.all(np.isfinite(values)):
        raise ValueError("Phase contains non-finite values")
    if units == "siemens":
        if np.any((values < 0) | (values > 4095)):
            raise ValueError("Siemens phase must be unsigned 12-bit counts")
        return (values * 2.0 - 4096.0) * (np.pi / 4096.0)
    if units == "signed":
        if np.any(np.abs(values) > 4096):
            raise ValueError("Signed Siemens phase must be in [-4096,4096]")
        return values * (np.pi / 4096.0)
    if units == "radians":
        if np.any(np.abs(values) > np.pi + 1e-4):
            raise ValueError("Radian phase must be wrapped in [-pi,pi]")
        return values
    raise ValueError("Unknown phase units")


def foreground_mask(magnitude: np.ndarray) -> np.ndarray:
    """Reproduce the starting pipeline's threshold and morphological cleanup.

    Its polynomial bias correction changes magnitude, but not the threshold
    mask returned to the caller. ROMEO receives the original magnitudes.
    """
    magnitude = np.asarray(magnitude, dtype=np.float32)
    if magnitude.ndim != 3 or not np.all(np.isfinite(magnitude)):
        raise ValueError("Magnitude must be a finite 3D volume")
    if np.any(magnitude < 0) or magnitude.max() <= 0:
        raise ValueError("Magnitude must be nonnegative and contain signal")
    mask = magnitude > 0.25 * magnitude.mean(dtype=np.float64)
    cross = np.array([[0, 1, 0], [1, 1, 1], [0, 1, 0]], dtype=bool)
    mask = ndimage.binary_erosion(mask, structure=cross[:, :, None])
    grid = np.indices((5, 5, 5)) - 2
    sphere = np.sum(grid * grid, axis=0) <= 4
    mask = ndimage.binary_opening(mask, structure=sphere)
    connectivity = np.ones((3, 3, 3), dtype=bool)
    labels, count = ndimage.label(mask, structure=connectivity)
    if count == 0:
        raise ValueError("Foreground mask is empty after cleanup")
    sizes = np.bincount(labels.ravel())
    sizes[0] = 0
    return ndimage.binary_fill_holes(labels == sizes.argmax(), structure=connectivity)


def validate_echo_times(values: list[float], count: int) -> list[float]:
    times = np.asarray(values, dtype=float)
    if (
        len(times) != count
        or count < 2
        or not np.all(np.isfinite(times))
        or np.any(times <= 0)
        or np.any(np.diff(times) <= 0)
    ):
        raise ValueError(
            "Need one positive, increasing echo time in ms per echo, at least two"
        )
    return times.tolist()


def reconstruct(
    magnitude: np.ndarray,
    phase: np.ndarray,
    affine: np.ndarray,
    echo_times_ms: list[float],
    output_dir: Path,
    max_seeds: int = 4000,
) -> tuple[np.ndarray, np.ndarray]:
    if magnitude.shape != phase.shape or magnitude.ndim != 4:
        raise ValueError("Magnitude and phase must have matching XYZ-echo shapes")
    times = validate_echo_times(echo_times_ms, magnitude.shape[3])
    if not np.all(np.isfinite(magnitude)) or np.any(magnitude < 0):
        raise ValueError("Magnitude must be finite and nonnegative")
    if not np.all(np.isfinite(phase)):
        raise ValueError("Phase must be finite")
    if not 1 <= max_seeds <= 100000:
        raise ValueError("maxseeds must be between 1 and 100000")
    mask = foreground_mask(magnitude[..., 0])
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, data in (("magnitude", magnitude), ("phase", phase), ("mask", mask)):
        nib.save(
            nib.Nifti1Image(data.astype(np.float32), affine), output_dir / f"{name}.nii"
        )
    command = ROMEO_COMMAND + [
        "-p",
        str(output_dir / "phase.nii"),
        "-m",
        str(output_dir / "magnitude.nii"),
        "-k",
        str(output_dir / "mask.nii"),
        "-t",
        "[" + ",".join(str(t) for t in times) + "]",
        "-o",
        str(output_dir / "unwrapped.nii"),
        "-B",
        "b0_hz",
        "--no-phase-rescale",
        "--max-seeds",
        str(max_seeds),
    ]
    environment = os.environ.copy()
    for key in ("LD_LIBRARY_PATH", "LD_PRELOAD"):
        environment.pop(key, None)
    subprocess.run(command, env=environment, check=True)
    result = nib.load(output_dir / "b0_hz.nii")
    field = result.get_fdata(dtype=np.float32)
    if field.shape != mask.shape or not np.allclose(result.affine, affine, atol=1e-4):
        raise ValueError("ROMEO output geometry disagrees with its inputs")
    if not np.all(np.isfinite(field[mask])):
        raise ValueError("ROMEO produced non-finite values inside the foreground")
    field = np.where(mask, field, 0).astype(np.float32)
    nib.save(nib.Nifti1Image(field, affine), output_dir / "b0_hz.nii")
    return field, mask


def _vector(header, name: str) -> np.ndarray:
    value = np.asarray(getattr(header, name), dtype=float)
    if value.shape != (3,) or not np.all(np.isfinite(value)):
        raise ValueError(f"Invalid MRD {name}")
    return value


def _planes(image: ismrmrd.Image) -> list[ismrmrd.Image]:
    """Split packed MRD volumes, whose position denotes their center."""
    data = np.asarray(image.data)
    if data.ndim != 4 or data.shape[0] != 1:
        raise ValueError("Input must be a single-channel reconstructed MRD image")
    if data.shape[1] == 1:
        return [image]
    header = image.getHead()
    spacing = float(header.field_of_view[2]) / data.shape[1]
    if spacing <= 0:
        raise ValueError("Packed MRD volume needs positive slice spacing")
    planes = []
    for z in range(data.shape[1]):
        plane = ismrmrd.Image.from_array(data[:, z : z + 1].copy(), transpose=False)
        head = copy.deepcopy(header)
        head.data_type = plane.data_type
        head.matrix_size[2] = 1
        head.field_of_view[2] = spacing
        head.position[:] = _vector(header, "position") + (
            z - (data.shape[1] - 1) / 2
        ) * spacing * _vector(header, "slice_dir")
        head.slice = z
        plane.setHead(head)
        plane.attribute_string = image.attribute_string
        planes.append(plane)
    return planes


def assemble(
    images: list[ismrmrd.Image], echo_times_ms: list[float], phase_units: str
) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[float], list[ismrmrd.Image]]:
    groups = defaultdict(list)
    series = defaultdict(set)
    for image in images:
        head = image.getHead()
        if head.image_type not in (ismrmrd.IMTYPE_MAGNITUDE, ismrmrd.IMTYPE_PHASE):
            raise ValueError("Only magnitude and phase MRD images are supported")
        # Do not silently combine repeated scans, distortion variants or dynamics.
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        series[head.image_type].add(
            (
                int(head.image_series_index),
                helpers._meta_text(meta, "SeriesInstanceUID"),
            )
        )
        if head.repetition != 0 or head.set != 0:
            raise ValueError(
                "Repeated scans and dynamic acquisitions must be sent separately"
            )
        groups[(int(head.image_type), int(head.contrast))].extend(_planes(image))
    if any(len(values) != 1 for values in series.values()):
        raise ValueError(
            "Send exactly one magnitude series and one matching phase series"
        )
    mag_echoes = sorted(e for kind, e in groups if kind == ismrmrd.IMTYPE_MAGNITUDE)
    phase_echoes = sorted(e for kind, e in groups if kind == ismrmrd.IMTYPE_PHASE)
    if mag_echoes != phase_echoes or len(mag_echoes) < 2:
        raise ValueError(
            "Magnitude and phase must have the same echo indices, at least two"
        )
    times = validate_echo_times(echo_times_ms, len(mag_echoes))
    reference = groups[(ismrmrd.IMTYPE_MAGNITUDE, mag_echoes[0])][0].getHead()
    axes = np.column_stack(
        [_vector(reference, n) for n in ("read_dir", "phase_dir", "slice_dir")]
    )
    if not np.allclose(axes.T @ axes, np.eye(3), atol=1e-4):
        raise ValueError("MRD image axes must be orthonormal")
    shape = tuple(int(v) for v in reference.matrix_size[:2])
    spacing_xy = _vector(reference, "field_of_view")[:2] / shape
    if np.any(spacing_xy <= 0):
        raise ValueError("MRD field of view must be positive")
    positions = None
    magnitudes, phases = [], []
    anchors = []
    for echo in mag_echoes:
        for kind, destination in (
            (ismrmrd.IMTYPE_MAGNITUDE, magnitudes),
            (ismrmrd.IMTYPE_PHASE, phases),
        ):
            ordered = sorted(
                groups[(kind, echo)],
                key=lambda im: np.dot(_vector(im.getHead(), "position"), axes[:, 2]),
            )
            current_positions = np.array(
                [_vector(im.getHead(), "position") for im in ordered]
            )
            if positions is None:
                positions = current_positions
                anchors = ordered
            elif positions.shape != current_positions.shape or not np.allclose(
                positions, current_positions, atol=1e-3
            ):
                raise ValueError(
                    "Magnitude/phase echoes have mismatched slice positions"
                )
            for im in ordered:
                h = im.getHead()
                if (
                    tuple(h.matrix_size[:2]) != shape
                    or not np.allclose(
                        _vector(h, "field_of_view")[:2],
                        _vector(reference, "field_of_view")[:2],
                        atol=1e-3,
                    )
                    or not np.allclose(
                        np.column_stack(
                            [
                                _vector(h, n)
                                for n in ("read_dir", "phase_dir", "slice_dir")
                            ]
                        ),
                        axes,
                        atol=1e-4,
                    )
                ):
                    raise ValueError("Echoes have inconsistent geometry")
            volume = np.stack([im.data[0, 0].T for im in ordered], axis=2).astype(
                np.float32
            )
            if kind == ismrmrd.IMTYPE_PHASE:
                volume = phase_radians(volume, phase_units)
            destination.append(volume)
    if len(positions) < 5:
        raise ValueError("Need at least five slices for 3D mask cleanup")
    steps = np.diff(positions, axis=0)
    spacing_z = float(np.dot(steps[0], axes[:, 2]))
    if spacing_z <= 0 or not np.allclose(steps, axes[:, 2] * spacing_z, atol=1e-3):
        raise ValueError("Slices must have unique, regular positions without gaps")
    affine_lps = np.eye(4)
    affine_lps[:3, :3] = axes * np.array([*spacing_xy, spacing_z])
    affine_lps[:3, 3] = positions[0] - axes[:, :2] @ (
        (np.array(shape) - 1) * spacing_xy / 2
    )
    affine = np.diag([-1, -1, 1, 1]) @ affine_lps
    return (
        np.stack(magnitudes, axis=3),
        np.stack(phases, axis=3),
        affine,
        times,
        anchors,
    )


def _dicom_group(dataset, frame, name: str):
    for source in (
        frame,
        getattr(dataset, "SharedFunctionalGroupsSequence", [None])[0],
    ):
        if source is not None and hasattr(source, name):
            return getattr(source, name)[0]
    return dataset


def read_dicoms(
    directory: Path, phase_units: str = "siemens"
) -> tuple[list[ismrmrd.Image], list[float]]:
    """Read pixel data and acquisition geometry without exporting patient tags."""
    records = []
    series = defaultdict(set)
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        try:
            dataset = pydicom.dcmread(path)
        except pydicom.errors.InvalidDicomError:
            continue
        if str(dataset.SOPClassUID) not in (
            "1.2.840.10008.5.1.4.1.1.4",
            "1.2.840.10008.5.1.4.1.1.4.1",
        ):
            continue
        pixels = dataset.pixel_array
        frames = getattr(dataset, "PerFrameFunctionalGroupsSequence", [dataset])
        if len(frames) != (pixels.shape[0] if pixels.ndim == 3 else 1):
            raise ValueError("DICOM frame metadata does not match pixel data")
        for i, frame in enumerate(frames):
            types = _dicom_group(dataset, frame, "MRImageFrameTypeSequence")
            tokens = getattr(types, "FrameType", dataset.get("ImageType", []))
            components = set(tokens) & {"M", "P"}
            if len(components) != 1:
                raise ValueError("DICOM input must identify magnitude or phase")
            kind = components.pop()
            series[kind].add(str(dataset.SeriesInstanceUID))
            echo = _dicom_group(dataset, frame, "MREchoSequence")
            te = float(getattr(echo, "EffectiveEchoTime", dataset.get("EchoTime", 0)))
            measures = _dicom_group(dataset, frame, "PixelMeasuresSequence")
            spacing = np.array(measures.PixelSpacing, dtype=float)
            thickness = float(measures.SliceThickness)
            orient = _dicom_group(dataset, frame, "PlaneOrientationSequence")
            axes = np.array(orient.ImageOrientationPatient, dtype=float).reshape(2, 3).T
            pos = _dicom_group(dataset, frame, "PlanePositionSequence")
            position = np.array(pos.ImagePositionPatient, dtype=float)
            plane = np.asarray(
                pixels[i] if pixels.ndim == 3 else pixels, dtype=np.float32
            )
            transform = _dicom_group(dataset, frame, "PixelValueTransformationSequence")
            slope = float(
                getattr(transform, "RescaleSlope", dataset.get("RescaleSlope", 1))
            )
            intercept = float(
                getattr(
                    transform, "RescaleIntercept", dataset.get("RescaleIntercept", 0)
                )
            )
            if kind == "P":
                if phase_units == "siemens":
                    # Enhanced Siemens MR encodes unsigned 12-bit phase with
                    # slope 2 and intercept -4096. Respect that exact transform.
                    if (slope, intercept) == (2, -4096):
                        plane = phase_radians(plane * slope + intercept, "signed")
                    elif (slope, intercept) == (1, 0):
                        plane = phase_radians(plane, "siemens")
                    else:
                        raise ValueError(
                            "Unknown Siemens DICOM phase rescale; select explicit phase units"
                        )
                else:
                    plane = phase_radians(plane * slope + intercept, phase_units)
            else:
                plane = plane * slope + intercept
            image = ismrmrd.Image.from_array(plane[None, None], transpose=False)
            header = image.getHead()
            header.image_type = (
                ismrmrd.IMTYPE_PHASE if kind == "P" else ismrmrd.IMTYPE_MAGNITUDE
            )
            header.image_series_index = 2 if kind == "P" else 1
            header.field_of_view[:] = [
                plane.shape[1] * spacing[1],
                plane.shape[0] * spacing[0],
                thickness,
            ]
            header.read_dir[:] = axes[:, 0]
            header.phase_dir[:] = axes[:, 1]
            header.slice_dir[:] = np.cross(axes[:, 0], axes[:, 1])
            header.position[:] = position + axes @ np.array(
                [
                    (plane.shape[1] - 1) * spacing[1] / 2,
                    (plane.shape[0] - 1) * spacing[0] / 2,
                ]
            )
            image.setHead(header)
            meta = ismrmrd.Meta()
            meta["EchoTime"] = str(te)
            image.attribute_string = meta.serialize()
            records.append((te, image))
    if set(series) != {"M", "P"} or any(len(s) != 1 for s in series.values()):
        raise ValueError(
            "DICOM directory must contain exactly one magnitude/phase series pair"
        )
    times = sorted({te for te, _ in records})
    validate_echo_times(times, len(times))
    for te, image in records:
        header = image.getHead()
        header.contrast = times.index(te)
        image.setHead(header)
    planes = defaultdict(list)
    for _, image in records:
        planes[(int(image.image_type), int(image.contrast))].append(image)
    for (_, echo), group in planes.items():
        axis = _vector(group[0].getHead(), "slice_dir")
        ordered = sorted(
            group, key=lambda im: np.dot(_vector(im.getHead(), "position"), axis)
        )
        for z, image in enumerate(ordered):
            header = image.getHead()
            header.slice = z
            header.image_index = echo * len(ordered) + z + 1
            image.setHead(header)
    return [im for _, im in records], times


def output_images(
    field: np.ndarray, anchors: list[ismrmrd.Image], series_index: int
) -> list[ismrmrd.Image]:
    """Return unsigned scanner pixels with a reversible Hz rescale."""
    maximum = float(np.max(np.abs(field)))
    scale = min(1.0, 2046.0 / maximum) if maximum else 1.0
    display = np.clip(np.rint(field * scale + 2048), 1, 4095).astype(np.uint16)
    identity = helpers._build_output_series_identity_from_name(
        anchors[0], series_index, "ROMEO B0 Hz"
    )
    outputs = []
    for z, source in enumerate(anchors):
        output = ismrmrd.Image.from_array(
            display[:, :, z].T[None, None], transpose=False
        )
        header = copy.deepcopy(source.getHead())
        header.data_type = output.data_type
        output.setHead(header)
        helpers._stamp_output_image(
            output,
            source,
            series_index,
            z,
            "ROMEO B0 Hz",
            ["Image", "Quantitative"],
            "B0MAP_ROMEO",
            ["ROMEO", "B0_HZ"],
            series_identity=identity,
            extra_meta={
                "RescaleSlope": str(1 / scale),
                "RescaleIntercept": str(-2048 / scale),
                "RescaleType": "Hz",
                "WindowCenter": "0",
                "WindowWidth": "400",
                "B0MapUnits": "Hz",
                "B0MapDisplayFormula": f"Hz = (stored - 2048) / {scale}",
                "NumberOfSlices": str(len(anchors)),
                "ImagesInAcquisition": str(len(anchors)),
                "slice_count": str(len(anchors)),
                "NumberInSeries": str(z + 1),
            },
        )
        outputs.append(output)
    return outputs


def _settings(config, metadata) -> dict:
    # The server can pass XML text when its optional scanner-info logging
    # fails, even though the acquisition header itself parsed successfully.
    if isinstance(metadata, (str, bytes)):
        metadata = ismrmrd.xsd.CreateFromDocument(metadata)
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            config = {}
    config = config if isinstance(config, dict) else {}
    parameters = config.get("parameters", config)
    if not isinstance(parameters, dict):
        raise ValueError("Config parameters must be an object")
    parameters = dict(parameters)
    user = getattr(metadata, "userParameters", None)
    for name in ("userParameterString", "userParameterLong", "userParameterDouble"):
        for item in getattr(user, name, []) or []:
            parameters.setdefault(item.name, item.value)
    raw = parameters.get("echotimesms", "")
    times = (
        [float(t) for t in str(raw).replace(",", " ").split()]
        if raw
        else list(
            getattr(getattr(metadata, "sequenceParameters", None), "TE", []) or []
        )
    )
    return {
        "times": times,
        "phase_units": parameters.get("phaseunits", "siemens"),
        "max_seeds": int(parameters.get("maxseeds", 4000)),
        "send_original": helpers._config_bool(parameters, "sendoriginal", False),
    }


def process(connection, config, metadata):
    try:
        images = []
        for item in connection:
            if item is None:
                break
            if not isinstance(item, ismrmrd.Image):
                raise ValueError("B0 mapping requires reconstructed image messages")
            images.append(item)
        if not images:
            return
        settings = _settings(config, metadata)
        mag, phase, affine, times, anchors = assemble(
            images, settings["times"], settings["phase_units"]
        )
        WORK_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="run_", dir=WORK_ROOT) as temporary:
            field, _ = reconstruct(
                mag, phase, affine, times, Path(temporary), settings["max_seeds"]
            )
            series = max(180, max(int(im.image_series_index) for im in images) + 1)
            outputs = output_images(field, anchors, series)
        if settings["send_original"]:
            # These shared helpers also restamp scanner MiniHead storage fields.
            originals = helpers._restamp_originals(images)
            by_series = defaultdict(list)
            for im in originals:
                by_series[int(im.image_series_index)].append(im)
            for batch in by_series.values():
                connection.send_image(batch)
        connection.send_image(outputs)
        logging.info("b0mapromeo %s returned %d B0 slices in Hz", VERSION, len(outputs))
    except Exception:
        logging.exception("B0 reconstruction failed")
        # Avoid reflecting patient metadata or file paths to the scanner log.
        connection.send_logging(
            3, "B0 reconstruction failed; check the local server log"
        )
    finally:
        connection.send_close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=f"b0mapromeo {VERSION}")
    parser.add_argument("--dicom-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--phase-units", choices=("siemens", "signed", "radians"), default="siemens"
    )
    parser.add_argument("--max-seeds", type=int, default=4000)
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        parser.error(
            "Output directory must be empty to avoid overwriting an earlier reconstruction"
        )
    images, times = read_dicoms(args.dicom_dir, args.phase_units)
    magnitude, phase, affine, times, _ = assemble(images, times, "radians")
    field, mask = reconstruct(
        magnitude, phase, affine, times, args.output_dir, args.max_seeds
    )
    print(
        f"B0 map complete: {len(times)} echoes, {field.shape[2]} slices, "
        f"{int(mask.sum())} foreground voxels; output units Hz"
    )


if __name__ == "__main__":
    main()
