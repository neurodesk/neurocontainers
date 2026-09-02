"""Reusable TopoFit inference and quality-control workflow.

The OpenRecon adapter and the command-line launcher both call this module. It
does not import ISMRMRD, so the model and geometry path can be exercised
without a scanner connection.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
import subprocess
from time import perf_counter
from typing import Callable, Sequence

import nibabel as nib
import numpy as np


RESEARCH_WARNING = "RESEARCH ONLY - NOT MOTION-CLEARED - NOT FOR PRESCRIPTION"
SURFACE_NAMES = (
    "lh.white",
    "rh.white",
    "lh.pial",
    "rh.pial",
    "lh.registration",
    "rh.registration",
)
MODEL_PRESETS = {
    "t1w_1mm": ("t1w", "1mm"),
    "synth_1mm": ("synth", "1mm"),
    "synth_random": ("synth", "random"),
}


@dataclass(frozen=True)
class TopoFitOptions:
    """Validated options for one inference run."""

    device: str = "cuda"
    preset: str = "t1w_1mm"
    conform: bool = True
    mock: bool = False


@dataclass(frozen=True)
class TopoFitResult:
    """Artifacts produced by a completed workflow."""

    status: str
    run_dir: str
    input_image: str
    qc_image: str
    manifest: str
    surfaces: dict[str, str]
    elapsed_seconds: float
    warning: str = RESEARCH_WARNING


def validate_options(options: TopoFitOptions) -> tuple[str, str]:
    """Validate external options and return BrainNet contrast and resolution."""

    if options.device not in {"cuda", "cpu"}:
        raise ValueError("device must be 'cuda' or 'cpu'")
    try:
        return MODEL_PRESETS[options.preset]
    except KeyError:
        valid = ", ".join(sorted(MODEL_PRESETS))
        raise ValueError(f"unknown model preset {options.preset!r}; choose {valid}") from None


def build_brainnet_command(
    input_path: Path,
    surface_dir: Path,
    options: TopoFitOptions,
) -> list[str]:
    """Build the pinned BrainNet CLI invocation for a workflow run."""

    contrast, resolution = validate_options(options)
    command = ["brainnet"]
    if options.conform:
        command.append("--conform")
    command.extend(
        [
            "--device",
            options.device,
            "topofit",
            "--contrast",
            contrast,
            "--resolution",
            resolution,
            str(input_path),
            str(surface_dir),
        ]
    )
    return command


def mrd_lps_to_nifti_ras_affine(
    position: Sequence[float],
    read_dir: Sequence[float],
    phase_dir: Sequence[float],
    slice_dir: Sequence[float],
    voxel_size: Sequence[float],
    in_plane_shape: Sequence[int],
) -> np.ndarray:
    """Convert centered MRD image geometry from LPS to a NIfTI RAS affine."""

    position_array = np.asarray(position, dtype=float)
    raw_directions = [
        np.asarray(read_dir, dtype=float),
        np.asarray(phase_dir, dtype=float),
        np.asarray(slice_dir, dtype=float),
    ]
    spacing = np.asarray(voxel_size, dtype=float)
    shape = np.asarray(in_plane_shape, dtype=int)
    if position_array.shape != (3,) or any(
        item.shape != (3,) for item in raw_directions
    ):
        raise ValueError("MRD position and direction vectors must each have three values")
    if not np.all(np.isfinite(position_array)) or any(
        not np.all(np.isfinite(item)) for item in raw_directions
    ):
        raise ValueError("MRD position and direction vectors must be finite")
    if spacing.shape != (3,) or not np.all(np.isfinite(spacing)) or np.any(spacing <= 0):
        raise ValueError(f"voxel size must contain three positive values, got {spacing}")
    if shape.shape != (2,) or np.any(shape < 2):
        raise ValueError(f"in-plane shape must contain two values greater than one, got {shape}")

    norms = [float(np.linalg.norm(item)) for item in raw_directions]
    if any(norm < 1e-8 for norm in norms):
        raise ValueError("MRD direction vectors must be non-zero")
    directions = [item / norm for item, norm in zip(raw_directions, norms)]
    gram = np.column_stack(directions).T @ np.column_stack(directions)
    if not np.allclose(gram, np.eye(3), atol=1e-3):
        raise ValueError("MRD direction vectors must form an orthonormal basis")

    # ImageHeader.position is the physical center of an MRD image, unlike
    # DICOM ImagePositionPatient. Each input here is one 2D slice, so move from
    # its in-plane center to the center of voxel (0, 0); the ordered first
    # slice already supplies the coordinate along the slice axis.
    origin_lps = (
        position_array
        - directions[0] * spacing[0] * (shape[0] - 1) / 2.0
        - directions[1] * spacing[1] * (shape[1] - 1) / 2.0
    )

    lps_to_ras = np.diag([-1.0, -1.0, 1.0])
    affine = np.eye(4, dtype=float)
    affine[:3, :3] = np.column_stack(
        [lps_to_ras @ direction * step for direction, step in zip(directions, spacing)]
    )
    affine[:3, 3] = lps_to_ras @ origin_lps
    if not np.all(np.isfinite(affine)) or abs(float(np.linalg.det(affine[:3, :3]))) < 1e-8:
        raise ValueError("MRD geometry produced a singular or non-finite NIfTI affine")
    return affine


def validate_nifti_input(input_path: Path) -> nib.spatialimages.SpatialImage:
    """Load and validate the three-dimensional image at the system boundary."""

    if not input_path.is_file():
        raise FileNotFoundError(f"input image does not exist: {input_path}")
    image = nib.load(str(input_path))
    if len(image.shape) != 3 or any(int(size) < 2 for size in image.shape):
        raise ValueError(f"TopoFit requires a 3D image, got shape {image.shape}")
    if not np.all(np.isfinite(image.affine)):
        raise ValueError("input image affine contains non-finite values")
    if abs(float(np.linalg.det(image.affine[:3, :3]))) < 1e-8:
        raise ValueError("input image affine is singular")
    zooms = np.asarray(image.header.get_zooms()[:3], dtype=float)
    if not np.all(np.isfinite(zooms)) or np.any(zooms <= 0):
        raise ValueError(f"input image has invalid voxel sizes: {zooms}")
    return image


def _mock_octahedron(
    image: nib.spatialimages.SpatialImage,
    hemisphere: str,
    scale: float,
) -> tuple[np.ndarray, np.ndarray]:
    shape = np.asarray(image.shape, dtype=float)
    center = (shape - 1.0) / 2.0
    center[0] += (-0.16 if hemisphere == "lh" else 0.16) * shape[0]
    radii = scale * np.asarray([0.16, 0.31, 0.31]) * shape
    unit_vertices = np.asarray(
        [
            [-1.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, -1.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, -1.0],
            [0.0, 0.0, 1.0],
        ],
        dtype=float,
    )
    faces = np.asarray(
        [
            [0, 2, 4],
            [2, 1, 4],
            [1, 3, 4],
            [3, 0, 4],
            [2, 0, 5],
            [1, 2, 5],
            [3, 1, 5],
            [0, 3, 5],
        ],
        dtype=np.int32,
    )
    voxel_vertices = center + unit_vertices * radii
    world_vertices = nib.affines.apply_affine(image.affine, voxel_vertices)
    return world_vertices.astype(np.float32), faces


def write_mock_surfaces(
    image: nib.spatialimages.SpatialImage,
    surface_dir: Path,
) -> None:
    """Write small geometry-valid surfaces for transport and geometry tests."""

    surface_dir.mkdir(parents=True, exist_ok=True)
    for hemisphere in ("lh", "rh"):
        for surface, scale in (("white", 0.86), ("pial", 1.0)):
            vertices, faces = _mock_octahedron(image, hemisphere, scale)
            nib.freesurfer.write_geometry(
                str(surface_dir / f"{hemisphere}.{surface}"), vertices, faces
            )
        vertices, faces = _mock_octahedron(image, hemisphere, 1.0)
        centered = vertices - vertices.mean(axis=0, keepdims=True)
        norms = np.linalg.norm(centered, axis=1, keepdims=True)
        registration = centered / np.maximum(norms, 1e-6) * 100.0
        nib.freesurfer.write_geometry(
            str(surface_dir / f"{hemisphere}.registration"),
            registration.astype(np.float32),
            faces,
        )


def validate_surface_outputs(surface_dir: Path) -> dict[str, Path]:
    """Fail closed unless all bilateral white, pial, and registration meshes exist."""

    outputs: dict[str, Path] = {}
    for name in SURFACE_NAMES:
        path = surface_dir / name
        if not path.is_file() or path.stat().st_size == 0:
            raise RuntimeError(f"TopoFit did not produce required surface {name}")
        vertices, faces = nib.freesurfer.read_geometry(str(path))
        if vertices.ndim != 2 or vertices.shape[1] != 3 or vertices.shape[0] < 4:
            raise RuntimeError(f"surface {name} has invalid vertices shape {vertices.shape}")
        if faces.ndim != 2 or faces.shape[1] != 3 or faces.shape[0] < 4:
            raise RuntimeError(f"surface {name} has invalid faces shape {faces.shape}")
        if not np.all(np.isfinite(vertices)):
            raise RuntimeError(f"surface {name} contains non-finite vertices")
        if int(faces.min()) < 0 or int(faces.max()) >= vertices.shape[0]:
            raise RuntimeError(f"surface {name} contains out-of-range face indices")
        outputs[name] = path
    return outputs


def _scaled_anatomy(image: nib.spatialimages.SpatialImage) -> np.ndarray:
    data = np.asarray(image.get_fdata(dtype=np.float32))
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        raise ValueError("input image contains no finite voxel values")
    low, high = np.percentile(finite, (1.0, 99.0))
    if high <= low:
        high = float(np.max(finite))
        low = float(np.min(finite))
    if high <= low:
        return np.zeros(data.shape, dtype=np.int16)
    scaled = np.clip((np.nan_to_num(data, nan=low) - low) / (high - low), 0.0, 1.0)
    return np.rint(scaled * 3000.0).astype(np.int16)


def _surface_voxel_mask(
    image: nib.spatialimages.SpatialImage,
    paths: Sequence[Path],
) -> np.ndarray:
    mask = np.zeros(image.shape, dtype=bool)
    inverse_affine = np.linalg.inv(image.affine)
    for path in paths:
        vertices, _ = nib.freesurfer.read_geometry(str(path))
        voxel_vertices = np.rint(
            nib.affines.apply_affine(inverse_affine, vertices)
        ).astype(int)
        inside = np.all(voxel_vertices >= 0, axis=1) & np.all(
            voxel_vertices < np.asarray(image.shape), axis=1
        )
        voxel_vertices = voxel_vertices[inside]
        if voxel_vertices.size:
            mask[tuple(voxel_vertices.T)] = True

    dilated = mask.copy()
    for axis in (0, 1):
        dilated |= np.roll(mask, 1, axis=axis)
        dilated |= np.roll(mask, -1, axis=axis)
    return dilated


def write_qc_overlay(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    output_path: Path,
) -> Path:
    """Rasterize white and pial vertices onto the source voxel grid."""

    output = _scaled_anatomy(image)
    pial_mask = _surface_voxel_mask(
        image, [surfaces["lh.pial"], surfaces["rh.pial"]]
    )
    white_mask = _surface_voxel_mask(
        image, [surfaces["lh.white"], surfaces["rh.white"]]
    )
    output[pial_mask] = 3500
    output[white_mask] = 4095

    header = image.header.copy()
    header.set_data_dtype(np.int16)
    header["descrip"] = RESEARCH_WARNING.encode("ascii")[:79]
    qc_image = nib.Nifti1Image(output, image.affine, header=header)
    qc_image.set_qform(image.affine, code=1)
    qc_image.set_sform(image.affine, code=1)
    nib.save(qc_image, str(output_path))
    return output_path


def _run_command(
    command: Sequence[str],
    runner: Callable[..., subprocess.CompletedProcess[str]],
) -> subprocess.CompletedProcess[str]:
    logging.info("Running TopoFit command: %s", " ".join(command))
    completed = runner(command, check=False, capture_output=True, text=True)
    if completed.stdout and completed.stdout.strip():
        logging.info("TopoFit stdout:\n%s", completed.stdout.rstrip())
    if completed.stderr and completed.stderr.strip():
        logging.info("TopoFit stderr:\n%s", completed.stderr.rstrip())
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(
            completed.returncode,
            command,
            output=completed.stdout,
            stderr=completed.stderr,
        )
    return completed


def run_topofit_workflow(
    input_path: Path,
    run_dir: Path,
    options: TopoFitOptions,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> TopoFitResult:
    """Run TopoFit, validate its meshes, and create a source-grid QC image."""

    validate_options(options)
    input_path = input_path.resolve()
    image = validate_nifti_input(input_path)
    run_dir.mkdir(parents=True, exist_ok=True)
    surface_dir = run_dir / "surf"
    surface_dir.mkdir(parents=True, exist_ok=True)
    command = build_brainnet_command(input_path, surface_dir, options)

    started = perf_counter()
    if options.mock:
        logging.warning("TopoFit mock mode is active; no neural network was run")
        write_mock_surfaces(image, surface_dir)
    else:
        _run_command(command, runner)

    surfaces = validate_surface_outputs(surface_dir)
    qc_path = write_qc_overlay(image, surfaces, run_dir / "topofit_qc.nii.gz")
    elapsed = perf_counter() - started
    manifest_path = run_dir / "topofit_manifest.json"
    result = TopoFitResult(
        status="SURFACE_READY_RESEARCH_ONLY",
        run_dir=str(run_dir.resolve()),
        input_image=str(input_path),
        qc_image=str(qc_path.resolve()),
        manifest=str(manifest_path.resolve()),
        surfaces={name: str(path.resolve()) for name, path in surfaces.items()},
        elapsed_seconds=round(elapsed, 3),
    )
    manifest = asdict(result)
    manifest["options"] = asdict(options)
    manifest["brainnet_command"] = command
    manifest["prescription_coordinates"] = None
    manifest["coordinate_status"] = "WITHHELD_UNTIL_VALIDATED_ANALYSIS_STAGE"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return result
