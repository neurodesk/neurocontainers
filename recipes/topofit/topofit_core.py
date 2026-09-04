"""Reusable TopoFit inference and quality-control workflow.

The OpenRecon adapter and the command-line launcher both call this module. It
does not import ISMRMRD, so the model and geometry path can be exercised
without a scanner connection.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import Callable, Sequence

import nibabel as nib
import numpy as np
from scipy.spatial import cKDTree
from topofit_geometry import (
    CURVATURE_SIGN_CONVENTION,
    MIDDLE_DEPTH_FRACTION,
    SulcalHemisphere,
    identify_sulcal_middepth,
    triangle_voxel_mask,
)

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
FLAT_PATCH_RADIUS_MM = 10.0
FLAT_PATCH_NORMAL_LENGTH_MM = 20.0
MAX_OVERLAY_THICKNESS = 3
MAX_PATCH_CANDIDATES = 64
DEFAULT_SULCAL_CURVATURE_THRESHOLD_MM_INV = 0.1


@dataclass(frozen=True)
class TopoFitOptions:
    """Validated options for one inference run."""

    device: str = "cuda"
    preset: str = "t1w_1mm"
    conform: bool = True
    mock: bool = False
    find_flat_patches: bool = False
    find_sulcal_middepth: bool = False
    sulcal_curvature_threshold_mm_inv: float = (
        DEFAULT_SULCAL_CURVATURE_THRESHOLD_MM_INV
    )
    overlay_thickness: int = 1


@dataclass(frozen=True)
class FlatPatch:
    """One locally planar pial-surface candidate in NIfTI world RAS."""

    surface: str
    center_ras_mm: tuple[float, float, float]
    normal_ras: tuple[float, float, float]
    radius_mm: float
    area_mm2: float
    rms_distance_mm: float
    vertex_count: int


@dataclass(frozen=True)
class _DetectedFlatPatch:
    """Flat-patch measurements plus private mesh support for the QC renderer."""

    patch: FlatPatch
    vertex_indices: np.ndarray


@dataclass(frozen=True)
class TopoFitResult:
    """Artifacts produced by a completed workflow."""

    status: str
    run_dir: str
    input_image: str
    qc_image: str
    manifest: str
    surfaces: dict[str, str]
    flat_patches: dict[str, FlatPatch]
    sulcal_middepth_mask: str | None
    sulci: dict[str, dict[str, float | int | str]]
    elapsed_seconds: float
    warning: str = RESEARCH_WARNING


def validate_options(options: TopoFitOptions) -> tuple[str, str]:
    """Validate external options and return BrainNet contrast and resolution."""

    if options.device not in {"cuda", "cpu"}:
        raise ValueError("device must be 'cuda' or 'cpu'")
    if (
        not isinstance(options.overlay_thickness, int)
        or isinstance(options.overlay_thickness, bool)
        or not 0 <= options.overlay_thickness <= MAX_OVERLAY_THICKNESS
    ):
        raise ValueError(
            f"overlay thickness must be an integer from 0 to {MAX_OVERLAY_THICKNESS}"
        )
    if (
        not isinstance(options.sulcal_curvature_threshold_mm_inv, (int, float))
        or isinstance(options.sulcal_curvature_threshold_mm_inv, bool)
        or not np.isfinite(options.sulcal_curvature_threshold_mm_inv)
        or options.sulcal_curvature_threshold_mm_inv <= 0
    ):
        raise ValueError("sulcal curvature threshold must be a positive finite value")
    try:
        return MODEL_PRESETS[options.preset]
    except KeyError:
        valid = ", ".join(sorted(MODEL_PRESETS))
        raise ValueError(
            f"unknown model preset {options.preset!r}; choose {valid}"
        ) from None


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
        raise ValueError(
            "MRD position and direction vectors must each have three values"
        )
    if not np.all(np.isfinite(position_array)) or any(
        not np.all(np.isfinite(item)) for item in raw_directions
    ):
        raise ValueError("MRD position and direction vectors must be finite")
    if (
        spacing.shape != (3,)
        or not np.all(np.isfinite(spacing))
        or np.any(spacing <= 0)
    ):
        raise ValueError(
            f"voxel size must contain three positive values, got {spacing}"
        )
    if shape.shape != (2,) or np.any(shape < 2):
        raise ValueError(
            f"in-plane shape must contain two values greater than one, got {shape}"
        )

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
    if (
        not np.all(np.isfinite(affine))
        or abs(float(np.linalg.det(affine[:3, :3]))) < 1e-8
    ):
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
            raise RuntimeError(
                f"surface {name} has invalid vertices shape {vertices.shape}"
            )
        if faces.ndim != 2 or faces.shape[1] != 3 or faces.shape[0] < 4:
            raise RuntimeError(f"surface {name} has invalid faces shape {faces.shape}")
        if not np.all(np.isfinite(vertices)):
            raise RuntimeError(f"surface {name} contains non-finite vertices")
        if int(faces.min()) < 0 or int(faces.max()) >= vertices.shape[0]:
            raise RuntimeError(f"surface {name} contains out-of-range face indices")
        outputs[name] = path
    return outputs


def _face_geometry(
    vertices: np.ndarray,
    faces: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    triangles = vertices[faces]
    cross_products = np.cross(
        triangles[:, 1] - triangles[:, 0],
        triangles[:, 2] - triangles[:, 0],
    )
    twice_area = np.linalg.norm(cross_products, axis=1)
    valid = twice_area > 1e-8
    if int(valid.sum()) < 3:
        raise ValueError(
            "flat-patch analysis requires at least three non-degenerate faces"
        )

    valid_faces = faces[valid]
    centers = triangles[valid].mean(axis=1)
    areas = twice_area[valid] * 0.5
    normals = cross_products[valid] / twice_area[valid, np.newaxis]

    mesh_center = vertices.mean(axis=0)
    orientation = np.sum(areas * np.einsum("ij,ij->i", normals, centers - mesh_center))
    if orientation < 0:
        normals = -normals
    return valid_faces, centers, normals, areas


def _fit_face_patch(
    vertices: np.ndarray,
    faces: np.ndarray,
    normals: np.ndarray,
    areas: np.ndarray,
    face_indices: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, float, float, float]:
    selected_faces = faces[face_indices]
    selected_areas = areas[face_indices]
    points = vertices[selected_faces].reshape(-1, 3)
    weights = np.repeat(selected_areas / 3.0, 3)
    total_area = float(selected_areas.sum())
    center = np.average(points, axis=0, weights=weights)
    centered = points - center
    covariance = (centered * weights[:, np.newaxis]).T @ centered / weights.sum()
    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    normal = eigenvectors[:, int(np.argmin(eigenvalues))]

    mean_normal = np.sum(normals[face_indices] * selected_areas[:, np.newaxis], axis=0)
    if float(np.dot(normal, mean_normal)) < 0:
        normal = -normal
    normal /= np.linalg.norm(normal)
    residuals = centered @ normal
    rms_distance = float(np.sqrt(np.average(residuals**2, weights=weights)))
    coherence = float(
        np.average(
            np.abs(normals[face_indices] @ normal),
            weights=selected_areas,
        )
    )
    return center, normal, rms_distance, total_area, coherence


def _candidate_face_indices(
    vertices: np.ndarray,
    faces: np.ndarray,
    normals: np.ndarray,
    areas: np.ndarray,
    centers: np.ndarray,
    radius_mm: float,
) -> list[int]:
    vertex_normal_sums = np.zeros_like(vertices, dtype=float)
    weighted_normals = normals * areas[:, np.newaxis]
    for corner in range(3):
        np.add.at(vertex_normal_sums, faces[:, corner], weighted_normals)
    magnitudes = np.linalg.norm(vertex_normal_sums, axis=1)
    valid = magnitudes > 1e-8
    vertex_normal_sums[valid] /= magnitudes[valid, np.newaxis]
    alignment = np.einsum("ij,ikj->ik", normals, vertex_normal_sums[faces])
    local_bending = 1.0 - np.mean(np.clip(alignment, -1.0, 1.0), axis=1)
    ranked = np.lexsort((np.arange(faces.shape[0]), local_bending))
    if ranked.size <= MAX_PATCH_CANDIDATES:
        return [int(index) for index in ranked]

    selected: list[int] = []
    minimum_separation = radius_mm * 0.5
    for index in ranked:
        point = centers[index]
        if all(
            float(np.linalg.norm(point - centers[other])) >= minimum_separation
            for other in selected
        ):
            selected.append(int(index))
            if len(selected) == MAX_PATCH_CANDIDATES:
                break
    if not selected:
        selected.append(int(ranked[0]))
    return selected


def _connected_face_component(
    faces: np.ndarray,
    face_indices: np.ndarray,
    seed_index: int,
) -> np.ndarray:
    """Keep the vertex-connected part of a local neighborhood containing its seed."""

    face_set = {int(index) for index in face_indices}
    if seed_index not in face_set:
        face_set.add(seed_index)
    vertex_faces: dict[int, list[int]] = {}
    for face_index in face_set:
        for vertex_index in faces[face_index]:
            vertex_faces.setdefault(int(vertex_index), []).append(face_index)

    connected = {seed_index}
    pending = [seed_index]
    while pending:
        face_index = pending.pop()
        for vertex_index in faces[face_index]:
            for neighbor in vertex_faces[int(vertex_index)]:
                if neighbor not in connected:
                    connected.add(neighbor)
                    pending.append(neighbor)
    return np.asarray(sorted(connected), dtype=np.int64)


def find_flattest_patch(
    vertices: np.ndarray,
    faces: np.ndarray,
    surface: str,
    radius_mm: float = FLAT_PATCH_RADIUS_MM,
) -> _DetectedFlatPatch:
    """Find the lowest-residual fixed-radius planar neighborhood on one mesh."""

    vertices = np.asarray(vertices, dtype=float)
    faces = np.asarray(faces, dtype=np.int64)
    if vertices.ndim != 2 or vertices.shape[1] != 3:
        raise ValueError("flat-patch vertices must have shape (n, 3)")
    if faces.ndim != 2 or faces.shape[1] != 3:
        raise ValueError("flat-patch faces must have shape (n, 3)")
    if not np.isfinite(radius_mm) or radius_mm <= 0:
        raise ValueError("flat-patch radius must be positive")

    valid_faces, centers, normals, areas = _face_geometry(vertices, faces)
    candidate_indices = _candidate_face_indices(
        vertices,
        valid_faces,
        normals,
        areas,
        centers,
        radius_mm,
    )
    tree = cKDTree(centers)
    target_area = np.pi * radius_mm**2 * 0.25
    best = None
    for seed_index in candidate_indices:
        neighborhood = np.asarray(
            tree.query_ball_point(centers[seed_index], radius_mm), dtype=np.int64
        )
        neighborhood = _connected_face_component(valid_faces, neighborhood, seed_index)
        if neighborhood.size < 3:
            neighborhood = np.asarray([seed_index], dtype=np.int64)
        center, normal, rms, area, coherence = _fit_face_patch(
            vertices, valid_faces, normals, areas, neighborhood
        )
        aligned = neighborhood[
            np.abs(normals[neighborhood] @ normal) >= np.cos(np.deg2rad(45.0))
        ]
        if aligned.size >= 3 and aligned.size != neighborhood.size:
            neighborhood = aligned
            center, normal, rms, area, coherence = _fit_face_patch(
                vertices, valid_faces, normals, areas, neighborhood
            )
        coverage_penalty = max(0.0, target_area - area) / target_area * radius_mm
        score = rms + radius_mm * (1.0 - coherence) + coverage_penalty
        candidate = (score, seed_index, neighborhood, center, normal, rms, area)
        if best is None or candidate[:2] < best[:2]:
            best = candidate

    assert best is not None
    _, _, support_faces, center, normal, rms, area = best
    vertex_indices = np.unique(valid_faces[support_faces].reshape(-1))
    patch = FlatPatch(
        surface=surface,
        center_ras_mm=tuple(float(value) for value in center),
        normal_ras=tuple(float(value) for value in normal),
        radius_mm=float(radius_mm),
        area_mm2=area,
        rms_distance_mm=rms,
        vertex_count=int(vertex_indices.size),
    )
    return _DetectedFlatPatch(patch=patch, vertex_indices=vertex_indices)


def find_flat_patches(
    surfaces: dict[str, Path],
) -> dict[str, _DetectedFlatPatch]:
    """Find one fixed-radius flat pial candidate per hemisphere."""

    detected = {}
    for hemisphere in ("lh", "rh"):
        surface = f"{hemisphere}.pial"
        vertices, faces = nib.freesurfer.read_geometry(str(surfaces[surface]))
        detected[hemisphere] = find_flattest_patch(vertices, faces, surface)
    return detected


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


def _dilate_in_plane(mask: np.ndarray, radius: int) -> np.ndarray:
    """Expand a mask in image axes 0 and 1 without wrapping at the edges."""

    dilated = mask.copy()
    for _ in range(radius):
        expanded = dilated.copy()
        expanded[1:, :, :] |= dilated[:-1, :, :]
        expanded[:-1, :, :] |= dilated[1:, :, :]
        expanded[:, 1:, :] |= dilated[:, :-1, :]
        expanded[:, :-1, :] |= dilated[:, 1:, :]
        dilated = expanded
    return dilated


def _world_points_voxel_mask(
    image: nib.spatialimages.SpatialImage,
    world_points: np.ndarray,
    thickness: int,
) -> np.ndarray:
    mask = np.zeros(image.shape, dtype=bool)
    inverse_affine = np.linalg.inv(image.affine)
    voxel_points = np.rint(
        nib.affines.apply_affine(inverse_affine, world_points)
    ).astype(int)
    inside = np.all(voxel_points >= 0, axis=1) & np.all(
        voxel_points < np.asarray(image.shape), axis=1
    )
    voxel_points = voxel_points[inside]
    if voxel_points.size:
        mask[tuple(voxel_points.T)] = True
    return _dilate_in_plane(mask, thickness)


def _surface_voxel_mask(
    image: nib.spatialimages.SpatialImage,
    paths: Sequence[Path],
    thickness: int,
) -> np.ndarray:
    mask = np.zeros(image.shape, dtype=bool)
    for path in paths:
        vertices, _ = nib.freesurfer.read_geometry(str(path))
        mask |= _world_points_voxel_mask(image, vertices, 0)
    return _dilate_in_plane(mask, thickness)


def _flat_patch_masks(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    detections: dict[str, _DetectedFlatPatch],
    thickness: int,
) -> tuple[np.ndarray, np.ndarray]:
    patch_mask = np.zeros(image.shape, dtype=bool)
    normal_mask = np.zeros(image.shape, dtype=bool)
    sample_step = max(min(image.header.get_zooms()[:3]) * 0.5, 0.1)
    sample_count = int(np.ceil(FLAT_PATCH_NORMAL_LENGTH_MM / sample_step)) + 1
    distances = np.linspace(0.0, FLAT_PATCH_NORMAL_LENGTH_MM, sample_count)
    for detection in detections.values():
        vertices, _ = nib.freesurfer.read_geometry(
            str(surfaces[detection.patch.surface])
        )
        patch_mask |= _world_points_voxel_mask(
            image,
            vertices[detection.vertex_indices],
            thickness,
        )
        center = np.asarray(detection.patch.center_ras_mm)
        normal = np.asarray(detection.patch.normal_ras)
        line_points = center + distances[:, np.newaxis] * normal
        normal_mask |= _world_points_voxel_mask(
            image,
            line_points,
            thickness,
        )
    return patch_mask, normal_mask


def _find_sulcal_middepth(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    threshold_mm_inv: float,
) -> tuple[dict[str, SulcalHemisphere], dict[str, np.ndarray]]:
    detections = {}
    masks = {}
    for hemisphere in ("lh", "rh"):
        white_vertices, white_faces = nib.freesurfer.read_geometry(
            str(surfaces[f"{hemisphere}.white"])
        )
        pial_vertices, pial_faces = nib.freesurfer.read_geometry(
            str(surfaces[f"{hemisphere}.pial"])
        )
        detection = identify_sulcal_middepth(
            hemisphere,
            white_vertices,
            white_faces,
            pial_vertices,
            pial_faces,
            threshold_mm_inv,
        )
        detections[hemisphere] = detection
        masks[hemisphere] = triangle_voxel_mask(
            tuple(int(value) for value in image.shape),
            image.affine,
            detection.middle_vertices_ras_mm,
            detection.selected_faces,
        )
    return detections, masks


def write_sulcal_middepth_mask(
    image: nib.spatialimages.SpatialImage,
    masks: dict[str, np.ndarray],
    output_path: Path,
) -> Path:
    """Write a bilateral label image on the source voxel grid."""

    labels = np.zeros(image.shape, dtype=np.uint8)
    labels[masks["lh"]] |= 1
    labels[masks["rh"]] |= 2
    header = image.header.copy()
    header.set_data_dtype(np.uint8)
    header["descrip"] = b"TopoFit sulcal mid-depth research mask"
    output = nib.Nifti1Image(labels, image.affine, header=header)
    output.set_qform(image.affine, code=1)
    output.set_sform(image.affine, code=1)
    nib.save(output, str(output_path))
    return output_path


def write_qc_overlay(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    output_path: Path,
    overlay_thickness: int = 1,
    flat_patches: dict[str, _DetectedFlatPatch] | None = None,
    sulcal_middepth_masks: dict[str, np.ndarray] | None = None,
) -> Path:
    """Rasterize white and pial vertices onto the source voxel grid."""

    output = _scaled_anatomy(image)
    pial_mask = _surface_voxel_mask(
        image,
        [surfaces["lh.pial"], surfaces["rh.pial"]],
        overlay_thickness,
    )
    white_mask = _surface_voxel_mask(
        image,
        [surfaces["lh.white"], surfaces["rh.white"]],
        overlay_thickness,
    )
    output[pial_mask] = 3500
    if sulcal_middepth_masks:
        output[sulcal_middepth_masks["lh"] | sulcal_middepth_masks["rh"]] = 3650
    output[white_mask] = 4095
    if flat_patches:
        patch_mask, normal_mask = _flat_patch_masks(
            image,
            surfaces,
            flat_patches,
            overlay_thickness,
        )
        output[patch_mask] = 3800
        output[normal_mask] = 4095

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
    detected_flat_patches = (
        find_flat_patches(surfaces) if options.find_flat_patches else {}
    )
    sulcal_detections, sulcal_masks = (
        _find_sulcal_middepth(
            image, surfaces, options.sulcal_curvature_threshold_mm_inv
        )
        if options.find_sulcal_middepth
        else ({}, {})
    )
    sulcal_mask_path = (
        write_sulcal_middepth_mask(
            image, sulcal_masks, run_dir / "topofit_sulcal_middepth_mask.nii.gz"
        )
        if sulcal_masks
        else None
    )
    qc_path = write_qc_overlay(
        image,
        surfaces,
        run_dir / "topofit_qc.nii.gz",
        overlay_thickness=options.overlay_thickness,
        flat_patches=detected_flat_patches,
        sulcal_middepth_masks=sulcal_masks,
    )
    elapsed = perf_counter() - started
    manifest_path = run_dir / "topofit_manifest.json"
    flat_patches = {
        hemisphere: detection.patch
        for hemisphere, detection in detected_flat_patches.items()
    }
    sulci = {
        hemisphere: {
            "hemisphere": detection.hemisphere,
            "threshold_mm_inv": detection.threshold_mm_inv,
            "selected_vertex_count": detection.selected_vertex_count,
            "selected_face_count": detection.selected_face_count,
            "intersecting_voxel_count": int(sulcal_masks[hemisphere].sum()),
            "curvature_min_mm_inv": detection.curvature_min_mm_inv,
            "curvature_median_mm_inv": detection.curvature_median_mm_inv,
        }
        for hemisphere, detection in sulcal_detections.items()
    }
    result = TopoFitResult(
        status="SURFACE_READY_RESEARCH_ONLY",
        run_dir=str(run_dir.resolve()),
        input_image=str(input_path),
        qc_image=str(qc_path.resolve()),
        manifest=str(manifest_path.resolve()),
        surfaces={name: str(path.resolve()) for name, path in surfaces.items()},
        flat_patches=flat_patches,
        sulcal_middepth_mask=(
            str(sulcal_mask_path.resolve()) if sulcal_mask_path else None
        ),
        sulci=sulci,
        elapsed_seconds=round(elapsed, 3),
    )
    manifest = asdict(result)
    manifest["options"] = asdict(options)
    manifest["brainnet_command"] = command
    manifest["prescription_coordinates"] = None
    manifest["coordinate_status"] = "WITHHELD_UNTIL_VALIDATED_ANALYSIS_STAGE"
    manifest["flat_patch_status"] = (
        "CANDIDATES_REPORTED_RESEARCH_ONLY" if flat_patches else "DISABLED"
    )
    manifest["sulcal_middepth_status"] = (
        "VOXELS_REPORTED_RESEARCH_ONLY" if sulcal_mask_path else "DISABLED"
    )
    manifest["sulcal_middepth_definition"] = {
        "depth_fraction": MIDDLE_DEPTH_FRACTION,
        "curvature_surface": "pial",
        "curvature_method": "cotangent_mean_curvature",
        "curvature_units": "mm^-1",
        "curvature_sign_convention": CURVATURE_SIGN_CONVENTION,
        "face_selection": "all_vertices_at_or_below_negative_threshold",
        "mask_labels": {"1": "left", "2": "right", "3": "bilateral_overlap"},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return result
