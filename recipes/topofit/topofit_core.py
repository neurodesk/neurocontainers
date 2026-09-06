"""Reusable TopoFit inference and quality-control workflow.

The OpenRecon adapter and the command-line launcher both call this module. It
does not import ISMRMRD, so the model and geometry path can be exercised
without a scanner connection.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from time import perf_counter
from typing import Callable, Sequence

import nibabel as nib
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from scipy.spatial import cKDTree
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from topofit_geometry import (
    CURVATURE_SIGN_CONVENTION,
    MIDDLE_DEPTH_FRACTION,
    SulcalHemisphere,
    identify_sulcal_middepth,
    triangle_voxel_mask,
    _outward_vertex_normals,
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
MIN_PATCH_NORMAL_COHERENCE = 0.9
DEFAULT_SULCAL_CURVATURE_THRESHOLD_MM_INV = 0.1
PATCH_QC_ANATOMY_MAX = 2000
PATCH_QC_PATCH_INTENSITY = 3000
PATCH_QC_NORMAL_INTENSITY = 4095
PATCH_QC_GLYPH_RADIUS_MM = 4.0
CORTEX_ATLAS_DIR = Path("/opt/topofit-atlas")
MIN_RIBBON_SEPARATION_MM = 0.5
MEDIAL_WALL_MARGIN_MM = 5.0


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
    patch_roi: str | None = None
    patch_count: int = 3
    patch_radius_mm: float = FLAT_PATCH_RADIUS_MM
    patch_max_rms_mm: float = 0.5
    patch_min_area_fraction: float = 0.25
    patch_hemisphere: str = "both"
    patch_search_region: str = "cortex"


@dataclass(frozen=True)
class FlatPatch:
    """One locally planar surface candidate in NIfTI world RAS."""

    surface: str
    center_ras_mm: tuple[float, float, float]
    normal_ras: tuple[float, float, float]
    radius_mm: float
    area_mm2: float
    rms_distance_mm: float
    vertex_count: int
    depth_fraction: float = MIDDLE_DEPTH_FRACTION
    median_ribbon_separation_mm: float = 0.0
    patch_id: str = ""
    normal_coherence: float = 1.0
    score: float = 0.0


@dataclass(frozen=True)
class _DetectedFlatPatch:
    """Flat-patch measurements plus private mesh support for the QC renderer."""

    patch: FlatPatch
    faces: np.ndarray
    vertices: np.ndarray

    @property
    def vertex_indices(self) -> np.ndarray:
        return np.unique(self.faces.reshape(-1))


@dataclass(frozen=True)
class TopoFitResult:
    """Artifacts produced by a completed workflow."""

    status: str
    run_dir: str
    input_image: str
    qc_image: str
    patch_qc_image: str | None
    patch_geometry: str | None
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
    if options.patch_roi is not None and not options.find_flat_patches:
        raise ValueError("patch ROI requires flat-patch analysis")
    if (isinstance(options.patch_count, bool) or not isinstance(options.patch_count, int)
            or not 1 <= options.patch_count <= 10):
        raise ValueError("patch count must be an integer from 1 to 10")
    for name, value, minimum, maximum in (
        ("patch radius", options.patch_radius_mm, 5.0, 20.0),
        ("patch maximum RMS", options.patch_max_rms_mm, 0.01, 2.0),
        ("patch minimum area fraction", options.patch_min_area_fraction, 0.1, 1.0),
    ):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not np.isfinite(value) or not minimum <= value <= maximum):
            raise ValueError(f"{name} must be between {minimum} and {maximum}")
    if options.patch_hemisphere not in {"both", "lh", "rh"}:
        raise ValueError("patch hemisphere must be both, lh, or rh")
    if options.patch_search_region not in {"cortex", "roi"}:
        raise ValueError("patch search region must be cortex or roi")
    if options.patch_search_region == "roi" and not options.patch_roi:
        raise ValueError("ROI search requires a native-grid patch ROI file")
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
    # Subdivide the test mesh so a local 10 mm neighborhood contains faces.
    for _ in range(4):
        edges, inverse = np.unique(
            np.sort(faces[:, [[0, 1], [1, 2], [2, 0]]].reshape(-1, 2), axis=1),
            axis=0, return_inverse=True,
        )
        midpoints = unit_vertices[edges].mean(axis=1)
        midpoints /= np.linalg.norm(midpoints, axis=1, keepdims=True)
        a, b, c = (inverse.reshape(-1, 3) + len(unit_vertices)).T
        v0, v1, v2 = faces.T
        faces = np.concatenate([
            np.column_stack((v0, a, c)), np.column_stack((a, v1, b)),
            np.column_stack((c, b, v2)), np.column_stack((a, b, c)),
        ])
        unit_vertices = np.vstack((unit_vertices, midpoints))
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
            np.clip(normals[face_indices] @ normal, 0.0, 1.0),
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
    limit: int = MAX_PATCH_CANDIDATES,
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
    if ranked.size <= limit:
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
            if len(selected) == limit:
                break
    if not selected:
        selected.append(int(ranked[0]))
    return selected


def _connected_face_component(
    faces: np.ndarray,
    face_indices: np.ndarray,
    seed_index: int,
) -> np.ndarray:
    """Keep the edge-connected part of a local neighborhood containing its seed."""

    face_set = {int(index) for index in face_indices}
    if seed_index not in face_set:
        face_set.add(seed_index)
    edge_faces: dict[tuple[int, int], list[int]] = {}
    for face_index in face_set:
        a, b, c = map(int, faces[face_index])
        for edge in ((a, b), (b, c), (c, a)):
            edge_faces.setdefault(tuple(sorted(edge)), []).append(face_index)

    connected = {seed_index}
    pending = [seed_index]
    while pending:
        face_index = pending.pop()
        a, b, c = map(int, faces[face_index])
        for edge in ((a, b), (b, c), (c, a)):
            for neighbor in edge_faces[tuple(sorted(edge))]:
                if neighbor not in connected:
                    connected.add(neighbor)
                    pending.append(neighbor)
    return np.asarray(sorted(connected), dtype=np.int64)


def _surface_edge_graph(vertices: np.ndarray, faces: np.ndarray) -> csr_matrix:
    edges = np.unique(np.sort(np.concatenate([
        faces[:, [0, 1]], faces[:, [1, 2]], faces[:, [2, 0]],
    ]), axis=1), axis=0)
    lengths = np.linalg.norm(vertices[edges[:, 0]] - vertices[edges[:, 1]], axis=1)
    return csr_matrix((np.tile(lengths, 2), (
        np.concatenate((edges[:, 0], edges[:, 1])),
        np.concatenate((edges[:, 1], edges[:, 0])),
    )), shape=(len(vertices), len(vertices)))


def erode_cortex_mask(
    vertices: np.ndarray, faces: np.ndarray, cortex_mask: np.ndarray,
) -> np.ndarray:
    """Exclude the uncertain transition bordering the mapped medial wall."""

    noncortex = np.flatnonzero(~cortex_mask)
    if not len(noncortex):
        return cortex_mask.copy()
    distance = dijkstra(
        _surface_edge_graph(vertices, faces), indices=noncortex,
        min_only=True, limit=MEDIAL_WALL_MARGIN_MM,
    )
    return cortex_mask & (distance > MEDIAL_WALL_MARGIN_MM)


def find_ranked_patches(
    vertices: np.ndarray,
    faces: np.ndarray,
    surface: str,
    radius_mm: float = FLAT_PATCH_RADIUS_MM,
    eligible_vertices: np.ndarray | None = None,
    *,
    count: int = 3,
    max_rms_mm: float = 0.5,
    min_area_fraction: float = 0.25,
) -> list[_DetectedFlatPatch]:
    """Rank acceptable cortical neighborhoods and suppress shared mesh vertices."""

    vertices = np.asarray(vertices, dtype=float)
    faces = np.asarray(faces, dtype=np.int64)
    if vertices.ndim != 2 or vertices.shape[1] != 3:
        raise ValueError("flat-patch vertices must have shape (n, 3)")
    if faces.ndim != 2 or faces.shape[1] != 3:
        raise ValueError("flat-patch faces must have shape (n, 3)")
    if not np.isfinite(radius_mm) or radius_mm <= 0:
        raise ValueError("flat-patch radius must be positive")

    if eligible_vertices is not None:
        eligible_vertices = np.asarray(eligible_vertices, dtype=bool)
        if eligible_vertices.shape != (len(vertices),):
            raise ValueError("cortical mask must have one value per vertex")
        faces = faces[np.all(eligible_vertices[faces], axis=1)]
    if not len(faces):
        raise ValueError(f"{surface}: no eligible cortical faces")
    valid_faces, centers, normals, areas = _face_geometry(vertices, faces)
    candidate_indices = _candidate_face_indices(
        vertices,
        valid_faces,
        normals,
        areas,
        centers,
        radius_mm,
        limit=max(MAX_PATCH_CANDIDATES, count * 32),
    )
    graph = _surface_edge_graph(vertices, valid_faces)
    target_area = np.pi * radius_mm**2 * min_area_fraction
    candidates = []
    for seed_index in candidate_indices:
        seed_vertex = valid_faces[seed_index, np.argmin(np.linalg.norm(
            vertices[valid_faces[seed_index]] - centers[seed_index], axis=1
        ))]
        distances = dijkstra(graph, indices=int(seed_vertex), limit=radius_mm)
        neighborhood = np.flatnonzero(np.all(
            distances[valid_faces] <= radius_mm, axis=1
        ))
        if seed_index not in neighborhood:
            continue
        center, normal, rms, area, coherence = _fit_face_patch(
            vertices, valid_faces, normals, areas, neighborhood
        )
        aligned = neighborhood[
            (normals[neighborhood] @ normal) >= np.cos(np.deg2rad(45.0))
        ]
        if seed_index not in aligned:
            continue
        if aligned.size != neighborhood.size:
            neighborhood = _connected_face_component(valid_faces, aligned, seed_index)
            center, normal, rms, area, coherence = _fit_face_patch(
                vertices, valid_faces, normals, areas, neighborhood
            )
        if rms > max_rms_mm or area < target_area or coherence < MIN_PATCH_NORMAL_COHERENCE:
            continue
        score = rms + radius_mm * (1.0 - coherence)
        candidates.append((score, seed_index, neighborhood, center, normal, rms, area, coherence))

    accepted = []
    used_vertices = np.zeros(len(vertices), dtype=bool)
    for score, _, support, center, normal, rms, area, coherence in sorted(
        candidates, key=lambda candidate: candidate[:2]
    ):
        selected_faces = valid_faces[support]
        indices = np.unique(selected_faces)
        if np.any(used_vertices[indices]):
            continue
        used_vertices[indices] = True
        center = vertices[indices[np.argmin(np.linalg.norm(vertices[indices] - center, axis=1))]]
        patch = FlatPatch(
            surface=surface, center_ras_mm=tuple(float(v) for v in center),
            normal_ras=tuple(float(v) for v in normal), radius_mm=float(radius_mm),
            area_mm2=area, rms_distance_mm=rms, vertex_count=int(indices.size),
            normal_coherence=coherence, score=score,
        )
        accepted.append(_DetectedFlatPatch(patch=patch, faces=selected_faces, vertices=vertices))
        if len(accepted) == count:
            break
    return accepted


def cortical_ribbon_mask(
    white: np.ndarray, pial: np.ndarray, cortex_mask: np.ndarray,
) -> np.ndarray:
    """Exclude atlas non-cortex and near-collapsed white/pial pairs."""

    return np.asarray(cortex_mask, dtype=bool) & (
        np.linalg.norm(pial - white, axis=1) >= MIN_RIBBON_SEPARATION_MM
    )


def mapped_cortex_mask(
    registration: np.ndarray,
    hemisphere: str,
    atlas_dir: Path = CORTEX_ATLAS_DIR,
) -> np.ndarray:
    """Transfer fsaverage cortex membership through the registration sphere."""

    reference, _ = nib.freesurfer.read_geometry(
        str(atlas_dir / f"{hemisphere}.sphere.reg")
    )
    indices = nib.freesurfer.read_label(str(atlas_dir / f"{hemisphere}.cortex.label"))
    if not len(indices) or indices.min() < 0 or indices.max() >= len(reference):
        raise ValueError("invalid fsaverage cortex label")
    masks = np.zeros(len(reference), dtype=bool)
    masks[indices] = True
    for sphere in (reference, registration):
        if not np.all(np.isfinite(sphere)) or np.any(
            np.linalg.norm(sphere, axis=1) < 1e-6
        ):
            raise ValueError("invalid cortical registration sphere")
    reference = reference / np.linalg.norm(reference, axis=1)[:, None]
    target = registration / np.linalg.norm(registration, axis=1)[:, None]
    _, nearest = cKDTree(reference).query(target)
    return masks[nearest]


def _roi_membership(
    vertices: np.ndarray, roi: nib.spatialimages.SpatialImage,
) -> np.ndarray:
    voxels = np.rint(nib.affines.apply_affine(
        np.linalg.inv(roi.affine), vertices
    )).astype(int)
    inside = np.all((voxels >= 0) & (voxels < np.asarray(roi.shape)), axis=1)
    selected = np.zeros(len(vertices), dtype=bool)
    selected[inside] = np.asarray(roi.dataobj)[tuple(voxels[inside].T)] > 0
    return selected


def find_flat_patches(
    surfaces: dict[str, Path],
    options: TopoFitOptions = TopoFitOptions(),
    *,
    atlas_dir: Path = CORTEX_ATLAS_DIR,
    roi: nib.spatialimages.SpatialImage | None = None,
    mock: bool = False,
) -> dict[str, _DetectedFlatPatch]:
    """Select mid-ribbon cortex, excluding the atlas medial-wall closure."""

    detected = {}
    eligible_face_count = 0
    for hemisphere in ("lh", "rh"):
        if options.patch_hemisphere not in {"both", hemisphere}:
            continue
        pial, faces = nib.freesurfer.read_geometry(str(surfaces[f"{hemisphere}.pial"]))
        white, white_faces = nib.freesurfer.read_geometry(
            str(surfaces[f"{hemisphere}.white"])
        )
        registration, reg_faces = nib.freesurfer.read_geometry(
            str(surfaces[f"{hemisphere}.registration"])
        )
        if white.shape != pial.shape or registration.shape != pial.shape or not (
            np.array_equal(faces, white_faces) and np.array_equal(faces, reg_faces)
        ):
            raise ValueError("patch analysis requires corresponding surface topology")
        cortex = np.ones(len(pial), dtype=bool) if mock else mapped_cortex_mask(
            registration, hemisphere, atlas_dir
        )
        middle = white + MIDDLE_DEPTH_FRACTION * (pial - white)
        cortex = erode_cortex_mask(middle, faces, cortex)
        eligible = cortical_ribbon_mask(white, pial, cortex)
        if roi is not None:
            eligible &= _roi_membership(middle, roi)
        count = int(np.count_nonzero(np.all(eligible[faces], axis=1)))
        eligible_face_count += count
        if not count:
            continue
        candidates = find_ranked_patches(
            middle, faces, f"{hemisphere}.mid", eligible_vertices=eligible,
            radius_mm=options.patch_radius_mm, count=options.patch_count,
            max_rms_mm=options.patch_max_rms_mm,
            min_area_fraction=options.patch_min_area_fraction,
        )
        for rank, detection in enumerate(candidates, 1):
            indices = detection.vertex_indices
            normal = np.asarray(detection.patch.normal_ras)
            if np.dot(normal, np.mean(pial[indices] - white[indices], axis=0)) < 0:
                normal = -normal
            patch_id = f"{hemisphere.upper()}{rank:02d}"
            detected[patch_id] = replace(detection, patch=replace(
                detection.patch, patch_id=patch_id, normal_ras=tuple(float(v) for v in normal),
                median_ribbon_separation_mm=float(np.median(
                    np.linalg.norm(pial[indices] - white[indices], axis=1)
                )),
            ))
    if not eligible_face_count:
        raise ValueError("ROI contains no eligible cortical patch" if roi is not None
                         else "No eligible cortical faces in the requested hemisphere")
    return detected


def _scaled_anatomy(
    image: nib.spatialimages.SpatialImage,
    maximum: int = 3000,
) -> np.ndarray:
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
    return np.rint(scaled * maximum).astype(np.int16)


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
        vertices = detection.vertices
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


def _draw_line_in_plane(
    mask: np.ndarray,
    slice_index: int,
    start: np.ndarray,
    end: np.ndarray,
) -> None:
    sample_count = max(int(np.ceil(np.linalg.norm(end - start) * 2.0)) + 1, 2)
    points = np.rint(
        np.linspace(start, end, sample_count, dtype=float)
    ).astype(int)
    inside = np.all(points >= 0, axis=1) & np.all(
        points < np.asarray(mask.shape[:2]), axis=1
    )
    points = points[inside]
    if points.size:
        mask[points[:, 0], points[:, 1], slice_index] = True


def _normal_glyph_mask(
    image: nib.spatialimages.SpatialImage,
    patch_mask: np.ndarray,
    patch: FlatPatch,
) -> np.ndarray:
    """Draw one slice-native glyph without changing the measured normal."""

    glyph = np.zeros(image.shape, dtype=bool)
    occupied_slices = np.flatnonzero(np.any(patch_mask, axis=(0, 1)))
    if occupied_slices.size == 0:
        return glyph

    inverse_affine = np.linalg.inv(image.affine)
    center_voxel = nib.affines.apply_affine(
        inverse_affine, np.asarray(patch.center_ras_mm, dtype=float)
    )
    slice_index = int(
        occupied_slices[np.argmin(np.abs(occupied_slices - center_voxel[2]))]
    )
    center = np.rint(center_voxel[:2]).astype(int)
    center = np.clip(center, 0, np.asarray(image.shape[:2]) - 1)

    zooms = np.asarray(image.header.get_zooms()[:2], dtype=float)
    bounds = np.ceil(PATCH_QC_GLYPH_RADIUS_MM / zooms).astype(int) + 1
    axis_0 = np.arange(center[0] - bounds[0], center[0] + bounds[0] + 1)
    axis_1 = np.arange(center[1] - bounds[1], center[1] + bounds[1] + 1)
    grid_0, grid_1 = np.meshgrid(axis_0, axis_1, indexing="ij")
    distance_mm = np.sqrt(
        ((grid_0 - center[0]) * zooms[0]) ** 2
        + ((grid_1 - center[1]) * zooms[1]) ** 2
    )
    ring = np.abs(distance_mm - PATCH_QC_GLYPH_RADIUS_MM) <= max(zooms) * 0.6
    inside = (
        (grid_0 >= 0)
        & (grid_0 < image.shape[0])
        & (grid_1 >= 0)
        & (grid_1 < image.shape[1])
        & ring
    )
    glyph[grid_0[inside], grid_1[inside], slice_index] = True

    voxel_normal = inverse_affine[:3, :3] @ (
        FLAT_PATCH_NORMAL_LENGTH_MM
        * np.asarray(patch.normal_ras, dtype=float)
    )
    in_plane_endpoint = center.astype(float) + voxel_normal[:2]
    _draw_line_in_plane(
        glyph,
        slice_index,
        center.astype(float),
        in_plane_endpoint,
    )

    in_plane_length = float(np.linalg.norm(voxel_normal[:2]))
    if in_plane_length >= 3.0:
        direction = voxel_normal[:2] / in_plane_length
        perpendicular = np.asarray([-direction[1], direction[0]])
        arm_length = min(3.0, in_plane_length * 0.4)
        for side in (-1.0, 1.0):
            arm_endpoint = (
                in_plane_endpoint
                - direction * arm_length
                + perpendicular * side * arm_length * 0.6
            )
            _draw_line_in_plane(
                glyph,
                slice_index,
                in_plane_endpoint,
                arm_endpoint,
            )

    if voxel_normal[2] >= 0:
        glyph[center[0], center[1], slice_index] = True
    else:
        for offset in range(-2, 3):
            for sign in (-1, 1):
                point = center + (offset, sign * offset)
                if np.all(point >= 0) and np.all(point < np.asarray(image.shape[:2])):
                    glyph[point[0], point[1], slice_index] = True
    if patch.patch_id:
        label = Image.new("L", (image.shape[0], image.shape[1]))
        draw = ImageDraw.Draw(label)
        font = ImageFont.load_default(size=10)
        width = int(draw.textlength(patch.patch_id, font=font)) + 2
        x = int(np.clip(center[0] + 8, 0, max(image.shape[0] - width, 0)))
        y = int(np.clip(center[1] - 14, 0, max(image.shape[1] - 12, 0)))
        draw.text((x, y), patch.patch_id, font=font, fill=255)
        glyph[:, :, slice_index] |= np.asarray(label).T > 0
    return glyph


def write_patch_qc(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    detections: dict[str, _DetectedFlatPatch],
    output_path: Path,
) -> Path:
    """Write a source-grid illustration of selected patches and normals."""

    output = _scaled_anatomy(image, PATCH_QC_ANATOMY_MAX)
    patch_mask = np.zeros(image.shape, dtype=bool)
    normal_mask = np.zeros(image.shape, dtype=bool)
    for detection in detections.values():
        vertices = detection.vertices
        hemisphere = detection.patch.surface.split(".")[0]
        for boundary, intensity in (("white", 2400), ("pial", 2700)):
            boundary_vertices, _ = nib.freesurfer.read_geometry(
                str(surfaces[f"{hemisphere}.{boundary}"])
            )
            boundary_mask = triangle_voxel_mask(
                tuple(image.shape), image.affine, boundary_vertices, detection.faces
            )
            output[boundary_mask] = intensity
        selected_patch = triangle_voxel_mask(
            tuple(int(value) for value in image.shape),
            image.affine,
            vertices,
            detection.faces,
        )
        patch_mask |= selected_patch
        normal_mask |= _normal_glyph_mask(image, selected_patch, detection.patch)

    output[patch_mask] = PATCH_QC_PATCH_INTENSITY
    output[normal_mask] = PATCH_QC_NORMAL_INTENSITY
    header = image.header.copy()
    header.set_data_dtype(np.int16)
    header["descrip"] = b"TopoFit selected patch and normal illustration"
    patch_qc = nib.Nifti1Image(output, image.affine, header=header)
    patch_qc.set_qform(image.affine, code=1)
    patch_qc.set_sform(image.affine, code=1)
    nib.save(patch_qc, str(output_path))
    return output_path


def write_patch_geometry(
    surfaces: dict[str, Path],
    detections: dict[str, _DetectedFlatPatch],
    output_path: Path,
) -> Path:
    """Export paired ribbon coordinates and local mid-surface normals for analysis."""

    arrays = {"depth_fraction": np.asarray(MIDDLE_DEPTH_FRACTION)}
    normal_cache = {}
    for patch_id, detection in detections.items():
        hemisphere = detection.patch.surface.split(".")[0]
        white, faces = nib.freesurfer.read_geometry(str(surfaces[f"{hemisphere}.white"]))
        pial, _ = nib.freesurfer.read_geometry(str(surfaces[f"{hemisphere}.pial"]))
        middle = detection.vertices
        if hemisphere not in normal_cache:
            normals, _ = _outward_vertex_normals(middle, faces, pial - white)
            signs = np.einsum("ij,ij->i", normals, pial - white)
            normals[signs < 0] *= -1
            normal_cache[hemisphere] = normals
        normals = normal_cache[hemisphere]
        indices = detection.vertex_indices
        arrays.update({
            f"{patch_id}_vertex_indices": indices,
            f"{patch_id}_faces": np.searchsorted(indices, detection.faces),
            f"{patch_id}_white_ras_mm": white[indices],
            f"{patch_id}_pial_ras_mm": pial[indices],
            f"{patch_id}_mid_ras_mm": middle[indices],
            f"{patch_id}_normals_ras": normals[indices],
            f"{patch_id}_ribbon_separation_mm": np.linalg.norm(
                pial[indices] - white[indices], axis=1
            ),
        })
    np.savez_compressed(output_path, **arrays)
    return output_path


def _find_sulcal_middepth(
    image: nib.spatialimages.SpatialImage,
    surfaces: dict[str, Path],
    threshold_mm_inv: float,
    *,
    mock: bool = False,
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
        registration, _ = nib.freesurfer.read_geometry(
            str(surfaces[f"{hemisphere}.registration"])
        )
        cortex = np.ones(len(pial_vertices), dtype=bool) if mock else mapped_cortex_mask(
            registration, hemisphere
        )
        cortex = erode_cortex_mask((white_vertices + pial_vertices) / 2, pial_faces, cortex)
        eligible = cortical_ribbon_mask(white_vertices, pial_vertices, cortex)
        detection = identify_sulcal_middepth(
            hemisphere,
            white_vertices,
            white_faces,
            pial_vertices,
            pial_faces,
            threshold_mm_inv,
            eligible_vertices=eligible,
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
    roi = validate_nifti_input(Path(options.patch_roi)) if options.patch_roi else None
    if roi is not None:
        if roi.shape != image.shape or not np.allclose(roi.affine, image.affine, atol=1e-4):
            raise ValueError("patch ROI must be registered on the input image grid")
        roi_data = np.asarray(roi.dataobj)
        if not np.all(np.isfinite(roi_data)) or np.any(roi_data < 0):
            raise ValueError("patch ROI must contain finite nonnegative mask values")
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
        find_flat_patches(surfaces, options, roi=roi, mock=options.mock)
        if options.find_flat_patches else {}
    )
    sulcal_detections, sulcal_masks = (
        _find_sulcal_middepth(
            image, surfaces, options.sulcal_curvature_threshold_mm_inv, mock=options.mock
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
    patch_qc_path = (
        write_patch_qc(
            image,
            surfaces,
            detected_flat_patches,
            run_dir / "topofit_patch_qc.nii.gz",
        )
        if detected_flat_patches
        else None
    )
    patch_geometry_path = (
        write_patch_geometry(surfaces, detected_flat_patches, run_dir / "topofit_patch_geometry.npz")
        if detected_flat_patches else None
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
        patch_qc_image=(
            str(patch_qc_path.resolve()) if patch_qc_path else None
        ),
        patch_geometry=str(patch_geometry_path.resolve()) if patch_geometry_path else None,
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
        "CANDIDATES_REPORTED_RESEARCH_ONLY" if flat_patches else
        "NO_PATCH_MEETS_CRITERIA" if options.find_flat_patches else "DISABLED"
    )
    manifest["patch_counts"] = {
        hemisphere: sum(p.surface.startswith(hemisphere + ".") for p in flat_patches.values())
        for hemisphere in ("lh", "rh")
    }
    if options.find_flat_patches:
        logging.info("Cortical patches: %s; requested up to %d per hemisphere",
                     manifest["patch_counts"], options.patch_count)
    manifest["flat_patch_definition"] = {
        "surface": "white + 0.5 * (pial - white)",
        "cortex_mask": "mock_only" if options.mock else "fsaverage_cortex_via_registration",
        "minimum_ribbon_separation_mm": MIN_RIBBON_SEPARATION_MM,
        "neighborhood": "mesh_edge_geodesic_radius",
        "roi": options.patch_roi,
        "medial_wall_margin_mm": MEDIAL_WALL_MARGIN_MM,
        "selection": "independent_candidates_not_homologous_controls",
        "overlap": "no_shared_vertices_within_hemisphere",
        "minimum_normal_coherence": MIN_PATCH_NORMAL_COHERENCE,
        "minimum_area_mm2": np.pi * options.patch_radius_mm**2 * options.patch_min_area_fraction,
        "schema_version": 2,
        "normal_ras": "representative_patch_plane_normal",
        "local_normals": "topofit_patch_geometry.npz; outward mid-surface unit normals",
    }
    manifest["sulcal_middepth_status"] = (
        "VOXELS_REPORTED_RESEARCH_ONLY" if sulcal_mask_path else "DISABLED"
    )
    manifest["sulcal_middepth_definition"] = {
        "depth_fraction": MIDDLE_DEPTH_FRACTION,
        "cortex_mask": "mock_only" if options.mock else "fsaverage_cortex_via_registration",
        "medial_wall_margin_mm": MEDIAL_WALL_MARGIN_MM,
        "minimum_ribbon_separation_mm": MIN_RIBBON_SEPARATION_MM,
        "curvature_surface": "pial",
        "curvature_method": "cotangent_mean_curvature",
        "curvature_units": "mm^-1",
        "curvature_sign_convention": CURVATURE_SIGN_CONVENTION,
        "face_selection": "all_vertices_at_or_below_negative_threshold",
        "mask_labels": {"1": "left", "2": "right", "3": "bilateral_overlap"},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return result
