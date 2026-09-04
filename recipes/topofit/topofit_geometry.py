"""Curvature and source-grid geometry for TopoFit cortical surfaces."""

from __future__ import annotations

from dataclasses import dataclass

import nibabel as nib
import numpy as np


CURVATURE_SIGN_CONVENTION = "positive_convex_negative_sulcal"
MIDDLE_DEPTH_FRACTION = 0.5


@dataclass(frozen=True)
class SulcalHemisphere:
    """Measurements and mesh support for one hemisphere's sulcal selection."""

    hemisphere: str
    threshold_mm_inv: float
    selected_vertex_count: int
    selected_face_count: int
    curvature_min_mm_inv: float
    curvature_median_mm_inv: float
    middle_vertices_ras_mm: np.ndarray
    selected_faces: np.ndarray


def _validated_surface_pair(
    white_vertices: np.ndarray,
    white_faces: np.ndarray,
    pial_vertices: np.ndarray,
    pial_faces: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    white = np.asarray(white_vertices, dtype=float)
    pial = np.asarray(pial_vertices, dtype=float)
    white_topology = np.asarray(white_faces, dtype=np.int64)
    pial_topology = np.asarray(pial_faces, dtype=np.int64)
    if white.ndim != 2 or white.shape[1] != 3 or pial.shape != white.shape:
        raise ValueError("white and pial surfaces must have corresponding (n, 3) vertices")
    if white_topology.ndim != 2 or white_topology.shape[1] != 3:
        raise ValueError("white and pial surfaces must have triangular faces")
    if not np.array_equal(white_topology, pial_topology):
        raise ValueError("white and pial surfaces must have identical face topology")
    if not np.all(np.isfinite(white)) or not np.all(np.isfinite(pial)):
        raise ValueError("white and pial surfaces must contain finite vertices")
    return white, pial, pial_topology


def _outward_vertex_normals(
    vertices: np.ndarray,
    faces: np.ndarray,
    cortical_directions: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    triangles = vertices[faces]
    face_vectors = np.cross(
        triangles[:, 1] - triangles[:, 0], triangles[:, 2] - triangles[:, 0]
    )
    twice_area = np.linalg.norm(face_vectors, axis=1)
    if np.any(twice_area <= 1e-8):
        raise ValueError("curvature analysis does not support degenerate faces")
    normals = np.zeros_like(vertices)
    for corner in range(3):
        np.add.at(normals, faces[:, corner], face_vectors)
    lengths = np.linalg.norm(normals, axis=1)
    if np.any(lengths <= 1e-8):
        raise ValueError("curvature analysis found a vertex without a valid normal")
    normals /= lengths[:, np.newaxis]
    if float(np.median(np.einsum("ij,ij->i", normals, cortical_directions))) < 0:
        normals = -normals
    return normals, twice_area * 0.5


def signed_mean_curvature(
    white_vertices: np.ndarray,
    white_faces: np.ndarray,
    pial_vertices: np.ndarray,
    pial_faces: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return convex-positive cotangent mean curvature and the mid-depth mesh."""

    white, pial, faces = _validated_surface_pair(
        white_vertices, white_faces, pial_vertices, pial_faces
    )
    normals, face_areas = _outward_vertex_normals(pial, faces, pial - white)
    vertex_areas = np.zeros(pial.shape[0], dtype=float)
    for corner in range(3):
        np.add.at(vertex_areas, faces[:, corner], face_areas / 3.0)
    if np.any(vertex_areas <= 1e-12):
        raise ValueError("curvature analysis found a vertex without surface area")

    laplacian = np.zeros_like(pial)
    for opposite, first, second in ((0, 1, 2), (1, 2, 0), (2, 0, 1)):
        a = pial[faces[:, first]] - pial[faces[:, opposite]]
        b = pial[faces[:, second]] - pial[faces[:, opposite]]
        cotangent = np.einsum("ij,ij->i", a, b) / np.linalg.norm(
            np.cross(a, b), axis=1
        )
        i = faces[:, first]
        j = faces[:, second]
        contribution = cotangent[:, np.newaxis] * (pial[j] - pial[i])
        np.add.at(laplacian, i, contribution)
        np.add.at(laplacian, j, -contribution)
    laplacian /= 2.0 * vertex_areas[:, np.newaxis]
    curvature = -0.5 * np.einsum("ij,ij->i", laplacian, normals)
    middle = white + MIDDLE_DEPTH_FRACTION * (pial - white)
    return curvature, middle, faces


def identify_sulcal_middepth(
    hemisphere: str,
    white_vertices: np.ndarray,
    white_faces: np.ndarray,
    pial_vertices: np.ndarray,
    pial_faces: np.ndarray,
    threshold_mm_inv: float,
) -> SulcalHemisphere:
    """Select concave pial faces and place them at middle cortical depth."""

    if not np.isfinite(threshold_mm_inv) or threshold_mm_inv <= 0:
        raise ValueError("sulcal curvature threshold must be a positive finite value")
    curvature, middle, faces = signed_mean_curvature(
        white_vertices, white_faces, pial_vertices, pial_faces
    )
    selected_vertices = curvature <= -threshold_mm_inv
    selected_faces = faces[np.all(selected_vertices[faces], axis=1)]
    values = curvature[selected_vertices]
    return SulcalHemisphere(
        hemisphere=hemisphere,
        threshold_mm_inv=float(threshold_mm_inv),
        selected_vertex_count=int(selected_vertices.sum()),
        selected_face_count=int(selected_faces.shape[0]),
        curvature_min_mm_inv=float(curvature.min()),
        curvature_median_mm_inv=float(np.median(values)) if values.size else 0.0,
        middle_vertices_ras_mm=middle,
        selected_faces=selected_faces,
    )


def _triangles_intersect_unit_boxes(
    triangles: np.ndarray,
    centers: np.ndarray,
) -> np.ndarray:
    """Test triangle-box pairs in one vectorized separating-axis pass."""

    shifted = triangles - centers[:, np.newaxis, :]
    edges = np.stack(
        (
            shifted[:, 1] - shifted[:, 0],
            shifted[:, 2] - shifted[:, 1],
            shifted[:, 0] - shifted[:, 2],
        ),
        axis=1,
    )
    box_axes = np.broadcast_to(np.eye(3), (triangles.shape[0], 3, 3))
    face_normals = np.cross(edges[:, 0], edges[:, 1])[:, np.newaxis, :]
    edge_axes = np.cross(edges[:, :, np.newaxis, :], np.eye(3)[np.newaxis, :, :])
    axes = np.concatenate(
        (box_axes, face_normals, edge_axes.reshape(-1, 9, 3)), axis=1
    )
    projections = np.einsum("mva,mka->mvk", shifted, axes)
    radii = 0.5 * np.abs(axes).sum(axis=2)
    separated = (projections.min(axis=1) > radii) | (
        projections.max(axis=1) < -radii
    )
    nonzero_axes = np.einsum("mka,mka->mk", axes, axes) > 1e-16
    return ~np.any(separated & nonzero_axes, axis=1)


def triangle_voxel_mask(
    shape: tuple[int, int, int],
    affine: np.ndarray,
    vertices_ras_mm: np.ndarray,
    faces: np.ndarray,
) -> np.ndarray:
    """Mark source-grid voxel cells intersected by any selected mesh triangle."""

    mask = np.zeros(shape, dtype=bool)
    if faces.size == 0:
        return mask
    voxel_vertices = np.asarray(
        nib.affines.apply_affine(np.linalg.inv(affine), vertices_ras_mm)
    )
    triangles = voxel_vertices[faces]
    upper = np.asarray(shape) - 1
    lower_bounds = np.maximum(np.ceil(triangles.min(axis=1) - 0.5), 0).astype(int)
    upper_bounds = np.minimum(
        np.floor(triangles.max(axis=1) + 0.5), upper
    ).astype(int)
    candidate_centers = []
    candidate_faces = []
    for face_index, (lower, candidate_upper) in enumerate(
        zip(lower_bounds, upper_bounds)
    ):
        if np.any(candidate_upper < lower):
            continue
        grid = np.indices(tuple(candidate_upper - lower + 1)).reshape(3, -1).T
        candidate_centers.append(grid + lower)
        candidate_faces.append(np.full(grid.shape[0], face_index, dtype=np.int64))
    if not candidate_centers:
        return mask

    centers = np.concatenate(candidate_centers)
    face_indices = np.concatenate(candidate_faces)
    chunk_size = 100_000
    for start in range(0, centers.shape[0], chunk_size):
        stop = min(start + chunk_size, centers.shape[0])
        chunk_centers = centers[start:stop]
        intersects = _triangles_intersect_unit_boxes(
            triangles[face_indices[start:stop]], chunk_centers
        )
        selected = chunk_centers[intersects]
        mask[tuple(selected.T)] = True
    return mask
