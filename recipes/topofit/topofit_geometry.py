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


def _triangle_intersects_unit_box(triangle: np.ndarray, center: np.ndarray) -> bool:
    shifted = triangle - center
    edges = (shifted[1] - shifted[0], shifted[2] - shifted[1], shifted[0] - shifted[2])
    axes = [np.eye(3)[axis] for axis in range(3)]
    axes.append(np.cross(edges[0], edges[1]))
    for edge in edges:
        axes.extend(np.cross(edge, np.eye(3)[axis]) for axis in range(3))
    for axis in axes:
        if float(np.dot(axis, axis)) <= 1e-16:
            continue
        projections = shifted @ axis
        radius = 0.5 * float(np.abs(axis).sum())
        if float(projections.min()) > radius or float(projections.max()) < -radius:
            return False
    return True


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
    upper = np.asarray(shape) - 1
    for face in faces:
        triangle = voxel_vertices[face]
        lower_bound = np.maximum(np.ceil(triangle.min(axis=0) - 0.5), 0).astype(int)
        upper_bound = np.minimum(np.floor(triangle.max(axis=0) + 0.5), upper).astype(int)
        for i in range(lower_bound[0], upper_bound[0] + 1):
            for j in range(lower_bound[1], upper_bound[1] + 1):
                for k in range(lower_bound[2], upper_bound[2] + 1):
                    index = np.asarray((i, j, k))
                    if _triangle_intersects_unit_box(triangle, index):
                        mask[i, j, k] = True
    return mask
