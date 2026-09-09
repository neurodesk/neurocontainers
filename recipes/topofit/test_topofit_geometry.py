"""Deterministic tests for TopoFit curvature and voxel intersection."""

from __future__ import annotations

import unittest

import numpy as np
from topofit_geometry import signed_mean_curvature, triangle_voxel_mask


class TopoFitGeometryTests(unittest.TestCase):
    def test_outward_convex_surface_has_positive_curvature(self):
        pial = np.asarray(
            [
                [-1.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
                [0.0, -1.0, 0.0],
                [0.0, 1.0, 0.0],
                [0.0, 0.0, -1.0],
                [0.0, 0.0, 1.0],
            ]
        )
        white = pial * 0.8
        faces = np.asarray(
            [
                [0, 2, 4], [2, 1, 4], [1, 3, 4], [3, 0, 4],
                [2, 0, 5], [1, 2, 5], [3, 1, 5], [0, 3, 5],
            ]
        )

        curvature, _, _ = signed_mean_curvature(white, faces, pial, faces)

        self.assertTrue(np.all(curvature > 0.0), curvature)

    def test_concave_center_has_negative_curvature_and_midpoint_depth(self):
        x, y = np.meshgrid(np.arange(3.0), np.arange(3.0), indexing="ij")
        pial = np.column_stack((x.ravel(), y.ravel(), np.zeros(9)))
        pial[4, 2] = -1.0
        white = pial.copy()
        white[:, 2] -= 2.0
        faces = []
        for i in range(2):
            for j in range(2):
                lower = i * 3 + j
                faces.extend(
                    ([lower, lower + 3, lower + 1], [lower + 1, lower + 3, lower + 4])
                )
        faces = np.asarray(faces, dtype=np.int32)

        curvature, middle, returned_faces = signed_mean_curvature(
            white, faces, pial, faces
        )

        self.assertLess(curvature[4], 0.0)
        np.testing.assert_allclose(middle, (white + pial) / 2.0)
        np.testing.assert_array_equal(returned_faces, faces)

    def test_triangle_marks_voxel_even_when_no_vertex_rounds_into_it(self):
        vertices = np.asarray(
            [[-0.49, 0.49, 0.0], [1.49, 0.49, 0.0], [0.5, 1.49, 0.0]]
        )
        faces = np.asarray([[0, 1, 2]], dtype=np.int32)

        mask = triangle_voxel_mask((3, 3, 2), np.eye(4), vertices, faces)

        self.assertTrue(mask[0, 0, 0])
        self.assertTrue(mask[1, 0, 0])
        self.assertTrue(mask[0, 1, 0])

    def test_surface_correspondence_is_required(self):
        vertices = np.asarray(
            [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
        )
        faces = np.asarray([[0, 1, 2], [0, 3, 1], [0, 2, 3], [1, 3, 2]])
        with self.assertRaisesRegex(ValueError, "identical face topology"):
            signed_mean_curvature(vertices, faces, vertices, faces[::-1])


if __name__ == "__main__":
    unittest.main()
