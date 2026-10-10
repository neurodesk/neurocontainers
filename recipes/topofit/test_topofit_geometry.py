"""Deterministic tests for TopoFit curvature and voxel intersection."""

from __future__ import annotations

import unittest

import numpy as np
from topofit_geometry import (
    SIEMENS_PLANE_ORDERS, siemens_plane_orientation,
    signed_mean_curvature, triangle_voxel_mask,
)


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


class SiemensPlaneOrientationTests(unittest.TestCase):
    def test_six_signed_single_obliques(self):
        sine, cosine = np.sin(np.deg2rad(20)), np.cos(np.deg2rad(20))
        fixtures = (
            ((-cosine, sine, 0), ("Sag", "Cor", "Tra"), 20),
            ((-cosine, 0, sine), ("Sag", "Tra", "Cor"), 20),
            ((sine, cosine, 0), ("Cor", "Sag", "Tra"), -20),
            ((0, cosine, sine), ("Cor", "Tra", "Sag"), -20),
            ((sine, 0, cosine), ("Tra", "Sag", "Cor"), -20),
            ((0, sine, cosine), ("Tra", "Cor", "Sag"), -20),
        )
        for normal, order, expected in fixtures:
            for sign in (-1, 1):
                with self.subTest(order=order, sign=sign):
                    orientation = siemens_plane_orientation(np.asarray(normal) * sign)
                    self.assertEqual(orientation.primary.order, order)
                    self.assertAlmostEqual(orientation.primary.first_angle_deg, expected)
                    self.assertEqual(orientation.primary.second_angle_deg, 0)
                    self.assertEqual(orientation.primary_text, f"{order[0]}>{order[1]}({expected:+.1f})")

    def test_published_siemens_transverse_acquisitions(self):
        fixtures = (
            ((-0.1087323357, 0.06708937544, 0.9918045649),
             "Tra>Sag(+6.3)>Cor(-3.8)", 6.256397394, -3.846827503),
            ((-0.07818081918, 0.4036100151, 0.9115847274),
             "Tra>Cor(-23.9)>Sag(+4.5)", -23.881676286, 4.484006801),
        )
        for normal, text, first, second in fixtures:
            orientation = siemens_plane_orientation(normal)
            self.assertEqual(orientation.primary_text, text)
            self.assertAlmostEqual(orientation.primary.first_angle_deg, first, places=8)
            self.assertAlmostEqual(orientation.primary.second_angle_deg, second, places=8)
            self.assertIsNone(orientation.in_plane_rotation_deg)

    def test_cardinal_planes_and_exact_singular_flags(self):
        for index, plane in enumerate(("Sag", "Cor", "Tra")):
            normal = np.eye(3)[index]
            orientation = siemens_plane_orientation(normal)
            self.assertEqual(orientation, siemens_plane_orientation(-normal))
            self.assertEqual(orientation.primary_text, plane)
            self.assertEqual(sum(not p.first_angle_defined for p in orientation.variants), 2)
            for pair in orientation.variants:
                if not pair.first_angle_defined:
                    self.assertEqual(pair.first_angle_deg, 0)
                    self.assertEqual(abs(pair.second_angle_deg), 90)
                    self.assertIn("undef*", pair.degree_pair_text)
        near = siemens_plane_orientation((1e-12, 0, 1))
        self.assertTrue(all(pair.first_angle_defined for pair in near.variants))

    def test_ties_zero_base_and_displayed_zero(self):
        orientation = siemens_plane_orientation((1, -1, 1))
        self.assertEqual(orientation.primary.order, ("Sag", "Cor", "Tra"))
        self.assertEqual(orientation, siemens_plane_orientation((-1, 1, -1)))
        zero_base = siemens_plane_orientation((0, -1, 2))
        self.assertAlmostEqual(zero_base.variants[0].first_angle_deg, 90)
        self.assertLess(zero_base.variants[0].second_angle_deg, 0)
        small = siemens_plane_orientation((1e-8, -1e-8, 1))
        self.assertEqual(small.primary_text, "Tra")
        self.assertEqual(small.primary.format(), "Tra>Sag(+0.0)>Cor(+0.0)")
        self.assertNotEqual(small.primary.first_angle_deg, 0)

    def test_all_orders_reconstruct_same_plane_by_rotations(self):
        def rotate(vector, axis, angle):
            return (vector * np.cos(angle) + np.cross(axis, vector) * np.sin(angle)
                    + axis * np.dot(axis, vector) * (1 - np.cos(angle)))

        axes = {plane: np.eye(3)[index] for index, plane in enumerate(("Sag", "Cor", "Tra"))}
        for normal in np.random.default_rng(37).normal(size=(100, 3)):
            original = normal.copy()
            orientation = siemens_plane_orientation(normal)
            np.testing.assert_array_equal(normal, original)
            self.assertEqual(tuple(pair.order for pair in orientation.variants), SIEMENS_PLANE_ORDERS)
            self.assertEqual(orientation, siemens_plane_orientation(-normal))
            for pair in orientation.variants:
                base, target, remaining = (axes[plane] for plane in pair.order)
                if pair.order[0] == "Sag":
                    base = -base
                    sign = 1
                else:
                    sign = -1
                intermediate = rotate(base, np.cross(base, target), sign * np.deg2rad(pair.first_angle_deg))
                second_axis = np.cross(intermediate, remaining)
                reconstructed = rotate(intermediate, second_axis, sign * np.deg2rad(pair.second_angle_deg))
                self.assertAlmostEqual(abs(np.dot(reconstructed, normal / np.linalg.norm(normal))), 1)
                self.assertLessEqual(abs(pair.first_angle_deg), 90)
                self.assertLessEqual(abs(pair.second_angle_deg), 90)

    def test_extreme_scales_and_invalid_inputs(self):
        normal = np.asarray((-0.25, 0.5, 1))
        expected = siemens_plane_orientation(normal)
        for scale in (1e-300, 1e300):
            actual = siemens_plane_orientation(normal * scale)
            self.assertEqual(actual, expected)
        for normal in ((0, 0, 0), (1, 2), [[1, 2, 3]], (np.nan, 0, 1),
                       (np.inf, 1, 0), (1j, 0, 1), ("1", "2", "3")):
            with self.subTest(normal=normal), self.assertRaises(ValueError):
                siemens_plane_orientation(normal)


if __name__ == "__main__":
    unittest.main()
