"""Deterministic tests for the reusable TopoFit workflow."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import nibabel as nib
import numpy as np
import topofit_core
from topofit_core import (
    RESEARCH_WARNING,
    SURFACE_NAMES,
    TopoFitOptions,
    _dilate_in_plane,
    build_brainnet_command,
    find_ranked_patches,
    mrd_lps_to_nifti_ras_affine,
    run_topofit_workflow,
    validate_options,
)


class TopoFitCoreTests(unittest.TestCase):
    def test_multiple_patches_are_ranked_disjoint_and_quality_limited(self):
        x, y = np.meshgrid(np.arange(41.), np.arange(21.), indexing="ij")
        vertices = np.column_stack((x.ravel(), y.ravel(), np.zeros(x.size)))
        faces = []
        for i in range(40):
            for j in range(20):
                k = i * 21 + j
                faces.extend(((k, k + 21, k + 1), (k + 1, k + 21, k + 22)))
        faces = np.asarray(faces)
        patches = topofit_core.find_ranked_patches(
            vertices, faces, "lh.mid", count=3, radius_mm=5,
        )
        self.assertEqual(len(patches), 3)
        used = set()
        for detection in patches:
            indices = set(detection.vertex_indices)
            self.assertTrue(used.isdisjoint(indices))
            used.update(indices)
            self.assertLessEqual(detection.patch.rms_distance_mm, 0.5)
            self.assertGreaterEqual(detection.patch.area_mm2, 0.25 * np.pi * 5**2)
        poor = vertices.copy()
        poor[:, 2] = 0.03 * (x.ravel() - 20)**2 + 0.02 * (y.ravel() - 10)**2
        self.assertEqual(topofit_core.find_ranked_patches(
            poor, faces, "lh.mid", count=3, radius_mm=5, max_rms_mm=1e-8,
        ), [])

    def test_patch_parameters_are_validated(self):
        from dataclasses import replace
        defaults = TopoFitOptions()
        for field, value in (("patch_count", 0), ("patch_count", 11),
                             ("patch_radius_mm", 0), ("patch_max_rms_mm", float("nan")),
                             ("patch_min_area_fraction", 1.1), ("patch_hemisphere", "bad")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_options(replace(defaults, **{field: value}))

    def test_mock_mesh_has_local_neighborhoods_on_full_size_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = nib.Nifti1Image(np.zeros((256, 256, 192), dtype=np.uint8), np.eye(4))
            topofit_core.write_mock_surfaces(image, root)
            surfaces = topofit_core.validate_surface_outputs(root)
            detections = topofit_core.find_flat_patches(surfaces, mock=True)
            self.assertTrue(detections)
            self.assertEqual({d.patch.surface for d in detections.values()}, {"lh.mid", "rh.mid"})

    def test_medial_wall_margin_follows_surface_distance(self):
        x, y = np.meshgrid(np.arange(12.0), np.arange(3.0), indexing="ij")
        vertices = np.column_stack((np.zeros(x.size), x.ravel(), y.ravel()))
        faces = []
        for i in range(11):
            for j in range(2):
                k = i * 3 + j
                faces.extend(((k, k + 3, k + 1), (k + 1, k + 3, k + 4)))
        cortex = x.ravel() > 0
        result = topofit_core.erode_cortex_mask(vertices, np.asarray(faces), cortex)
        np.testing.assert_array_equal(result, x.ravel() > 5)
        self.assertTrue(result[-1])  # Genuine medial cortex at RAS x=0 survives.

    def test_atlas_label_mapping_uses_registration_not_vertex_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sphere = np.asarray([[1, 0, 0], [0, 1, 0], [-1, 0, 0], [0, 0, 1.]])
            nib.freesurfer.write_geometry(str(root / "lh.sphere.reg"), sphere,
                                         np.asarray([[0, 1, 3], [1, 2, 3]]))
            (root / "lh.cortex.label").write_text(
                "#!ascii label\n2\n0 0 0 0 0\n3 0 0 0 0\n"
            )
            mapped = topofit_core.mapped_cortex_mask(sphere[[3, 1, 0, 2]] * 100, "lh", root)
            np.testing.assert_array_equal(mapped, [True, False, True, False])

    def test_roi_restricts_geometry_and_empty_roi_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = nib.Nifti1Image(np.ones((32, 32, 32), dtype=np.float32), np.eye(4))
            topofit_core.write_mock_surfaces(image, root / "surf")
            surfaces = topofit_core.validate_surface_outputs(root / "surf")
            mask = np.zeros(image.shape, dtype=np.uint8)
            mask[:16] = 1
            roi = nib.Nifti1Image(mask, image.affine)
            options = TopoFitOptions(patch_count=1, patch_radius_mm=5, patch_min_area_fraction=0.1)
            detections = topofit_core.find_flat_patches(surfaces, options, roi=roi, mock=True)
            self.assertEqual(set(detections), {"LH01"})
            detection = detections["LH01"]
            self.assertTrue(np.all(topofit_core._roi_membership(
                detection.vertices[detection.vertex_indices], roi
            )))
            empty = nib.Nifti1Image(np.zeros_like(mask), image.affine)
            with self.assertRaisesRegex(ValueError, "ROI contains no eligible cortical"):
                topofit_core.find_flat_patches(surfaces, roi=empty, mock=True)

    def test_cortical_selection_rejects_flat_collapsed_closure(self):
        x, y = np.meshgrid(np.arange(9.0), np.arange(9.0), indexing="ij")
        closure = np.column_stack((x.ravel(), y.ravel(), np.zeros(x.size)))
        cortex = closure + (20.0, 0.0, 2.0)
        cortex[:, 2] += 0.02 * (x.ravel() - 4.0) ** 2
        white = np.vstack((closure, cortex))
        pial = white + np.vstack((
            np.tile((0.0, 0.0, 0.1), (81, 1)),
            np.tile((0.0, 0.0, 3.0), (81, 1)),
        ))
        faces = []
        for offset in (0, 81):
            for i in range(8):
                for j in range(8):
                    k = offset + i * 9 + j
                    faces.extend(((k, k + 9, k + 1), (k + 1, k + 9, k + 10)))
        faces = np.asarray(faces)
        # Even a mislabeled closure must be rejected by the ribbon check.
        eligible = topofit_core.cortical_ribbon_mask(
            white, pial, np.ones(len(white), dtype=bool)
        )
        result = find_ranked_patches(
            (white + pial) / 2, faces, "lh.mid", radius_mm=4.5,
            eligible_vertices=eligible, count=1,
        )[0]
        self.assertTrue(np.all(result.vertex_indices >= 81))
        self.assertGreater(result.patch.center_ras_mm[0], 20.0)
        self.assertGreater(result.patch.center_ras_mm[2], 3.4)

    def test_cortex_mask_preserves_medial_cortex_and_rejects_closure(self):
        white = np.asarray([[0, 0, 0], [0, 5, 0], [20, 0, 0]], dtype=float)
        pial = white + [[0, 0, 3], [0, 0, 0.1], [0, 0, 3]]
        mask = topofit_core.cortical_ribbon_mask(
            white, pial, np.asarray([True, True, False])
        )
        np.testing.assert_array_equal(mask, [True, False, False])

    def test_no_eligible_cortex_does_not_fall_back_to_whole_mesh(self):
        with self.assertRaisesRegex(ValueError, "eligible cortical"):
            find_ranked_patches(
                np.asarray([[0., 0., 0.], [1., 0., 0.], [0., 1., 0.]]),
                np.asarray([[0, 1, 2]]), "lh.mid",
                eligible_vertices=np.zeros(3, dtype=bool),
            )

    def test_command_uses_pinned_cli_contract(self):
        command = build_brainnet_command(
            Path("/data/input.nii.gz"),
            Path("/data/output/surf"),
            TopoFitOptions(device="cpu", preset="synth_random", conform=True),
        )
        self.assertEqual(
            command,
            [
                "brainnet",
                "--conform",
                "--device",
                "cpu",
                "topofit",
                "--contrast",
                "synth",
                "--resolution",
                "random",
                "/data/input.nii.gz",
                "/data/output/surf",
            ],
        )

    def test_mrd_lps_geometry_is_converted_to_nifti_ras(self):
        affine = mrd_lps_to_nifti_ras_affine(
            position=(10.0, 20.0, 30.0),
            read_dir=(1.0, 0.0, 0.0),
            phase_dir=(0.0, 1.0, 0.0),
            slice_dir=(0.0, 0.0, 1.0),
            voxel_size=(1.0, 2.0, 3.0),
            in_plane_shape=(5, 3),
        )
        np.testing.assert_allclose(
            affine,
            [
                [-1.0, 0.0, 0.0, -8.0],
                [0.0, -2.0, 0.0, -18.0],
                [0.0, 0.0, 3.0, 30.0],
                [0.0, 0.0, 0.0, 1.0],
            ],
        )
        np.testing.assert_allclose(
            nib.affines.apply_affine(affine, (2.0, 1.0, 0.0)),
            (-10.0, -20.0, 30.0),
        )

    def test_invalid_model_preset_is_rejected_at_boundary(self):
        with self.assertRaisesRegex(ValueError, "unknown model preset"):
            validate_options(TopoFitOptions(preset="t1w_random"))

    def test_invalid_overlay_thickness_is_rejected_at_boundary(self):
        for thickness in (-1, 4):
            with (
                self.subTest(thickness=thickness),
                self.assertRaisesRegex(ValueError, "overlay thickness"),
            ):
                validate_options(TopoFitOptions(overlay_thickness=thickness))

    def test_invalid_sulcal_curvature_threshold_is_rejected_at_boundary(self):
        for threshold in (0.0, -0.1, float("nan")):
            with (
                self.subTest(threshold=threshold),
                self.assertRaisesRegex(ValueError, "sulcal curvature threshold"),
            ):
                validate_options(
                    TopoFitOptions(sulcal_curvature_threshold_mm_inv=threshold)
                )

    def test_in_plane_dilation_is_configurable_without_edge_wrapping(self):
        mask = np.zeros((5, 5, 3), dtype=bool)
        mask[0, 0, 1] = True

        thin = _dilate_in_plane(mask, 0)
        current_width = _dilate_in_plane(mask, 1)

        self.assertEqual(int(thin.sum()), 1)
        self.assertEqual(int(current_width.sum()), 3)
        self.assertFalse(current_width[-1, 0, 1])
        self.assertFalse(current_width[0, -1, 1])

    def test_flat_patch_finder_prefers_a_planar_neighborhood(self):
        grid_x, grid_y = np.meshgrid(
            np.linspace(-4.0, 4.0, 5),
            np.linspace(-4.0, 4.0, 5),
            indexing="ij",
        )
        plane = np.column_stack(
            [grid_x.ravel(), grid_y.ravel(), np.full(grid_x.size, 8.0)]
        )
        curved = np.column_stack(
            [
                grid_x.ravel() + 20.0,
                grid_y.ravel(),
                0.18 * (grid_x.ravel() ** 2 + grid_y.ravel() ** 2),
            ]
        )
        vertices = np.vstack([plane, curved])
        faces = []
        for offset in (0, plane.shape[0]):
            for x_index in range(4):
                for y_index in range(4):
                    lower_left = offset + x_index * 5 + y_index
                    faces.extend(
                        [
                            [lower_left, lower_left + 5, lower_left + 1],
                            [lower_left + 1, lower_left + 5, lower_left + 6],
                        ]
                    )

        detected = find_ranked_patches(
            vertices,
            np.asarray(faces, dtype=np.int32),
            surface="lh.pial",
            radius_mm=4.5, count=1,
        )[0]

        self.assertLess(detected.patch.center_ras_mm[0], 10.0)
        self.assertGreater(abs(detected.patch.normal_ras[2]), 0.99)
        self.assertLess(detected.patch.rms_distance_mm, 1e-6)
        self.assertGreater(detected.patch.area_mm2, 0.0)
        self.assertGreater(detected.vertex_indices.size, 3)

    def test_patch_qc_makes_through_plane_normal_visible_on_patch_slice(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            image = nib.Nifti1Image(
                np.full((32, 32, 7), 500.0, dtype=np.float32),
                np.eye(4),
            )
            vertices = np.asarray(
                [
                    [10.0, 10.0, 3.2],
                    [22.0, 10.0, 3.2],
                    [22.0, 22.0, 3.2],
                    [10.0, 22.0, 3.2],
                ],
                dtype=np.float32,
            )
            faces = np.asarray([[0, 1, 2], [0, 2, 3]], dtype=np.int32)
            surface_path = root / "lh.pial"
            nib.freesurfer.write_geometry(str(surface_path), vertices, faces)
            white_path = root / "lh.white"
            nib.freesurfer.write_geometry(str(white_path), vertices - (0, 0, 2), faces)

            def render(normal, name):
                patch = topofit_core.FlatPatch(
                    surface="lh.pial",
                    center_ras_mm=(16.0, 16.0, 3.2),
                    normal_ras=normal,
                    radius_mm=10.0,
                    area_mm2=144.0,
                    rms_distance_mm=0.0,
                    vertex_count=4,
                )
                detection = SimpleNamespace(patch=patch, faces=faces, vertices=vertices)
                output_path = root / name
                topofit_core.write_patch_qc(
                    image,
                    {"lh.pial": surface_path, "lh.white": white_path},
                    {"lh": detection},
                    output_path,
                )
                return nib.load(output_path)

            positive = render((0.0, 0.0, 1.0), "positive.nii.gz")
            negative = render((0.0, 0.0, -1.0), "negative.nii.gz")
            positive_data = np.asarray(positive.dataobj)
            negative_data = np.asarray(negative.dataobj)

            self.assertEqual(positive.shape, image.shape)
            np.testing.assert_allclose(positive.affine, image.affine)
            self.assertEqual(
                int(positive_data[14, 16, 3]),
                topofit_core.PATCH_QC_PATCH_INTENSITY,
            )
            self.assertEqual(
                int(positive_data[16, 16, 3]),
                topofit_core.PATCH_QC_NORMAL_INTENSITY,
            )
            self.assertLessEqual(
                int(positive_data[:, :, (0, 1, 2, 4, 5, 6)].max()),
                2700,  # White/pial boundaries, but no mid-patch or normal glyph.
            )

            # A center dot means increasing slice position. Diagonal strokes
            # mean decreasing slice position. Both remain visible in this one
            # acquired plane even though the physical ray is through-plane.
            self.assertNotEqual(
                int(positive_data[18, 18, 3]),
                topofit_core.PATCH_QC_NORMAL_INTENSITY,
            )
            self.assertEqual(
                int(negative_data[18, 18, 3]),
                topofit_core.PATCH_QC_NORMAL_INTENSITY,
            )

    def test_mock_workflow_writes_surface_qc_and_non_actionable_manifest(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            input_path = root / "mprage.nii.gz"
            run_dir = root / "run"
            grid = np.indices((32, 36, 28), dtype=np.float32)
            center = np.asarray([15.5, 17.5, 13.5], dtype=np.float32)
            squared_distance = sum(
                ((grid[axis] - center[axis]) / radius) ** 2
                for axis, radius in enumerate((12.0, 14.0, 10.0))
            )
            data = np.where(squared_distance <= 1.0, 900.0, 20.0).astype(np.float32)
            affine = np.asarray(
                [
                    [-1.0, 0.0, 0.0, 16.0],
                    [0.0, -1.0, 0.0, 18.0],
                    [0.0, 0.0, 1.2, -16.8],
                    [0.0, 0.0, 0.0, 1.0],
                ]
            )
            nib.save(nib.Nifti1Image(data, affine), input_path)

            result = run_topofit_workflow(
                input_path,
                run_dir,
                TopoFitOptions(
                    device="cpu",
                    mock=True,
                    find_flat_patches=True,
                    patch_count=1,
                    patch_radius_mm=5,
                    patch_min_area_fraction=0.1,
                    find_sulcal_middepth=True,
                    sulcal_curvature_threshold_mm_inv=100.0,
                    overlay_thickness=0,
                ),
            )

            self.assertEqual(result.status, "SURFACE_READY_RESEARCH_ONLY")
            self.assertEqual(result.warning, RESEARCH_WARNING)
            self.assertEqual(set(result.surfaces), set(SURFACE_NAMES))
            self.assertEqual(set(result.flat_patches), {"LH01", "RH01"})
            for patch in result.flat_patches.values():
                self.assertAlmostEqual(np.linalg.norm(patch.normal_ras), 1.0)
                self.assertGreater(patch.area_mm2, 0.0)
            for path in result.surfaces.values():
                vertices, faces = nib.freesurfer.read_geometry(path)
                self.assertEqual(vertices.shape[1], 3)
                self.assertEqual(faces.shape[1], 3)

            qc = nib.load(result.qc_image)
            self.assertEqual(qc.shape, data.shape)
            np.testing.assert_allclose(qc.affine, affine)
            qc_data = np.asarray(qc.dataobj)
            self.assertEqual(int(qc_data.max()), 4095)
            self.assertIn(3800, np.unique(qc_data))
            self.assertIsNotNone(result.patch_qc_image)
            with np.load(result.patch_geometry) as geometry:
                for hemisphere in result.flat_patches:
                    white = geometry[f"{hemisphere}_white_ras_mm"]
                    pial = geometry[f"{hemisphere}_pial_ras_mm"]
                    middle = geometry[f"{hemisphere}_mid_ras_mm"]
                    normals = geometry[f"{hemisphere}_normals_ras"]
                    np.testing.assert_allclose(middle, (white + pial) / 2)
                    np.testing.assert_allclose(np.linalg.norm(normals, axis=1), 1)
                    self.assertTrue(np.all(np.einsum("ij,ij->i", normals, pial - white) >= 0))
            patch_qc = nib.load(result.patch_qc_image)
            self.assertEqual(patch_qc.shape, data.shape)
            np.testing.assert_allclose(patch_qc.affine, affine)
            patch_qc_data = np.asarray(patch_qc.dataobj)
            self.assertIn(
                topofit_core.PATCH_QC_PATCH_INTENSITY,
                np.unique(patch_qc_data),
            )
            self.assertIn(
                topofit_core.PATCH_QC_NORMAL_INTENSITY,
                np.unique(patch_qc_data),
            )
            self.assertIsNotNone(result.sulcal_middepth_mask)
            sulcal_mask = nib.load(result.sulcal_middepth_mask)
            self.assertEqual(sulcal_mask.shape, data.shape)
            np.testing.assert_allclose(sulcal_mask.affine, affine)
            self.assertEqual(sulcal_mask.get_data_dtype(), np.dtype(np.uint8))
            inverse_affine = np.linalg.inv(qc.affine)
            for patch in result.flat_patches.values():
                center = np.asarray(patch.center_ras_mm)
                normal = np.asarray(patch.normal_ras)
                line = center + np.linspace(0.0, 20.0, 81)[:, None] * normal
                voxels = np.rint(nib.affines.apply_affine(inverse_affine, line)).astype(
                    int
                )
                inside = np.all(voxels >= 0, axis=1) & np.all(
                    voxels < np.asarray(qc.shape), axis=1
                )
                self.assertGreater(int(inside.sum()), 1)
                ray_values = qc_data[tuple(voxels[inside].T)]
                self.assertGreater(np.count_nonzero(ray_values == 4095), 1)
                if inside[-1]:
                    self.assertEqual(int(qc_data[tuple(voxels[-1])]), 4095)

            manifest = json.loads(Path(result.manifest).read_text(encoding="utf-8"))
            self.assertIsNone(manifest["prescription_coordinates"])
            self.assertEqual(
                manifest["coordinate_status"],
                "WITHHELD_UNTIL_VALIDATED_ANALYSIS_STAGE",
            )
            self.assertEqual(
                manifest["flat_patch_status"],
                "CANDIDATES_REPORTED_RESEARCH_ONLY",
            )
            self.assertEqual(set(manifest["flat_patches"]), {"LH01", "RH01"})
            self.assertTrue(manifest["options"]["mock"])
            self.assertEqual(
                manifest["sulcal_middepth_status"],
                "VOXELS_REPORTED_RESEARCH_ONLY",
            )
            self.assertEqual(set(manifest["sulci"]), {"lh", "rh"})
            self.assertEqual(
                manifest["sulcal_middepth_definition"]["depth_fraction"], 0.5
            )


if __name__ == "__main__":
    unittest.main()
