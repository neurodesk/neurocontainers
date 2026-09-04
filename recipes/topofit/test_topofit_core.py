"""Deterministic tests for the reusable TopoFit workflow."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nibabel as nib
import numpy as np
from topofit_core import (
    RESEARCH_WARNING,
    SURFACE_NAMES,
    TopoFitOptions,
    _dilate_in_plane,
    build_brainnet_command,
    find_flattest_patch,
    mrd_lps_to_nifti_ras_affine,
    run_topofit_workflow,
    validate_options,
)


class TopoFitCoreTests(unittest.TestCase):
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

        detected = find_flattest_patch(
            vertices,
            np.asarray(faces, dtype=np.int32),
            surface="lh.pial",
            radius_mm=4.5,
        )

        self.assertLess(detected.patch.center_ras_mm[0], 10.0)
        self.assertGreater(abs(detected.patch.normal_ras[2]), 0.99)
        self.assertLess(detected.patch.rms_distance_mm, 1e-6)
        self.assertGreater(detected.patch.area_mm2, 0.0)
        self.assertGreater(detected.vertex_indices.size, 3)

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
                    find_sulcal_middepth=True,
                    sulcal_curvature_threshold_mm_inv=100.0,
                    overlay_thickness=0,
                ),
            )

            self.assertEqual(result.status, "SURFACE_READY_RESEARCH_ONLY")
            self.assertEqual(result.warning, RESEARCH_WARNING)
            self.assertEqual(set(result.surfaces), set(SURFACE_NAMES))
            self.assertEqual(set(result.flat_patches), {"lh", "rh"})
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
            self.assertIsNotNone(result.sulcal_middepth_mask)
            sulcal_mask = nib.load(result.sulcal_middepth_mask)
            self.assertEqual(sulcal_mask.shape, data.shape)
            np.testing.assert_allclose(sulcal_mask.affine, affine)
            self.assertEqual(sulcal_mask.get_data_dtype(), np.dtype(np.uint8))
            inverse_affine = np.linalg.inv(affine)
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
                endpoint = voxels[np.flatnonzero(inside)[-1]]
                self.assertEqual(int(qc_data[tuple(endpoint)]), 4095)

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
            self.assertEqual(set(manifest["flat_patches"]), {"lh", "rh"})
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
