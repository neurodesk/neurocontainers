"""Deterministic tests for the reusable TopoFit workflow."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import nibabel as nib
import numpy as np

from topofit_core import (
    RESEARCH_WARNING,
    SURFACE_NAMES,
    TopoFitOptions,
    build_brainnet_command,
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
                TopoFitOptions(device="cpu", mock=True),
            )

            self.assertEqual(result.status, "SURFACE_READY_RESEARCH_ONLY")
            self.assertEqual(result.warning, RESEARCH_WARNING)
            self.assertEqual(set(result.surfaces), set(SURFACE_NAMES))
            for path in result.surfaces.values():
                vertices, faces = nib.freesurfer.read_geometry(path)
                self.assertEqual(vertices.shape[1], 3)
                self.assertEqual(faces.shape[1], 3)

            qc = nib.load(result.qc_image)
            self.assertEqual(qc.shape, data.shape)
            np.testing.assert_allclose(qc.affine, affine)
            qc_data = np.asarray(qc.dataobj)
            self.assertEqual(int(qc_data.max()), 4095)
            self.assertIn(3500, np.unique(qc_data))

            manifest = json.loads(Path(result.manifest).read_text(encoding="utf-8"))
            self.assertIsNone(manifest["prescription_coordinates"])
            self.assertEqual(
                manifest["coordinate_status"],
                "WITHHELD_UNTIL_VALIDATED_ANALYSIS_STAGE",
            )
            self.assertTrue(manifest["options"]["mock"])


if __name__ == "__main__":
    unittest.main()
