import json
from pathlib import Path
import tempfile
import unittest

import nibabel as nib
import numpy as np

from b0mapromeo_shim import compute_shim


class ShimTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.affine = np.eye(4)
        x, y, _ = np.indices((5, 5, 5), dtype=float)
        self.profiles = np.stack((x - 2, y - 2), axis=-1) * 10
        self.mask = np.ones((5, 5, 5), dtype=bool)
        self.baseline = [0.4, -0.2]
        self.target = np.array([0.1, 0.3])
        self.field = -np.einsum('...c,c->...', self.profiles,
                                self.target - self.baseline) + 50
        self.calibration = {
            'scanner_model': 'MAGNETOM Cima.X', 'field_strength_t': 3,
            'profile_units': 'Hz/A', 'current_units': 'A', 'settings_mode': 'absolute',
            'calibration_id': 'synthetic-only', 'channels': ['X', 'Y'],
            'coil_profiles': 'profiles.nii',
            'absolute_current_bounds_a': [[-1, 1], [-1, 1]],
        }
        self.path = self.directory / 'calibration.json'

    def compute(self, field=None, baseline=None):
        self.path.write_text(json.dumps(self.calibration))
        nib.save(nib.Nifti1Image(self.profiles, self.affine), self.directory / 'profiles.nii')
        return compute_shim(self.field if field is None else field, self.mask,
                            self.affine, self.path,
                            self.baseline if baseline is None else baseline)

    def test_signed_absolute_and_offset(self):
        result = self.compute(baseline={'Y': -0.2, 'X': 0.4})
        np.testing.assert_allclose(result.absolute_current_a, self.target, atol=2e-5)
        self.assertLess(result.predicted_std_hz, 1e-3)
        shifted = self.compute(self.field + 500)
        np.testing.assert_allclose(shifted.absolute_current_a, result.absolute_current_a, atol=1e-8)
        self.assertIn('ABSOLUTE A: X=', result.comment)

    def test_box_saturation(self):
        self.calibration['absolute_current_bounds_a'][1] = [-0.25, 0.25]
        result = self.compute()
        self.assertAlmostEqual(result.absolute_current_a[1], 0.25, places=5)

    def test_asymmetric_bounds(self):
        self.baseline = [0.4, 0.2]
        self.calibration["absolute_current_bounds_a"] = [[0.1, 1], [0.1, 1]]
        self.target = np.array([0.2, 0.4])
        self.field = -np.einsum("...c,c->...", self.profiles,
                                self.target - self.baseline)
        result = self.compute()
        np.testing.assert_allclose(result.absolute_current_a, self.target, atol=2e-5)
        self.calibration["absolute_current_bounds_a"] = [[-0.2, 1], [-0.2, 1]]
        self.target = np.array([0.7, 0.4])
        self.field = -np.einsum("...c,c->...", self.profiles,
                                self.target - self.baseline)
        result = self.compute()
        np.testing.assert_allclose(result.absolute_current_a, self.target, atol=2e-5)

    def test_absolute_sum_and_large_delta(self):
        self.baseline = [0.8, 0]
        self.calibration['total_absolute_current_limit_a'] = 0.8
        self.target = np.array([-0.6, 0])
        self.field = -np.einsum('...c,c->...', self.profiles, self.target - self.baseline)
        result = self.compute()
        np.testing.assert_allclose(result.absolute_current_a, self.target, atol=2e-5)
        self.assertGreater(abs(result.absolute_current_a[0] - 0.8), 0.8)
        self.field = -np.einsum('...c,c->...', self.profiles, np.array([1., 1.]) - self.baseline)
        result = self.compute()
        self.assertLessEqual(sum(abs(a) for a in result.absolute_current_a), 0.8000001)

    def test_zero_adjusted_field(self):
        self.field = np.einsum('...c,c->...', self.profiles, self.baseline)
        result = self.compute()
        self.assertEqual(result.absolute_current_a, (0., 0.))

    def test_zero_total_limit(self):
        self.baseline = [0, 0]
        self.calibration["total_absolute_current_limit_a"] = 0
        result = self.compute()
        self.assertEqual(result.absolute_current_a, (0., 0.))
        self.assertGreater(result.predicted_std_hz, 0)

    def test_zero_outside_bounds(self):
        self.baseline = [0.2, 0.2]
        self.calibration["absolute_current_bounds_a"] = [[0.1, 1], [0.1, 1]]
        self.field = np.einsum("...c,c->...", self.profiles, self.baseline)
        result = self.compute()
        np.testing.assert_allclose(result.absolute_current_a, [0.1, 0.1], atol=1e-7)
        self.assertAlmostEqual(result.predicted_std_hz, 2.0, places=6)

    def test_invalid_contracts(self):
        for key, value in [('profile_units', 'Hz/mA'), ('current_units', 'mA'),
                           ('scanner_model', 'Prisma'), ('channels', ['X', 'X']),
                           ('absolute_current_bounds_a', [[0, 1]])]:
            with self.subTest(key=key):
                old = self.calibration[key]
                self.calibration[key] = value
                with self.assertRaises(ValueError):
                    self.compute()
                self.calibration[key] = old
        with self.assertRaises(ValueError):
            self.compute(baseline=[0.4])
        self.profiles[..., 1] = self.profiles[..., 0]
        with self.assertRaises(ValueError):
            self.compute()

    def test_registration_and_finiteness(self):
        self.compute()
        moved = self.affine.copy()
        moved[0, 3] = 1
        nib.save(nib.Nifti1Image(self.profiles, moved), self.directory / 'profiles.nii')
        with self.assertRaises(ValueError):
            compute_shim(self.field, self.mask, self.affine, self.path, self.baseline)
        self.field[0, 0, 0] = np.nan
        with self.assertRaises(ValueError):
            self.compute()

    def test_unavailable_and_missing_baseline(self):
        result = compute_shim(self.field, self.mask, self.affine)
        self.assertEqual(result.status, 'unavailable')
        self.assertEqual(result.absolute_current_a, ())
        for calibration, baseline in [(self.path, None), (None, self.baseline)]:
            with self.assertRaises(ValueError):
                compute_shim(self.field, self.mask, self.affine, calibration, baseline)

    def test_empty_explicit_parameters_fail(self):
        from b0mapromeo import _settings
        for baseline in [[], {}]:
            settings = _settings({"shimcurrenta": baseline}, None)
            with self.assertRaises(ValueError):
                compute_shim(self.field, self.mask, self.affine,
                             settings["shim_calibration"], settings["shim_current_a"])
        settings = _settings({"shimcalibration": "", "shimcurrenta": ""}, None)
        self.assertEqual(compute_shim(self.field, self.mask, self.affine,
                                     settings["shim_calibration"],
                                     settings["shim_current_a"]).status, "unavailable")

    def test_image_comments(self):
        import ismrmrd
        from b0mapromeo import output_images
        anchors = [ismrmrd.Image.from_array(np.ones((1, 1, 5, 5), dtype=np.uint16),
                                           transpose=False) for _ in range(5)]
        for shim in [self.compute(), None]:
            outputs = output_images(self.field, anchors, 180, shim)
            for image in outputs:
                meta = ismrmrd.Meta.deserialize(image.attribute_string)
                self.assertEqual(meta['ImageComment'], meta['ImageComments'])
                self.assertEqual(meta['B0ShimStatus'], 'available' if shim else 'unavailable')
                self.assertIn('MAGNETOM Cima.X', meta['ImageComment'])


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ShimTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print('Calibrated absolute shim tests passed')
