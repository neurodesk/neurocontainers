import unittest

import ismrmrd
import numpy as np

from b0_images import output_images
from b0mapromeo_t2star import fit_t2star, output_t2star_images
from shared_mount_fixture import images_from_geometry


class T2StarTests(unittest.TestCase):
    def test_known_varying_decay_and_missing_echoes(self):
        times = np.array([2.3, 5.1, 9.7, 15.4])
        expected = np.linspace(4, 150, 8 * 7 * 6).reshape(8, 7, 6)
        magnitude = 800 * np.exp(-times / expected[..., None])
        magnitude[0, 0, 0, 1] = 0
        magnitude[0, 0, 1, 2] = np.nan
        support = np.ones(expected.shape, bool)
        support[-1] = False
        values, valid = fit_t2star(magnitude, times, support)
        np.testing.assert_array_equal(valid, support)
        np.testing.assert_allclose(values[valid], expected[valid], rtol=1e-6)
        self.assertTrue(np.all(values[~valid] == 0))

    def test_invalid_fits_and_empty_support(self):
        magnitude = np.array([
            [0, 0, 0], [2, 2, 2], [1, 2, 3], [1, np.nan, 0],
            [np.inf, -1, 0], [3, 2, 1],
        ], dtype=float).reshape(6, 1, 1, 3)
        values, valid = fit_t2star(magnitude, [2, 4, 7], np.ones((6, 1, 1)))
        np.testing.assert_array_equal(valid.ravel(), [False] * 5 + [True])
        self.assertTrue(np.all(values.ravel()[:5] == 0))
        values, valid = fit_t2star(magnitude, [2, 4, 7], np.zeros((6, 1, 1)))
        self.assertFalse(valid.any())
        self.assertFalse(values.any())

    def test_slice_fit_matches_independent_voxel_regression(self):
        rng = np.random.default_rng(723)
        times = np.array([1.7, 3.5, 8.2, 12.8, 20.1])
        expected = rng.uniform(10, 80, (4, 3, 5))
        magnitude = 1000 * np.exp(-times / expected[..., None])
        magnitude *= rng.uniform(0.98, 1.02, magnitude.shape)
        magnitude[rng.random(magnitude.shape) < 0.2] = 0
        magnitude[1, 1, 1, 0] = np.nan
        support = rng.random(expected.shape) > 0.1
        values, valid = fit_t2star(magnitude, times, support)
        for index in np.ndindex(expected.shape):
            samples = np.isfinite(magnitude[index]) & (magnitude[index] > 0)
            if not support[index] or samples.sum() < 2:
                self.assertFalse(valid[index])
                self.assertEqual(values[index], 0)
                continue
            slope = np.polyfit(times[samples], np.log(magnitude[index][samples]), 1)[0]
            self.assertTrue(valid[index])
            np.testing.assert_allclose(values[index], -1 / slope, rtol=1e-6)

    def test_underflow_fit_is_invalid(self):
        values, valid = fit_t2star(
            np.array([2.0, 1.0]).reshape(1, 1, 1, 2),
            [1e-50, 2e-50], np.ones((1, 1, 1)),
        )
        self.assertFalse(valid.any())
        self.assertFalse(values.any())

    def test_near_flat_unrepresentable_fit_is_invalid(self):
        values, valid = fit_t2star(
            np.array([1.0, np.nextafter(1.0, 0.0)]).reshape(1, 1, 1, 2),
            [1e30, 2e30], np.ones((1, 1, 1)),
        )
        self.assertFalse(valid.any())
        self.assertFalse(values.any())

    def test_bad_echo_contract(self):
        magnitude = np.ones((2, 3, 4, 3))
        for times in ([2, 2, 4], [2, 4], [0, 2, 4], [2, 4, np.nan]):
            with self.subTest(times=times), self.assertRaises(ValueError):
                fit_t2star(magnitude, times, np.ones((2, 3, 4)))
        with self.assertRaises(ValueError):
            fit_t2star(magnitude, [2, 4, 6], np.ones((2, 3, 5)))

    def test_scanner_geometry_scaling_identity_and_padding(self):
        anchors, _, _ = images_from_geometry((8, 7, 6), np.eye(3), [1, 2, 3], [4, 5, 6])
        values = np.linspace(0.1, 9000, 8 * 7 * 6).reshape(8, 7, 6)
        valid = np.ones(values.shape, bool)
        valid[0, 0, :] = False
        t2_images = output_t2star_images(values, valid, anchors, 181)
        b0_images = output_images(values, anchors, 180)
        uids = set()
        for z, image in enumerate(t2_images):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            b0_meta = ismrmrd.Meta.deserialize(b0_images[z].attribute_string)
            self.assertEqual(image.data.dtype, np.uint16)
            self.assertEqual(image.image_series_index, 181)
            self.assertEqual(meta["RescaleType"], "ms")
            self.assertEqual(meta["T2StarMapUnits"], "ms")
            self.assertEqual(meta["PixelPaddingValue"], "0")
            self.assertEqual(meta["ImageType"], "DERIVED\\PRIMARY\\M\\T2STAR_MAP")
            self.assertFalse(any(key.startswith("B0") for key in meta.keys()))
            self.assertNotEqual(meta["SeriesInstanceUID"], b0_meta["SeriesInstanceUID"])
            uids.add(meta["SOPInstanceUID"])
            for field in ("position", "read_dir", "phase_dir", "slice_dir",
                          "field_of_view"):
                np.testing.assert_array_equal(
                    getattr(image, field), getattr(anchors[z], field)
                )
            slope = float(meta["RescaleSlope"])
            decoded = image.data[0, 0].T * slope + float(meta["RescaleIntercept"])
            np.testing.assert_allclose(
                decoded[valid[:, :, z]], values[:, :, z][valid[:, :, z]],
                atol=slope / 2 + 1e-5,
            )
            self.assertTrue(np.all(image.data[0, 0].T[~valid[:, :, z]] == 0))
            self.assertTrue(np.all(image.data[0, 0].T[valid[:, :, z]] > 0))
        self.assertEqual(len(uids), len(anchors))
        empty = output_t2star_images(
            np.zeros(values.shape), np.zeros(values.shape, bool), anchors, 181
        )
        self.assertTrue(all(not image.data.any() for image in empty))
        with self.assertRaises(ValueError):
            output_t2star_images(values, valid, anchors, 65536)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(T2StarTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)
    print("Quantitative T2 star tests passed")
