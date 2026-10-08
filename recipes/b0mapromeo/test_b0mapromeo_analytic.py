import copy
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
from scipy.spatial.transform import Rotation

from b0mapromeo_analytic import _profiles, _read_model
from b0mapromeo_shim import compute_shim


def polynomial(points, count=8):
    x, y, z = points
    gamma = 42577478.517832555
    return np.stack(
        [gamma * 1e-9 * v for v in (x, y, z)]
        + [
            gamma * 1e-12 * v
            for v in (
                z * z - (x * x + y * y) / 2,
                2 * z * x,
                2 * z * y,
                x * x - y * y,
                2 * x * y,
            )
        ],
        axis=-1,
    )[..., :count]


class AnalyticalTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.path = self.root / "model.json"
        self.shape = (7, 8, 9)
        self.affine = np.eye(4)
        self.affine[:3, :3] = Rotation.from_euler(
            "xyz", [20, 35, 10], degrees=True
        ).as_matrix() @ np.diag([1.2, 2.3, 3.1])
        self.affine[:3, 3] = [7, -13, 21]
        self.transform = np.eye(4)
        self.transform[:3, :3] = Rotation.from_euler(
            "xyz", [11, -30, 40], degrees=True
        ).as_matrix()
        self.transform[:3, 3] = [-13, 23, -31]
        self.names = ["X", "Y", "Z", "Z2", "ZX", "ZY", "X2-Y2", "XY"]
        self.config = dict(
            configuration_id="synthetic-only",
            scanner_model="MAGNETOM Cima.X",
            field_strength_t=3,
            orders=[1, 2],
            patient_ras_mm_to_shim_lai_mm=self.transform.tolist(),
            absolute_native_bounds={n: [-1000, 1000] for n in self.names},
        )
        self.baseline = np.array([0.4, -0.2, 0.3, 2, -3, 4, -5, 6])
        self.target = np.array([-0.3, 0.1, 0.2, -1, 2, -4, 3, -2])
        v = np.indices(self.shape).reshape(3, -1)
        p = self.affine[:3, :3] @ v + self.affine[:3, 3, None]
        q = self.transform[:3, :3] @ p + self.transform[:3, 3, None]
        self.profiles = polynomial(q).reshape(self.shape + (8,))
        self.mask = np.ones(self.shape, bool)
        self.field = 71 - self.profiles @ (self.target - self.baseline)

    def solve(self, field=None, baseline=None):
        self.path.write_text(json.dumps(self.config))
        native = dict(zip(self.names, self.baseline if baseline is None else baseline))
        return compute_shim(
            self.field if field is None else field,
            self.mask,
            self.affine,
            analytical_model_path=self.path,
            acquisition_native=native,
        )

    def test_landmarks_affine_and_isocentre(self):
        self.solve()
        model = _read_model(self.path)
        np.testing.assert_allclose(
            _profiles(self.shape, self.affine, model), self.profiles, atol=1e-13
        )
        point = np.eye(4)
        point[:3, 3] = -self.transform[:3, :3].T @ self.transform[:3, 3]
        np.testing.assert_allclose(_profiles((1, 1, 1), point, model), 0, atol=1e-14)
        self.config["patient_ras_mm_to_shim_lai_mm"][0][3] += 7
        self.path.write_text(json.dumps(self.config))
        shifted = _profiles(self.shape, self.affine, _read_model(self.path))
        q = (
            self.transform[:3, :3]
            @ (
                self.affine[:3, :3] @ np.indices(self.shape).reshape(3, -1)
                + self.affine[:3, 3, None]
            )
            + self.transform[:3, 3, None]
        )
        q[0] += 7
        np.testing.assert_allclose(
            shifted, polynomial(q).reshape(self.shape + (8,)), atol=1e-13
        )

    def test_recover_native_and_offset(self):
        result = self.solve()
        np.testing.assert_allclose(
            [s.absolute for s in result.settings], self.target, atol=2e-4
        )
        self.assertLess(result.predicted_std_hz, 1e-6)
        shifted = self.solve(self.field + 900)
        np.testing.assert_allclose(
            [s.absolute for s in shifted.settings], self.target, atol=2e-4
        )

    def test_rotated_patient_frame(self):
        first = self.solve()
        frame = np.eye(4)
        frame[:3, :3] = Rotation.from_euler(
            "xyz", [24, 41, -16], degrees=True
        ).as_matrix()
        self.affine = frame @ self.affine
        self.config["patient_ras_mm_to_shim_lai_mm"] = (
            self.transform @ np.linalg.inv(frame)
        ).tolist()
        second = self.solve()
        np.testing.assert_allclose(
            [s.absolute for s in first.settings],
            [s.absolute for s in second.settings],
            atol=1e-7,
        )

    def test_first_order_and_small_scale(self):
        self.names = self.names[:3]
        self.config["orders"] = [1]
        self.config["absolute_native_bounds"] = {n: [-10, 10] for n in self.names}
        self.baseline, self.target = self.baseline[:3], self.target[:3]
        self.field = 12 - self.profiles[..., :3] @ (self.target - self.baseline)
        np.testing.assert_allclose(
            [s.absolute for s in self.solve().settings], self.target, atol=2e-5
        )
        self.setUp()
        self.affine[:3, :3] *= 0.01
        self.affine[:3, 3] = -self.transform[:3, :3].T @ self.transform[:3, 3]
        q = (
            self.transform[:3, :3]
            @ self.affine[:3, :3]
            @ np.indices(self.shape).reshape(3, -1)
        )
        self.field = 2 - polynomial(q).reshape(self.shape + (8,)) @ (
            self.target - self.baseline
        )
        np.testing.assert_allclose(
            [s.absolute for s in self.solve().settings], self.target, atol=2e-3
        )

    def test_zero_target_with_small_constrained_effects(self):
        from scipy.optimize import lsq_linear

        for size in (1, 0.1, 0.01, 0.001):
            with self.subTest(size=size):
                affine = self.affine.copy()
                affine[:3, :3] *= size
                affine[:3, 3] = -self.transform[:3, :3].T @ self.transform[:3, 3]
                points = (
                    self.transform[:3, :3]
                    @ affine[:3, :3]
                    @ np.indices(self.shape).reshape(3, -1)
                )
                profiles = polynomial(points).reshape(self.shape + (8,))
                bounds = np.array([[-1, 1]] * 3 + [[1, 20]] * 5, float)
                baseline = np.array([0, 0, 0, 10, 10, 10, 10, 10.])
                self.config["absolute_native_bounds"] = dict(zip(self.names, bounds.tolist()))
                self.path.write_text(json.dumps(self.config))
                result = compute_shim(
                    profiles @ baseline, self.mask, affine,
                    analytical_model_path=self.path,
                    acquisition_native=dict(zip(self.names, baseline)),
                )
                matrix = profiles.reshape(-1, 8)
                matrix -= matrix.mean(axis=0)
                channel_scale = np.sqrt(np.mean(matrix ** 2, axis=0))
                field_scale = np.std(matrix @ np.clip(np.zeros(8), bounds[:, 0], bounds[:, 1]))
                reference = lsq_linear(
                    matrix / channel_scale, np.zeros(matrix.shape[0]),
                    bounds=(bounds[:, 0] * channel_scale / field_scale,
                            bounds[:, 1] * channel_scale / field_scale),
                    tol=1e-14,
                ).x * field_scale / channel_scale
                np.testing.assert_allclose(
                    [s.absolute for s in result.settings], reference, atol=1e-5, rtol=0
                )
                np.testing.assert_allclose(
                    result.predicted_std_hz, np.std(matrix @ reference), rtol=1e-8
                )

    def test_fixed_all_fixed_and_rank(self):
        self.config["absolute_native_bounds"]["XY"] = [6, 6]
        result = self.solve()
        self.assertEqual(result.settings[-1].absolute, 6)
        self.config["absolute_native_bounds"] = {
            n: [b, b] for n, b in zip(self.names, self.baseline)
        }
        self.mask[:] = False
        self.mask[0, 0, 0] = True
        result = self.solve()
        self.assertEqual(result.predicted_std_hz, 0)
        self.config["absolute_native_bounds"]["X"] = [-1, 1]
        with self.assertRaisesRegex(ValueError, "independent"):
            self.solve()

    def test_fixed_zero_effects_with_one_free_channel(self):
        self.affine = np.eye(4)
        self.config["patient_ras_mm_to_shim_lai_mm"] = np.eye(4).tolist()
        self.config["absolute_native_bounds"] = {
            n: [b, b] for n, b in zip(self.names, self.baseline)
        }
        self.config["absolute_native_bounds"]["X"] = [-1, 1]
        self.mask[:] = False
        self.mask[:, 0, 0] = True
        result = self.solve(np.zeros(self.shape))
        np.testing.assert_allclose(
            [s.absolute for s in result.settings], self.baseline, atol=1e-6
        )

    def test_bounds_and_zero_target(self):
        self.affine = np.eye(4)
        self.config["patient_ras_mm_to_shim_lai_mm"] = np.eye(4).tolist()
        self.profiles = polynomial(np.indices(self.shape))
        self.names = self.names[:3]
        self.config["orders"] = [1]
        self.baseline = np.array([0.4, 0.4, 0.4])
        self.config["absolute_native_bounds"] = {n: [0.1, 1] for n in self.names}
        result = self.solve(self.profiles[..., :3] @ self.baseline)
        np.testing.assert_allclose(
            [s.absolute for s in result.settings], [0.1] * 3, atol=1e-6
        )
        with self.assertRaises(ValueError):
            self.solve(baseline=[2, 0.4, 0.4])
        self.config["absolute_native_bounds"]["X"] = [0.1, 0.2]
        self.baseline[0] = 0.2
        result = self.solve(-self.profiles[..., :3] @ (np.ones(3) - self.baseline))
        self.assertAlmostEqual(result.settings[0].absolute, 0.2, places=6)

    def test_correlated_active_bounds(self):
        from scipy.optimize import lsq_linear

        self.config["absolute_native_bounds"]["X"] = [-0.1, 0.4]
        result = self.solve()
        matrix = self.profiles[self.mask]
        matrix -= matrix.mean(axis=0)
        field = (
            self.field[self.mask]
            - self.field[self.mask].mean()
            - matrix @ self.baseline
        )
        bounds = np.array(
            [self.config["absolute_native_bounds"][n] for n in self.names]
        )
        expected = lsq_linear(
            matrix, -field, bounds=(bounds[:, 0], bounds[:, 1]), tol=1e-12
        ).x
        np.testing.assert_allclose(
            [s.absolute for s in result.settings], expected, atol=2e-3
        )
        self.assertAlmostEqual(result.settings[0].absolute, -0.1, places=6)

    def test_mrd_pixel_axes_override_header(self):
        import ismrmrd
        from b0mapromeo import assemble, output_images
        from smoke_test import synthetic_dicoms
        import b0mapromeo as app

        synthetic_dicoms(self.root / "dicoms")
        images, times = app.read_dicoms(self.root / "dicoms")
        for image in images:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["ImageRowDir"] = [1, 0, 0]
            meta["ImageColumnDir"] = [0, 1, 0]
            meta["ImageSliceDir"] = [0, 0, 1]
            image.attribute_string = meta.serialize()
        mag, _, affine, _, anchors = assemble(images, times, "radians")
        expected = np.diag([-1.0, -1.5, 2.0, 1.0])
        position = np.array(anchors[0].position)
        expected[:3, 3] = np.array([-1, -1, 1]) * (position - np.array([9.5, 15.75, 0]))
        np.testing.assert_allclose(affine, expected)
        outputs = output_images(np.zeros(mag.shape[:3]), anchors, 180)
        for output, anchor in zip(outputs, anchors):
            np.testing.assert_allclose(output.read_dir, anchor.read_dir)
            meta = ismrmrd.Meta.deserialize(output.attribute_string)
            np.testing.assert_allclose(
                np.asarray(meta["ImageRowDir"], float), [1, 0, 0]
            )
            np.testing.assert_allclose(
                np.asarray(meta["ImageColumnDir"], float), [0, 1, 0]
            )
        for image in images:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["ImageRowDir"] = [-1, 0, 0]
            image.attribute_string = meta.serialize()
        _, _, flipped, _, flipped_anchors = assemble(images, times, "radians")
        expected[0, 0] = 1
        expected[:3, 3] = np.array([-1, -1, 1]) * (
            position - np.array([-9.5, 15.75, 0])
        )
        np.testing.assert_allclose(flipped, expected)
        self.assertLess(np.linalg.det(flipped[:3, :3]), 0)
        for output in output_images(np.zeros(mag.shape[:3]), flipped_anchors, 180):
            meta = ismrmrd.Meta.deserialize(output.attribute_string)
            np.testing.assert_allclose(
                np.asarray(meta["ImageRowDir"], float), [-1, 0, 0]
            )
            np.testing.assert_allclose(
                np.asarray(meta["ImageSliceNormDir"], float), [0, 0, 1]
            )
        valid = images[-1].attribute_string
        for change in ("partial", "nonorthogonal", "nonfinite"):
            invalid = ismrmrd.Meta.deserialize(valid)
            if change == "partial":
                del invalid["ImageColumnDir"]
            elif change == "nonorthogonal":
                invalid["ImageColumnDir"] = [1, 0, 0]
            else:
                invalid["ImageRowDir"] = ["NaN", 0, 0]
            images[-1].attribute_string = invalid.serialize()
            with self.subTest(change=change), self.assertRaises(ValueError):
                assemble(images, times, "radians")

    def test_schema_and_selection(self):
        self.solve()
        original = copy.deepcopy(self.config)
        for key, value in [
            ("orders", [2]),
            ("configuration_id", ""),
            ("current_units", "A"),
            ("patient_ras_mm_to_shim_lai_mm", np.diag([-1, 1, 1, 1]).tolist()),
            ("absolute_native_bounds", {"X": [0, 1]}),
        ]:
            self.config = copy.deepcopy(original)
            self.config[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.solve()
        for kwargs in [
            dict(analytical_model_path=self.path),
            dict(acquisition_native={}),
            dict(calibration_path="c", analytical_model_path=self.path),
            dict(acquisition_current_a={}, acquisition_native={}),
        ]:
            with self.assertRaises(ValueError):
                compute_shim(self.field, self.mask, self.affine, **kwargs)
        self.assertEqual(
            compute_shim(
                self.field,
                self.mask,
                self.affine,
                "",
                "",
                analytical_model_path="",
                acquisition_native="",
            ).status,
            "unavailable",
        )
        self.config = original
        self.path.write_text(json.dumps(original))
        for native in [[], {"X": 0}, {n: True for n in self.names}]:
            with self.assertRaises(ValueError):
                compute_shim(
                    self.field,
                    self.mask,
                    self.affine,
                    analytical_model_path=self.path,
                    acquisition_native=native,
                )

    def test_serialization_comments_and_header_defaults(self):
        import ismrmrd
        from b0mapromeo import _settings, output_images

        result = self.solve()
        result.write(self.root)
        record = json.loads((self.root / "shim_settings.json").read_text())
        self.assertEqual(record["validation"], "ideal_field_estimate")
        self.assertEqual(record["model_kind"], "siemens_analytical")
        self.assertEqual(record["settings"][3]["unit"], "uT/m^2")
        self.assertFalse(any("current" in k or "calibration" in k for k in record))
        self.assertEqual(record["settings"][0]["baseline"], 0.4)
        anchors = [
            ismrmrd.Image.from_array(np.ones((1, 1, 8, 7), np.uint16), transpose=False)
            for _ in range(9)
        ]
        for im in output_images(self.field, anchors, 180, result):
            meta = ismrmrd.Meta.deserialize(im.attribute_string)
            self.assertEqual(meta["ImageComment"], meta["ImageComments"])
            self.assertIn("ABSOLUTE ANALYTICAL ESTIMATE", meta["ImageComment"])
        metadata = ismrmrd.xsd.ismrmrdHeader(
            userParameters=ismrmrd.xsd.userParametersType(
                userParameterString=[
                    ismrmrd.xsd.userParameterStringType(
                        name="shimanalyticalmodel", value=str(self.path)
                    ),
                    ismrmrd.xsd.userParameterStringType(
                        name="shimnativesettings",
                        value=json.dumps(dict(zip(self.names, self.baseline))),
                    ),
                ]
            )
        )
        settings = _settings(
            {"shimanalyticalmodel": "", "shimnativesettings": ""}, metadata
        )
        self.assertEqual(settings["shim_analytical_model"], str(self.path))
        self.assertEqual(json.loads(settings["shim_native_settings"])["X"], 0.4)
        self.assertEqual(
            _settings({"shimnativesettings": {}}, None)["shim_native_settings"], {}
        )


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(AnalyticalTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)
    print("Analytical native shim tests passed")
