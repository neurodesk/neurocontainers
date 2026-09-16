"""Synthetic runtime contracts for both capture-first FIRE applications."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ismrmrd
import numpy as np
import pydicom

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fire_poc as poc


def metadata(n=12):
    x = ismrmrd.xsd
    space = x.encodingSpaceType(
        matrixSize=x.matrixSizeType(x=n, y=n, z=1),
        fieldOfView_mm=x.fieldOfViewMm(x=120, y=120, z=4),
    )
    return x.ismrmrdHeader(
        experimentalConditions=x.experimentalConditionsType(
            H1resonanceFrequency_Hz=123000000
        ),
        encoding=[
            x.encodingType(
                encodedSpace=space,
                reconSpace=space,
                encodingLimits=x.encodingLimitsType(
                    kspace_encoding_step_1=x.limitType(
                        minimum=0, maximum=n - 1, center=n // 2
                    )
                ),
                trajectory=x.trajectoryType.CARTESIAN,
            )
        ],
    )


def acquisitions(data, slice_index=0, flag=None, lines=None):
    result = []
    for ky in range(data.shape[1]) if lines is None else lines:
        a = ismrmrd.Acquisition()
        a.resize(data.shape[2], data.shape[0])
        a.data[:] = data[:, ky, :]
        a.measurement_uid = 42
        a.center_sample = data.shape[2] // 2
        a.idx.kspace_encode_step_1 = ky
        a.idx.slice = slice_index
        a.position = (0, 0, 5 * slice_index)
        a.read_dir, a.phase_dir, a.slice_dir = (1, 0, 0), (0, 1, 0), (0, 0, 1)
        a.scan_counter = ky
        a.idx.user[3] = 17
        if flag:
            a.set_flag(flag)
        result.append(a)
    result[-1].set_flag(ismrmrd.ACQ_LAST_IN_SLICE)
    return result


class Connection:
    def __init__(self, items):
        self.items, self.images, self.logs = items, [], []
        self.closed = False

    def __iter__(self):
        return iter(self.items)

    def send_image(self, image):
        self.images.append(image)

    def send_logging(self, level, message):
        self.logs.append((level, message))

    def send_close(self):
        self.closed = True


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.metadata = metadata()
        self.rng = np.random.default_rng(19)
        self.data = (
            self.rng.normal(size=(2, 12, 12)) + 1j * self.rng.normal(size=(2, 12, 12))
        ).astype(np.complex64)

    def tearDown(self):
        self.temp.cleanup()

    def capture(self, items, app="acsrss", params=None):
        conn = Connection(items + [None])
        with patch.dict(os.environ, FIRE_POC_CAPTURE_ROOT=str(self.root)):
            poc.process(conn, {"parameters": params or {}}, self.metadata, app)
        directory = next(self.root.iterdir())
        return conn, directory

    def test_live_acs_returns_rss_and_originals_without_files(self):
        for domain in ("kx-ky", "x-ky"):
            with self.subTest(domain=domain):
                acqs = acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                original = ismrmrd.Image.from_array(np.ones((12, 12), np.int16))
                original.image_series_index = 7
                conn = Connection(acqs + [original, None])
                with (
                    patch.object(poc, "Capture", side_effect=AssertionError("capture")),
                    patch.object(
                        ismrmrd, "Dataset", side_effect=AssertionError("disk")
                    ),
                ):
                    poc.process_acs(
                        conn, {"parameters": {"inputdomain": domain}}, self.metadata
                    )
                self.assertTrue(conn.closed)
                self.assertIs(conn.images[0], original)
                self.assertEqual(len(conn.images), 2)
                axes = (-2,) if domain == "x-ky" else (-2, -1)
                coils = np.fft.fftshift(
                    np.fft.ifftn(
                        np.fft.ifftshift(self.data, axes=axes), axes=axes, norm="ortho"
                    ),
                    axes=axes,
                )
                expected = np.sqrt(np.sum(np.abs(coils) ** 2, axis=0))
                np.testing.assert_allclose(
                    conn.images[1].data[0, 0], expected, rtol=1e-6
                )
                self.assertEqual(conn.images[1].image_series_index, 60000)
                self.assertFalse(any(level == 3 for level, _ in conn.logs))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_live_acs_empty_and_unflagged_streams_close_without_images(self):
        for items in ([], acquisitions(self.data)):
            conn = Connection(items + [None])
            poc.process_acs(conn, {"parameters": None}, self.metadata)
            self.assertTrue(conn.closed)
            self.assertEqual(conn.images, [])
            self.assertFalse(any(level == 3 for level, _ in conn.logs))

    def test_lossless_capture_all_message_types_and_originals(self):
        acqs = acquisitions(
            self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING
        )
        image = ismrmrd.Image.from_array(np.ones((12, 12), np.int16), transpose=False)
        image.image_series_index = 7
        image.attribute_string = ismrmrd.Meta({"Keep_image_geometry": 1}).serialize()
        waveform = ismrmrd.Waveform()
        waveform.resize(5, 2)
        waveform.data[:] = 9
        conn, directory = self.capture(acqs + [image, waveform])
        self.assertTrue(conn.closed)
        self.assertIs(conn.images[0], image)
        summary = json.loads((directory / "summary.json").read_text())
        self.assertEqual(summary["status"], "captured")
        self.assertEqual(summary["counts"]["Acquisition"], 12)
        ds = ismrmrd.Dataset(str(directory / "input.h5"), create_if_needed=False)
        np.testing.assert_array_equal(ds.read_acquisition(0).data, acqs[0].data)
        self.assertEqual(
            bytes(ds.read_acquisition(0).getHead()), bytes(acqs[0].getHead())
        )
        np.testing.assert_array_equal(ds.read_waveform(0).data, waveform.data)
        events = [
            json.loads(s) for s in (directory / "events.jsonl").read_text().splitlines()
        ]
        saved = ds.read_image(events[12]["dataset"], 0)
        np.testing.assert_array_equal(saved.data, image.data)
        self.assertEqual(saved.attribute_string, image.attribute_string)
        self.assertEqual(events[0]["header"]["idx"]["user"][3], 17)
        ds.close()

    def test_acs_both_domains_and_encoded_grid_placement(self):
        lines = list(range(3, 9))
        conn, directory = self.capture(
            acquisitions(
                self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION, lines=lines
            )
        )
        params = {"inputdomain": "kx-ky", "preprocessed": True}
        images = list(
            poc.reconstruct(
                directory / "input.h5",
                "acsrss",
                {"parameters": params},
                self.metadata,
            )
        )
        padded = np.zeros_like(self.data)
        padded[:, lines] = self.data[:, lines]
        expected = poc.rss(padded, "kx-ky")
        np.testing.assert_allclose(images[0].data[0, 0], expected, rtol=1e-6)
        hybrid = poc.ifft_centered(padded, (-1,))
        np.testing.assert_allclose(poc.rss(hybrid, "x-ky"), expected, rtol=1e-6)
        self.assertEqual(images[0].image_series_index, 60000)
        self.assertEqual(list(images[0].field_of_view), [120, 120, 4])
        output = self.root / "derived"
        self.assertEqual(
            poc.write_outputs(images, output, self.metadata, dicom=True), 1
        )
        ds = pydicom.dcmread(output / "000001.dcm")
        np.testing.assert_allclose(
            ds.pixel_array * float(ds.RescaleSlope),
            expected,
            atol=expected.max() / 65535,
        )
        np.testing.assert_allclose(ds.ImagePositionPatient, [-55, -55, 0])

    def test_reconstruction_session_returns_original_and_derived_images(self):
        original = ismrmrd.Image.from_array(
            np.ones((12, 12), np.int16), transpose=False
        )
        original.image_series_index = 7
        conn, directory = self.capture(
            acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
            + [original],
            params=dict(mode="reconstruct", inputdomain="kx-ky", preprocessed=True),
        )
        self.assertTrue(conn.closed)
        self.assertEqual(
            [image.image_series_index for image in conn.images], [7, 60000]
        )
        self.assertIs(conn.images[0], original)
        summary = json.loads((directory / "summary.json").read_text())
        self.assertEqual(summary["status"], "reconstructed")
        self.assertEqual(summary["output_images"], 1)
        self.assertTrue((directory / "reconstruction" / "000001.dcm").is_file())

    def test_unknown_domain_fails_after_capture_without_losing_original(self):
        original = ismrmrd.Image.from_array(
            np.ones((12, 12), np.int16), transpose=False
        )
        with self.assertLogs(level="ERROR"):
            conn, directory = self.capture(
                acquisitions(self.data) + [original], params={"mode": "reconstruct"}
            )
        summary = json.loads((directory / "summary.json").read_text())
        self.assertEqual(summary["status"], "reconstruction_failed")
        self.assertIn("inputdomain", summary["error"])
        self.assertEqual(len(conn.images), 1)
        self.assertTrue((directory / "input.h5").is_file())
        self.assertTrue(conn.closed)

    def test_parameterless_adjustment_session_preserves_config_and_samples(self):
        conn = Connection(acquisitions(self.data) + [None])
        config = {"version": "1.1.0", "parameters": None}
        with patch.dict(os.environ, FIRE_POC_CAPTURE_ROOT=str(self.root)):
            poc.process(conn, config, self.metadata, "acsrss")
        directory = next(self.root.iterdir())
        self.assertEqual(json.loads((directory / "config.json").read_text()), config)
        summary = json.loads((directory / "summary.json").read_text())
        self.assertEqual(summary["status"], "captured")
        self.assertEqual(summary["counts"]["Acquisition"], 12)
        self.assertFalse(any(level == 3 for level, _ in conn.logs))
        self.assertTrue(conn.closed)

    def test_aborted_empty_session_does_not_attempt_reconstruction(self):
        conn, directory = self.capture(
            [],
            params=dict(mode="reconstruct", inputdomain="unknown", preprocessed=False),
        )
        summary = json.loads((directory / "summary.json").read_text())
        self.assertEqual(summary["status"], "empty")
        self.assertFalse((directory / "reconstruction").exists())
        self.assertFalse(any(level == 3 for level, _ in conn.logs))
        self.assertTrue(any("reconstruction skipped" in w for w in summary["warnings"]))
        self.assertTrue(conn.closed)

    def test_kernel_generalizes_to_unseen_sms_frame(self):
        # Two independent coil sensitivity vectors permit exact slice separation.
        train = np.zeros((2, 2, 12, 12), np.complex64)
        train[0, 0] = self.data[0]
        train[0, 1] = 0.2 * self.data[0]
        train[1, 0] = 0.3 * self.data[1]
        train[1, 1] = self.data[1]
        weights = poc.train_kernel(train, 3, 1e-8)
        test = train * (0.3 + 0.7j)
        test = np.roll(test, 2, axis=-1)
        actual = poc.apply_kernel(test.sum(axis=0), weights, 2, 3)
        np.testing.assert_allclose(actual, test, rtol=1e-4, atol=1e-5)

    def test_slice_grappa_reference_geometry_caipi_and_replay(self):
        refs = np.zeros((2, 2, 12, 12), np.complex64)
        refs[0, 0], refs[1, 1] = self.data
        shifts = [0, 0.5]
        ramps = np.exp(
            -2j * np.pi * np.array(shifts)[:, None] * (np.arange(12)[None, :] - 6)
        )
        sms = (refs * ramps[:, None, :, None]).sum(axis=0)
        items = acquisitions(refs[0], 0, ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
        items += acquisitions(refs[1], 1, ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
        items += acquisitions(sms, 0)
        conn, directory = self.capture(items, "slicegrappa")
        params = dict(
            inputdomain="kx-ky",
            preprocessed=True,
            kernelsize=3,
            ridge=1e-8,
            smsmap={"0": {"slices": [0, 1], "shifts": shifts}},
        )
        images = list(
            poc.reconstruct(
                directory / "input.h5",
                "slicegrappa",
                {"parameters": params},
                self.metadata,
                artifacts=self.root / "artifacts",
            )
        )
        saved = np.load(next((self.root / "artifacts").glob("separated-*.npz")))
        np.testing.assert_allclose(saved["kspace"], refs, atol=1e-5)
        self.assertEqual(len(list((self.root / "artifacts").glob("kernel-*.npz"))), 1)
        self.assertEqual([i.slice for i in images], [0, 1])
        self.assertEqual([i.position[2] for i in images], [0, 5])
        for image, ref in zip(images, refs):
            np.testing.assert_allclose(
                image.data[0, 0], poc.rss(ref, "kx-ky"), atol=1e-5
            )

    def test_unique_sessions_and_missing_original_diagnostic(self):
        with patch.dict(os.environ, FIRE_POC_CAPTURE_ROOT=str(self.root)):
            for _ in range(2):
                poc.process(Connection([None]), {}, self.metadata, "acsrss")
        paths = list(self.root.iterdir())
        self.assertEqual(len(paths), 2)
        summary = json.loads((paths[0] / "summary.json").read_text())
        self.assertTrue(summary["close_message_received"])
        self.assertTrue(any("No original images" in w for w in summary["warnings"]))

    def test_original_series_collision_is_rejected(self):
        original = ismrmrd.Image.from_array(
            np.ones((12, 12), np.int16), transpose=False
        )
        original.image_series_index = 60000
        _, directory = self.capture(
            acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
            + [original]
        )
        config = {"parameters": {"inputdomain": "kx-ky", "preprocessed": True}}
        with self.assertRaisesRegex(ValueError, "collides"):
            list(
                poc.reconstruct(
                    directory / "input.h5", "acsrss", config, self.metadata
                )
            )

    def test_undersampled_sms_rejected(self):
        _, directory = self.capture(acquisitions(self.data, lines=range(0, 12, 2)))
        config = {
            "parameters": dict(
                inputdomain="kx-ky",
                preprocessed=True,
                smsmap={"0": {"slices": [0, 1], "shifts": [0, 0]}},
            )
        }
        with self.assertRaisesRegex(ValueError, "full PE sampling"):
            list(
                poc.reconstruct(
                    directory / "input.h5",
                    "slicegrappa",
                    config,
                    self.metadata,
                )
            )

    def test_reject_duplicate_reverse_missing_pe_and_invalid_ridge(self):
        group = acquisitions(self.data)
        with self.assertRaisesRegex(ValueError, "repeated"):
            poc.assemble(group + group[:1], self.metadata, "kx-ky")
        group[0].set_flag(ismrmrd.ACQ_IS_REVERSE)
        with self.assertRaisesRegex(ValueError, "polarity"):
            poc.assemble(group, self.metadata, "kx-ky")
        with self.assertRaisesRegex(ValueError, "ridge"):
            poc.train_kernel(np.stack([self.data, self.data]), ridge=0)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeTests)
    )
    if not result.wasSuccessful():
        sys.exit(1)
    print("FIRE POC smoke tests passed")
