"""Synthetic runtime contracts for both capture-first FIRE applications."""

import json
import os
import sys
import tempfile
import unittest
from copy import deepcopy
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

    def assert_display_image(self, image, expected):
        self.assertEqual(image.data.dtype, np.dtype("int16"))
        target = expected / expected.max() * 4095 if expected.max() > 0 else expected
        np.testing.assert_allclose(image.data[0, 0], target, atol=0.501, rtol=0)
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        self.assertEqual(meta["WindowCenter"], "2048")
        self.assertEqual(meta["WindowWidth"], "4096")

    def test_live_display_handles_zero_and_tiny_signal(self):
        for scale in (0, 1e-20):
            conn = Connection(acquisitions(
                self.data * scale, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION
            ) + [None])
            with self.assertLogs(level="INFO") as logs:
                poc.process_acs(conn, {}, self.metadata)
            self.assertEqual(len(conn.images), 1)
            self.assert_display_image(conn.images[0], poc.rss(self.data * scale, "x-ky"))
            self.assertTrue(any("ACS pixel stats:" in msg for msg in logs.output))
            self.assertEqual(any("zero RSS signal" in msg for _, msg in conn.logs), scale == 0)

    def capture(self, items, app="acsrss", params=None):
        conn = Connection(items + [None])
        with patch.dict(os.environ, FIRE_POC_CAPTURE_ROOT=str(self.root)):
            poc.process(conn, {"parameters": params or {}}, self.metadata, app)
        directory = next(self.root.iterdir())
        return conn, directory

    def test_default_completes_ice_readout_transform(self):
        # Asymmetric landmarks expose sign reversals and one-pixel shifts.
        coils = np.zeros_like(self.data)
        coils[0, 2, 3] = 1
        coils[1, 8, 9] = 0.5j
        hybrid = np.fft.fftshift(np.fft.ifft(
            np.fft.ifftshift(coils, axes=(-2,)), axis=-2, norm="ortho"
        ), axes=(-2,)).astype(np.complex64)
        expected = np.sqrt(np.sum(np.abs(coils) ** 2, axis=0))
        for config in ({}, {"parameters": None}, {"parameters": {}}):
            conn = Connection(acquisitions(
                hybrid, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION
            ) + [None])
            poc.process_acs(conn, config, self.metadata)
            self.assertTrue(conn.closed)
            self.assertEqual(len(conn.images), 1)
            self.assert_display_image(conn.images[0], expected)
            self.assertEqual(list(conn.images[0].read_dir), [1, 0, 0])
            self.assertEqual(list(conn.images[0].phase_dir), [0, 1, 0])
            self.assertFalse(any(level == 3 for level, _ in conn.logs))

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
                transform = np.fft.fftn if domain == "x-ky" else np.fft.ifftn
                coils = np.fft.fftshift(
                    transform(
                        np.fft.ifftshift(self.data, axes=axes), axes=axes, norm="ortho"
                    ),
                    axes=axes,
                )
                expected = np.sqrt(np.sum(np.abs(coils) ** 2, axis=0))
                self.assert_display_image(conn.images[1], expected)
                self.assertEqual(conn.images[1].image_series_index, 60000)
                self.assertFalse(any(level == 3 for level, _ in conn.logs))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_oversampled_readout_lands_on_the_encoded_grid(self):
        # OpenRecon reports encodedSpace at base resolution while the ADC keeps
        # the vendor 2x readout oversampling, so the crop happens in image space.
        wide = np.zeros((2, 12, 24), np.complex64)
        wide[..., 6:18] = poc.ifft_centered(self.data, (-1,))
        expected = poc.rss(self.data, "kx-ky")
        streams = (("kx-ky", poc.fft_centered(wide, (-1,))), ("x-ky", wide))
        for domain, streamed in streams:
            with self.subTest(domain=domain):
                conn = Connection(
                    acquisitions(streamed, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    + [None]
                )
                poc.process_acs(
                    conn, {"parameters": {"inputdomain": domain}}, self.metadata
                )
                self.assertEqual([level for level, _ in conn.logs if level == 3], [])
                self.assertEqual(conn.images[0].data.shape, (1, 1, 12, 12))
                display_expected = (
                    expected[(-np.arange(12)) % 12]
                    if domain == "x-ky" else expected
                )
                self.assert_display_image(conn.images[0], display_expected)
                self.assertEqual(list(conn.images[0].field_of_view), [120, 120, 4])

    def test_multishot_epi_acs_segments_form_one_frame(self):
        # Segmented EPI splits one contiguous ACS region across shots; each shot
        # alone is not contiguous, so the segments must reconstruct together.
        shots = {0: [2, 3, 6, 7], 1: [4, 5, 8, 9]}
        items = []
        for segment, lines in shots.items():
            acqs = acquisitions(
                self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION, lines=lines
            )
            for acq in acqs:
                acq.idx.segment = segment
            items += acqs
        conn = Connection(items + [None])
        poc.process_acs(conn, {"parameters": {"inputdomain": "kx-ky"}}, self.metadata)
        self.assertEqual([level for level, _ in conn.logs if level == 3], [])
        self.assertEqual(len(conn.images), 1)
        padded = np.zeros_like(self.data)
        padded[:, sum(shots.values(), [])] = self.data[:, sum(shots.values(), [])]
        self.assert_display_image(conn.images[0], poc.rss(padded, "kx-ky"))

    def test_repeated_segment_coverage_returns_independent_acs_images(self):
        # Scanner layout: two segments each carry 54 lines on a 110 PE grid.
        header = metadata(110)
        header.encoding[0].encodedSpace = deepcopy(header.encoding[0].encodedSpace)
        header.encoding[0].encodedSpace.matrixSize.x = 220
        header.encoding[0].encodedSpace.fieldOfView_mm.x = 240
        data = (
            self.rng.normal(size=(2, 110, 110))
            + 1j * self.rng.normal(size=(2, 110, 110))
        ).astype(np.complex64)
        lines = list(range(28, 82))
        for domain in ("kx-ky", "x-ky"):
            items = []
            expected = []
            for segment, scale in enumerate((1, 2j)):
                shot = acquisitions(
                    data * scale, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION,
                    lines=lines,
                )
                for acq in shot:
                    acq.idx.segment = segment
                    acq.center_sample = 109
                    acq.scan_counter += segment * 4332
                    acq.clear_flag(ismrmrd.ACQ_LAST_IN_SLICE)
                items.extend(shot)
                padded = np.zeros_like(data)
                padded[:, lines] = (data * scale)[:, lines]
                expected.append(poc.rss(padded, domain))
            conn = Connection(items + [None])
            poc.process_acs(conn, {"parameters": {"inputdomain": domain}}, header)
            self.assertEqual([msg for level, msg in conn.logs if level == 3], [])
            self.assertTrue(conn.closed)
            self.assertEqual(len(conn.images), 2)
            self.assertEqual([im.image_index for im in conn.images], [1, 2])
            for image, values in zip(conn.images, expected):
                self.assert_display_image(image, values)

    def test_duplicate_within_segment_is_still_rejected(self):
        group = acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
        conn = Connection(group + [group[0], None])
        poc.process_acs(conn, {}, self.metadata)
        self.assertTrue(any(level == 3 and "repeated PE line" in msg for level, msg in conn.logs))
        self.assertEqual(conn.images, [])
        self.assertTrue(conn.closed)

    def test_recon_width_readout_with_stale_encoded_header(self):
        # Scanner case: 110 samples, encoded RO 220/396 mm, recon RO 110/198 mm,
        # and stale center_sample=109. Use a smaller equivalent numerical case.
        header = deepcopy(self.metadata)
        header.encoding[0].encodedSpace = deepcopy(header.encoding[0].encodedSpace)
        header.encoding[0].encodedSpace.matrixSize.x = 24
        header.encoding[0].encodedSpace.fieldOfView_mm.x = 240
        for domain in ("kx-ky", "x-ky"):
            for center in (6, 11, 12):
                with self.subTest(domain=domain, center=center):
                    acqs = acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
                    for acq in acqs:
                        acq.center_sample = center
                    conn = Connection(acqs + [None])
                    poc.process_acs(conn, {"parameters": {"inputdomain": domain}}, header)
                    self.assertEqual([msg for level, msg in conn.logs if level == 3], [])
                    self.assertTrue(conn.closed)
                    self.assertEqual(len(conn.images), 1)
                    self.assert_display_image(conn.images[0], poc.rss(self.data, domain))
                    self.assertEqual(list(conn.images[0].field_of_view), [120, 120, 4])
                    self.assertEqual(header.encoding[0].encodedSpace.matrixSize.x, 24)
                    self.assertEqual(acqs[0].center_sample, center)

        acqs = acquisitions(self.data)
        acqs[0].center_sample = 3
        with self.assertRaisesRegex(ValueError, "Asymmetric"):
            poc.assemble(acqs, header, "kx-ky")
        header.encoding[0].encodedSpace.fieldOfView_mm.x = 180
        with self.assertRaisesRegex(ValueError, "integer multiple"):
            poc.assemble(acquisitions(self.data), header, "kx-ky")

    def test_ambiguous_pe_frames_report_both_acquisitions(self):
        group = acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)
        repeated = acquisitions(self.data, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION)[0]
        repeated.scan_counter = 100
        repeated.idx.segment = 2
        conn = Connection(group + [repeated, None])
        poc.process_acs(conn, {}, self.metadata)
        error = next(message for level, message in conn.logs if level == 3)
        self.assertIn("repeated PE line", error)
        self.assertIn("previous: scan_counter=0", error)
        self.assertIn("current: scan_counter=100", error)
        self.assertIn("'segment': 2", error)
        self.assertTrue(conn.closed)
        self.assertEqual(conn.images, [])

        group[0].idx.kspace_encode_step_1 = 12
        conn = Connection(group + [None])
        poc.process_acs(conn, {}, self.metadata)
        error = next(message for level, message in conn.logs if level == 3)
        self.assertIn("Out-of-range PE line: mapped_ky=12, grid_y=12", error)
        self.assertIn("minimum=0, maximum=11, center=6", error)
        self.assertTrue(conn.closed)
        self.assertEqual(conn.images, [])

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
        np.testing.assert_allclose(
            poc.rss(hybrid, "x-ky"), expected[(-np.arange(12)) % 12], rtol=1e-6
        )
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
        with self.assertRaisesRegex(ValueError, "integer multiple"):
            poc.assemble(
                acquisitions(self.data[..., :10]), self.metadata, "kx-ky"
            )
        with self.assertRaisesRegex(ValueError, "ridge"):
            poc.train_kernel(np.stack([self.data, self.data]), ridge=0)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeTests)
    )
    if not result.wasSuccessful():
        sys.exit(1)
    print("FIRE POC smoke tests passed")
