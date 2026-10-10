import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import constants
import ismrmrd
import nibabel as nib
import numpy as np

import b0mapromeo
import b0_artifact
from b0_artifact import read_map
from b0_settings import _settings as shared_settings
from shared_mount_fixture import Connection, PARAMETERS, images_from_geometry


class ProducerArtifactTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        env = patch.dict(os.environ, {"B0_MAP_STORE": str(self.root / "maps")})
        env.start()
        self.addCleanup(env.stop)
        work = patch.object(b0mapromeo, "WORK_ROOT", self.root / "scratch")
        work.start()
        self.addCleanup(work.stop)
        planes, _, _ = images_from_geometry((8, 8, 6), np.eye(3), [1, 1, 1], [0, 0, 0])
        self.images = []
        for echo in range(2):
            for kind in (ismrmrd.IMTYPE_MAGNITUDE, ismrmrd.IMTYPE_PHASE):
                for plane in planes:
                    image = copy.deepcopy(plane)
                    head = image.getHead()
                    head.contrast = echo
                    head.image_type = kind
                    head.image_series_index = kind
                    image.setHead(head)
                    if kind == ismrmrd.IMTYPE_PHASE:
                        image.data[:] = 0
                    self.images.append(image)
        self.field = np.ones((8, 8, 6)) * 23
        self.support = np.ones(self.field.shape, bool)
        reconstruct = patch.object(
            b0mapromeo, "reconstruct", return_value=(self.field, self.support)
        )
        self.reconstruct = reconstruct.start()
        self.addCleanup(reconstruct.stop)
        self.config = {
            "phaseunits": "radians",
            "echotimesms": "2,4",
            "sendoriginal": False,
        }

    def test_phase_defaults_and_configuration_precedence(self):
        for config in (None, {}, "b0mapromeo", '{"parameters": {}}'):
            with self.subTest(config=config):
                settings = b0mapromeo._settings(config, None)
                self.assertEqual(settings["phase_units"], "signed")
                self.assertTrue(settings["send_original"])
        legacy = shared_settings({}, None)
        self.assertEqual(legacy["phase_units"], "siemens")
        self.assertFalse(legacy["send_original"])
        label = json.loads(Path(b0mapromeo.__file__).with_name("OpenReconLabel.json").read_text())
        defaults = {p["id"]: p["default"] for p in label["parameters"]}
        self.assertNotIn("phaseunits", defaults)
        settings = b0mapromeo._settings({"parameters": defaults}, None)
        self.assertEqual(settings["phase_units"], "signed")
        self.assertTrue(settings["send_original"])
        for units in ("signed", "siemens", "radians"):
            metadata = ismrmrd.xsd.ismrmrdHeader(
                userParameters=ismrmrd.xsd.userParametersType(
                    userParameterString=[
                        ismrmrd.xsd.userParameterStringType(
                            name="phaseunits", value=units
                        ),
                    ],
                    userParameterLong=[
                        ismrmrd.xsd.userParameterLongType(name="sendoriginal", value=0),
                    ],
                ),
            )
            xml = ismrmrd.xsd.ToXML(metadata)
            with self.subTest(units=units):
                settings = b0mapromeo._settings("b0mapromeo", xml)
                self.assertEqual(settings["phase_units"], units)
                self.assertFalse(settings["send_original"])
                override = {
                    "parameters": {"phaseunits": "signed", "sendoriginal": True}
                }
                settings = b0mapromeo._settings(json.dumps(override), xml)
                self.assertEqual(settings["phase_units"], "signed")
                self.assertTrue(settings["send_original"])

    def test_default_signed_float_counts_reach_reconstruction_in_radians(self):
        images = copy.deepcopy(self.images)
        counts = (100.25, 500.75)
        for image in images:
            if image.image_type == ismrmrd.IMTYPE_PHASE:
                image.data[:] = counts[image.contrast]
        connection = Connection(images)
        b0mapromeo.process(connection, {"echotimesms": "2,4"}, None)
        self.assertFalse(connection.logs)
        self.reconstruct.assert_called_once()
        phase = self.reconstruct.call_args.args[1]
        np.testing.assert_allclose(phase[..., 0], counts[0] * np.pi / 4096)
        np.testing.assert_allclose(phase[..., 1], counts[1] * np.pi / 4096)
        field_hz = (phase[..., 1] - phase[..., 0]) / (2 * np.pi * 0.002)
        np.testing.assert_allclose(field_hz, 24.444580078125, rtol=1e-6)
        originals = [
            im for im in connection.sent
            if not any(key in ismrmrd.Meta.deserialize(im.attribute_string)
                       for key in ("B0MapUnits", "T2StarMapUnits"))
        ]
        self.assertEqual(len(originals), len(images))

    def test_process_returns_t2star_after_b0_and_preserves_originals(self):
        for image in self.images:
            if image.image_type == ismrmrd.IMTYPE_MAGNITUDE:
                image.data[:] = 1000 * np.exp(-[2, 4][image.contrast] / 37)
        connection = Connection(self.images)
        b0mapromeo.process(connection, {**self.config, "sendoriginal": True}, None)
        self.assertFalse(connection.logs)
        self.assertTrue(connection.closed)
        metadata = [ismrmrd.Meta.deserialize(im.attribute_string) for im in connection.sent]
        b0 = [im for im, meta in zip(connection.sent, metadata) if "B0MapUnits" in meta]
        t2 = [im for im, meta in zip(connection.sent, metadata) if "T2StarMapUnits" in meta]
        originals = [im for im, meta in zip(connection.sent, metadata)
                     if "B0MapUnits" not in meta and "T2StarMapUnits" not in meta]
        self.assertEqual(len(b0), self.field.shape[2])
        self.assertEqual(len(t2), len(b0))
        self.assertEqual(connection.sent[-len(t2):], t2)
        self.assertEqual(connection.sent[-len(t2)-len(b0):-len(t2)], b0)
        self.assertEqual(sorted(im.data.tobytes() for im in originals),
                         sorted(im.data.tobytes() for im in self.images))
        for image in t2:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            decoded = image.data * float(meta["RescaleSlope"]) + float(meta["RescaleIntercept"])
            np.testing.assert_allclose(decoded, 37, atol=0.51)
            self.assertNotEqual(image.image_series_index, b0[0].image_series_index)

    def test_cli_saves_t2star_and_validity_with_b0_affine(self):
        for image in self.images:
            if image.image_type == ismrmrd.IMTYPE_MAGNITUDE:
                image.data[:] = 1000 * np.exp(-[2, 4][image.contrast] / 37)
        output = self.root / "t2star-cli"
        self.run_cli(output)
        t2star = nib.load(output / "t2star_ms.nii")
        validity = nib.load(output / "t2star_valid_mask.nii")
        b0 = nib.load(output / "b0_hz.nii")
        np.testing.assert_allclose(t2star.get_fdata(), 37, rtol=1e-5)
        np.testing.assert_array_equal(validity.get_fdata(), self.support)
        np.testing.assert_array_equal(t2star.affine, b0.affine)
        np.testing.assert_array_equal(validity.affine, b0.affine)

    def test_invalid_phase_reaches_scanner_error_log_without_outputs(self):
        for units, low, high, reason, interval in (
            ("siemens", -1, 4095, "unsigned 12-bit counts", "[0,4095]"),
            ("siemens", 0, 4096, "unsigned 12-bit counts", "[0,4095]"),
            ("signed", -4097, 4096, "Signed Siemens phase", "[-4096,4096]"),
            ("signed", -4096, 4097, "Signed Siemens phase", "[-4096,4096]"),
            ("radians", -4, 3, "Radian phase", "[-pi,pi]"),
            ("radians", -3, 4, "Radian phase", "[-pi,pi]"),
        ):
            with self.subTest(units=units, low=low, high=high):
                images = copy.deepcopy(self.images)
                for image in images:
                    if image.image_type == ismrmrd.IMTYPE_PHASE:
                        image.data[:] = low
                        image.data.flat[-1] = high
                connection = Connection(images)
                with self.assertLogs(level="INFO") as captured:
                    b0mapromeo.process(
                        connection, {**self.config, "phaseunits": units}, None
                    )
                self.assertTrue(connection.closed)
                self.assertFalse(connection.sent)
                self.reconstruct.assert_not_called()
                self.assertFalse((self.root / "maps").exists())
                self.assertEqual(len(connection.logs), 1)
                level, message = connection.logs[0]
                self.assertEqual(level, constants.MRD_LOGGING_ERROR)
                self.assertIn(f"b0mapromeo {b0mapromeo.VERSION}", message)
                self.assertIn("Traceback (most recent call last)", message)
                self.assertIn("ValueError:", message)
                self.assertIn(reason, message)
                self.assertIn(f"phaseunits={units}", message)
                self.assertIn(f"observed range [{low},{high}]", message)
                self.assertIn(interval, message)
                self.assertIn("only if", message)
                self.assertTrue(any(
                    record.levelname == "ERROR" and record.exc_info
                    for record in captured.records
                ))
                self.assertTrue(any(
                    f"b0mapromeo {b0mapromeo.VERSION}" in record.getMessage()
                    and f"phaseunits={units}" in record.getMessage()
                    for record in captured.records
                ))

    def test_explicit_phase_scales_preserve_conversion_boundaries(self):
        for units, values, expected in (
            ("siemens", [0, 2048, 4095], [-np.pi, 0, 4094 * np.pi / 4096]),
            ("signed", [-4096, 0, 4096], [-np.pi, 0, np.pi]),
            ("radians", [-np.pi, 0, np.pi], [-np.pi, 0, np.pi]),
        ):
            with self.subTest(units=units):
                np.testing.assert_allclose(
                    b0mapromeo.phase_radians(np.array(values), units),
                    expected, rtol=1e-6,
                )

    def run_cli(self, output: Path) -> None:
        def reconstruct(
            magnitude: np.ndarray,
            phase: np.ndarray,
            affine: np.ndarray,
            times: list[float],
            output_dir: Path,
            max_seeds: int,
        ) -> tuple[np.ndarray, np.ndarray]:
            output_dir.mkdir(parents=True)
            nib.save(nib.Nifti1Image(self.field, affine), output_dir / "b0_hz.nii")
            return self.field, self.support

        self.reconstruct.side_effect = reconstruct
        with (
            patch.object(b0mapromeo, "read_dicoms", return_value=(self.images, [2, 4])),
            patch("sys.argv", [
                "b0mapromeo", "--dicom-dir", str(self.root / "dicoms"),
                "--output-dir", str(output), "--b0mapid", "cli-map",
            ]),
        ):
            b0mapromeo.main()

    def test_cli_defaults_to_local_bundle_without_writable_shared_store(self):
        os.environ.pop("B0_MAP_STORE", None)
        blocked = self.root / "blocked-share"
        blocked.write_text("not a directory")
        resolve_store = b0_artifact._store

        def isolated_store(store):
            return blocked if store is None else resolve_store(store)

        output = self.root / "output"
        with patch.object(b0_artifact, "_store", side_effect=isolated_store):
            self.run_cli(output)
        saved = read_map("cli-map", store=output / "b0maps")
        np.testing.assert_array_equal(saved.field_hz, self.field)
        np.testing.assert_array_equal(saved.support, self.support)
        self.assertEqual(json.loads((output / "shim_settings.json").read_text())["status"], "unavailable")
        self.assertEqual(blocked.read_text(), "not a directory")

    def test_cli_honors_explicit_shared_store(self):
        output = self.root / "output"
        self.run_cli(output)
        saved = read_map("cli-map")
        self.assertEqual(saved.path, self.root / "maps" / "cli-map")
        np.testing.assert_array_equal(saved.field_hz, self.field)
        self.assertTrue((output / "shim_settings.json").is_file())
        self.assertFalse((output / "b0maps").exists())

    def test_cli_retains_settings_when_explicit_store_cannot_publish(self):
        blocked = self.root / "blocked-share"
        blocked.write_text("not a directory")
        os.environ["B0_MAP_STORE"] = str(blocked)
        output = self.root / "output"
        with self.assertRaises(OSError):
            self.run_cli(output)
        np.testing.assert_array_equal(nib.load(output / "b0_hz.nii").get_fdata(), self.field)
        self.assertEqual(json.loads((output / "shim_settings.json").read_text())["status"], "unavailable")
        self.assertFalse((output / "b0maps").exists())

    def test_generated_and_selected_id_survive_scratch_and_scanner_comments(self):
        for selected in ("", "operator-selected"):
            connection = Connection(self.images)
            b0mapromeo.process(connection, {**self.config, "b0mapid": selected}, None)
            self.assertTrue(connection.closed and not connection.logs)
            self.assertTrue(connection.sent)
            identifiers = set()
            for image in connection.sent:
                meta = ismrmrd.Meta.deserialize(image.attribute_string)
                if "T2StarMapUnits" in meta:
                    continue
                identifiers.add(meta["B0MapId"])
                self.assertEqual(meta["ImageComment"], meta["ImageComments"])
                self.assertIn("B0MapId=" + meta["B0MapId"], meta["ImageComment"])
            self.assertEqual(len(identifiers), 1)
            name = identifiers.pop()
            if selected:
                self.assertEqual(name, selected)
            saved = read_map(name)
            np.testing.assert_array_equal(saved.field_hz, self.field)
            self.assertTrue((saved.path / "b0_hz.nii.gz").is_file())
            self.assertEqual(list(b0mapromeo.WORK_ROOT.iterdir()), [])

    def test_conflicting_phase_or_later_echo_identity_fails_before_reconstruction(self):
        for key in ("StudyInstanceUID", "FrameOfReferenceUID"):
            for kind, echo in (
                (ismrmrd.IMTYPE_PHASE, 0), (ismrmrd.IMTYPE_MAGNITUDE, 1)
            ):
                images = copy.deepcopy(self.images)
                for image in images:
                    meta = ismrmrd.Meta.deserialize(image.attribute_string)
                    conflict = image.image_type == kind and image.contrast == echo
                    meta[key] = "other" if conflict else "source"
                    image.attribute_string = meta.serialize()
                connection = Connection(images)
                with (
                    self.subTest(key=key, kind=kind, echo=echo),
                    self.assertLogs(level="ERROR"),
                ):
                    b0mapromeo.process(connection, self.config, None)
                self.assertTrue(
                    connection.closed and connection.logs and not connection.sent
                )
                self.reconstruct.assert_not_called()
                self.assertFalse((self.root / "maps").exists())

    def test_collision_fails_request_without_replacing_measurement(self):
        config = {**self.config, "b0mapid": "same"}
        first = Connection(self.images)
        b0mapromeo.process(first, config, None)
        digest = read_map("same").digest
        self.field[:] = 900
        second = Connection(self.images)
        with self.assertLogs(level="ERROR"):
            b0mapromeo.process(second, config, None)
        self.assertTrue(second.closed and second.logs and not second.sent)
        self.assertEqual(read_map("same").digest, digest)
        np.testing.assert_array_equal(read_map("same").field_hz, 23)

    def test_published_measurement_survives_optional_solver_failure(self):
        for requested_id in ("retained", ""):
            with self.subTest(requested_id=requested_id):
                connection = Connection(self.images)
                with (
                    patch.object(
                        b0mapromeo, "compute_shim", side_effect=RuntimeError("solver failed")
                    ),
                    self.assertLogs(level="INFO") as captured,
                ):
                    b0mapromeo.process(
                        connection, {**self.config, **PARAMETERS, "b0mapid": requested_id}, None
                    )
                self.assertTrue(connection.closed and connection.logs and not connection.sent)
                published = [
                    record.getMessage().split("=", 1)[1]
                    for record in captured.records
                    if record.getMessage().startswith("b0mapromeo published B0MapId=")
                ]
                self.assertEqual(len(published), 1)
                saved = read_map(published[0])
                if requested_id:
                    self.assertEqual(saved.id, requested_id)
                self.assertEqual(saved.context["kind"], "analytical")
                self.assertEqual(saved.context["baseline"]["X"], 0.4)
                self.assertEqual(list(b0mapromeo.WORK_ROOT.iterdir()), [])

    def test_partial_source_configuration_is_still_invalid(self):
        connection = Connection(self.images)
        with self.assertLogs(level="ERROR"):
            b0mapromeo.process(
                connection, {**self.config, "shimcurrenta": "[0.2]"}, None
            )
        self.assertTrue(connection.closed and connection.logs and not connection.sent)
        self.reconstruct.assert_not_called()
        self.assertFalse((self.root / "maps").exists())


if __name__ == "__main__":
    result = unittest.main(exit=False)
    if not result.result.wasSuccessful():
        raise SystemExit(1)
    print("Durable B0 artifact tests passed")
