import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import ismrmrd
import numpy as np

import b0mapromeo
from b0_artifact import read_map
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
        self.config = {"phaseunits": "radians", "echotimesms": "2,4"}

    def test_generated_and_selected_id_survive_scratch_and_scanner_comments(self):
        for selected in ("", "operator-selected"):
            connection = Connection(self.images)
            b0mapromeo.process(connection, {**self.config, "b0mapid": selected}, None)
            self.assertTrue(connection.closed and not connection.logs)
            self.assertTrue(connection.sent)
            identifiers = set()
            for image in connection.sent:
                meta = ismrmrd.Meta.deserialize(image.attribute_string)
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
