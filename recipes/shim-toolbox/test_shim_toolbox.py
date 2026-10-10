import concurrent.futures
import copy
import hashlib
import itertools
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import ismrmrd
import nibabel as nib
import numpy as np

import shim_toolbox
from b0_artifact import publish_map, read_map, source_context, evaluate_target
from b0_geometry import assemble_target
from b0_images import output_images
from b0_settings import _settings
from shared_mount_fixture import (
    ABSOLUTE,
    PARAMETERS,
    Connection,
    decode,
    images_from_geometry,
    target_images,
    world_field,
)


class TargetTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        env = patch.dict(os.environ, {"B0_MAP_STORE": str(self.root / "maps")})
        env.start()
        self.addCleanup(env.stop)
        self.images, self.points, self.affine = images_from_geometry(
            (20, 22, 18), np.eye(3), [1.5, 1.7, 2], [0, 0, 0]
        )
        self.affine = assemble_target(self.images).affine
        self.field = world_field(self.points).reshape(20, 22, 18)
        self.mask = np.ones(self.field.shape, bool)
        self.settings = _settings(PARAMETERS, None)

    def save(self, name="map1", context=None, support=None):
        return publish_map(
            self.field,
            self.mask if support is None else support,
            self.affine,
            source_context({}) if context is None else context,
            assemble_target(self.images).identity,
            requested_id=name,
        )

    def test_bundle_immutable_roundtrip_and_safe_ids(self):
        saved = self.save()
        np.testing.assert_array_equal(saved.field_hz, self.field)
        np.testing.assert_array_equal(saved.affine, self.affine)
        self.assertFalse(saved.field_hz.flags.writeable)
        with self.assertRaises(ValueError):
            self.save()
        for name in ("../escape", "/absolute", ".", "a/b", "a b", "é", "x" * 65):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.save(name)
        generated = self.save(None)
        self.assertTrue(generated.id)
        self.assertEqual(read_map(generated.id).digest, generated.digest)
        self.assertFalse(
            any(p.name.startswith(".pending") for p in saved.path.parent.iterdir())
        )

    def test_concurrent_same_id_has_one_winner_and_never_partial_read(self):
        def writer(i):
            try:
                return self.save("race").digest
            except ValueError:
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(writer, range(8)))
        self.assertEqual(sum(value is not None for value in outcomes), 1)
        self.assertIn(read_map("race").digest, outcomes)
        np.testing.assert_array_equal(read_map("race").field_hz, self.field)

    def test_read_during_staging_and_existing_empty_destination(self):
        import b0_artifact

        reached, resume = threading.Event(), threading.Event()
        original = b0_artifact._write

        def delayed(path, data):
            original(path, data)
            if path.name == "manifest.json":
                reached.set()
                if not resume.wait(10):
                    raise RuntimeError("test publication did not resume")

        with (
            patch.object(b0_artifact, "_write", delayed),
            concurrent.futures.ThreadPoolExecutor() as pool,
        ):
            future = pool.submit(self.save, "staging")
            try:
                self.assertTrue(reached.wait(10))
                with self.assertRaises(ValueError):
                    read_map("staging")
            finally:
                resume.set()
            saved = future.result(timeout=10)
        self.assertEqual(read_map("staging").digest, saved.digest)
        empty = self.root / "maps" / "empty"
        empty.mkdir()
        with self.assertRaises(ValueError):
            self.save("empty")
        link = self.root / "maps" / "link"
        link.symlink_to(saved.path, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.save("link")

    def test_read_rejects_corruption_symlinks_incomplete_and_bad_manifests(self):
        for problem in (
            "digest",
            "schema",
            "path",
            "payload_link",
            "directory_link",
            "missing",
            "nonbinary",
            "affine",
            "nifti_units",
            "nifti_affine",
        ):
            with self.subTest(problem=problem):
                saved = self.save(problem)
                manifest = saved.path / "manifest.json"
                data = json.loads(manifest.read_text())
                if problem == "digest":
                    (saved.path / "b0_hz.nii.gz").write_bytes(b"corrupt")
                elif problem == "payload_link":
                    p = saved.path / "b0_hz.nii.gz"
                    data_path = self.root / "real.npy"
                    data_path.write_bytes(p.read_bytes())
                    p.unlink()
                    p.symlink_to(data_path)
                elif problem == "directory_link":
                    saved.path.rename(self.root / "external")
                    saved.path.symlink_to(
                        self.root / "external", target_is_directory=True
                    )
                elif problem == "missing":
                    (saved.path / "support.nii.gz").unlink()
                else:
                    if problem == "schema":
                        data["schema"] = 99
                    elif problem == "path":
                        data["payloads"]["../external.npy"] = "invalid"
                    elif problem == "affine":
                        data["affine"] = np.zeros((4, 4)).tolist()
                    elif problem in ("nifti_units", "nifti_affine"):
                        p = saved.path / "b0_hz.nii.gz"
                        affine = self.affine.copy()
                        if problem == "nifti_affine":
                            affine[0, 3] += 2
                        image = nib.Nifti1Image(self.field, affine)
                        image.header.set_xyzt_units(
                            "meter" if problem == "nifti_units" else "mm"
                        )
                        nib.save(image, p)
                        data["payloads"]["b0_hz.nii.gz"] = hashlib.sha256(
                            p.read_bytes()
                        ).hexdigest()
                    elif problem == "nonbinary":
                        p = saved.path / "support.nii.gz"
                        image = nib.Nifti1Image(
                            np.full(self.field.shape, 2, dtype=np.uint8), self.affine
                        )
                        image.header.set_xyzt_units("mm")
                        nib.save(image, p)
                        data["payloads"]["support.nii.gz"] = hashlib.sha256(
                            p.read_bytes()
                        ).hexdigest()
                    manifest.write_text(json.dumps(data))
                with self.assertRaises((ValueError, OSError)):
                    read_map(problem)
        (self.root / "maps" / ".pending-interrupted").mkdir()
        with self.assertRaises(ValueError):
            read_map(".pending-interrupted")

    def test_geometry_shuffled_packed_single_and_metadata_axes(self):
        images, points, expected = target_images()
        for selected in (images, list(reversed(images))):
            target = assemble_target(selected)
            np.testing.assert_allclose(target.affine, expected, atol=1e-6)
        axes = np.column_stack(
            [images[0].read_dir, images[0].phase_dir, images[0].slice_dir]
        )
        positions = np.array([im.position for im in images])
        packed, _, packed_expected = images_from_geometry(
            (9, 8, 7), axes, [1.1, 1.4, 1.7], positions.mean(axis=0), packed=True
        )
        np.testing.assert_allclose(
            assemble_target(packed).affine, packed_expected, atol=1e-6
        )
        single, _, single_expected = images_from_geometry(
            (9, 8, 1), axes, [1.1, 1.4, 1.7], [1.3, -2.1, 0.9]
        )
        np.testing.assert_allclose(
            assemble_target(single).affine, single_expected, atol=1e-6
        )
        altered = copy.deepcopy(images)
        for image in altered:
            head = image.getHead()
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["ImageRowDir"], meta["ImageColumnDir"], meta["ImageSliceDir"] = (
                axes.T.tolist()
            )
            head.read_dir[:], head.phase_dir[:], head.slice_dir[:] = np.eye(3)
            image.setHead(head)
            image.attribute_string = meta.serialize()
        np.testing.assert_allclose(assemble_target(altered).affine, expected, atol=1e-6)

    def test_geometry_rejects_mixed_duplicate_irregular_dynamic(self):
        for problem in (
            "series",
            "contrast",
            "duplicate",
            "irregular",
            "axes",
            "fov",
            "dynamic",
            "cardiac_phase",
            "average",
            "phase",
            "partial_axes",
        ):
            images = copy.deepcopy(self.images)
            head = images[0].getHead()
            if problem == "series":
                head.image_series_index += 1
            elif problem == "contrast":
                head.contrast += 1
            elif problem == "duplicate":
                images.append(images[0])
            elif problem == "irregular":
                head.position[2] += 0.5
            elif problem == "axes":
                head.read_dir[:] = [0, 0, 0]
            elif problem == "fov":
                head.field_of_view[0] *= 2
            elif problem == "dynamic":
                head.repetition = 1
            elif problem == "cardiac_phase":
                head.phase = 1
            elif problem == "average":
                head.average = 1
            elif problem == "phase":
                head.image_type = ismrmrd.IMTYPE_PHASE
            elif problem == "partial_axes":
                meta = ismrmrd.Meta.deserialize(images[0].attribute_string)
                meta["ImageRowDir"] = [1, 0, 0]
                images[0].attribute_string = meta.serialize()
            images[0].setHead(head)
            with self.subTest(problem=problem), self.assertRaises(ValueError):
                assemble_target(images)

    def test_malformed_json_configuration_fails_before_field_only_output(self):
        saved = self.save()
        metadata = ismrmrd.xsd.ismrmrdHeader(
            userParameters=ismrmrd.xsd.userParametersType(
                userParameterString=[
                    ismrmrd.xsd.userParameterStringType(name="b0mapid", value=saved.id)
                ]
            )
        )
        for config in ('{"shimnativebaseline": "1,2,3",', '[1,2,3]', 'null'):
            with self.subTest(config=config):
                connection = Connection(self.images)
                with self.assertLogs(level="ERROR"):
                    shim_toolbox.process(connection, config, metadata)
                self.assertTrue(connection.closed and connection.logs)
                self.assertFalse(connection.sent)
        self.assertEqual(_settings("shim_toolbox", metadata)["b0mapid"], saved.id)

    def test_independent_world_field_late_hfs_prediction_and_pixels(self):
        saved = self.save()
        images, points, affine = target_images()
        target = assemble_target(list(reversed(images)))
        evaluation = evaluate_target(saved, target, self.settings)
        expected = world_field(points).reshape(target.magnitude.shape)
        np.testing.assert_allclose(evaluation.field_hz, expected, atol=2e-7)
        np.testing.assert_allclose(
            [s.absolute for s in evaluation.shim.settings], ABSOLUTE, atol=2e-6
        )
        np.testing.assert_allclose(evaluation.shim.predicted_field_hz, 37, atol=2e-7)
        connection = Connection(list(reversed(images)))
        metadata = ismrmrd.xsd.ismrmrdHeader(
            userParameters=ismrmrd.xsd.userParametersType(
                userParameterString=[
                    ismrmrd.xsd.userParameterStringType(name=k, value=v)
                    for k, v in PARAMETERS.items()
                ]
            )
        )
        shim_toolbox.process(
            connection, {"b0mapid": saved.id}, ismrmrd.xsd.ToXML(metadata)
        )
        self.assertTrue(connection.closed)
        self.assertFalse(connection.logs)
        self.assertEqual(len(connection.sent), len(images) * 2)
        np.testing.assert_allclose(
            decode(connection.sent[: len(images)]), expected, atol=0.5
        )
        np.testing.assert_allclose(decode(connection.sent[len(images) :]), 37, atol=0.5)
        series, instances = set(), set()
        for index, image in enumerate(connection.sent):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            self.assertEqual(meta["ImageComment"], meta["ImageComments"])
            for value in (
                "ABSOLUTE ANALYTICAL ESTIMATE",
                "ideal field estimate",
                "B0MapId=map1",
                "supported ROI 504/504",
                "operator supplied",
            ):
                self.assertIn(value, meta["ImageComments"])
            self.assertEqual(meta["PixelPaddingValue"], "0")
            np.testing.assert_allclose(
                image.position, images[index % len(images)].position
            )
            series.add(meta["SeriesInstanceUID"])
            instances.add(meta["SOPInstanceUID"])
        self.assertEqual(len(series), 2)
        self.assertEqual(len(instances), len(connection.sent))
        self.assertEqual(read_map(saved.id).context["kind"], "unknown")

    def test_complete_interpolation_support_holes_partial_overlap_and_padding(self):
        mask = self.mask.copy()
        mask[9, 10, 8] = False
        saved = self.save(support=mask)
        images, points, _ = images_from_geometry(
            (22, 24, 20), np.eye(3), [1.5, 1.7, 2], [0.45, 0.51, 0.6]
        )
        images[9].data[0, 0, 9, 9] = 0
        target = assemble_target(images)
        result = evaluate_target(saved, target)
        expected_support = np.zeros(target.magnitude.shape, bool)
        coordinates = (points - self.affine[:3, 3, None]) / np.diag(
            self.affine[:3, :3]
        )[:, None]
        for j, point in enumerate(coordinates.T):
            corners = list(
                itertools.product(*[(int(np.floor(v)), int(np.ceil(v))) for v in point])
            )
            valid = all(
                all(0 <= c[d] < mask.shape[d] for d in range(3)) and mask[c]
                for c in corners
            )
            expected_support.flat[j] = valid
        np.testing.assert_array_equal(result.support, expected_support)
        self.assertLess(result.support.sum(), result.support.size)
        self.assertFalse(result.fit_roi[9, 9, 9])
        expected = world_field(points).reshape(target.magnitude.shape)
        np.testing.assert_allclose(
            result.field_hz[result.support], expected[result.support], atol=3e-7
        )
        outputs = output_images(
            result.field_hz, target.anchors, 190, support=result.support
        )
        stored = np.stack([im.data[0, 0].T for im in outputs], axis=2)
        self.assertTrue(np.all(stored[~result.support] == 0))
        self.assertTrue(np.all(stored[result.support] > 0))
        zero = output_images(
            np.zeros_like(result.field_hz), target.anchors, 191, support=result.support
        )
        encoded = np.stack([im.data[0, 0].T for im in zero], axis=2)
        self.assertTrue(np.all(encoded[result.support] == 2048))
        np.testing.assert_array_equal(
            evaluate_target(saved, assemble_target(self.images)).support, mask
        )

    def test_unknown_rank_no_overlap_identity_and_conflict(self):
        saved = self.save(context=source_context(self.settings))
        target = assemble_target(self.images)
        conflict = _settings({**PARAMETERS, "shimnativebaseline": "0,0,0"}, None)
        with self.assertRaisesRegex(ValueError, "baseline conflicts"):
            evaluate_target(saved, target, conflict)
        mismatch = copy.deepcopy(self.images)
        for image in mismatch:
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            meta["StudyInstanceUID"] = "different"
            image.attribute_string = meta.serialize()
        with self.assertRaisesRegex(ValueError, "different Study"):
            evaluate_target(saved, assemble_target(mismatch))
        single = assemble_target([self.images[0]])
        result = evaluate_target(saved, single)
        self.assertIsNone(result.shim.predicted_field_hz)
        self.assertIn("rank-unidentifiable", result.comment)
        unknown = self.save("unknown")
        result = evaluate_target(unknown, target)
        self.assertIsNone(result.shim.predicted_field_hz)
        self.assertIn("source acquisition settings unavailable", result.comment)
        far, _, _ = images_from_geometry((3, 3, 3), np.eye(3), [1, 1, 1], [1000, 0, 0])
        with self.assertRaisesRegex(ValueError, "no supported"):
            evaluate_target(saved, assemble_target(far))
        connection = Connection(self.images)
        shim_toolbox.process(connection, {"b0mapid": unknown.id}, None)
        self.assertTrue(connection.closed and not connection.logs)
        self.assertEqual(len(connection.sent), len(self.images))

    def test_numerical_errors_are_failed_requests_not_unavailable_predictions(self):
        saved = self.save(context=source_context(self.settings))
        connection = Connection(self.images)
        with (
            patch(
                "b0_artifact.compute_shim", side_effect=ValueError("numerical failure")
            ),
            self.assertLogs(level="ERROR"),
        ):
            shim_toolbox.process(connection, {"b0mapid": saved.id}, None)
        self.assertTrue(connection.closed and connection.logs)
        self.assertFalse(connection.sent)

    def test_calibration_snapshot_strict_target_grid_and_corrupt_config(self):
        profile = np.indices(self.field.shape)[0].astype(float)[..., None]
        nib.save(nib.Nifti1Image(profile, self.affine), self.root / "coil.nii")
        config = {
            "calibration_id": "test",
            "scanner_model": "MAGNETOM Cima.X",
            "field_strength_t": 3,
            "profile_units": "Hz/A",
            "current_units": "A",
            "settings_mode": "absolute",
            "channels": ["X"],
            "absolute_current_bounds_a": [[-10, 10]],
            "coil_profiles": "coil.nii",
        }
        path = self.root / "cal.json"
        path.write_text(json.dumps(config))
        settings = _settings(
            {"shimcalibration": str(path), "shimcurrenta": "[0.4]"}, None
        )
        saved = self.save(context=source_context(settings))
        (self.root / "coil.nii").unlink()
        path.unlink()
        target = assemble_target(self.images)
        result = evaluate_target(read_map(saved.id), target)
        self.assertIsNotNone(result.shim.predicted_field_hz)
        target = assemble_target(target_images()[0])
        result = evaluate_target(saved, target)
        self.assertIn("unsupported calibration geometry", result.comment)
        self.assertIsNone(result.shim.predicted_field_hz)
        for partial in (
            {"shimcalibration": "missing"},
            {"shimnativebaseline": "1,2,3"},
        ):
            with self.assertRaises(ValueError):
                source_context(_settings(partial, None))


if __name__ == "__main__":
    result = unittest.main(exit=False)
    if not result.result.wasSuccessful():
        raise SystemExit(1)
    print("Shared B0 target shimming tests passed")
