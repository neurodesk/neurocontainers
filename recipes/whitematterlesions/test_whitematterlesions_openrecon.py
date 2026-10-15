"""OpenRecon transport tests for the FLAMeS white matter lesion adapter.

Set WHITEMATTERLESIONS_TEST_FLAIR to a FLAIR NIfTI to also run the real
`flames` command through the adapter and compare it with a direct run.
"""

from __future__ import annotations

import os
import random
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import constants
import ismrmrd
import nibabel as nib
import numpy as np
import whitematterlesions as wml

LPS = np.diag([-1.0, -1.0, 1.0])


class RecordingConnection:
    def __init__(self, items):
        self.items = list(items) + [None]
        self.image_batches = []
        self.logs = []
        self.closed = False

    def __iter__(self):
        return iter(self.items)

    def send_image(self, images):
        self.image_batches.append(list(images))

    def send_logging(self, level, message):
        self.logs.append((level, message))

    def send_close(self):
        self.closed = True

    def series(self):
        grouped = {}
        for batch in self.image_batches:
            for image in batch:
                grouped.setdefault(int(image.image_series_index), []).append(image)
        return grouped


def nifti_to_mrd_images(volume_xyz, affine, series_index=3, description="t2_flair_tra"):
    """Slice a NIfTI volume into MRD images the way a scanner sends them."""

    matrix = LPS @ affine[:3, :3]
    spacing = np.linalg.norm(matrix, axis=0)
    directions = matrix / spacing
    nx, ny, nz = volume_xyz.shape
    images = []
    for k in range(nz):
        plane = np.ascontiguousarray(volume_xyz[:, :, k].T)
        image = ismrmrd.Image.from_array(plane[np.newaxis, np.newaxis], transpose=False)
        header = image.getHead()
        center = LPS @ affine[:3, 3] + matrix @ np.array([(nx - 1) / 2, (ny - 1) / 2, k])
        header.matrix_size[:] = (nx, ny, 1)
        header.field_of_view[:] = (nx * spacing[0], ny * spacing[1], spacing[2])
        header.position[:] = center
        header.read_dir[:] = directions[:, 0]
        header.phase_dir[:] = directions[:, 1]
        header.slice_dir[:] = directions[:, 2]
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.image_series_index = series_index
        header.image_index = k + 1
        header.slice = k
        image.setHead(header)
        meta = ismrmrd.Meta()
        meta["SeriesDescription"] = description
        meta["SOPInstanceUID"] = f"1.2.3.{k}"
        meta["WindowCenter"] = "2048"
        meta["WindowWidth"] = "4096"
        meta["RescaleSlope"] = "2"
        meta["RescaleIntercept"] = "10"
        image.attribute_string = meta.serialize()
        images.append(image)
    return images


def oblique_affine():
    rotation, _ = np.linalg.qr(np.array([[1.0, 0.2, 0.1], [-0.15, 1.0, 0.05], [0.1, -0.1, 1.0]]))
    affine = np.eye(4)
    affine[:3, :3] = rotation @ np.diag([1.1, 0.9, 2.5])
    affine[:3, 3] = (-90.0, 110.0, -40.0)
    return affine


def phantom(shape=(20, 16, 6)):
    rng = np.random.default_rng(7)
    volume = rng.integers(0, 1000, size=shape).astype(np.int16)
    volume[4:10, 5:11, 2:5] = 950
    return volume


def images_by_position(images):
    return {tuple(np.round(image.getHead().position, 3)): image for image in images}


def dice(a, b):
    return 2 * np.logical_and(a, b).sum() / (a.sum() + b.sum())


class FakeFlames:
    """Stands in for the `flames` command: lesions are voxels brighter than 800."""

    def __call__(self, input_path, output_dir, model, skull_stripped):
        self.call = (model, skull_stripped)
        source = nib.load(str(input_path))
        volume = np.asarray(source.dataobj)
        output_dir.mkdir()
        mask_path = output_dir / "flair_lesions.nii"
        probability_path = output_dir / "flair_lesion_probability.nii"
        nib.save(nib.Nifti1Image((volume > 800).astype(np.uint8), source.affine), mask_path)
        nib.save(nib.Nifti1Image((volume / 1000).astype(np.float32), source.affine), probability_path)
        return wml.FlamesResult(
            mask_path, probability_path, output_dir / "flair_lesions.tsv", 4, 1.25, {}
        )


class DownsamplingFakeFlames(FakeFlames):
    """As `flames` 0.3 on fine input: outputs on a grid twice as coarse in-plane."""

    def __call__(self, input_path, output_dir, model, skull_stripped):
        result = super().__call__(input_path, output_dir, model, skull_stripped)
        source = nib.load(str(input_path))
        lesions = (np.asarray(source.dataobj) > 800).astype(np.float32)
        nx, ny, nz = lesions.shape
        coarse = lesions.reshape(nx // 2, 2, ny // 2, 2, nz).mean(axis=(1, 3))
        affine = source.affine.copy()
        affine[:3, :2] *= 2
        affine[:3, 3] += 0.5 * (source.affine[:3, 0] + source.affine[:3, 1])
        nib.save(nib.Nifti1Image(coarse.astype(np.float32), affine), result.probability)
        nib.save(nib.Nifti1Image((coarse > 0.5).astype(np.uint8), affine), result.mask)
        return result


class WhiteMatterLesionsOpenReconTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory()
        self.addCleanup(workspace.cleanup)
        patcher = mock.patch.object(wml, "WORKSPACE", Path(workspace.name))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_oblique_series_round_trips_to_the_same_volume_and_affine(self):
        volume, affine = phantom(), oblique_affine()
        images = nifti_to_mrd_images(volume, affine)
        random.Random(1).shuffle(images)
        ordered, volume_xyz, recovered = wml.mrd_series_to_volume(images)
        np.testing.assert_array_equal(volume_xyz, volume)
        np.testing.assert_allclose(recovered, affine, atol=1e-4)
        self.assertEqual([image.image_index for image in ordered], list(range(1, 7)))

    def test_left_handed_series_keeps_its_slice_order(self):
        volume, affine = phantom(), oblique_affine()
        affine[:3, 2] *= -1
        images = nifti_to_mrd_images(volume, affine)
        random.Random(3).shuffle(images)
        _, volume_xyz, recovered = wml.mrd_series_to_volume(images)
        np.testing.assert_array_equal(volume_xyz, volume)
        np.testing.assert_allclose(recovered, affine, atol=1e-4)

    def test_lesion_series_return_on_the_source_slices(self):
        volume = phantom()
        sources = nifti_to_mrd_images(volume, oblique_affine())
        shuffled = list(sources)
        random.Random(2).shuffle(shuffled)
        connection = RecordingConnection(shuffled)
        fake = FakeFlames()
        config = {"parameters": {"wmlprobability": True, "wmlmodel": "ensemble"}}
        with mock.patch.object(wml, "run_flames", fake):
            wml.process(connection, config, None)

        self.assertTrue(connection.closed)
        self.assertEqual(fake.call, ("ensemble", False))
        self.assertEqual(
            [level for level, _ in connection.logs], [constants.MRD_LOGGING_INFO]
        )
        self.assertIn("4 lesions, 1.25 ml", connection.logs[0][1])
        series = connection.series()
        self.assertEqual(sorted(series), [4, 5, 6, 7])
        descriptions = {
            index: ismrmrd.Meta.deserialize(images[0].attribute_string)["SeriesDescription"]
            for index, images in series.items()
        }
        self.assertEqual(descriptions, {
            4: "t2_flair_tra_original",
            5: "t2_flair_tra_lesion_overlay",
            6: "t2_flair_tra_lesions",
            7: "t2_flair_tra_lesion_probability",
        })

        source_at = images_by_position(sources)
        outline = wml.lesion_outline(volume > 800)
        peak = volume.max()
        for image in series[4]:
            source = source_at[tuple(np.round(image.getHead().position, 3))]
            np.testing.assert_array_equal(image.data, source.data)
        for overlay, mask, probability in zip(series[5], series[6], series[7]):
            key = tuple(np.round(mask.getHead().position, 3))
            source_plane = np.squeeze(source_at[key].data)
            k = source_at[key].getHead().slice
            np.testing.assert_array_equal(np.squeeze(mask.data), source_plane > 800)
            np.testing.assert_array_equal(
                np.squeeze(probability.data), np.rint(source_plane / 1000 * 1000)
            )
            expected_overlay = np.where(outline[:, :, k].T, peak, source_plane)
            np.testing.assert_array_equal(np.squeeze(overlay.data), expected_overlay)
            meta = ismrmrd.Meta.deserialize(mask.attribute_string)
            self.assertNotIn("SOPInstanceUID", meta.keys())
            self.assertIn(wml.RESEARCH_WARNING, meta["ImageComment"])
            for image, display_max in ((mask, 1), (probability, 1000)):
                meta = ismrmrd.Meta.deserialize(image.attribute_string)
                self.assertEqual(
                    (float(meta["WindowCenter"]), float(meta["WindowWidth"])),
                    ((display_max + 1) / 2, display_max + 1),
                )
                self.assertNotIn("RescaleSlope", meta.keys())
                self.assertNotIn("RescaleIntercept", meta.keys())
            overlay_meta = ismrmrd.Meta.deserialize(overlay.attribute_string)
            self.assertEqual(overlay_meta["RescaleSlope"], "2")
        self.assertTrue(outline.any() and (outline != (volume > 800)).any())

    def test_rounded_positions_of_a_long_fine_series_are_accepted(self):
        # Rounded to 0.001 mm, 0.9995 mm steps alternate between 0.999 and 1.000.
        affine = np.diag([1.0, 1.0, 0.9995, 1.0])
        affine[:3, 3] = (-4.0, -3.0, -60.0)
        volume = np.zeros((8, 6, 256), dtype=np.int16)
        images = nifti_to_mrd_images(volume, affine)
        for image in images:
            header = image.getHead()
            header.position[:] = np.round(np.asarray(header.position), 3)
            image.setHead(header)
        _, _, recovered = wml.mrd_series_to_volume(images)
        np.testing.assert_allclose(recovered, affine, atol=1e-3)

    def test_slices_off_the_series_grid_are_rejected(self):
        def shift(header):
            header.position[:] = np.asarray(header.position) + 10.0 * np.asarray(header.read_dir)

        def tilt(header):
            header.read_dir[:] = (0.0, 1.0, 0.0)
            header.phase_dir[:] = (-1.0, 0.0, 0.0)

        for change in (shift, tilt):
            images = nifti_to_mrd_images(phantom(), oblique_affine())
            header = images[3].getHead()
            change(header)
            images[3].setHead(header)
            with self.subTest(change.__name__):
                with self.assertRaisesRegex(ValueError, "does not lie on the series grid"):
                    wml.mrd_series_to_volume(images)

    def test_outputs_on_a_coarser_flames_grid_return_on_the_source_slices(self):
        volume = np.zeros((20, 16, 6), dtype=np.int16)
        volume[4:10, 6:12, 2:5] = 950
        sources = nifti_to_mrd_images(volume, oblique_affine())
        connection = RecordingConnection(sources)
        config = {"parameters": {"wmlprobability": True}}
        with mock.patch.object(wml, "run_flames", DownsamplingFakeFlames()):
            wml.process(connection, config, None)
        self.assertEqual(connection.logs[-1][0], constants.MRD_LOGGING_INFO)
        series = connection.series()
        self.assertEqual(sorted(series), [4, 5, 6, 7])
        source_at = images_by_position(sources)
        mask = np.zeros(volume.shape, dtype=bool)
        for image in series[6]:
            source = source_at[tuple(np.round(image.getHead().position, 3))]
            self.assertEqual(np.squeeze(image.data).shape, np.squeeze(source.data).shape)
            mask[:, :, source.getHead().slice] = np.squeeze(image.data).T > 0
        self.assertGreater(dice(mask, volume > 800), 0.8)
        # The summary describes the returned mask, not the coarse one flames counted.
        voxel_ml = abs(np.linalg.det(oblique_affine()[:3, :3])) / 1000
        self.assertIn(f"1 lesions, {mask.sum() * voxel_ml:.2f} ml", connection.logs[-1][1])

    def test_lesions_on_the_edge_of_a_coarser_grid_are_kept(self):
        volume = np.zeros((20, 16, 6), dtype=np.int16)
        volume[0:4, 0:4, :] = 950
        connection = RecordingConnection(nifti_to_mrd_images(volume, oblique_affine()))
        with mock.patch.object(wml, "run_flames", DownsamplingFakeFlames()):
            wml.process(connection, {}, None)
        mask = np.zeros(volume.shape, dtype=bool)
        for image in connection.series()[6]:
            mask[:, :, image.getHead().slice] = np.squeeze(image.data).T > 0
        self.assertTrue(mask[0, 0].all(), "edge voxels were zeroed")

    def test_default_run_omits_the_probability_map(self):
        connection = RecordingConnection(nifti_to_mrd_images(phantom(), oblique_affine()))
        with mock.patch.object(wml, "run_flames", FakeFlames()):
            wml.process(connection, {}, None)
        self.assertEqual(sorted(connection.series()), [4, 5, 6])

    def test_flames_failure_is_reported_to_the_scanner(self):
        with tempfile.TemporaryDirectory() as directory:
            failing = Path(directory) / "flames"
            failing.write_text("#!/bin/sh\necho 'SynthStrip found no brain' >&2\nexit 1\n")
            failing.chmod(0o755)
            connection = RecordingConnection(nifti_to_mrd_images(phantom(), oblique_affine()))
            with mock.patch.object(wml, "FLAMES", str(failing)):
                wml.process(connection, {}, None)
        self.assertTrue(connection.closed)
        self.assertEqual(connection.logs[-1][0], constants.MRD_LOGGING_ERROR)
        self.assertIn("SynthStrip found no brain", connection.logs[-1][1])
        # The originals were sent before FLAMeS ran, so the scanner still gets them.
        series = connection.series()
        self.assertEqual(sorted(series), [4])
        meta = ismrmrd.Meta.deserialize(series[4][0].attribute_string)
        self.assertEqual(meta["SeriesDescription"], "t2_flair_tra_original")

    def test_originals_are_sent_before_flames_runs(self):
        connection = RecordingConnection(nifti_to_mrd_images(phantom(), oblique_affine()))
        fake = FakeFlames()

        def flames(*args):
            self.assertEqual(sorted(connection.series()), [4])
            return fake(*args)

        with mock.patch.object(wml, "run_flames", flames):
            wml.process(connection, {}, None)
        self.assertEqual(sorted(connection.series()), [4, 5, 6])

    def test_without_sendoriginal_a_failure_returns_nothing(self):
        def flames(*args):
            raise RuntimeError("FLAMeS failed")

        connection = RecordingConnection(nifti_to_mrd_images(phantom(), oblique_affine()))
        with mock.patch.object(wml, "run_flames", flames):
            wml.process(connection, {"parameters": {"sendoriginal": False}}, None)
        self.assertEqual(connection.image_batches, [])
        self.assertIn("FLAMeS failed", connection.logs[-1][1])

    @unittest.skipUnless(
        os.environ.get("WHITEMATTERLESIONS_TEST_FLAIR"), "no FLAIR NIfTI supplied"
    )
    def test_scanner_path_matches_a_direct_flames_run(self):
        flair = nib.load(os.environ["WHITEMATTERLESIONS_TEST_FLAIR"])
        volume = np.asarray(flair.dataobj, dtype=np.float32)
        with tempfile.TemporaryDirectory() as directory:
            reference = Path(directory) / "reference"
            subprocess.run([wml.FLAMES, flair.get_filename(), str(reference)], check=True)
            reference_mask = next(reference.glob("*_lesions.nii"))
            expected = np.asarray(nib.load(str(reference_mask)).dataobj) > 0

        connection = RecordingConnection(nifti_to_mrd_images(volume, flair.affine))
        wml.process(connection, {"parameters": {"sendoriginal": False}}, None)
        self.assertEqual(connection.logs[-1][0], constants.MRD_LOGGING_INFO, connection.logs)
        _, mask_xyz, affine = wml.mrd_series_to_volume(connection.series()[5])
        np.testing.assert_allclose(affine, flair.affine, atol=1e-3)
        agreement = dice(mask_xyz > 0, expected)
        print(f"scanner path against direct flames: Dice {agreement:.5f}, "
              f"{int(expected.sum())} reference voxels")
        self.assertGreaterEqual(agreement, 0.998)


if __name__ == "__main__":
    unittest.main()
