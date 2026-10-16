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


class CoarseFlames(FakeFlames):
    """Writes its outputs on a grid twice as coarse in-plane, as FLAMeS does for fine FLAIR."""

    def __call__(self, input_path, output_dir, model, skull_stripped):
        source = nib.load(str(input_path))
        volume = np.asarray(source.dataobj)
        nx, ny, nz = volume.shape
        coarse = volume.reshape(nx // 2, 2, ny // 2, 2, nz).mean(axis=(1, 3))
        affine = source.affine @ np.array(
            [[2, 0, 0, 0.5], [0, 2, 0, 0.5], [0, 0, 1, 0], [0, 0, 0, 1]], dtype=float
        )
        output_dir.mkdir()
        mask_path = output_dir / "flair_lesions.nii"
        probability_path = output_dir / "flair_lesion_probability.nii"
        nib.save(nib.Nifti1Image((coarse > 800).astype(np.uint8), affine), mask_path)
        nib.save(nib.Nifti1Image((coarse / 1000).astype(np.float32), affine), probability_path)
        return wml.FlamesResult(
            mask_path, probability_path, output_dir / "flair_lesions.tsv", 1, 0.5, {}
        )


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
        self.assertIn("12 lesions, 1.21 ml", connection.logs[0][1])
        series = connection.series()
        self.assertEqual(sorted(series), [4, 5, 6, 7, 8])
        descriptions = {
            index: ismrmrd.Meta.deserialize(images[0].attribute_string)["SeriesDescription"]
            for index, images in series.items()
        }
        self.assertEqual(descriptions, {
            4: "t2_flair_tra_original",
            5: "t2_flair_tra_lesion_overlay",
            6: "t2_flair_tra_lesions",
            7: "t2_flair_tra_lesion_probability",
            8: "t2_flair_tra_lesion_metrics",
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

    def test_outputs_on_a_coarser_flames_grid_return_on_the_source_slices(self):
        volume = np.full((20, 16, 6), 100, dtype=np.int16)
        volume[4:10, 6:12, 2:5] = 950
        sources = nifti_to_mrd_images(volume, oblique_affine())
        connection = RecordingConnection(sources)
        config = {"parameters": {"wmlprobability": True, "sendoriginal": False}}
        with mock.patch.object(wml, "run_flames", CoarseFlames()):
            wml.process(connection, config, None)

        self.assertEqual(connection.logs[-1][0], constants.MRD_LOGGING_INFO, connection.logs)
        series = connection.series()
        self.assertEqual([len(series[index]) for index in (5, 6)], [len(sources)] * 2)
        source_at = images_by_position(sources)
        for mask, probability in zip(series[5], series[6]):
            source = source_at[tuple(np.round(mask.getHead().position, 3))]
            source_plane = np.squeeze(source.data)
            np.testing.assert_array_equal(np.squeeze(mask.data), source_plane > 800)
            if source.getHead().slice == 3:
                # MRD planes are (y, x). Linear interpolation from the 2x grid:
                # interior and background keep their values, and the lesion corner
                # (x=4, y=6) sits 3/4 inside along both axes:
                # 0.5625 * 950 + 0.4375 * 100 = 578.
                plane = np.squeeze(probability.data)
                self.assertEqual((plane[8, 6], plane[0, 0], plane[6, 4]), (950, 100, 578))

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

    def test_resampled_lesion_summary_describes_the_returned_mask(self):
        volume = np.full((20, 16, 6), 100, dtype=np.int16)
        volume[4:10, 6:12, 2:5] = 950
        connection = RecordingConnection(nifti_to_mrd_images(volume, oblique_affine()))
        with mock.patch.object(wml, "run_flames", CoarseFlames()):
            wml.process(connection, {}, None)
        # CoarseFlames reports 1 lesion of 0.5 ml on its own grid; the scanner gets
        # the 6 x 6 x 3 block on the source grid.
        voxel_ml = abs(np.linalg.det(oblique_affine()[:3, :3])) / 1000
        expected = f"1 lesions, {108 * voxel_ml:.2f} ml"
        self.assertNotEqual(expected, "1 lesions, 0.50 ml")
        self.assertIn(expected, connection.logs[-1][1])
        comment = ismrmrd.Meta.deserialize(connection.series()[6][0].attribute_string)
        self.assertIn(expected, comment["ImageComment"])

    def test_default_run_omits_the_probability_map(self):
        connection = RecordingConnection(nifti_to_mrd_images(phantom(), oblique_affine()))
        with mock.patch.object(wml, "run_flames", FakeFlames()):
            wml.process(connection, {}, None)
        self.assertEqual(sorted(connection.series()), [4, 5, 6, 7])
        meta = ismrmrd.Meta.deserialize(connection.series()[7][0].attribute_string)
        self.assertEqual(meta["SeriesDescription"], "t2_flair_tra_lesion_metrics")

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
        self.assertEqual(sorted(connection.series()), [4, 5, 6, 7])

    def test_metrics_measure_26_connected_lesions_in_ras_coordinates(self):
        mask = np.zeros((8, 8, 4), dtype=bool)
        mask[1, 2, 0] = mask[2, 3, 1] = True
        mask[5:7, 5:7, 2:4] = True
        affine = np.diag([2.0, 3.0, 4.0, 1.0])
        affine[:3, 3] = (10, -20, 30)
        rows = wml.lesion_metrics(mask, affine)
        self.assertEqual([row.lesion for row in rows], [1, 2])
        self.assertEqual([row.voxels for row in rows], [2, 8])
        np.testing.assert_allclose([row.volume_ml for row in rows], [0.048, 0.192])
        np.testing.assert_allclose(
            [row.centroid_ras_mm for row in rows], [(13, -12.5, 32), (21, -3.5, 40)]
        )
        oblique_rows = wml.lesion_metrics(mask, oblique_affine())
        np.testing.assert_allclose(
            oblique_rows[0].centroid_ras_mm,
            oblique_affine()[:3, :3] @ [1.5, 2.5, 0.5] + oblique_affine()[:3, 3],
        )

    def test_metrics_report_uses_explicit_page_geometry_and_fresh_identity(self):
        source = nifti_to_mrd_images(phantom(), oblique_affine())[3]
        meta = ismrmrd.Meta.deserialize(source.attribute_string)
        meta.update({
            "IceMiniHead": "source scanner geometry",
            "DicomEngineDimString": "source dimensions",
            "MFInstanceNumber": "4",
            "ReferencedImageSequenceSOPInstanceUID": "1.2.3.4",
            "ReferencedGSPSInstanceUID": "1.2.3.5",
            "SeriesInstanceUID": "1.2.3.6",
            "StudyInstanceUID": "1.2.3.7",
            "ImageRowDir": ["0", "1", "0"],
            "ImageColumnDir": ["1", "0", "0"],
            "slice_count": "6",
            "NumberInSeries": "4",
        })
        source.attribute_string = meta.serialize()
        rows = [wml.LesionMetric(1, 10, 0.01, (10, 20, 30))]
        pages = [np.full((768, 768), value, dtype=np.uint16) for value in (123, 456)]
        with mock.patch.object(wml, "render_metrics_report_pages", return_value=pages):
            image, = wml.metrics_report_series(rows, source, 8, "1 lesions, 0.01 ml")
        header = image.getHead()
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        self.assertEqual(list(header.matrix_size), [768, 768, 2])
        self.assertEqual(list(header.field_of_view), [768, 768, 2])
        self.assertEqual(list(header.position), [0, 0, 0])
        self.assertEqual(list(header.read_dir), [1, 0, 0])
        self.assertEqual(list(header.phase_dir), [0, 1, 0])
        self.assertEqual(list(header.slice_dir), [0, 0, 1])
        self.assertEqual((header.image_series_index, header.image_index, header.slice), (8, 1, 0))
        np.testing.assert_array_equal(image.data[0], np.stack(pages))
        self.assertEqual(meta["SeriesDescription"], "t2_flair_tra_lesion_metrics")
        self.assertEqual(meta["DataRole"], "Segmentation")
        self.assertEqual(meta["Keep_image_geometry"], "0")
        self.assertEqual(meta["ImageRowDir"], [f"{v:.18f}" for v in (1, 0, 0)])
        self.assertEqual(meta["ImageColumnDir"], [f"{v:.18f}" for v in (0, 1, 0)])
        self.assertEqual(meta["ImageSliceNormDir"], [f"{v:.18f}" for v in (0, 0, 1)])
        self.assertEqual(meta["SlicePosLightMarker"], [f"{v:.18f}" for v in (0, 0, 0)])
        for key in ("slice_count", "NumberOfSlices", "ImagesInAcquisition"):
            self.assertEqual(meta[key], "2")
        self.assertEqual(meta["LesionMetricsRows"], "1")
        self.assertEqual(meta["NumberInSeries"], "1")
        self.assertEqual((meta["WindowCenter"], meta["WindowWidth"]), ("2040", "4080"))
        self.assertNotEqual(meta["SeriesInstanceUID"], "1.2.3.6")
        self.assertNotEqual(meta["SOPInstanceUID"], "1.2.3.3")
        self.assertEqual(meta["StudyInstanceUID"], "1.2.3.7")
        for key in (
            "IceMiniHead", "DicomEngineDimString", "MFInstanceNumber",
            "ReferencedImageSequenceSOPInstanceUID", "ReferencedGSPSInstanceUID",
            "RescaleSlope", "RescaleIntercept",
        ):
            self.assertNotIn(key, meta)
        self.assertEqual(meta["ImageComments"], "1 lesions, 0.01 ml")

    def test_metrics_renderer_paginates_without_losing_rows(self):
        rows = [wml.LesionMetric(i, 10, 0.012345, (-12.5, 23.5, 34.5)) for i in range(1, 81)]
        text = []
        original_text = wml.ImageDraw.ImageDraw.text

        def record_text(draw, xy, value, *args, **kwargs):
            text.append((xy, value))
            return original_text(draw, xy, value, *args, **kwargs)

        with mock.patch.object(wml.ImageDraw.ImageDraw, "text", record_text):
            pages = wml.render_metrics_report_pages(rows)
        self.assertEqual(len(pages), 4)
        self.assertEqual([value for (x, y), value in text if x == 32 and y > 228],
                         [str(i) for i in range(1, 81)])
        values = [value for _, value in text]
        self.assertEqual(values.count("0.012345"), 80)
        self.assertEqual(values.count("-12.50"), 80)
        self.assertIn("Page 4/4    Lesions 79-80 of 80", values)
        for page in pages:
            self.assertEqual(page.shape, (1024, 1024))
            self.assertEqual(page.dtype, np.dtype(np.uint16))
            self.assertGreater(np.count_nonzero(page), 1000)
            upright = np.rot90(page, 2)
            self.assertGreater(np.count_nonzero(upright[32:60, 32:700]), 100)

    def test_empty_mask_returns_a_zero_lesion_metrics_page(self):
        volume = np.zeros((20, 16, 6), dtype=np.int16)
        connection = RecordingConnection(nifti_to_mrd_images(volume, oblique_affine()))
        with mock.patch.object(wml, "run_flames", FakeFlames()):
            wml.process(connection, {"parameters": {"sendoriginal": False}}, None)
        self.assertEqual(sorted(connection.series()), [4, 5, 6])
        report, = connection.series()[6]
        meta = ismrmrd.Meta.deserialize(report.attribute_string)
        self.assertEqual(meta["LesionMetricsRows"], "0")
        self.assertEqual(meta["slice_count"], "1")
        self.assertIn("0 lesions, 0.00 ml", meta["ImageComments"])
        self.assertEqual(report.data.shape, (1, 1, 1024, 1024))
        self.assertGreater(np.count_nonzero(report.data), 1000)

    def test_multiple_source_series_have_separate_metrics_series(self):
        sources = nifti_to_mrd_images(phantom(), oblique_affine(), series_index=3)
        sources += nifti_to_mrd_images(phantom(), oblique_affine(), series_index=7)
        connection = RecordingConnection(sources)
        with mock.patch.object(wml, "run_flames", FakeFlames()):
            wml.process(connection, {}, None)
        series = connection.series()
        self.assertEqual(sorted(series), list(range(8, 16)))
        reports = [series[12][0], series[15][0]]
        metas = [ismrmrd.Meta.deserialize(image.attribute_string) for image in reports]
        self.assertEqual([meta["LesionMetricsRows"] for meta in metas], ["12", "12"])
        self.assertNotEqual(metas[0]["SeriesInstanceUID"], metas[1]["SeriesInstanceUID"])

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
            # FLAMeS writes a FLAIR finer than its grid on the resampled grid.
            expected = wml.on_source_grid(
                reference_mask, flair.affine, flair.shape, order=0
            ) > 0

        connection = RecordingConnection(nifti_to_mrd_images(volume, flair.affine))
        wml.process(connection, {"parameters": {"sendoriginal": False}}, None)
        self.assertEqual(connection.logs[-1][0], constants.MRD_LOGGING_INFO, connection.logs)
        _, mask_xyz, affine = wml.mrd_series_to_volume(connection.series()[5])
        np.testing.assert_allclose(affine, flair.affine, atol=1e-3)
        agreement = dice(mask_xyz > 0, expected)
        print(f"scanner path against direct flames: Dice {agreement:.5f}, "
              f"{int(expected.sum())} reference voxels")
        self.assertGreaterEqual(agreement, 0.998)
        report, = connection.series()[6]
        report_meta = ismrmrd.Meta.deserialize(report.attribute_string)
        metrics = wml.lesion_metrics(mask_xyz > 0, affine)
        self.assertEqual(report_meta["LesionMetricsRows"], str(len(metrics)))
        self.assertIn(
            f"{len(metrics)} lesions, {sum(row.volume_ml for row in metrics):.2f} ml",
            report_meta["ImageComments"],
        )
        self.assertGreater(np.count_nonzero(report.data), 1000)


if __name__ == "__main__":
    unittest.main()
