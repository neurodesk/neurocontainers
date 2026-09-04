"""OpenRecon transport test using real ISMRMRD image objects and mock surfaces."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import constants
import ismrmrd
import nibabel as nib
import numpy as np

import topofit


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


def _mrd_volume(
    shape=(24, 20, 8),
    *,
    series_description="MPRAGE_TEST",
    protocol_name="MPRAGE_TEST",
    series_index=7,
):
    x_size, y_size, z_size = shape
    grid_y, grid_x = np.indices((y_size, x_size), dtype=np.float32)
    images = []
    for slice_index in range(z_size):
        radius = 5.0 + min(slice_index, z_size - 1 - slice_index)
        plane = np.where(
            ((grid_x - (x_size - 1) / 2) ** 2)
            + ((grid_y - (y_size - 1) / 2) ** 2)
            <= radius**2,
            900,
            20,
        ).astype(np.int16)
        image = ismrmrd.Image.from_array(
            plane[np.newaxis, np.newaxis, :, :], transpose=False
        )
        header = image.getHead()
        for index, value in enumerate((x_size, y_size, 1)):
            header.matrix_size[index] = value
        for index, value in enumerate((x_size, y_size, 1.0)):
            header.field_of_view[index] = float(value)
        for index, value in enumerate((0.0, 0.0, float(slice_index))):
            header.position[index] = value
        for field, values in (
            (header.read_dir, (1.0, 0.0, 0.0)),
            (header.phase_dir, (0.0, 1.0, 0.0)),
            (header.slice_dir, (0.0, 0.0, 1.0)),
        ):
            for index, value in enumerate(values):
                field[index] = value
        header.image_type = ismrmrd.IMTYPE_MAGNITUDE
        header.image_series_index = series_index
        header.image_index = slice_index + 1
        header.slice = slice_index
        image.setHead(header)
        meta = ismrmrd.Meta()
        meta["SeriesDescription"] = series_description
        meta["ProtocolName"] = protocol_name
        meta["FrameOfReferenceUID"] = "1.2.826.0.1.3680043.10.999"
        meta["ImageRowDir"] = ["0.0", "1.0", "0.0"]
        meta["ImageColumnDir"] = ["-1.0", "0.0", "0.0"]
        meta["ImageSliceNormDir"] = ["0.0", "0.0", "1.0"]
        image.attribute_string = meta.serialize()
        images.append(image)
    return images


class TopoFitOpenReconTests(unittest.TestCase):
    def test_mp2rage_processes_only_uni_den_contrast(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            topofit.WORKSPACE = Path(temporary_directory)
            images = (
                _mrd_volume(
                    series_description="T1_MP2RAGE_INV1",
                    protocol_name="T1_MP2RAGE",
                    series_index=5,
                )
                + _mrd_volume(
                    series_description="T1_MP2RAGE_UNI_DEN",
                    protocol_name="T1_MP2RAGE",
                    series_index=6,
                )
                + _mrd_volume(
                    series_description="T1_MP2RAGE_INV2",
                    protocol_name="T1_MP2RAGE",
                    series_index=7,
                )
            )
            connection = RecordingConnection(images)
            config = {
                "parameters": {
                    "sendoriginal": False,
                    "tfdevice": "cpu",
                    "tfmodel": "t1w_1mm",
                    "tfconform": True,
                    "tfdebugmock": True,
                }
            }

            topofit.process(connection, config, metadata=None)

            errors = [
                message
                for level, message in connection.logs
                if level == constants.MRD_LOGGING_ERROR
            ]
            self.assertEqual(errors, [])
            outputs = [image for batch in connection.image_batches for image in batch]
            self.assertEqual(len(outputs), 8)
            self.assertEqual({int(image.image_series_index) for image in outputs}, {8})
            output_meta = ismrmrd.Meta.deserialize(outputs[0].attribute_string)
            self.assertEqual(
                output_meta["SeriesDescription"],
                "T1_MP2RAGE_UNI_DEN_topofit_qc",
            )
            manifests = list(Path(temporary_directory).glob("*/topofit_manifest.json"))
            self.assertEqual(len(manifests), 1)

    def test_mp2rage_without_uni_den_fails_closed(self):
        connection = RecordingConnection(
            _mrd_volume(
                series_description="T1_MP2RAGE_INV2",
                protocol_name="T1_MP2RAGE",
            )
        )

        topofit.process(connection, {}, metadata=None)

        self.assertTrue(connection.closed)
        self.assertEqual(connection.image_batches, [])
        errors = [
            message
            for level, message in connection.logs
            if level == constants.MRD_LOGGING_ERROR
        ]
        self.assertEqual(len(errors), 1)
        self.assertIn("no UNI-DEN contrast", errors[0])

    def test_mock_mrd_series_returns_source_grid_qc_and_manifest(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            topofit.WORKSPACE = Path(temporary_directory)
            connection = RecordingConnection(_mrd_volume())
            config = {
                "parameters": {
                    "sendoriginal": False,
                    "tfdevice": "cpu",
                    "tfmodel": "t1w_1mm",
                    "tfconform": True,
                    "tfdebugmock": True,
                }
            }

            topofit.process(connection, config, metadata=None)

            self.assertTrue(connection.closed)
            errors = [
                message
                for level, message in connection.logs
                if level == constants.MRD_LOGGING_ERROR
            ]
            self.assertEqual(errors, [])
            outputs = [image for batch in connection.image_batches for image in batch]
            self.assertEqual(len(outputs), 8)
            self.assertEqual({int(image.image_series_index) for image in outputs}, {8})
            self.assertTrue(all(image.data.shape == (1, 1, 20, 24) for image in outputs))
            self.assertEqual(max(int(np.max(image.data)) for image in outputs), 4095)

            output_meta = ismrmrd.Meta.deserialize(outputs[0].attribute_string)
            self.assertEqual(
                output_meta["FrameOfReferenceUID"],
                "1.2.826.0.1.3680043.10.999",
            )
            self.assertEqual(
                output_meta["TopoFitStatus"], "SURFACE_READY_RESEARCH_ONLY"
            )
            self.assertEqual(output_meta["TopoFitPrescriptionStatus"], "WITHHELD")

            manifests = list(Path(temporary_directory).glob("*/topofit_manifest.json"))
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertIsNone(manifest["prescription_coordinates"])
            self.assertEqual(len(manifest["surfaces"]), 6)

    def test_mrd_affine_uses_image_center_and_reconstructed_pixel_directions(self):
        images = _mrd_volume()
        _, _, affine = topofit._mrd_series_to_nifti(images)

        np.testing.assert_allclose(affine[:3, 0], (0.0, -1.0, 0.0))
        np.testing.assert_allclose(affine[:3, 1], (1.0, 0.0, 0.0))
        np.testing.assert_allclose(
            nib.affines.apply_affine(affine, (11.5, 9.5, 0.0)),
            (0.0, 0.0, 0.0),
        )

    def test_mrd_series_rejects_duplicate_slice_positions(self):
        images = _mrd_volume()
        for image in images:
            header = image.getHead()
            header.position[:] = (0.0, 0.0, 0.0)
            image.setHead(header)

        with self.assertRaisesRegex(ValueError, "duplicate or unresolved"):
            topofit._mrd_series_to_nifti(images)


if __name__ == "__main__":
    unittest.main()
