"""Exercise OpenRecon on a single Enhanced MR volume and export research QC.

Run inside the container with PYTHONPATH=/opt/code/python-ismrmrd-server.
This is an offline MRD adapter check, not a scanner/network certification.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
from pathlib import Path

import ismrmrd
import nibabel as nib
import numpy as np
import pydicom
from pydicom.dataset import Dataset
from pydicom.sequence import Sequence
from pydicom.uid import EnhancedMRImageStorage, ExplicitVRLittleEndian, generate_uid

import topofit
from test_topofit_openrecon import RecordingConnection


def verify_dicom(input_path: Path, output_dir: Path) -> None:
    source = pydicom.dcmread(input_path)
    if source.SOPClassUID != EnhancedMRImageStorage:
        raise ValueError("This verification harness expects a single Enhanced MR volume")
    pixels = source.pixel_array
    if pixels.ndim != 3:
        raise ValueError("Expected frames, rows, columns")
    shared = source.SharedFunctionalGroupsSequence[0]
    images = []
    for index, frame in enumerate(source.PerFrameFunctionalGroupsSequence):
        def group(name):
            return getattr(frame if hasattr(frame, name) else shared, name)[0]

        orientation = np.asarray(group("PlaneOrientationSequence").ImageOrientationPatient, float)
        spacing = np.asarray(group("PixelMeasuresSequence").PixelSpacing, float)
        origin = np.asarray(group("PlanePositionSequence").ImagePositionPatient, float)
        read, phase = orientation[:3], orientation[3:]
        center = origin + read * spacing[1] * (source.Columns - 1) / 2 + phase * spacing[0] * (source.Rows - 1) / 2
        image = ismrmrd.Image.from_array(np.ascontiguousarray(pixels[index]), transpose=False)
        image.field_of_view = (source.Columns * spacing[1], source.Rows * spacing[0], float(group("PixelMeasuresSequence").SliceThickness))
        image.position = tuple(center)
        image.read_dir, image.phase_dir = tuple(read), tuple(phase)
        image.slice_dir = tuple(np.cross(read, phase))
        image.slice = index
        image.image_index = index + 1
        image.image_series_index = 1
        image.image_type = ismrmrd.IMTYPE_MAGNITUDE
        meta = ismrmrd.Meta()
        for name in ("SeriesDescription", "ProtocolName", "FrameOfReferenceUID"):
            meta[name] = str(getattr(source, name, ""))
        image.attribute_string = meta.serialize()
        images.append(image)

    output_dir.mkdir(parents=True, exist_ok=True)
    topofit.WORKSPACE = output_dir / "runs"
    connection = RecordingConnection(images)
    topofit.process(connection, {"parameters": {
        "tfdevice": "cpu", "tfflatpatches": True,
    }}, None)
    outputs = [image for batch in connection.image_batches for image in batch]
    assert connection.closed and len(outputs) == 3 * len(images), connection.logs
    patch_images = [image for image in outputs if
        "_topofit_patch_qc" in ismrmrd.Meta.deserialize(image.attribute_string)["SeriesDescription"]]
    assert len(patch_images) == len(images)
    patch_images.sort(key=lambda image: image.slice)
    qc_pixels = np.stack([np.squeeze(image.data) for image in patch_images]).astype("<u2")
    assert np.count_nonzero(qc_pixels == 3000) > 0
    assert np.count_nonzero(qc_pixels == 4095) > 0
    for original, derived in zip(images, patch_images):
        np.testing.assert_allclose(original.position, derived.position)
        np.testing.assert_allclose(original.read_dir, derived.read_dir)
        np.testing.assert_allclose(original.phase_dir, derived.phase_dir)

    derived = copy.deepcopy(source)
    derived.SOPInstanceUID = generate_uid()
    derived.SeriesInstanceUID = generate_uid()
    derived.SeriesNumber = int(source.SeriesNumber) + 1000
    derived.SeriesDescription = "TopoFit multi-patch QC RESEARCH"
    derived.ImageType = ["DERIVED", "PRIMARY", "M", "NONE"]
    derived.BurnedInAnnotation = "YES"
    derived.ContentQualification = "RESEARCH"
    derived.DerivationDescription = "Offline OpenRecon adapter QC; mid-cortical patch and normal. NOT FOR PRESCRIPTION."
    derived.ImageComments = ismrmrd.Meta.deserialize(patch_images[0].attribute_string)["ImageComments"]
    reference = Dataset()
    reference.ReferencedSOPClassUID = source.SOPClassUID
    reference.ReferencedSOPInstanceUID = source.SOPInstanceUID
    derived.SourceImageSequence = Sequence([reference])
    derived.BitsAllocated, derived.BitsStored, derived.HighBit = 16, 12, 11
    derived.PixelRepresentation = 0
    derived.PhotometricInterpretation = "MONOCHROME2"
    for element in derived.iterall():
        if element.keyword in ("WindowCenter", "WindowWidth", "RescaleSlope", "RescaleIntercept"):
            element.value = {"WindowCenter": 2048, "WindowWidth": 4096, "RescaleSlope": 1, "RescaleIntercept": 0}[element.keyword]
        elif element.keyword == "FrameType":
            element.value = ["DERIVED", "PRIMARY", "M", "NONE"]
    derived.WindowCenter, derived.WindowWidth = 2048, 4096
    derived.PixelData = qc_pixels.tobytes()
    derived["PixelData"].is_undefined_length = False
    derived.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    derived.file_meta.MediaStorageSOPInstanceUID = derived.SOPInstanceUID
    output_path = output_dir / "topofit_cortical_patch_qc.dcm"
    derived.save_as(output_path, enforce_file_format=True)
    np.testing.assert_array_equal(pydicom.dcmread(output_path).pixel_array, qc_pixels)
    run_dir, = topofit.WORKSPACE.iterdir()
    manifest = json.loads((run_dir / "topofit_manifest.json").read_text())
    assert manifest["options"]["mock"] is False
    for key, expected in (("patch_count", 3), ("patch_radius_mm", 10),
                          ("patch_max_rms_mm", 0.5), ("patch_min_area_fraction", 0.25)):
        assert manifest["options"][key] == expected
    assert manifest["flat_patch_definition"]["medial_wall_margin_mm"] == 5
    # Prove the exported DICOM contains the same pixels returned by the adapter.
    ordered, volume, affine = topofit._mrd_series_to_nifti(patch_images)
    qc = nib.load(run_dir / "topofit_patch_qc.nii.gz")
    np.testing.assert_array_equal(volume, np.asarray(qc.dataobj))
    np.testing.assert_allclose(affine, qc.affine, atol=1e-5)
    print(json.dumps({"run_dir": str(run_dir), "dicom": str(output_path),
                      "mrd_images_returned": len(outputs), "flat_patches": manifest["flat_patches"]}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    verify_dicom(args.input, args.output_dir)
