"""Exercise actual ROMEO, DICOM conversion and MRD outputs using synthetic data."""

from __future__ import annotations

import copy
import tempfile
from pathlib import Path

import ismrmrd
import numpy as np
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid
from scipy import ndimage

import b0mapromeo as app


def item(**attributes):
    dataset = Dataset()
    for name, value in attributes.items():
        setattr(dataset, name, value)
    return dataset


def synthetic_dicoms(directory: Path, enhanced: bool = True):
    directory.mkdir(parents=True)
    shape = (20, 22, 18)
    x, y, z = np.indices(shape)
    object_mask = ((x - 9.5) ** 2 + (y - 10.5) ** 2 + (z - 8.5) ** 2) < 7.5**2
    magnitude = np.where(object_mask, 1000, 0).astype(np.uint16)
    field = (35.0 + 9.0 * (x - 9.5)).astype(np.float32)
    times = [2.46, 4.92, 7.38]
    ids = {kind: generate_uid() for kind in ("M", "P")}
    for kind in ("M", "P"):
        for echo, te in enumerate(times):
            phase = (2 * np.pi * field * te / 1000 + 0.4 + np.pi) % (2 * np.pi) - np.pi
            volume = (
                magnitude
                if kind == "M"
                else np.rint((phase / np.pi + 1) * 2048).clip(0, 4095).astype(np.uint16)
            )
            order = list(reversed(range(shape[2])))
            for file_index, indices in enumerate(
                [order] if enhanced else [[i] for i in order]
            ):
                meta = FileMetaDataset()
                meta.TransferSyntaxUID = ExplicitVRLittleEndian
                meta.MediaStorageSOPClassUID = (
                    "1.2.840.10008.5.1.4.1.1.4.1"
                    if enhanced
                    else "1.2.840.10008.5.1.4.1.1.4"
                )
                meta.MediaStorageSOPInstanceUID = generate_uid()
                dataset = FileDataset(None, {}, file_meta=meta, preamble=b"\0" * 128)
                dataset.SOPClassUID = meta.MediaStorageSOPClassUID
                dataset.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
                dataset.SeriesInstanceUID = ids[kind]
                dataset.ImageType = ["ORIGINAL", "PRIMARY", kind, "NONE"]
                dataset.Rows, dataset.Columns = shape[1], shape[0]
                dataset.SamplesPerPixel = 1
                dataset.PhotometricInterpretation = "MONOCHROME2"
                dataset.BitsAllocated, dataset.BitsStored, dataset.HighBit = 16, 12, 11
                dataset.PixelRepresentation = 0
                slope, intercept = (2, -4096) if kind == "P" else (1, 0)
                # A rotated, anisotropic acquisition tests physical geometry.
                orientation = [0, 1, 0, -1, 0, 0]
                if enhanced:
                    dataset.NumberOfFrames = len(indices)
                    dataset.SharedFunctionalGroupsSequence = [
                        item(
                            PixelMeasuresSequence=[
                                item(PixelSpacing=[1.5, 1.0], SliceThickness=2.0)
                            ],
                            PlaneOrientationSequence=[
                                item(ImageOrientationPatient=orientation)
                            ],
                            PixelValueTransformationSequence=[
                                item(
                                    RescaleSlope=slope,
                                    RescaleIntercept=intercept,
                                    RescaleType="US",
                                )
                            ],
                        )
                    ]
                    dataset.PerFrameFunctionalGroupsSequence = [
                        item(
                            MREchoSequence=[item(EffectiveEchoTime=te)],
                            MRImageFrameTypeSequence=[
                                item(FrameType=dataset.ImageType)
                            ],
                            PlanePositionSequence=[
                                item(ImagePositionPatient=[10, 20, i * 2.0])
                            ],
                        )
                        for i in indices
                    ]
                else:
                    dataset.PixelSpacing = [1.5, 1.0]
                    dataset.SliceThickness = 2.0
                    dataset.ImageOrientationPatient = orientation
                    dataset.ImagePositionPatient = [10, 20, indices[0] * 2.0]
                    dataset.EchoTime = te
                    dataset.RescaleSlope, dataset.RescaleIntercept = slope, intercept
                data = np.stack([volume[:, :, i].T for i in indices])
                dataset.PixelData = data.astype("<u2").tobytes()
                dataset.save_as(
                    directory / f"{kind}-{echo}-{file_index}.dcm",
                    enforce_file_format=True,
                )
    return field, times


class Connection:
    def __init__(self, images):
        self.images = images
        self.sent = []
        self.logs = []
        self.closed = False

    def __iter__(self):
        return iter(self.images)

    def send_image(self, images):
        self.sent.extend(images)

    def send_logging(self, level, message):
        self.logs.append((level, message))

    def send_close(self):
        self.closed = True


def rejects(action):
    try:
        action()
    except ValueError:
        return
    raise AssertionError("Invalid acquisition was accepted")


def main():
    with tempfile.TemporaryDirectory(prefix="b0-synthetic-") as temporary:
        root = Path(temporary)
        expected, times = synthetic_dicoms(root / "enhanced")
        images, read_times = app.read_dicoms(root / "enhanced")
        mag, phase, affine, read_times, anchors = app.assemble(
            images, read_times, "radians"
        )
        assert read_times == times
        assert mag.shape == (*expected.shape, 3)
        np.testing.assert_allclose(affine[:3, :3], [[0, 1.5, 0], [-1, 0, 0], [0, 0, 2]])
        np.testing.assert_allclose(affine[:3, 3], [-10, -20, 0])
        synthetic_dicoms(root / "classic", enhanced=False)
        classic, classic_times = app.read_dicoms(root / "classic")
        classic_mag, classic_phase, classic_affine, _, _ = app.assemble(
            classic, classic_times, "radians"
        )
        np.testing.assert_array_equal(classic_mag, mag)
        np.testing.assert_array_equal(classic_phase, phase)
        np.testing.assert_array_equal(classic_affine, affine)

        rejects(lambda: app.assemble(images[:-1], times, "radians"))
        rejects(lambda: app.assemble(images + [images[0]], times, "radians"))
        rejects(lambda: app.assemble(images, [], "radians"))
        rejects(lambda: app.assemble(images, [2, 2, 7], "radians"))
        rejects(lambda: app.foreground_mask(np.zeros((20, 20, 20))))
        rejects(lambda: app.phase_radians(np.array([4096]), "siemens"))
        np.testing.assert_allclose(
            app.phase_radians(np.array([0, 2048]), "siemens"), [-np.pi, 0]
        )

        # Assemble a packed volume as well as the source slice stream.
        packed = []
        for kind in (ismrmrd.IMTYPE_MAGNITUDE, ismrmrd.IMTYPE_PHASE):
            for echo in range(3):
                source = next(
                    im for im in images if im.image_type == kind and im.contrast == echo
                )
                volume = (mag if kind == ismrmrd.IMTYPE_MAGNITUDE else phase)[..., echo]
                image = ismrmrd.Image.from_array(
                    volume.transpose(2, 1, 0)[None].copy(), transpose=False
                )
                header = copy.deepcopy(source.getHead())
                header.data_type = image.data_type
                header.matrix_size[2] = expected.shape[2]
                header.field_of_view[2] = expected.shape[2] * 2
                header.position[:] = [
                    -(expected.shape[1] - 1) * 1.5 / 2 + 10,
                    (expected.shape[0] - 1) / 2 + 20,
                    expected.shape[2] - 1,
                ]
                image.setHead(header)
                image.attribute_string = source.attribute_string
                packed.append(image)
        packed_mag, packed_phase, packed_affine, _, _ = app.assemble(
            packed, times, "radians"
        )
        np.testing.assert_array_equal(packed_mag, mag)
        np.testing.assert_array_equal(packed_phase, phase)
        np.testing.assert_array_equal(packed_affine, affine)

        # Use real ROMEO, not a replacement executable, through process().
        # Exercise scanner label values arriving in MRD userParameters.
        metadata = ismrmrd.xsd.ismrmrdHeader(
            sequenceParameters=ismrmrd.xsd.sequenceParametersType(TE=times),
            userParameters=ismrmrd.xsd.userParametersType(
                userParameterString=[
                    ismrmrd.xsd.userParameterStringType(
                        name="phaseunits", value="radians"
                    )
                ]
            ),
        )
        scanner_images = copy.deepcopy(images)
        for image in scanner_images:
            if image.image_type == ismrmrd.IMTYPE_PHASE:
                image.data[:] = np.rint((image.data / np.pi + 1) * 2048)
        connection = Connection(scanner_images)
        app.WORK_ROOT = root / "work"
        app.process(
            connection,
            {"parameters": {"phaseunits": "siemens", "sendoriginal": True}},
            ismrmrd.xsd.ToXML(metadata),
        )
        assert connection.closed and not connection.logs
        derived = [
            im
            for im in connection.sent
            if "B0MapUnits" in ismrmrd.Meta.deserialize(im.attribute_string)
        ]
        originals = [im for im in connection.sent if im not in derived]
        assert len(derived) == expected.shape[2]
        assert len(originals) == len(scanner_images)
        assert connection.sent[-len(derived) :] == derived
        # Pass-through may regroup echoes, but must preserve every input plane.
        assert sorted(im.data.tobytes() for im in originals) == sorted(
            im.data.tobytes() for im in scanner_images
        )
        recovered = []
        uids = set()
        for z, image in enumerate(derived):
            meta = ismrmrd.Meta.deserialize(image.attribute_string)
            assert image.data.dtype == np.uint16
            assert image.image_index == z + 1 and image.slice == z
            assert meta["RescaleType"] == "Hz" and meta["Keep_image_geometry"] == "1"
            assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\B0MAP_ROMEO"
            uids.add(meta["SOPInstanceUID"])
            np.testing.assert_allclose(image.position, anchors[z].position)
            recovered.append(
                image.data[0, 0].T * float(meta["RescaleSlope"])
                + float(meta["RescaleIntercept"])
            )
        assert len(uids) == len(derived)
        recovered = np.stack(recovered, axis=2)
        mask = app.foreground_mask(mag[..., 0])
        # ROMEO's default phase-offset estimation has edge error on this small
        # anisotropic object. Check the interior against quantization precision
        # and bound the error over the entire mask separately.
        interior = ndimage.binary_erosion(mask, iterations=2)
        assert interior.sum() > 100
        np.testing.assert_allclose(recovered[interior], expected[interior], atol=0.7)
        error = np.abs(recovered[mask] - expected[mask])
        assert error.mean() < 0.7 and error.max() < 5.0
        assert np.all(recovered[~mask] == 0)
        assert not list(app.WORK_ROOT.iterdir())

        # Wider fields must retain sign/range with a reversible display rescale.
        wide = expected * 100
        encoded = app.output_images(wide, anchors, 181)
        meta = ismrmrd.Meta.deserialize(encoded[0].attribute_string)
        decoded = encoded[0].data[0, 0].T * float(meta["RescaleSlope"]) + float(
            meta["RescaleIntercept"]
        )
        np.testing.assert_allclose(
            decoded, wide[:, :, 0], atol=0.51 * float(meta["RescaleSlope"])
        )

        failed = Connection(images)
        app.process(failed, {}, None)
        assert failed.closed and failed.logs and not failed.sent
        print("Synthetic DICOM/MRD ROMEO B0 smoke test passed")


if __name__ == "__main__":
    main()
