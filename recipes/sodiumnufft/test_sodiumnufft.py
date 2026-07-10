import importlib
from pathlib import Path
import sys
import types

import ismrmrd
import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent


class ReferenceHead:
    read_dir = (1.0, 0.0, 0.0)
    phase_dir = (0.0, 1.0, 0.0)
    slice_dir = (0.0, 0.0, 1.0)
    position = (10.0, 20.0, 30.0)


class MeasurementInformation:
    protocolName = "tpiTqf_23Na_n28_TE05_FIRE"


class MatrixSize:
    x = 2
    y = 3
    z = 2


class FieldOfViewMm:
    x = 20.0
    y = 30.0
    z = 40.0


class ReconSpace:
    matrixSize = MatrixSize()
    fieldOfView_mm = FieldOfViewMm()


class EncodedSpace:
    matrixSize = MatrixSize()
    fieldOfView_mm = FieldOfViewMm()


class Encoding:
    reconSpace = ReconSpace()
    encodedSpace = EncodedSpace()


class Metadata:
    measurementInformation = MeasurementInformation()
    encoding = [Encoding()]


def _import_sodiumnufft_with_runtime_stubs(monkeypatch):
    constants = types.ModuleType("constants")
    mrdhelper = types.ModuleType("mrdhelper")
    sigpy = types.ModuleType("sigpy")

    def update_img_header_from_raw(image_header, reference_head):
        del reference_head
        return image_header

    mrdhelper.update_img_header_from_raw = update_img_header_from_raw

    monkeypatch.syspath_prepend(str(RECIPE_DIR))
    monkeypatch.setitem(sys.modules, "constants", constants)
    monkeypatch.setitem(sys.modules, "mrdhelper", mrdhelper)
    monkeypatch.setitem(sys.modules, "sigpy", sigpy)
    monkeypatch.delitem(sys.modules, "sodiumnufft", raising=False)
    return importlib.import_module("sodiumnufft")


def test_build_output_images_preserves_reconstructed_slice_count(monkeypatch):
    sodiumnufft = _import_sodiumnufft_with_runtime_stubs(monkeypatch)
    volume = np.ones((2, 3, 4), dtype=np.float32)

    images = sodiumnufft._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    assert len(images) == 4

    first = images[0]
    first_head = first.getHead()
    assert first.data.shape == (1, 1, 3, 2)
    assert first.data.dtype == np.uint16
    assert int(first.data.min()) == 0
    assert int(first.data.max()) == 0
    assert [int(value) for value in first_head.matrix_size] == [2, 3, 1]
    assert first_head.image_type == ismrmrd.IMTYPE_MAGNITUDE
    assert first_head.image_series_index == 1
    assert first.image_series_index == 1
    assert first_head.image_index == 1
    assert first_head.slice == 0
    assert [float(value) for value in first_head.position] == [10.0, 20.0, 0.0]
    assert [float(value) for value in first_head.field_of_view] == [80.0, 80.0, 20.0]

    second_head = images[1].getHead()
    assert second_head.image_index == 2
    assert second_head.slice == 1
    assert [float(value) for value in second_head.position] == [10.0, 20.0, 20.0]

    last_head = images[-1].getHead()
    assert last_head.image_index == 4
    assert last_head.slice == 3
    assert [float(value) for value in last_head.position] == [10.0, 20.0, 60.0]

    meta = ismrmrd.Meta.deserialize(first.attribute_string)
    assert meta["SeriesDescription"] == "tpiTqf_23Na_n28_TE05_FIRE_sodiumnufft"
    assert meta["SequenceDescription"] == meta["SeriesDescription"]
    assert meta["ProtocolName"] == meta["SeriesDescription"]
    assert meta["SeriesNumberRangeNameUID"] == "tpiTqf_23Na_n28_TE05_FIRE_sodiumnufft_1"
    assert meta["SeriesInstanceUID"].startswith("2.25.")
    assert meta["SOPInstanceUID"].startswith("2.25.")
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\SODIUMNUFFT"
    assert meta["DicomImageType"] == "DERIVED\\PRIMARY\\M\\SODIUMNUFFT"
    assert meta["ImageTypeValue4"] == "SODIUMNUFFT"
    assert meta["ComplexImageComponent"] == "MAGNITUDE"
    assert meta["Keep_image_geometry"] == "1"
    assert meta["partition_count"] == "1"
    assert meta["slice_count"] == "4"
    assert meta["NumberOfSlices"] == "4"
    assert meta["ImagesInAcquisition"] == "4"
    assert meta["NumberInSeries"] == "1"
    assert meta["SliceNo"] == "0"
    assert meta["IsmrmrdSliceNo"] == "0"
    assert meta["SodiumNUFFTDisplayScale"] == "1"
    assert meta["SodiumNUFFTDisplayInputMin"] == "1"
    assert meta["SodiumNUFFTDisplayInputMax"] == "1"
    assert meta["SodiumNUFFTDisplayMin"] == "0"
    assert meta["SodiumNUFFTDisplayMax"] == "0"
    assert meta["SodiumNUFFTDisplayFormula"] == "value = display + 1"
    assert meta["ImageComment"] == (
        "23Na NUFFT Sum-of-Squares; scanner display uint16 0-4096; "
        "value = display + 1"
    )
    assert meta["ImageComments"] == meta["ImageComment"]

    last_meta = ismrmrd.Meta.deserialize(images[-1].attribute_string)
    assert last_meta["SeriesInstanceUID"] == meta["SeriesInstanceUID"]
    assert last_meta["SOPInstanceUID"] != meta["SOPInstanceUID"]
    assert last_meta["NumberInSeries"] == "4"
    assert last_meta["SliceNo"] == "3"
    assert last_meta["IsmrmrdSliceNo"] == "3"
