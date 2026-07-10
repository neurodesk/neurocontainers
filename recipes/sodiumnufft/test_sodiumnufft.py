import importlib
import logging
from pathlib import Path
import sys
import threading
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
    constants.MRD_LOGGING_INFO = 1
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


def test_build_output_images_emits_one_slab_as_3d_partitions(monkeypatch):
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
    assert second_head.slice == 0
    assert [float(value) for value in second_head.position] == [10.0, 20.0, 20.0]

    last_head = images[-1].getHead()
    assert last_head.image_index == 4
    assert last_head.slice == 0
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
    assert meta["partition_count"] == "4"
    assert meta["slice_count"] == "1"
    assert meta["NumberOfSlices"] == "1"
    assert meta["ImagesInAcquisition"] == "4"
    assert meta["NumberInSeries"] == "1"
    assert meta["SliceNo"] == "0"
    assert meta["IsmrmrdSliceNo"] == "0"
    assert meta["AnatomicalSliceNo"] == "0"
    assert meta["ChronSliceNo"] == "0"
    assert meta["ProtocolSliceNumber"] == "0"
    assert meta["Actual3DImagePartNumber"] == "0"
    assert meta["Actual3DImaPartNumber"] == "0"
    assert meta["AnatomicalPartitionNo"] == "0"
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
    assert last_meta["SliceNo"] == "0"
    assert last_meta["IsmrmrdSliceNo"] == "0"
    assert last_meta["AnatomicalSliceNo"] == "0"
    assert last_meta["ChronSliceNo"] == "3"
    assert last_meta["ProtocolSliceNumber"] == "0"
    assert last_meta["Actual3DImagePartNumber"] == "3"
    assert last_meta["Actual3DImaPartNumber"] == "3"
    assert last_meta["AnatomicalPartitionNo"] == "3"


def test_output_partition_data_and_positions_advance_together(monkeypatch):
    sodiumnufft = _import_sodiumnufft_with_runtime_stubs(monkeypatch)
    volume = np.broadcast_to(
        np.arange(4, dtype=np.float32),
        (2, 3, 4),
    ).copy()

    images = sodiumnufft._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    partition_values = [int(np.asarray(image.data).mean()) for image in images]
    partition_positions = [float(image.getHead().position[2]) for image in images]

    assert partition_values == [0, 1365, 2731, 4096]
    assert partition_positions == [0.0, 20.0, 40.0, 60.0]


def test_log_cpu_resources_reports_container_limits(monkeypatch, caplog):
    sodiumnufft = _import_sodiumnufft_with_runtime_stubs(monkeypatch)
    monkeypatch.setattr(sodiumnufft.os, "cpu_count", lambda: 32)
    monkeypatch.setattr(
        sodiumnufft.os,
        "sched_getaffinity",
        lambda process_id: {2, 3, 4, 5},
        raising=False,
    )
    monkeypatch.setattr(sodiumnufft, "_cgroup_cpu_limit", lambda: "600000 100000")
    monkeypatch.setattr(sodiumnufft, "_cgroup_cpuset", lambda: "2-5")

    with caplog.at_level(logging.INFO):
        sodiumnufft._log_cpu_resources(
            configured_max_workers=6,
            effective_coil_workers=3,
        )

    assert (
        "FIRE CPU resources: os_cpu_count=32 affinity_count=4 "
        "affinity_cpus=2,3,4,5 cgroup_cpu_limit='600000 100000' "
        "cgroup_cpuset='2-5' configured_maxworkers=6 "
        "effective_coil_workers=3"
    ) in caplog.text


def test_process_raw_parallel_coils_match_serial_reconstruction(monkeypatch):
    sodiumnufft = _import_sodiumnufft_with_runtime_stubs(monkeypatch)

    acquisitions = []
    readout_data = (
        np.arange(24, dtype=np.float32).reshape(2, 3, 4) + 1.0
    ).astype(np.complex64)
    readout_data[:, 1, :] *= 1j
    readout_data[:, 2, 1::2] *= -1
    for readout_index in range(readout_data.shape[0]):
        acquisition = ismrmrd.Acquisition(data=readout_data[readout_index])
        header = acquisition.getHead()
        header.position = ReferenceHead.position
        header.read_dir = ReferenceHead.read_dir
        header.phase_dir = ReferenceHead.phase_dir
        header.slice_dir = ReferenceHead.slice_dir
        acquisition.setHead(header)
        acquisitions.append(acquisition)

    trajectory = np.zeros((4, 2, 3), dtype=np.float32)
    trajectory[:, :, 0] = np.arange(1, 5, dtype=np.float32)[:, np.newaxis]
    trajectory[:, :, 1] = np.array([0.25, 0.5], dtype=np.float32)
    monkeypatch.setattr(sodiumnufft, "_load_trajectory", lambda group, config: trajectory)
    monkeypatch.setattr(sodiumnufft, "_ensure_debug_folder", lambda: None)
    monkeypatch.setattr(
        sodiumnufft,
        "_config_int",
        lambda config, key, default: int(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumnufft,
        "_config_float",
        lambda config, key, default: float(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumnufft,
        "_config_bool",
        lambda config, key, default: bool(config.get(key, default)),
    )

    class Connection:
        def __init__(self):
            self.logs = []

        def send_logging(self, level, message):
            self.logs.append((level, message))

    def reconstruct(max_workers, require_parallel):
        saved_arrays = {}
        thread_ids = set()
        thread_ids_lock = threading.Lock()
        barrier = threading.Barrier(3, timeout=2.0) if require_parallel else None

        def fake_nufft_adjoint(weighted_data, coordinates, output_shape):
            del coordinates
            with thread_ids_lock:
                thread_ids.add(threading.get_ident())
            if barrier is not None:
                barrier.wait()
            fingerprint = np.vdot(
                np.arange(1, weighted_data.size + 1, dtype=np.float32),
                weighted_data,
            )
            return np.full(output_shape, fingerprint, dtype=np.complex64)

        def capture_array(path, array):
            saved_arrays[Path(path).name] = np.array(array, copy=True)

        monkeypatch.setattr(
            sodiumnufft.sigpy,
            "nufft_adjoint",
            fake_nufft_adjoint,
            raising=False,
        )
        monkeypatch.setattr(sodiumnufft.np, "save", capture_array)
        images = sodiumnufft.process_raw(
            acquisitions,
            Connection(),
            {
                "matrixsize": 2,
                "fovcm": 2.0,
                "rejectbadreadouts": False,
                "applyfermifilter": False,
                "dcfiterations": 0,
                "maxworkers": max_workers,
            },
            Metadata(),
        )
        return images, saved_arrays, thread_ids

    serial_images, serial_arrays, serial_threads = reconstruct(
        max_workers=1,
        require_parallel=False,
    )
    parallel_images, parallel_arrays, parallel_threads = reconstruct(
        max_workers=3,
        require_parallel=True,
    )

    assert len(serial_threads) == 1
    assert len(parallel_threads) == 3
    serial_coils = serial_arrays["sodiumnufft_coil_images.npy"]
    assert serial_coils.shape == (3, 2, 2, 2)
    assert len({complex(coil[0, 0, 0]) for coil in serial_coils}) == 3
    np.testing.assert_array_equal(
        parallel_arrays["sodiumnufft_coil_images.npy"],
        serial_coils,
    )
    expected_magnitude = np.sqrt(np.sum(np.abs(serial_coils) ** 2, axis=0))
    np.testing.assert_allclose(
        serial_arrays["sodiumnufft_magnitude_volume.npy"],
        expected_magnitude,
    )
    np.testing.assert_array_equal(
        parallel_arrays["sodiumnufft_magnitude_volume.npy"],
        serial_arrays["sodiumnufft_magnitude_volume.npy"],
    )
    for serial_image, parallel_image in zip(serial_images, parallel_images, strict=True):
        np.testing.assert_array_equal(parallel_image.data, serial_image.data)
