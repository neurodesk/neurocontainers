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
    read_dir = (-1.0, 0.0, 0.0)
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


def _import_sodiumgridding_with_runtime_stubs(monkeypatch):
    constants = types.ModuleType("constants")
    constants.MRD_LOGGING_INFO = 1
    mrdhelper = types.ModuleType("mrdhelper")

    def update_img_header_from_raw(image_header, reference_head):
        del reference_head
        return image_header

    mrdhelper.update_img_header_from_raw = update_img_header_from_raw

    monkeypatch.syspath_prepend(str(RECIPE_DIR))
    monkeypatch.setitem(sys.modules, "constants", constants)
    monkeypatch.setitem(sys.modules, "mrdhelper", mrdhelper)
    monkeypatch.delitem(sys.modules, "sodiumgridding", raising=False)
    return importlib.import_module("sodiumgridding")


def test_build_output_images_emits_one_explicit_volume(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.ones((2, 3, 4), dtype=np.float32)

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    assert len(images) == 1

    first = images[0]
    first_head = first.getHead()
    assert first.data.shape == (1, 4, 3, 2)
    assert first.data.dtype == np.uint16
    assert int(first.data.min()) == 0
    assert int(first.data.max()) == 0
    assert [int(value) for value in first_head.matrix_size] == [2, 3, 4]
    assert first_head.image_type == ismrmrd.IMTYPE_MAGNITUDE
    assert first_head.image_series_index == 1
    assert first.image_series_index == 1
    assert first_head.image_index == 1
    assert first_head.slice == 0
    assert [float(value) for value in first_head.position] == [10.0, 20.0, 30.0]
    assert [float(value) for value in first_head.field_of_view] == [80.0, 80.0, 80.0]
    assert [float(value) for value in first_head.read_dir] == [1.0, 0.0, 0.0]
    assert [float(value) for value in first_head.phase_dir] == [0.0, -1.0, 0.0]

    meta = ismrmrd.Meta.deserialize(first.attribute_string)
    assert meta["SeriesDescription"] == "tpiTqf_23Na_n28_TE05_FIRE_sodiumgridding"
    assert meta["SequenceDescription"] == meta["SeriesDescription"]
    assert meta["ProtocolName"] == meta["SeriesDescription"]
    assert meta["SeriesNumberRangeNameUID"] == "tpiTqf_23Na_n28_TE05_FIRE_sodiumgridding_1"
    assert meta["SeriesInstanceUID"].startswith("2.25.")
    assert meta["SOPInstanceUID"].startswith("2.25.")
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\SODIUMGRIDDING"
    assert meta["DicomImageType"] == "DERIVED\\PRIMARY\\M\\SODIUMGRIDDING"
    assert meta["ImageTypeValue4"] == "SODIUMGRIDDING"
    assert meta["ImageProcessingHistory"] == [
        "PYTHON",
        "NUMBA",
        "KAISERBESSEL",
        "GRIDDING",
    ]
    assert meta["ComplexImageComponent"] == "MAGNITUDE"
    assert meta["Keep_image_geometry"] == "0"
    assert meta["partition_count"] == "1"
    assert meta["slice_count"] == "4"
    assert meta["NumberOfSlices"] == "4"
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
    assert meta["SodiumGriddingDisplayScale"] == "1"
    assert meta["SodiumGriddingDisplayInputMin"] == "1"
    assert meta["SodiumGriddingDisplayInputMax"] == "1"
    assert meta["SodiumGriddingDisplayMin"] == "0"
    assert meta["SodiumGriddingDisplayMax"] == "0"
    assert meta["SodiumGriddingDisplayFormula"] == "value = display + 1"
    assert meta["ImageComment"] == (
        "23Na Kaiser-Bessel Gridding; scanner display uint16 0-4096; "
        "value = display + 1"
    )
    assert meta["ImageComments"] == meta["ImageComment"]

    assert meta["ImageRowDir"] == [
        "1.000000000000000000",
        "-0.000000000000000000",
        "-0.000000000000000000",
    ]
    assert meta["ImageColumnDir"] == [
        "-0.000000000000000000",
        "-1.000000000000000000",
        "-0.000000000000000000",
    ]


def test_output_volume_data_preserves_slice_order(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.broadcast_to(
        np.arange(4, dtype=np.float32),
        (2, 3, 4),
    ).copy()

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    slice_values = [
        int(np.asarray(images[0].data)[0, index].mean())
        for index in range(4)
    ]

    assert slice_values == [0, 1365, 2731, 4096]


def test_output_volume_is_flipped_up_down_then_left_right(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(1, 7, dtype=np.float32).reshape(2, 3, 1)

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    display_volume, _ = sodiumgridding._scale_volume_to_display_range(volume)
    packed_without_display_flips = display_volume[:, :, 0].T
    expected = np.flip(packed_without_display_flips, axis=(0, 1))[np.newaxis, ...]

    np.testing.assert_array_equal(np.asarray(images[0].data)[0], expected)


def test_log_cpu_resources_reports_container_limits(monkeypatch, caplog):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    monkeypatch.setattr(sodiumgridding.os, "cpu_count", lambda: 32)
    monkeypatch.setattr(
        sodiumgridding.os,
        "sched_getaffinity",
        lambda process_id: {2, 3, 4, 5},
        raising=False,
    )
    monkeypatch.setattr(sodiumgridding, "_cgroup_cpu_limit", lambda: "600000 100000")
    monkeypatch.setattr(sodiumgridding, "_cgroup_cpuset", lambda: "2-5")

    with caplog.at_level(logging.INFO):
        sodiumgridding._log_cpu_resources(
            configured_max_workers=6,
            effective_coil_workers=3,
        )

    assert (
        "FIRE CPU resources: os_cpu_count=32 affinity_count=4 "
        "affinity_cpus=2,3,4,5 cgroup_cpu_limit='600000 100000' "
        "cgroup_cpuset='2-5' configured_maxworkers=6 "
        "effective_coil_workers=3"
    ) in caplog.text


def test_coil_compression_retains_dominant_signal_subspace(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    dominant = np.arange(1, 13, dtype=np.float32).reshape(3, 4).astype(np.complex64)
    coil_data = np.stack(
        [
            dominant,
            2.0j * dominant,
            np.full_like(dominant, 0.01 + 0.01j),
        ]
    )

    compressed, cumulative_variance, compression_matrix = (
        sodiumgridding._compress_coils_by_variance(
            coil_data,
            variance_retention=0.9,
        )
    )

    assert compressed.shape == (1, 3, 4)
    assert compression_matrix.shape == (3, 1)
    assert cumulative_variance[0] >= 0.9


def test_kaiser_bessel_gridding_reconstructs_a_finite_center_sample(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    coordinates = np.zeros((1, 3), dtype=np.float32)
    dcf = np.ones(1, dtype=np.float32)
    deapodization = sodiumgridding._compute_deapodization_kb(
        4,
        oversampling=2,
    )

    image = sodiumgridding._regrid_3d_kb(
        np.array([1.0 + 0.0j], dtype=np.complex64),
        coordinates,
        grid_size=4,
        dcf=dcf,
        deapodization=deapodization,
        oversampling=2,
    )

    assert image.shape == (4, 4, 4)
    assert np.all(np.isfinite(image))
    assert float(np.abs(image).max()) > 0.0


def test_iterative_kaiser_bessel_dcf_is_finite_and_normalized(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    coordinates = np.array(
        [
            [-0.2, 0.0, 0.0],
            [0.0, -0.2, 0.0],
            [0.0, 0.0, 0.0],
            [0.0, 0.2, 0.0],
            [0.2, 0.0, 0.0],
        ],
        dtype=np.float32,
    )

    dcf = sodiumgridding._compute_dcf_kb(
        coordinates,
        grid_size=4,
        oversampling=2,
        num_iterations=2,
    )

    assert dcf.shape == (5,)
    assert np.all(np.isfinite(dcf))
    assert np.all(dcf > 0.0)
    assert np.isclose(np.median(dcf), 1.0)


def test_process_raw_parallel_grids_match_serial_reconstruction(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

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
    monkeypatch.setattr(sodiumgridding, "_load_trajectory", lambda group, config: trajectory)
    monkeypatch.setattr(sodiumgridding, "_ensure_debug_folder", lambda: None)
    monkeypatch.setattr(
        sodiumgridding,
        "_config_int",
        lambda config, key, default: int(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_config_float",
        lambda config, key, default: float(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_config_bool",
        lambda config, key, default: bool(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_config_str",
        lambda config, key, default: str(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_compute_dcf_kb",
        lambda coordinates, grid_size, oversampling, num_iterations: np.ones(
            len(coordinates), dtype=np.float32
        ),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_compute_deapodization_kb",
        lambda grid_size, oversampling: np.ones(
            (grid_size, grid_size, grid_size), dtype=np.float32
        ),
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
        barrier = threading.Barrier(2, timeout=2.0) if require_parallel else None
        call_count = 0

        def fake_regrid(
            kspace_data,
            normalized_coordinates,
            grid_size,
            dcf,
            deapodization,
            oversampling,
        ):
            del normalized_coordinates, dcf, deapodization, oversampling
            nonlocal call_count
            with thread_ids_lock:
                thread_ids.add(threading.get_ident())
                call_count += 1
                current_call = call_count
            if barrier is not None and current_call <= 2:
                barrier.wait()
            fingerprint = np.vdot(
                np.arange(1, kspace_data.size + 1, dtype=np.float32),
                kspace_data,
            )
            return np.full(
                (grid_size, grid_size, grid_size),
                fingerprint,
                dtype=np.complex64,
            )

        def capture_array(path, array):
            saved_arrays[Path(path).name] = np.array(array, copy=True)

        monkeypatch.setattr(sodiumgridding, "_regrid_3d_kb", fake_regrid)
        monkeypatch.setattr(sodiumgridding.np, "save", capture_array)
        images = sodiumgridding.process_raw(
            acquisitions,
            Connection(),
            {
                "matrixsize": 2,
                "fovcm": 2.0,
                "rejectbadreadouts": False,
                "applyfermifilter": False,
                "dcfiterations": 0,
                "maxworkers": max_workers,
                "coilvarianceretention": 1.0,
                "coilcombinemode": "SoS",
                "applyn4biascorrection": False,
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
    assert len(parallel_threads) >= 2
    assert len(serial_images) == 1
    assert len(parallel_images) == 1
    assert serial_images[0].data.shape == (1, 2, 2, 2)
    assert [int(value) for value in serial_images[0].getHead().matrix_size] == [2, 2, 2]
    serial_coils = serial_arrays["sodiumgridding_coil_images.npy"]
    assert serial_coils.shape == (3, 2, 2, 2)
    assert len({complex(coil[0, 0, 0]) for coil in serial_coils}) == 3
    np.testing.assert_array_equal(
        parallel_arrays["sodiumgridding_coil_images.npy"],
        serial_coils,
    )
    expected_magnitude = np.sqrt(np.sum(np.abs(serial_coils) ** 2, axis=0))
    np.testing.assert_allclose(
        serial_arrays["sodiumgridding_magnitude_volume.npy"],
        expected_magnitude,
    )
    np.testing.assert_array_equal(
        parallel_arrays["sodiumgridding_magnitude_volume.npy"],
        serial_arrays["sodiumgridding_magnitude_volume.npy"],
    )
    for serial_image, parallel_image in zip(serial_images, parallel_images, strict=True):
        np.testing.assert_array_equal(parallel_image.data, serial_image.data)
