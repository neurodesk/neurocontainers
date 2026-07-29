import importlib
import logging
from pathlib import Path
import sys
import threading
import types

import ismrmrd
import numpy as np
import pytest


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
    assert first.data.shape == (1, 2, 3, 4)
    assert first.data.dtype == np.uint16
    assert int(first.data.min()) == 0
    assert int(first.data.max()) == 0
    assert [int(value) for value in first_head.matrix_size] == [4, 3, 2]
    assert first_head.image_type == ismrmrd.IMTYPE_MAGNITUDE
    assert first_head.image_series_index == 1
    assert first.image_series_index == 1
    assert first_head.image_index == 1
    assert first_head.slice == 0
    assert [float(value) for value in first_head.position] == [10.0, 20.0, 30.0]
    assert [float(value) for value in first_head.field_of_view] == [80.0, 80.0, 80.0]
    # Canonicalized into the DICOM display frame: columns toward L, rows toward
    # P, slices toward H.
    assert [float(value) for value in first_head.read_dir] == [1.0, 0.0, 0.0]
    assert [float(value) for value in first_head.phase_dir] == [0.0, 1.0, 0.0]
    assert [float(value) for value in first_head.slice_dir] == [0.0, 0.0, 1.0]

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
    assert meta["Keep_image_geometry"] == "1"
    assert meta["partition_count"] == "1"
    assert meta["slice_count"] == "2"
    assert meta["NumberOfSlices"] == "2"
    assert meta["ImagesInAcquisition"] == "2"
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
        "0.000000000000000000",
        "1.000000000000000000",
        "0.000000000000000000",
    ]


def test_output_volume_data_preserves_slice_order(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.broadcast_to(
        np.arange(4, dtype=np.float32)[:, np.newaxis, np.newaxis],
        (4, 2, 3),
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

    # ReferenceHead's slice_dir already points toward the head, which is the
    # display frame's slice target, so stage 2 preserves the slice order. Stage 3
    # then reverses it to compensate for how ICE stacks the frames.
    assert slice_values == [4096, 2731, 1365, 0]

    # The header still describes the acquisition. Reversing the vector too would
    # cancel the compensation, because ICE positions frames from that vector.
    assert [float(value) for value in images[0].getHead().slice_dir] == [0.0, 0.0, 1.0]


def test_emitted_vectors_are_a_signed_permutation_of_the_acquisition(monkeypatch):
    """The emitted geometry must be a rigid relabelling of the acquisition's.

    No direction vector may be invented, no voxel resampled, and the declared
    matrix size must describe the data. The one deliberate exception is stage 3,
    which reverses the frame order without its vector to compensate for ICE;
    that is a reordering too, so every assertion here still holds, and its
    effect on header-driven consumers is pinned by
    ``test_emitted_mrd_is_mirrored_for_a_consumer_that_ignores_the_flag``.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(1, 25, dtype=np.float32).reshape(2, 3, 4)
    display_volume, _ = sodiumgridding._scale_volume_to_display_range(volume)
    acquisition_axes = [
        np.asarray(vector, dtype=float)
        for vector in (
            ReferenceHead.read_dir,
            ReferenceHead.phase_dir,
            ReferenceHead.slice_dir,
        )
    ]

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
    )
    head = images[0].getHead()
    data = np.asarray(images[0].data)[0]

    for emitted in (head.read_dir, head.phase_dir, head.slice_dir):
        emitted_vector = np.asarray([float(value) for value in emitted])
        assert any(
            np.allclose(emitted_vector, axis) or np.allclose(emitted_vector, -axis)
            for axis in acquisition_axes
        )

    # matrix_size is (columns, rows, slices) and must describe the data.
    assert [int(value) for value in head.matrix_size] == list(data.shape[::-1])
    assert data.shape[0] == int(
        ismrmrd.Meta.deserialize(images[0].attribute_string)["slice_count"]
    )

    # Only axis order changes: no resampling, no value loss.
    np.testing.assert_array_equal(
        np.sort(data, axis=None),
        np.sort(display_volume, axis=None),
    )


def test_default_trajectory_orientation_applies_the_configured_mapping(monkeypatch):
    """The default ``zyx`` selection leaves the gridded axes in place.

    This verifies the configuration contract, not that ``zyx`` is the correct
    scanner setting. A round phantom cannot settle the in-plane mapping; the
    debug sweep must be compared using asymmetric anatomy or a marker.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    class NoRotationHead:
        # Already in the display frame, so canonicalization is the identity and
        # the trajectory-to-axis mapping is observable on its own.
        read_dir = (1.0, 0.0, 0.0)
        phase_dir = (0.0, 1.0, 0.0)
        slice_dir = (0.0, 0.0, 1.0)
        position = (0.0, 0.0, 0.0)

    volume = np.arange(24, dtype=np.float32).reshape(2, 3, 4)

    images = sodiumgridding._build_output_images(
        volume,
        NoRotationHead(),
        Metadata(),
        output_fov_mm=80.0,
    )

    display_volume, _ = sodiumgridding._scale_volume_to_display_range(volume)
    # Canonicalization is the identity for this head, so the only difference
    # from the gridded volume is the stage 3 frame-stacking compensation.
    np.testing.assert_array_equal(
        np.asarray(images[0].data), display_volume[np.newaxis, ::-1, :, :]
    )


def test_orientation_transform_moves_the_expected_axes(monkeypatch):
    """The first stage maps trajectory components into acquisition axes."""
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(24, dtype=np.float32).reshape(2, 3, 4)

    oriented, key = sodiumgridding._orient_volume(volume, "zyx")
    assert key == "zyx"
    np.testing.assert_array_equal(oriented, volume)

    oriented, _ = sodiumgridding._orient_volume(volume, "zyx_fx")
    np.testing.assert_array_equal(oriented, volume[:, :, ::-1])

    oriented, _ = sodiumgridding._orient_volume(volume, "zyx_fy")
    np.testing.assert_array_equal(oriented, volume[:, ::-1, :])

    oriented, _ = sodiumgridding._orient_volume(volume, "zxy")
    np.testing.assert_array_equal(oriented, volume.transpose(0, 2, 1))

    oriented, _ = sodiumgridding._orient_volume(volume, "zxy_fxy")
    np.testing.assert_array_equal(
        oriented,
        volume.transpose(0, 2, 1)[:, ::-1, ::-1],
    )

    oriented, _ = sodiumgridding._orient_volume(volume, "zyx", flip_slice=True)
    np.testing.assert_array_equal(oriented, volume[::-1, :, :])


def test_orientation_debug_choice_enables_the_existing_sweep(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    monkeypatch.setattr(
        sodiumgridding,
        "_config_str",
        lambda config, key, default: str(config.get(key, default)),
    )
    monkeypatch.setattr(
        sodiumgridding,
        "_config_bool",
        lambda config, key, default: bool(config.get(key, default)),
    )

    orientation, emit_debug_series = sodiumgridding._resolve_orientation_config(
        {"orientation": sodiumgridding.ORIENTATION_DEBUG_SELECTION}
    )

    assert orientation == sodiumgridding.DEFAULT_ORIENTATION
    assert emit_debug_series is True


def test_every_ui_orientation_value_is_accepted_without_falling_back(monkeypatch):
    """A configured orientation must never degrade to the default silently.

    The debug selection is one of the values the scanner UI offers, so passing
    a configured value straight into _build_output_images has to enable the
    sweep rather than warn and emit a single default series.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(1, 25, dtype=np.float32).reshape(2, 3, 4)

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
        orientation=sodiumgridding.ORIENTATION_DEBUG_SELECTION,
    )
    assert len(images) == 2 * len(sodiumgridding.ORIENTATION_DEBUG_ORDER)

    for selection in sodiumgridding.ORIENTATION_IN_PLANE_TRANSFORMS:
        images = sodiumgridding._build_output_images(
            volume,
            ReferenceHead(),
            Metadata(),
            output_fov_mm=80.0,
            orientation=selection,
        )
        assert len(images) == 1
        meta = ismrmrd.Meta.deserialize(images[0].attribute_string)
        assert meta["SodiumGriddingOrientation"] == selection


def test_orientation_debug_sweep_is_canonicalized_after_each_transform(monkeypatch):
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(1, 25, dtype=np.float32).reshape(2, 3, 4)

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=80.0,
        emit_debug_series=True,
    )

    # Every in-plane mapping is swept against both slice orders. Sweeping the
    # in-plane keys alone cannot reach a volume whose through-plane axis is
    # reversed, which is the error 0.1.3 shipped.
    expected = [
        (orientation, slice_flip)
        for orientation in sodiumgridding.ORIENTATION_DEBUG_ORDER
        for slice_flip in (False, True)
    ]
    assert len(images) == len(expected) == 2 * len(
        sodiumgridding.ORIENTATION_DEBUG_ORDER
    )

    orderings = []
    for image, (orientation, slice_flip) in zip(images, expected):
        meta = ismrmrd.Meta.deserialize(image.attribute_string)
        suffix = f"_ori_{orientation}_fz{int(slice_flip)}"
        assert meta["SeriesDescription"].endswith(suffix)
        assert meta["SodiumGriddingOrientation"] == orientation
        assert meta["SodiumGriddingOrientationFlipSlice"] == str(int(slice_flip))
        # The header is canonicalized identically in every series, so only the
        # pixel order distinguishes them.
        assert sodiumgridding._direction_label(image.getHead().read_dir) == "R->L"
        assert sodiumgridding._direction_label(image.getHead().phase_dir) == "A->P"
        assert sodiumgridding._direction_label(image.getHead().slice_dir) == "F->H"
        orderings.append(np.asarray(image.data).tobytes())

    assert len(set(orderings)) == len(images)


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


def _import_mrd2nifti():
    spec = importlib.util.spec_from_file_location(
        "mrd2nifti", RECIPE_DIR / "mrd2nifti.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_mrd2nifti_transposes_cubic_volumes_to_read_phase_slice():
    """A cubic matrix must not skip the (z, y, x) -> (x, y, z) transpose.

    MRD image data is always (channels, z, y, x). When the matrix is cubic a
    shape-based check matches both the header order and its reverse, so an
    untransposed volume passes silently and the affine then maps read_dir onto
    the slice axis.
    """
    mrd2nifti = _import_mrd2nifti()

    for shape in ((2, 3, 4), (4, 4, 4)):
        volume_zyx = np.arange(np.prod(shape), dtype=np.uint16).reshape(shape)
        image = ismrmrd.Image.from_array(volume_zyx, transpose=False)

        canonical = mrd2nifti._to_canonical_volume(image, "magnitude")

        np.testing.assert_array_equal(canonical, volume_zyx.transpose(2, 1, 0))
        assert canonical.shape == tuple(
            int(value) for value in image.getHead().matrix_size
        )


def test_output_is_canonicalized_to_the_dicom_display_frame(monkeypatch):
    """The emitted volume must land in the native reconstruction's display view.

    The scanner disables NormOrientation, so nothing downstream rotates our
    images into the DICOM standard view that the native reconstruction uses.
    Measured from the reference series: R on the left edge, A at the top, and
    slice index increasing toward the head, which frame 48 of 128 at SP F28.4
    pins down for a volume centred at isocenter.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    # A single bright voxel at the first slice, first row, first column of the
    # acquisition frame, so the applied transform is directly observable.
    volume = np.zeros((8, 8, 8), dtype=np.float32)
    volume[0, 0, 0] = 1.0

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=220.0,
    )
    head = images[0].getHead()
    data = np.asarray(images[0].data)[0]

    # ReferenceHead's read_dir points toward R, so only the column axis is
    # reversed; phase_dir already points toward P and slice_dir toward H.
    assert sodiumgridding._direction_label(head.read_dir) == "R->L"
    assert sodiumgridding._direction_label(head.phase_dir) == "A->P"
    assert sodiumgridding._direction_label(head.slice_dir) == "F->H"

    # The pixels move with the metadata: stage 2 puts the marker voxel in the
    # last column and leaves its row alone, then stage 3 reverses the frames and
    # moves it to the last slice.
    marker = tuple(int(value) for value in np.argwhere(data == data.max())[0])
    assert marker == (7, 0, 7)

    # The emitted frame must be right-handed under the DICOM rule, like the
    # native reconstruction. A left-handed one is drawn from the opposite side,
    # which is how 0.1.3 came out mirrored against the reference. The incoming
    # acquisition frame is a separate matter: it is DICOM-left-handed by the
    # Siemens PRS convention, which is expected rather than a defect.
    handedness = float(
        np.dot(np.cross(head.read_dir, head.phase_dir), head.slice_dir)
    )
    assert handedness > 0.0


def test_display_frame_canonicalization_handles_an_oblique_acquisition(monkeypatch):
    """The transform derives the globally best mapping for an oblique scan."""
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    class ObliqueHead:
        # A valid orthonormal rotation where greedy target-by-target matching
        # picks the wrong assignment. Axes are deliberately far from cardinal.
        slice_dir = (-0.74455325, 0.66169736, 0.08830099)
        phase_dir = (-0.41733090, -0.35813228, -0.83521027)
        read_dir = (-0.52103300, -0.65870924, 0.54279531)
        position = (0.0, 0.0, 0.0)

    volume = np.zeros((2, 3, 4), dtype=np.float32)
    volume[0, 0, 0] = 1.0

    images = sodiumgridding._build_output_images(
        volume,
        ObliqueHead(),
        Metadata(),
        output_fov_mm=220.0,
    )
    head = images[0].getHead()
    data = np.asarray(images[0].data)[0]

    assert sodiumgridding._direction_label(head.read_dir) == "R->L"
    assert sodiumgridding._direction_label(head.phase_dir) == "A->P"
    assert sodiumgridding._direction_label(head.slice_dir) == "F->H"
    assert data.shape == (3, 4, 2)
    marker = tuple(int(value) for value in np.argwhere(data == data.max())[0])
    assert marker == (0, 3, 1)

    handedness = float(
        np.dot(np.cross(head.read_dir, head.phase_dir), head.slice_dir)
    )
    assert handedness > 0.0


def test_display_frame_targets_are_right_handed(monkeypatch):
    """A left-handed target set reverses one axis too many.

    Permuting and reversing a real acquisition frame reaches the targets only if
    they share its handedness. Version 0.1.3 targeted slices toward the feet
    against columns toward the left and rows toward the posterior, which is
    left-handed, and the scanner consequently drew the volume from the opposite
    side of the native reconstruction.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    slices, rows, columns = (
        np.asarray(target, dtype=float)
        for target in sodiumgridding.DISPLAY_FRAME_TARGETS
    )
    assert float(np.dot(np.cross(columns, rows), slices)) > 0.0

    with pytest.raises(ValueError):
        sodiumgridding._validate_display_frame_targets(
            ((0.0, 0.0, -1.0), (0.0, 1.0, 0.0), (1.0, 0.0, 0.0))
        )


def test_every_swept_geometry_is_a_proper_rotation_of_the_acquisition(monkeypatch):
    """No sweep entry may mirror the volume.

    The sweep exists to find the right pixel ordering, so every entry has to
    remain a rigid reordering of the acquisition. If one of them emitted a
    left-handed frame, selecting it would fix the display while silently
    mirroring the anatomy.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(1, 25, dtype=np.float32).reshape(2, 3, 4)

    images = sodiumgridding._build_output_images(
        volume,
        ReferenceHead(),
        Metadata(),
        output_fov_mm=220.0,
        emit_debug_series=True,
    )

    for image in images:
        head = image.getHead()
        handedness = float(
            np.dot(np.cross(head.read_dir, head.phase_dir), head.slice_dir)
        )
        assert handedness > 0.0


def test_ice_frame_stacking_compensation_moves_pixels_without_the_vector(monkeypatch):
    """Stage 3 must reverse the frames alone.

    ICE positions the frames of an emitted 3D volume against slice_dir, so the
    pixels have to be reversed to land on the right patient coordinates.
    Reversing slice_dir along with them would be a change of storage convention
    and would cancel: ICE would derive its positions from the negated vector and
    the anatomy would not move by a single slice. That is why the 0.1.4
    display-frame rotation, honest by construction, corrected the in-plane view
    but left the through-plane one inverted.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.arange(24, dtype=np.float32).reshape(2, 3, 4)
    slice_dir = np.array([0.0, 0.0, 1.0])

    compensated, content_dir, reversed_frames = (
        sodiumgridding._compensate_ice_frame_stacking(volume, slice_dir)
    )

    assert sodiumgridding.ICE_STACKS_FRAMES_AGAINST_SLICE_DIR is True
    assert reversed_frames is True
    np.testing.assert_array_equal(compensated, volume[::-1])
    # The reported content direction is reversed so the logging stays truthful,
    # but the header vector handed to the scanner is not.
    np.testing.assert_allclose(content_dir, [0.0, 0.0, -1.0])

    monkeypatch.setattr(
        sodiumgridding, "ICE_STACKS_FRAMES_AGAINST_SLICE_DIR", False
    )
    untouched, unchanged_dir, untouched_flag = (
        sodiumgridding._compensate_ice_frame_stacking(volume, slice_dir)
    )
    np.testing.assert_array_equal(untouched, volume)
    np.testing.assert_allclose(unchanged_dir, slice_dir)
    assert untouched_flag is False


def _emitted_volume_and_header(sodiumgridding, head, volume, fov_mm):
    images = sodiumgridding._build_output_images(
        volume, head, Metadata(), output_fov_mm=fov_mm
    )
    image = images[0]
    return image, np.asarray(image.data)[0], image.getHead()


def _centroid_position_from_header(data, header, fov_mm):
    """Locate the signal the way a header-driven consumer would.

    Deliberately independent of sodiumgridding's own helpers: this is the
    calculation an outside reader of the emitted MRD performs, so it must not
    reuse the code under test.
    """
    counts = np.asarray(data, dtype=float)
    axes_directions = (header.slice_dir, header.phase_dir, header.read_dir)
    position = np.asarray([float(value) for value in header.position])
    for axis, direction in enumerate(axes_directions):
        profile = counts.sum(axis=tuple(o for o in range(3) if o != axis))
        length = profile.size
        spacing = fov_mm / length
        index = float((profile * np.arange(length)).sum() / profile.sum())
        position = position + (index - (length - 1) / 2.0) * spacing * np.asarray(
            [float(value) for value in direction]
        )
    return position


def test_emitted_mrd_is_mirrored_for_a_consumer_that_ignores_the_flag(monkeypatch):
    """Pin the cost of the scanner workaround.

    Stage 3 makes the emitted pixels disagree with the emitted slice_dir on
    purpose, so a reader that builds geometry from the header alone places the
    volume mirrored through-plane. That is a real limitation of the emitted MRD,
    not a detail, and it must fail loudly here if anyone assumes otherwise.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    class MeasuredHead:
        read_dir = (-1.0, 0.0, 0.0)
        phase_dir = (0.0, 1.0, 0.0)
        slice_dir = (0.0, 0.0, 1.0)
        position = (0.0, 0.0, 0.0)

    # Bulk on acquisition slices 32..52 of 128, which is the feet side.
    volume = np.zeros((128, 8, 8), dtype=np.float32)
    volume[32:52] = 1.0

    image, data, header = _emitted_volume_and_header(
        sodiumgridding, MeasuredHead(), volume, 220.0
    )

    meta = ismrmrd.Meta.deserialize(image.attribute_string)
    assert meta[sodiumgridding.OUTPUT_FRAME_ORDER_REVERSED_ATTRIBUTE] == "1"

    naive = _centroid_position_from_header(data, header, 220.0)
    # Toward the head: the wrong side, because the flag was ignored.
    assert naive[2] > 0.0

    # Honouring the flag restores the feet side the reconstruction found.
    corrected = _centroid_position_from_header(data[::-1], header, 220.0)
    assert corrected[2] < 0.0
    np.testing.assert_allclose(corrected[2], -naive[2], atol=1e-6)


def test_mrd2nifti_undoes_the_frame_reversal(monkeypatch, tmp_path):
    """The bundled converter must export patient geometry, not display order.

    This exercises the real emitted image through the real converter, so it
    fails if either side of the contract is dropped.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    mrd2nifti = _import_mrd2nifti()

    class MeasuredHead:
        read_dir = (-1.0, 0.0, 0.0)
        phase_dir = (0.0, 1.0, 0.0)
        slice_dir = (0.0, 0.0, 1.0)
        position = (0.0, 0.0, 0.0)

    volume = np.zeros((32, 8, 8), dtype=np.float32)
    volume[4:10] = 1.0          # bulk on the feet side of the acquisition

    images = sodiumgridding._build_output_images(
        volume, MeasuredHead(), Metadata(), output_fov_mm=220.0
    )

    mrd_path = tmp_path / "emitted.h5"
    dataset = ismrmrd.Dataset(str(mrd_path), "dataset", create_if_needed=True)
    dataset.append_image("image_0", images[0])
    dataset.close()

    nifti_path = tmp_path / "emitted.nii.gz"
    mrd2nifti.convert_mrd_to_nifti(
        input_path=mrd_path, output_path=nifti_path, image_series="image_0"
    )

    nib = pytest.importorskip("nibabel")
    exported = nib.load(str(nifti_path))
    data = np.asarray(exported.dataobj)
    affine = exported.affine

    # Centre of mass in voxels, mapped to patient space through the affine the
    # converter wrote. The bulk must land toward the feet, matching the
    # reconstruction, not toward the head.
    total = data.sum()
    indices = [
        float((data.sum(axis=tuple(o for o in range(3) if o != axis))
               * np.arange(data.shape[axis])).sum() / total)
        for axis in range(3)
    ]
    patient = affine[:3, :3] @ np.asarray(indices) + affine[:3, 3]
    assert patient[2] < 0.0, patient


def test_emitted_content_lands_where_the_native_reference_shows_it(monkeypatch, caplog):
    """End to end against the 2026-07-28 measurement.

    The acquisition geometry, the reconstructed centroid and the native
    reference slice position are all taken from that run: the phantom bulk sits
    at F37 and the 64-slice reference shows a full cross-section at F43. The
    emitted volume must report its centroid on that same side.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    class MeasuredHead:
        read_dir = (-1.0, 0.0, 0.0)
        phase_dir = (0.0, 1.0, 0.0)
        slice_dir = (0.0, 0.0, 1.0)
        position = (0.0, 0.0, 0.0)

    # A bulk centred on acquisition slice 42 of 128, which is F37 for a 220 mm
    # volume at isocenter.
    volume = np.zeros((128, 8, 8), dtype=np.float32)
    volume[32:52] = 1.0

    with caplog.at_level(logging.INFO):
        images = sodiumgridding._build_output_images(
            volume,
            MeasuredHead(),
            Metadata(),
            output_fov_mm=220.0,
        )

    assert "intensity centroid at" in caplog.text
    centroid_line = next(
        line for line in caplog.text.splitlines() if "intensity centroid at" in line
    )
    # Toward the feet, matching the native reference, not toward the head.
    assert " F3" in centroid_line, centroid_line

    # The bulk must sit in the second half of the emitted frames, because ICE
    # numbers them from the head end.
    data = np.asarray(images[0].data)[0]
    bulk = np.flatnonzero(data.mean(axis=(1, 2)) > 0)
    assert bulk.min() > 128 // 2


def test_predicted_frame_position_matches_the_native_reference(monkeypatch):
    """Reproduce the SP value the native reconstruction displays.

    The reference series in sodiumgridding_v0.1.3.PNG holds 128 slices over
    220 mm centred at isocenter and shows frame 48 at SP F28.4. Emitting the
    same geometry must predict the same position, which is the single number
    that distinguishes the corrected slice direction from the 0.1.3 one.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)

    position = sodiumgridding._frame_position(
        center_position=(0.0, 0.0, 0.0),
        slice_dir=(0.0, 0.0, 1.0),
        frame_index=47,
        slice_count=128,
        slice_spacing_mm=220.0 / 128.0,
    )

    assert sodiumgridding._slice_position_label(position) == "F28.4"

    reversed_position = sodiumgridding._frame_position(
        center_position=(0.0, 0.0, 0.0),
        slice_dir=(0.0, 0.0, -1.0),
        frame_index=47,
        slice_count=128,
        slice_spacing_mm=220.0 / 128.0,
    )

    # What 0.1.3 emitted, and what the scanner displayed for it.
    assert sodiumgridding._slice_position_label(reversed_position) == "H28.4"


def test_emitted_geometry_logging_predicts_the_scanner_display(monkeypatch, caplog):
    """The log alone must be enough to tell a wrong header from an ignored one.

    Version 0.1.3 could not be diagnosed from its own output, because nothing it
    logged could be compared against the scanner screenshot.
    """
    sodiumgridding = _import_sodiumgridding_with_runtime_stubs(monkeypatch)
    volume = np.zeros((128, 4, 4), dtype=np.float32)
    volume[40] = 1.0

    class CenteredHead:
        read_dir = (-1.0, 0.0, 0.0)
        phase_dir = (0.0, 1.0, 0.0)
        slice_dir = (0.0, 0.0, 1.0)
        position = (0.0, 0.0, 0.0)

    with caplog.at_level(logging.INFO):
        sodiumgridding._build_output_images(
            volume,
            CenteredHead(),
            Metadata(),
            output_fov_mm=220.0,
        )

    # The edge letters the native transversal reconstruction shows.
    assert "left edge 'R'" in caplog.text
    assert "top edge 'A'" in caplog.text
    assert "viewed from 'F'" in caplog.text
    # A frame position that can be read straight off the scanner.
    assert "Predicted frame 1/128" in caplog.text
    assert "Predicted frame 128/128" in caplog.text
    # Where the signal actually sits, in patient coordinates rather than voxels.
    assert "intensity centroid at" in caplog.text
    assert "right-handed" in caplog.text
