import importlib
from pathlib import Path
import sys
import types

import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent


def _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch):
    constants = types.ModuleType("constants")
    constants.MRD_LOGGING_INFO = 1
    constants.MRD_LOGGING_ERROR = 2
    mrdhelper = types.ModuleType("mrdhelper")

    def update_img_header_from_raw(image_header, reference_head):
        del reference_head
        return image_header

    def get_json_config_param(config, key, default=None, type=None):
        del type
        return config.get(key, default)

    mrdhelper.update_img_header_from_raw = update_img_header_from_raw
    mrdhelper.get_json_config_param = get_json_config_param

    monkeypatch.syspath_prepend(str(RECIPE_DIR))
    monkeypatch.setitem(sys.modules, "constants", constants)
    monkeypatch.setitem(sys.modules, "mrdhelper", mrdhelper)
    monkeypatch.delitem(sys.modules, "sodiumgridding", raising=False)
    monkeypatch.delitem(sys.modules, "ptpi_dual_echo_lowrank_core", raising=False)
    monkeypatch.delitem(sys.modules, "sodiumgridding_ptpi", raising=False)
    return importlib.import_module("sodiumgridding_ptpi")


def test_split_dual_echo_data_reverses_second_echo(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    data = np.arange(2 * 4 * 6, dtype=np.complex64).reshape(2, 4, 6)

    te1, te2 = sodiumgridding_ptpi._split_dual_echo_data(data)

    np.testing.assert_array_equal(te1, data[:, :, 0::2])
    np.testing.assert_array_equal(te2, data[:, ::-1, 1::2])


def test_split_multi_echo_data_reverses_even_numbered_echoes(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    data = np.arange(1 * 4 * 16, dtype=np.complex64).reshape(1, 4, 16)

    echoes = sodiumgridding_ptpi._split_echo_data(data, 4)

    assert len(echoes) == 4
    np.testing.assert_array_equal(echoes[0], data[:, :, 0::4])
    np.testing.assert_array_equal(echoes[1], data[:, ::-1, 1::4])
    np.testing.assert_array_equal(echoes[2], data[:, :, 2::4])
    np.testing.assert_array_equal(echoes[3], data[:, ::-1, 3::4])


def test_echo_sample_times_use_forward_reverse_pair_delta_te(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    sodiumgridding_ptpi.core.FIELD_MAP_DELTA_TE_S = 0.005
    base_times = np.asarray([0.0, 0.001], dtype=np.float32)

    np.testing.assert_allclose(
        sodiumgridding_ptpi._echo_sample_times(base_times, 0),
        [0.0, 0.001],
    )
    np.testing.assert_allclose(
        sodiumgridding_ptpi._echo_sample_times(base_times, 1),
        [0.0, 0.001],
    )
    np.testing.assert_allclose(
        sodiumgridding_ptpi._echo_sample_times(base_times, 2),
        [0.010, 0.011],
    )
    np.testing.assert_allclose(
        sodiumgridding_ptpi._echo_sample_times(base_times, 3),
        [0.010, 0.011],
    )


def test_series_index_is_unique_by_echo_and_repetition(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    assert sodiumgridding_ptpi._series_index_for_echo(0, 0, 2) == 1
    assert sodiumgridding_ptpi._series_index_for_echo(0, 1, 2) == 2
    assert sodiumgridding_ptpi._series_index_for_echo(1, 0, 2) == 1001
    assert sodiumgridding_ptpi._series_index_for_echo(1, 1, 2) == 1002


def test_clip_to_common_shape_uses_shared_sample_and_readout_extent(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    k1 = np.zeros((5, 4, 3), dtype=np.float32)
    k2 = np.zeros((6, 3, 3), dtype=np.float32)
    t1 = np.arange(7, dtype=np.float32)
    t2 = np.arange(5, dtype=np.float32)
    d1 = np.zeros((2, 4, 5), dtype=np.complex64)
    d2 = np.zeros((2, 8, 3), dtype=np.complex64)

    clipped = sodiumgridding_ptpi._clip_to_common_shape(k1, k2, t1, t2, d1, d2)

    assert clipped[0].shape == (4, 3, 3)
    assert clipped[1].shape == (4, 3, 3)
    assert clipped[2].shape == (4,)
    assert clipped[3].shape == (4,)
    assert clipped[4].shape == (2, 4, 3)
    assert clipped[5].shape == (2, 4, 3)


def test_clip_echo_inputs_uses_common_extent_across_all_echoes(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    echo_inputs = [
        {
            "k": np.zeros((5, 4, 3), dtype=np.float32),
            "t": np.arange(6, dtype=np.float32),
            "data": np.zeros((2, 5, 4), dtype=np.complex64),
        },
        {
            "k": np.zeros((4, 3, 3), dtype=np.float32),
            "t": np.arange(4, dtype=np.float32),
            "data": np.zeros((2, 6, 3), dtype=np.complex64),
        },
        {
            "k": np.zeros((7, 5, 3), dtype=np.float32),
            "t": np.arange(8, dtype=np.float32),
            "data": np.zeros((2, 4, 5), dtype=np.complex64),
        },
    ]

    clipped = sodiumgridding_ptpi._clip_echo_inputs(echo_inputs)

    assert [item["k"].shape for item in clipped] == [(4, 3, 3)] * 3
    assert [item["t"].shape for item in clipped] == [(4,)] * 3
    assert [item["data"].shape for item in clipped] == [(2, 4, 3)] * 3


def test_configure_core_disables_standalone_outputs(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    sodiumgridding_ptpi._configure_core(
        {
            "dcfiterations": 2,
            "maxworkers": 3,
            "coilcompression": True,
            "coilvarianceretention": "0.95",
            "coilcompressionsource": "te1",
            "coilcombinemode": "SoS",
            "echonormalizationmode": "te1",
            "fieldmapdeltates": "0.0045",
            "fieldmapunwrapmethod": "axis",
            "runphasecorrection": False,
            "phaselowrankrank": 4,
            "applyn4biascorrection": False,
        },
        matrix_size=64,
        fov_cm=20.0,
    )

    core = sodiumgridding_ptpi.core
    assert core.N == 64
    assert core.FOV_CM == 20.0
    assert core.DCF_ITER == 2
    assert core.MAX_WORKERS == 3
    assert core.COIL_VARIANCE_RETENTION == 0.95
    assert core.COIL_COMPRESSION_SOURCE == "te1"
    assert core.COIL_COMBINE_MODE == "SoS"
    assert core.ECHO_NORMALIZATION_MODE == "te1"
    assert core.FIELD_MAP_DELTA_TE_S == 0.0045
    assert core.FIELD_MAP_UNWRAP_METHOD == "axis"
    assert core.RUN_TE1_PHASE_CORRECTION is False
    assert core.RUN_TE2_PHASE_CORRECTION is False
    assert core.TE2_PHASE_LOWRANK_RANK == 4
    assert core.APPLY_N4_BIAS_CORRECTION is False
    assert core.PLOT_RESULTS is False
    assert core.SAVE_C0_FILES is False
    assert core.SAVE_NIFTI_FILES is False


def test_build_echo_images_passes_echo_identity_to_output_builder(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    captured = {}

    def fake_build_output_images(volume, reference_head, metadata, **kwargs):
        del volume, reference_head, metadata
        captured.update(kwargs)
        captured["series_description"] = (
            sodiumgridding_ptpi.openrecon_base.OUTPUT_SERIES_DESCRIPTION
        )
        return ["te2-image"]

    monkeypatch.setattr(
        sodiumgridding_ptpi.openrecon_base,
        "_build_output_images",
        fake_build_output_images,
    )
    old_description = sodiumgridding_ptpi.openrecon_base.OUTPUT_SERIES_DESCRIPTION
    old_index = sodiumgridding_ptpi.openrecon_base.OUTPUT_IMAGE_SERIES_INDEX
    old_prefix_protocol = sodiumgridding_ptpi.openrecon_base.OUTPUT_SERIES_PREFIX_PROTOCOL

    images = sodiumgridding_ptpi._build_echo_images(
        np.ones((2, 2, 2), dtype=np.float32),
        "TE2",
        0.004,
        1001,
        1,
        2,
        4,
        3,
        1,
        object(),
        object(),
        220.0,
        "zyx",
        True,
        False,
        display_input_min=0.0,
        display_input_max=42.0,
    )

    assert images == ["te2-image"]
    assert captured["echo_index"] == 1
    assert captured["total_echoes"] == 2
    assert captured["repetition_index"] == 4
    assert captured["total_repetitions"] == 3
    assert captured["echo_time_s"] == 0.004
    assert captured["display_input_min"] == 0.0
    assert captured["display_input_max"] == 42.0
    assert captured["output_fov_mm"] == 220.0
    assert captured["orientation"] == "zyx"
    assert captured["flip_slice"] is True
    assert captured["series_description"] == "pTPI_TE2_rep2"
    assert sodiumgridding_ptpi.openrecon_base.OUTPUT_SERIES_PREFIX_PROTOCOL == old_prefix_protocol
    assert sodiumgridding_ptpi.openrecon_base.OUTPUT_SERIES_DESCRIPTION == old_description
    assert sodiumgridding_ptpi.openrecon_base.OUTPUT_IMAGE_SERIES_INDEX == old_index


def test_metadata_echo_times_uses_sequence_parameters_te(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    class SequenceParameters:
        TE = [0.3, 4.3, 8.3, 12.3]

    class Metadata:
        sequenceParameters = SequenceParameters()

    echo_times_s, source = sodiumgridding_ptpi._metadata_echo_times_s(
        Metadata(),
        4,
        0.004,
    )

    np.testing.assert_allclose(
        echo_times_s,
        [0.0003, 0.0043, 0.0083, 0.0123],
        rtol=1e-6,
    )
    assert source == "MRD sequenceParameters.TE"


def test_metadata_echo_times_falls_back_to_delta_te(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    echo_times_s, source = sodiumgridding_ptpi._metadata_echo_times_s(
        object(),
        4,
        0.004,
    )

    np.testing.assert_allclose(echo_times_s, [0.0, 0.004, 0.008, 0.012], rtol=1e-6)
    assert source == "fallback delta TE offsets"


def test_temporal_svd_denoising_preserves_shape_and_finite_values(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    rng = np.random.default_rng(1234)
    base = np.linspace(1.0, 2.0, 3, dtype=np.float32)[:, None, None, None]
    volumes = [
        (base[index] * np.ones((5, 5, 5), dtype=np.float32))
        + rng.normal(0.0, 0.01, size=(5, 5, 5)).astype(np.float32)
        for index in range(3)
    ]

    denoised = sodiumgridding_ptpi._denoise_temporal_svd_final_images(
        volumes,
        patch_size=3,
        stride=2,
        retain_fraction=0.5,
    )

    assert len(denoised) == 3
    assert [item.shape for item in denoised] == [(5, 5, 5)] * 3
    assert all(np.all(np.isfinite(item)) for item in denoised)
    assert all(item.dtype == np.float32 for item in denoised)


def test_single_echo_n4_correction_uses_te1_image(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    image = np.arange(8, dtype=np.float32).reshape(2, 2, 2) + 1.0
    mask = image > 2.0
    bias = np.full(image.shape, 2.0, dtype=np.float32)
    calls = []

    sodiumgridding_ptpi.core.APPLY_N4_BIAS_CORRECTION = True

    def fake_make_bias_mask(reference_mag):
        calls.append(("mask", reference_mag.copy()))
        return mask

    def fake_estimate_bias_field(reference_mag, input_mask):
        calls.append(("estimate", reference_mag.copy(), input_mask.copy()))
        return bias, input_mask

    def fake_apply_bias_field(input_image, input_bias, input_mask, should_correct):
        calls.append(("apply", input_image.copy(), input_bias.copy(), input_mask.copy(), should_correct))
        return input_image / input_bias, input_mask

    monkeypatch.setattr(sodiumgridding_ptpi.core, "make_bias_mask", fake_make_bias_mask)
    monkeypatch.setattr(
        sodiumgridding_ptpi.core,
        "estimate_bias_field_sitk_n4",
        fake_estimate_bias_field,
    )
    monkeypatch.setattr(
        sodiumgridding_ptpi.core,
        "apply_bias_field_to_image",
        fake_apply_bias_field,
    )

    corrected, bias_field, output_mask = sodiumgridding_ptpi._apply_n4_bias_correction_to_echoes(
        [image]
    )

    assert len(corrected) == 1
    np.testing.assert_allclose(corrected[0], image / 2.0)
    np.testing.assert_allclose(bias_field, bias)
    np.testing.assert_array_equal(output_mask, mask)
    assert [call[0] for call in calls] == ["mask", "estimate", "apply"]
    assert calls[-1][-1] is True


def test_shared_echo_display_range_uses_all_echoes(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    input_min, input_max = sodiumgridding_ptpi._shared_echo_display_range(
        [
            np.asarray([[[0.0, 1.0]]], dtype=np.float32),
            np.asarray([[[2.0, 7.0]]], dtype=np.float32),
        ]
    )

    assert input_min == 0.0
    assert input_max == 7.0


def test_detect_echo_count_prefers_acquisition_contrast(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    class Idx:
        def __init__(self, contrast):
            self.contrast = contrast

    class Head:
        def __init__(self, contrast):
            self.idx = Idx(contrast)

    class Acquisition:
        def __init__(self, contrast):
            self._head = Head(contrast)

        def getHead(self):
            return self._head

    count, source = sodiumgridding_ptpi._detect_echo_count(
        [Acquisition(0), Acquisition(1), Acquisition(2), Acquisition(3)],
        object(),
        {"numechoes": 2},
    )

    assert count == 4
    assert source == "acquisition contrast index"


def test_detect_echo_count_uses_metadata_contrast_limits(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    class Contrast:
        minimum = 0
        maximum = 7

    class EncodingLimits:
        contrast = Contrast()

    class Encoding:
        encodingLimits = EncodingLimits()

    class Metadata:
        encoding = [Encoding()]

    count, source = sodiumgridding_ptpi._detect_echo_count([], Metadata(), {"numechoes": 2})

    assert count == 8
    assert source == "MRD encodingLimits.contrast"


def test_detect_echo_count_uses_single_metadata_contrast(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    class Contrast:
        minimum = 0
        maximum = 0

    class EncodingLimits:
        contrast = Contrast()

    class Encoding:
        encodingLimits = EncodingLimits()

    class Metadata:
        encoding = [Encoding()]

    count, source = sodiumgridding_ptpi._detect_echo_count([], Metadata(), {})

    assert count == 1
    assert source == "MRD encodingLimits.contrast"


def test_split_acquisitions_by_repetition_preserves_receive_order(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    class Idx:
        def __init__(self, repetition):
            self.repetition = repetition

    class Head:
        def __init__(self, repetition):
            self.idx = Idx(repetition)

    class Acquisition:
        def __init__(self, label, repetition):
            self.label = label
            self._head = Head(repetition)

        def getHead(self):
            return self._head

    acquisitions = [
        Acquisition("rep2-a", 2),
        Acquisition("rep2-b", 2),
        Acquisition("rep5-a", 5),
        Acquisition("rep2-c", 2),
        Acquisition("rep5-b", 5),
    ]

    groups = sodiumgridding_ptpi._split_acquisitions_by_repetition(acquisitions)

    assert [repetition for repetition, _group in groups] == [2, 5]
    assert [[item.label for item in group] for _repetition, group in groups] == [
        ["rep2-a", "rep2-b", "rep2-c"],
        ["rep5-a", "rep5-b"],
    ]
