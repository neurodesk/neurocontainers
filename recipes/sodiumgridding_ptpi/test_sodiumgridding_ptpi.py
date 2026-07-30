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


def test_resolve_orientation_with_slice_flip_folds_suffix_into_selection(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)
    resolve = sodiumgridding_ptpi._resolve_orientation_with_slice_flip

    assert resolve({"orientation": "zxy_fxy"}) == ("zxy_fxy", False, False)
    assert resolve({"orientation": "zxy_fxy_fz"}) == ("zxy_fxy", True, False)
    assert resolve({"orientation": "zyx_fz"}) == ("zyx", True, False)
    assert resolve({"orientation": "debug"}) == ("zyx", False, True)

    # The retired boolean still works when supplied in a manual JSON config.
    assert resolve({"orientation": "zyx", "orientationflipslice": True}) == (
        "zyx",
        True,
        False,
    )


def test_machine_limits_fall_back_to_environment(monkeypatch):
    sodiumgridding_ptpi = _import_sodiumgridding_ptpi_with_runtime_stubs(monkeypatch)

    monkeypatch.setenv(sodiumgridding_ptpi.MAX_WORKERS_ENV_VAR, "3")
    assert sodiumgridding_ptpi._env_int_default("maxworkers", sodiumgridding_ptpi.MAX_WORKERS_ENV_VAR) == 3

    monkeypatch.setenv(sodiumgridding_ptpi.MAX_WORKERS_ENV_VAR, "not-a-number")
    assert sodiumgridding_ptpi._env_int_default("maxworkers", sodiumgridding_ptpi.MAX_WORKERS_ENV_VAR) == 8

    monkeypatch.delenv(sodiumgridding_ptpi.MAX_COILS_ENV_VAR, raising=False)
    assert sodiumgridding_ptpi._env_int_default("maxcoils", sodiumgridding_ptpi.MAX_COILS_ENV_VAR) == 0

    # An explicit config value still wins over the environment default.
    monkeypatch.setenv(sodiumgridding_ptpi.MAX_WORKERS_ENV_VAR, "3")
    sodiumgridding_ptpi._configure_core({"maxworkers": 5}, matrix_size=64, fov_cm=20.0)
    assert sodiumgridding_ptpi.core.MAX_WORKERS == 5

    sodiumgridding_ptpi._configure_core({}, matrix_size=64, fov_cm=20.0)
    assert sodiumgridding_ptpi.core.MAX_WORKERS == 3


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
