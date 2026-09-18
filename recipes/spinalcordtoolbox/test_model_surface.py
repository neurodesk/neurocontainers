import json
from pathlib import Path

import pytest

from sct_model_profile import FULL_TASKS, LITE_TASKS, configure, load_tasks, main
from test_spinalcordtoolbox_openrecon import _load_runtime_helpers_for_test

RECIPE_DIR = Path(__file__).resolve().parent


@pytest.mark.parametrize("profile", ["lite", "full"])
@pytest.mark.parametrize("label_name", ["OpenReconLabel.json", "OpenReconLabel.gpu.json"])
def test_profile_generates_matching_menu_and_runtime(tmp_path, monkeypatch, profile, label_name):
    label = tmp_path / "OpenReconLabel.json"
    label.write_bytes((RECIPE_DIR / label_name).read_bytes())
    gpu = label_name.endswith(".gpu.json")
    if profile == "lite" and gpu:
        with pytest.raises(ValueError, match="CPU-only"):
            configure(profile, label, gpu=gpu)
        return
    configure(profile, label, gpu=gpu)
    tasks = load_tasks(tmp_path / "sct_model_profile.json")
    assert set(tasks) == set(LITE_TASKS if profile == "lite" else FULL_TASKS)
    monkeypatch.setattr("test_spinalcordtoolbox_openrecon.load_tasks", lambda: tasks)
    runtime = _load_runtime_helpers_for_test(
        ["_resolve_requested_analyses", "_supported_analysis_ids"],
        ["SCT_DEEPSEG_TASKS", "SCT_ANALYSIS_REGISTRY", "SCT_ANALYSIS_BUNDLES"],
    )
    metadata = json.loads(label.read_text())
    choices = next(p["values"] for p in metadata["parameters"] if p["id"] == "analysis")
    assert {v["id"] for v in choices} == set(runtime["_supported_analysis_ids"]())
    for choice in choices:
        assert runtime["_resolve_requested_analyses"](choice["id"])
    for excluded in ("sct_deepseg_sc_mouse_t1", "sct_deepseg_gm_mouse_t1", "sct_bundle_mouse_t1"):
        with pytest.raises(ValueError, match="Unsupported SCT analysis"):
            runtime["_resolve_requested_analyses"](excluded)
    if profile == "lite":
        assert metadata["general"]["id"] == "spinalcordtoolbox_lite"
        for task in set(FULL_TASKS) - set(LITE_TASKS):
            with pytest.raises(ValueError, match="Unsupported SCT analysis"):
                runtime["_resolve_requested_analyses"](f"sct_deepseg_{task}")
        assert runtime["_resolve_requested_analyses"]("sct_bundle_t2s_gm") == (
            "sct_deepseg_spinalcord", "sct_deepseg_graymatter",
        )


def test_installer_uses_profile_tasks(monkeypatch):
    calls = []
    monkeypatch.setattr("sys.argv", ["sct_model_profile.py", "lite", "--install"])
    monkeypatch.setattr("sct_model_profile.subprocess.run", lambda argv, check: calls.append(argv))
    main()
    assert calls == [["sct_deepseg", task, "-install"] for task in LITE_TASKS]


def test_lite_menu_is_available_to_openrecon_packaging(tmp_path):
    from tools.sync_openrecon import prepare_recipe

    generated = tmp_path / "OpenReconLabel.json"
    generated.write_bytes((RECIPE_DIR / "OpenReconLabel.json").read_bytes())
    configure("lite", generated)
    target = tmp_path / "openrecon"
    result = prepare_recipe(
        RECIPE_DIR.parents[1], target, "spinalcordtoolbox", "7.3.3", variant="lite",
    )
    assert result is not None
    packaged = target / "recipes/spinalcordtoolbox_lite/OpenReconLabel.json"
    assert json.loads(packaged.read_text()) == json.loads(generated.read_text())


def test_lite_gpu_rejected_before_install(monkeypatch):
    monkeypatch.setattr("sys.argv", ["sct_model_profile.py", "lite", "--gpu", "--install"])
    monkeypatch.setattr("sct_model_profile.subprocess.run", lambda *a, **kw: pytest.fail("installer ran"))
    with pytest.raises(SystemExit):
        main()


def test_missing_manifest_fails_closed(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_tasks(tmp_path / "sct_model_profile.json")


@pytest.mark.parametrize("contents", ["not json", '{"profile": "unknown"}', '{}'])
def test_invalid_manifest_fails_closed(tmp_path, contents):
    path = tmp_path / "sct_model_profile.json"
    path.write_text(contents)
    with pytest.raises((ValueError, KeyError)):
        load_tasks(path)
