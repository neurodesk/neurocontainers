import ast
import json
import re
from pathlib import Path

import yaml


RECIPE_DIR = Path(__file__).resolve().parent
BUILD_PATH = RECIPE_DIR / "build.yaml"
WRAPPER_PATH = RECIPE_DIR / "spinalcordtoolbox.py"
README_PATH = RECIPE_DIR / "OpenReconREADME.md"
LABEL_PATHS = (
    RECIPE_DIR / "OpenReconLabel.json",
    RECIPE_DIR / "OpenReconLabel.gpu.json",
)

EXPECTED_DEEPSEG_TASKS = {
    "spinalcord",
    "sc_epi",
    "sc_lumbar_t2",
    "sc_mouse_t1",
    "graymatter",
    "gm_sc_7t_t2star",
    "gm_wm_exvivo_t2",
    "gm_mouse_t1",
    "lesion_ms_axial_t2",
    "lesion_ms_mp2rage",
    "lesion_sci_t2",
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
}
REMOVED_DEEPSEG_TASKS = {
    "gm_wm_mouse_t1",
    "lesion_ms",
    "spine",
    "tumor_edema_cavity_t1_t2",
}
EXPECTED_BUNDLES = {
    "sct_bundle_t2s_gm",
    "sct_bundle_mouse_t1",
}
REMOVED_BUNDLES = {
    "sct_bundle_t2_anatomy",
    "sct_bundle_t2_ms",
}


def _module_assignment(name):
    tree = ast.parse(WRAPPER_PATH.read_text())
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                return ast.literal_eval(node.value)
    raise AssertionError(f"Could not find assignment for {name}")


def _installed_deepseg_tasks():
    recipe = yaml.safe_load(BUILD_PATH.read_text())
    tasks = set()
    pattern = re.compile(r"^sct_deepseg ([a-z0-9_]+) -install$")
    for directive in recipe["build"]["directives"]:
        for command in directive.get("run", []):
            match = pattern.fullmatch(command.strip())
            if match:
                tasks.add(match.group(1))
    return tasks


def _analysis_choices(label_path):
    label = json.loads(label_path.read_text())
    analysis = next(
        parameter
        for parameter in label["parameters"]
        if parameter["id"] == "analysis"
    )
    return {value["id"] for value in analysis["values"]}


def test_installed_models_match_the_openrecon_surface():
    assert _installed_deepseg_tasks() == EXPECTED_DEEPSEG_TASKS
    assert set(_module_assignment("SCT_DEEPSEG_TASKS")) == EXPECTED_DEEPSEG_TASKS

    bundles = set(_module_assignment("SCT_ANALYSIS_BUNDLES"))
    assert bundles == EXPECTED_BUNDLES

    expected_choices = {
        *(f"sct_deepseg_{task}" for task in EXPECTED_DEEPSEG_TASKS),
        "sct_label_vertebrae",
        "sct_spinalcord_area",
        *EXPECTED_BUNDLES,
    }
    for label_path in LABEL_PATHS:
        assert _analysis_choices(label_path) == expected_choices


def test_removed_models_leave_no_openrecon_metadata():
    removed_analysis_ids = {
        *(f"sct_deepseg_{task}" for task in REMOVED_DEEPSEG_TASKS),
        *REMOVED_BUNDLES,
    }
    readme_identifiers = set(re.findall(r"`([^`]+)`", README_PATH.read_text()))
    assert readme_identifiers.isdisjoint(removed_analysis_ids)

    for label_path in LABEL_PATHS:
        choices = _analysis_choices(label_path)
        assert choices.isdisjoint(removed_analysis_ids)
