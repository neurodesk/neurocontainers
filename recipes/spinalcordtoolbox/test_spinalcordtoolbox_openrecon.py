import ast
import json
from pathlib import Path


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "spinalcordtoolbox.py"
LABEL_PATH = RECIPE_DIR / "OpenReconLabel.json"


EXPECTED_DEEPSEG_TASKS = {
    "spinalcord",
    "sc_epi",
    "sc_lumbar_t2",
    "sc_mouse_t1",
    "graymatter",
    "gm_sc_7t_t2star",
    "gm_wm_exvivo_t2",
    "gm_mouse_t1",
    "gm_wm_mouse_t1",
    "lesion_ms",
    "lesion_ms_axial_t2",
    "lesion_ms_mp2rage",
    "lesion_sci_t2",
    "tumor_edema_cavity_t1_t2",
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
    "totalspineseg",
}

EXPECTED_BATCH_PROCESSING_OPENRECON_CASES = {
    "batch_t2_deepseg_spinalcord": (
        "sct_deepseg_spinalcord",
        'sct_deepseg spinalcord -i t2.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    ),
    "batch_t2_label_vertebrae": (
        "sct_label_vertebrae",
        'sct_label_vertebrae -i t2.nii.gz -s t2_seg.nii.gz -c t2 -qc "$SCT_BP_QC_FOLDER"',
    ),
    "batch_t2s_deepseg_spinalcord": (
        "sct_deepseg_spinalcord",
        'sct_deepseg spinalcord -i t2s.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    ),
    "batch_t2s_deepseg_graymatter": (
        "sct_deepseg_graymatter",
        'sct_deepseg_gm -i t2s.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    ),
    "batch_t1_deepseg_spinalcord_t1": (
        "sct_deepseg_spinalcord",
        "sct_deepseg spinalcord -i t1.nii.gz",
    ),
    "batch_t1_deepseg_spinalcord_t2": (
        "sct_deepseg_spinalcord",
        "sct_deepseg spinalcord -i t2.nii.gz",
    ),
    "batch_mt_deepseg_spinalcord": (
        "sct_deepseg_spinalcord",
        'sct_deepseg spinalcord -i mt1_crop.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    ),
    "batch_dmri_deepseg_spinalcord": (
        "sct_deepseg_spinalcord",
        'sct_deepseg spinalcord -i dmri_moco_dwi_mean.nii.gz -qc "$SCT_BP_QC_FOLDER"',
    ),
}

EXPECTED_ANALYSIS_BUNDLES = {
    "sct_bundle_t2_anatomy": (
        "sct_deepseg_spinalcord",
        "sct_label_vertebrae",
        "sct_deepseg_sc_canal_t2",
        "sct_deepseg_totalspineseg",
    ),
    "sct_bundle_t2_ms": (
        "sct_deepseg_spinalcord",
        "sct_deepseg_lesion_ms",
        "sct_deepseg_lesion_ms_axial_t2",
    ),
    "sct_bundle_t2s_gm": (
        "sct_deepseg_spinalcord",
        "sct_deepseg_graymatter",
    ),
    "sct_bundle_mouse_t1": (
        "sct_deepseg_sc_mouse_t1",
        "sct_deepseg_gm_mouse_t1",
    ),
}


def _module_assignment(name):
    tree = ast.parse(WRAPPER_PATH.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"Could not find assignment for {name}")


def _analysis_choices():
    label = json.loads(LABEL_PATH.read_text())
    for parameter in label["parameters"]:
        if parameter["id"] == "analysis":
            return {value["id"] for value in parameter["values"]}
    raise AssertionError("OpenReconLabel.json does not define an analysis parameter")


def _label_parameter(parameter_id):
    label = json.loads(LABEL_PATH.read_text())
    for parameter in label["parameters"]:
        if parameter["id"] == parameter_id:
            return parameter
    raise AssertionError(f"OpenReconLabel.json does not define a {parameter_id} parameter")


def _function_names():
    tree = ast.parse(WRAPPER_PATH.read_text())
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def test_openrecon_exposes_all_supported_deepseg_tasks():
    deepseg_tasks = set(_module_assignment("SCT_DEEPSEG_TASKS"))
    assert deepseg_tasks == EXPECTED_DEEPSEG_TASKS

    choices = _analysis_choices()
    for task in deepseg_tasks:
        assert f"sct_deepseg_{task}" in choices
    assert "sct_label_vertebrae" in choices


def test_openrecon_exposes_combined_analysis_bundles():
    bundles = _module_assignment("SCT_ANALYSIS_BUNDLES")
    assert bundles == EXPECTED_ANALYSIS_BUNDLES

    choices = _analysis_choices()
    for bundle_id in EXPECTED_ANALYSIS_BUNDLES:
        assert bundle_id in choices

    wrapper_source = WRAPPER_PATH.read_text()
    assert "_resolve_requested_analyses" in wrapper_source
    assert "_sct_output_to_mrd_images" in wrapper_source
    assert "precomputed_outputs" in wrapper_source


def test_openrecon_exposes_segmentation_colourmap_lut_toggle():
    parameter = _label_parameter("segmentationcolormap")
    assert parameter["type"] == "boolean"
    assert parameter["default"] is False

    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert defaults["segmentationcolormap"] is False

    wrapper_source = WRAPPER_PATH.read_text()
    assert 'tmpMeta["LUTFileName"] = "MicroDeltaHotMetal.pal"' in wrapper_source
    assert 'boolean_checker(\n        "segmentationcolormap"' in wrapper_source
    assert "segmentation_colormap=segmentation_colormap" in wrapper_source


def test_wrapper_avoids_hardcoded_sct_install_version_paths():
    wrapper_source = WRAPPER_PATH.read_text()
    assert "spinalcordtoolbox-7.2" not in wrapper_source
    assert "spinalcordtoolbox-7.1" not in wrapper_source


def test_wrapper_preserves_nifti2mrd_axis_order_for_sct():
    wrapper_source = WRAPPER_PATH.read_text()
    assert "_slice_sort_indices" in wrapper_source
    assert "_estimate_slice_spacing" in wrapper_source
    assert "new_img.set_qform(affine, code=1)" in wrapper_source
    assert "new_img.set_sform(affine, code=1)" in wrapper_source
    assert "data_nifti = np.asarray(data)" in wrapper_source
    assert "data_nifti = np.asarray(data.transpose((1, 0, 2)))" not in wrapper_source


def test_batch_processing_openrecon_cases_are_declared_and_exposed():
    cases = _module_assignment("SCT_BATCH_PROCESSING_OPENRECON_CASES")
    by_name = {case["name"]: case for case in cases}
    assert set(by_name) == set(EXPECTED_BATCH_PROCESSING_OPENRECON_CASES)

    choices = _analysis_choices()
    for name, (analysis, source_command) in EXPECTED_BATCH_PROCESSING_OPENRECON_CASES.items():
        assert by_name[name]["analysis"] == analysis
        assert by_name[name]["source_command"] == source_command
        assert analysis in choices


def test_wrapper_can_generate_openrecon_configs_for_batch_processing_cases():
    wrapper_source = WRAPPER_PATH.read_text()
    function_names = _function_names()
    assert "iter_openrecon_batch_processing_test_configs" in function_names
    assert "write_openrecon_batch_processing_test_configs" in function_names
    assert "--write-openrecon-batch-processing-test-configs" in wrapper_source
    assert "_openrecon_config_for_analysis(case[\"analysis\"])" in wrapper_source
    assert '"segmentationcolormap": OPENRECON_DEFAULTS["segmentationcolormap"]' in wrapper_source
    assert "sct_deepseg_gm" in wrapper_source
