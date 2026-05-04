import ast
import copy
import json
from pathlib import Path

import numpy as np


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
    "tumor_t2",
    "rootlets",
    "sc_canal_t2",
    "totalspineseg",
}

EXPECTED_HIDDEN_ANALYSIS_CHOICES = {
    "sct_deepseg_gm_wm_mouse_t1",
    "sct_deepseg_tumor_edema_cavity_t1_t2",
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

EXPECTED_ANALYSIS_OUTPUTS = {
    "sct_deepseg_gm_sc_7t_t2star": (
        {
            "filename": "output_7t_multiclass_sc.nii.gz",
            "series_suffix": "sct_deepseg_gm_sc_7t_t2star_7t_multiclass_sc",
        },
        {
            "filename": "output_7t_multiclass_gm.nii.gz",
            "series_suffix": "sct_deepseg_gm_sc_7t_t2star_7t_multiclass_gm",
        },
    ),
    "sct_deepseg_gm_wm_exvivo_t2": (
        {
            "filename": "output_gmseg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_exvivo_t2_gmseg",
        },
        {
            "filename": "output_wmseg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_exvivo_t2_wmseg",
        },
    ),
    "sct_deepseg_gm_wm_mouse_t1": (
        {
            "filename": "output_GM_seg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_mouse_t1_gm_seg",
        },
        {
            "filename": "output_WM_seg.nii.gz",
            "series_suffix": "sct_deepseg_gm_wm_mouse_t1_wm_seg",
        },
    ),
    "sct_deepseg_lesion_ms_axial_t2": (
        {
            "filename": "output_sc_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_ms_axial_t2_sc_seg",
        },
        {
            "filename": "output_lesion_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_ms_axial_t2_lesion_seg",
        },
    ),
    "sct_deepseg_lesion_sci_t2": (
        {
            "filename": "output_lesion_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_sci_t2_lesion_seg",
        },
        {
            "filename": "output_sc_seg.nii.gz",
            "series_suffix": "sct_deepseg_lesion_sci_t2_sc_seg",
        },
    ),
    "sct_deepseg_totalspineseg": (
        {
            "filename": "output_step1_canal.nii.gz",
            "series_suffix": "sct_deepseg_totalspineseg_step1_canal",
        },
        {
            "filename": "output_step1_cord.nii.gz",
            "series_suffix": "sct_deepseg_totalspineseg_step1_cord",
        },
        {
            "filename": "output_step1_levels.nii.gz",
            "series_suffix": "sct_deepseg_totalspineseg_step1_levels",
        },
        {
            "filename": "output_step1_output.nii.gz",
            "series_suffix": "sct_deepseg_totalspineseg_step1_output",
        },
        {
            "filename": "output_step2_output.nii.gz",
            "series_suffix": "sct_deepseg_totalspineseg_step2_output",
        },
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


def _load_helper_for_test(helper_name):
    tree = ast.parse(WRAPPER_PATH.read_text())
    helper_nodes = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if names & {
                "SOURCE_PARENT_REFERENCE_META_KEYS",
                "SOURCE_PARENT_REFERENCE_META_PREFIXES",
            }:
                helper_nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == helper_name:
            helper_nodes.append(node)

    namespace = {"logging": type("Logger", (), {"warning": staticmethod(lambda *args, **kwargs: None)})}
    ast.fix_missing_locations(ast.Module(body=helper_nodes, type_ignores=[]))
    exec(compile(ast.Module(body=helper_nodes, type_ignores=[]), str(WRAPPER_PATH), "exec"), namespace)
    return namespace[helper_name]


def _load_runtime_helpers_for_test(function_names, assignments=()):
    tree = ast.parse(WRAPPER_PATH.read_text())
    helper_nodes = []
    wanted = set(function_names)
    wanted_assignments = set(assignments)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if names & wanted_assignments:
                helper_nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in wanted:
            helper_nodes.append(node)

    class FakeMeta(dict):
        def serialize(self):
            return json.dumps(dict(self))

        @staticmethod
        def deserialize(value):
            if isinstance(value, FakeMeta):
                return FakeMeta(value)
            if isinstance(value, dict):
                return FakeMeta(value)
            return FakeMeta(json.loads(value or "{}"))

    class FakeHead:
        def __init__(self, image_series_index=1, image_index=9, slice=8):
            self.image_series_index = image_series_index
            self.image_index = image_index
            self.slice = slice
            self.contrast = 0
            self.image_type = 1

    class FakeImage:
        def __init__(self, data):
            self.data = np.array(data, copy=True)
            self._head = FakeHead()
            self.attribute_string = "{}"

        @staticmethod
        def from_array(data, transpose=False):
            return FakeImage(data)

        def setHead(self, head):
            self._head = copy.deepcopy(head)

        def getHead(self):
            return self._head

    class FakeIsmrmrd:
        Image = FakeImage
        Meta = FakeMeta
        IMTYPE_MAGNITUDE = 1

    namespace = {
        "base64": __import__("base64"),
        "copy": copy,
        "ismrmrd": FakeIsmrmrd,
        "json": json,
        "logging": type(
            "Logger",
            (),
            {
                "INFO": 20,
                "warning": staticmethod(lambda *args, **kwargs: None),
                "info": staticmethod(lambda *args, **kwargs: None),
                "log": staticmethod(lambda *args, **kwargs: None),
            },
        ),
        "np": np,
        "Path": Path,
        "re": __import__("re"),
        "uuid": __import__("uuid"),
    }
    ast.fix_missing_locations(ast.Module(body=helper_nodes, type_ignores=[]))
    exec(compile(ast.Module(body=helper_nodes, type_ignores=[]), str(WRAPPER_PATH), "exec"), namespace)
    namespace["FakeHead"] = FakeHead
    namespace["FakeImage"] = FakeImage
    namespace["FakeMeta"] = FakeMeta
    return namespace


def test_openrecon_exposes_all_supported_deepseg_tasks():
    deepseg_tasks = set(_module_assignment("SCT_DEEPSEG_TASKS"))
    assert deepseg_tasks == EXPECTED_DEEPSEG_TASKS

    choices = _analysis_choices()
    for task in deepseg_tasks:
        analysis_id = f"sct_deepseg_{task}"
        if analysis_id not in EXPECTED_HIDDEN_ANALYSIS_CHOICES:
            assert analysis_id in choices
    for analysis_id in EXPECTED_HIDDEN_ANALYSIS_CHOICES:
        assert analysis_id not in choices
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


def test_wrapper_sends_openrecon_images_in_series_preserving_batches():
    wrapper_source = WRAPPER_PATH.read_text()
    function_names = _function_names()
    assert "_send_images_by_series" in function_names
    assert "OPENRECON_SEND_IMAGE_CHUNK_SIZE = 96" in wrapper_source
    assert "Sending %s batch: series_index=%s chunk=%d-%d/%d image_count=%d" in wrapper_source
    assert "connection.send_image(chunk)" in wrapper_source
    assert 'connection.send_image(image)' not in wrapper_source


def test_wrapper_validates_output_series_contract_like_musclemap():
    wrapper_source = WRAPPER_PATH.read_text()
    function_names = _function_names()
    assert "ConnectionSeriesAllocator" in wrapper_source
    assert "_build_connection_series_allocator" in function_names
    assert "_log_and_validate_output_series_contract" in function_names
    assert "_validate_output_series_contract" in function_names
    assert "SCT_OUTPUT_SERIES_CONTRACT" in wrapper_source
    assert "Invalid SCT output series contract before send" in wrapper_source
    assert "derived role {role} reuses input SeriesInstanceUID" in wrapper_source
    assert "output role {role} reuses input image_series_index" in wrapper_source
    assert "derived role {role} has Meta/IceMiniHead SeriesInstanceUID mismatch" in wrapper_source
    assert 'derived_series_allocator.allocate(output_spec["series_suffix"])' in wrapper_source
    assert 'derived_series_allocator.allocate("ORIGINAL")' in wrapper_source
    assert 'derived_series_allocator.allocate("PASSTHROUGH")' in wrapper_source
    assert "_restamp_passthrough_images" in function_names
    assert "_log_and_validate_output_series_contract(\n                output_images" in wrapper_source


def test_passthrough_restamp_uses_fresh_series_identity():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_passthrough_output_identity",
            "_clone_mrd_image",
            "_copy_meta",
            "_decode_ice_minihead",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_meta_text",
            "_json_log_default",
            "_log_json_event",
            "_restamp_passthrough_images",
            "_set_meta_scalar",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.getHead().image_series_index = 1
    image.getHead().image_index = 9
    image.getHead().slice = 8
    image.attribute_string = helpers["FakeMeta"]({
        "SeriesDescription": "source",
        "SeriesInstanceUID": "1.2.3",
        "SeriesNumberRangeNameUID": "source_group",
        "ImageTypeValue4": "M",
        "CONTROL.PSMultiFrameSOPInstanceUID": "source-parent",
    }).serialize()

    restamped = helpers["_restamp_passthrough_images"]([image], "ORIGINAL", 2)

    assert image.getHead().image_series_index == 1
    assert len(restamped) == 1
    output = restamped[0]
    output_meta = helpers["FakeMeta"].deserialize(output.attribute_string)
    assert output.getHead().image_series_index == 2
    assert output.getHead().image_index == 1
    assert output.getHead().slice == 0
    assert output_meta["SeriesInstanceUID"] != "1.2.3"
    assert output_meta["SeriesNumberRangeNameUID"] == "source_group_original"
    assert output_meta["ImageTypeValue4"] == "ORIGINAL"
    assert "CONTROL.PSMultiFrameSOPInstanceUID" not in output_meta


def test_output_series_contract_rejects_input_series_index_reuse():
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_sct_derived_roles",
            "_validate_output_series_contract",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_spinalcord": {"series_suffix": "sct_deepseg_spinalcord"}
    }
    helpers["SCT_ANALYSIS_OUTPUTS"] = {}
    helpers["RESERVED_SCANNER_SERIES_INDICES"] = {99}
    input_summary = [
        {
            "role": "M",
            "image_series_index": 1,
            "series_instance_uid": "1.2.3",
            "minihead_series_instance_uid": "1.2.3",
            "minihead_series_grouping": "source",
            "minihead_protocol_name": "source",
        }
    ]
    output_summary = [
        {
            "role": "ORIGINAL",
            "image_series_index": 1,
            "series_instance_uid": "2.25.4",
            "meta_series_instance_uid": "2.25.4",
            "minihead_series_instance_uid": "2.25.4",
            "meta_series_grouping": "source_original",
            "minihead_series_grouping": "source_original",
            "meta_protocol_name": "source_original",
            "minihead_protocol_name": "source_original",
        }
    ]

    try:
        helpers["_validate_output_series_contract"](output_summary, input_summary)
    except ValueError as exc:
        assert "output role ORIGINAL reuses input image_series_index 1" in str(exc)
    else:
        raise AssertionError("Expected validator to reject output series index reuse")


def test_get_image_series_index_logs_malformed_header_fallback():
    warnings = []
    helpers = _load_runtime_helpers_for_test(["_get_image_series_index"])
    helpers["logging"].warning = staticmethod(
        lambda *args, **kwargs: warnings.append((args, kwargs))
    )

    class BrokenImage:
        def getHead(self):
            raise RuntimeError("garbled header")

    assert helpers["_get_image_series_index"](BrokenImage()) == 0
    assert warnings
    assert "Could not read image_series_index" in warnings[0][0][0]
    assert warnings[0][1]["exc_info"] is True


def test_wrapper_logs_sct_output_statistics_before_mrd_conversion():
    wrapper_source = WRAPPER_PATH.read_text()
    assert "SCT output voxel statistics before MRD conversion" in wrapper_source
    assert "np.count_nonzero(data)" in wrapper_source
    assert "np.unique(data)" in wrapper_source


def test_openrecon_declares_multiclass_output_series():
    assert _module_assignment("SCT_ANALYSIS_OUTPUTS") == EXPECTED_ANALYSIS_OUTPUTS

    helpers = _load_runtime_helpers_for_test(
        [
            "_expected_sct_output_specs",
            "_sct_derived_roles",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
        ],
    )
    roles = helpers["_sct_derived_roles"]()
    for analysis, expected_outputs in EXPECTED_ANALYSIS_OUTPUTS.items():
        specs = helpers["_expected_sct_output_specs"](
            analysis,
            Path("/tmp/sct/output.nii.gz"),
        )
        assert tuple(
            (spec["path"].name, spec["series_suffix"])
            for spec in specs
        ) == tuple(
            (output["filename"], output["series_suffix"])
            for output in expected_outputs
        )
        for output in expected_outputs:
            assert output["series_suffix"].upper() in roles


def test_openrecon_requires_generated_multiclass_files(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_expected_sct_output_specs",
            "_require_sct_output_specs",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
        ],
    )
    for analysis, expected_outputs in EXPECTED_ANALYSIS_OUTPUTS.items():
        analysis_dir = tmp_path / analysis
        analysis_dir.mkdir()
        specs = helpers["_expected_sct_output_specs"](
            analysis,
            analysis_dir / "output.nii.gz",
        )
        for spec in specs:
            spec["path"].write_bytes(b"nii")

        assert helpers["_require_sct_output_specs"](
            analysis,
            specs,
        ) == specs

        specs[0]["path"].unlink()
        try:
            helpers["_require_sct_output_specs"](
                analysis,
                specs,
            )
        except FileNotFoundError as exc:
            assert expected_outputs[0]["filename"] in str(exc)
        else:
            raise AssertionError(f"Expected missing SCT output to fail for {analysis}")


def test_label_vertebrae_output_path_follows_segmentation_filename():
    helpers = _load_runtime_helpers_for_test(
        [
            "_nifti_output_stem",
            "_sct_label_vertebrae_output_path",
        ],
    )
    output_path = helpers["_sct_label_vertebrae_output_path"]

    assert output_path(
        Path("/tmp/openrecon/input_seg.nii.gz"),
        Path("/tmp/openrecon"),
    ) == Path("/tmp/openrecon/input_seg_labeled.nii.gz")
    assert output_path(
        Path("/tmp/openrecon/sct_deepseg_spinalcord/output.nii.gz"),
        Path("/tmp/openrecon/sct_label_vertebrae"),
    ) == Path("/tmp/openrecon/sct_label_vertebrae/output_labeled.nii.gz")


def test_wrapper_patches_openrecon_derived_series_identity_like_musclemap():
    wrapper_source = WRAPPER_PATH.read_text()
    function_names = _function_names()
    assert "_build_derived_series_instance_uid" in function_names
    assert "_patch_ice_minihead" in function_names
    assert "_replace_or_append_minihead_long_param" in function_names
    assert 'tmpMeta["ProtocolName"] = output_identity["sequence_description"]' in wrapper_source
    assert 'tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]' in wrapper_source
    assert 'tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)' in wrapper_source
    assert '"ProtocolName", sequence_description' in wrapper_source
    assert '"SeriesInstanceUID", series_instance_uid' in wrapper_source
    assert 'oldHeader.image_index = iImg + 1' in wrapper_source
    assert 'oldHeader.slice = iImg' in wrapper_source
    assert 'oldHeader.image_index = source_image_index' not in wrapper_source
    assert '_set_meta_scalar(tmpMeta, "NumberInSeries", iImg + 1)' in wrapper_source
    assert '"ChronSliceNo", iImg' in wrapper_source
    assert '"ProtocolSliceNumber", iImg' in wrapper_source
    assert 'tmpMeta["ImageSliceNormDir"]' in wrapper_source


def test_wrapper_strips_source_parent_references_from_derived_meta():
    strip_source_parent_refs = _load_helper_for_test("_strip_source_parent_refs")
    meta = {
        "MultiFrameSOPInstanceUID": "source-mf",
        "PSMultiFrameSOPInstanceUID": "source-ps-mf",
        "PSSeriesInstanceUID": "source-ps-series",
        "MFInstanceNumber": "1",
        "DicomEngineDimString": "source-dim",
        "CONTROL.MultiFrameSOPInstanceUID": "source-control-mf",
        "CONTROL.PSMultiFrameSOPInstanceUID": "source-control-ps-mf",
        "CONTROL.PSSeriesInstanceUID": "source-control-ps-series",
        "ReferencedGSPS.0.ReferencedImageSequence.0.ReferencedSOPClassUID": "source-gsps",
        "ReferencedImageSequence.0.ReferencedSOPInstanceUID": "source-sop",
        "ReferencedImageSequence.0.ReferencedSeriesInstanceUID": "source-series",
        "ReferencedImageSequence.0.ReferencedFrameNumber": "1",
        "SeriesInstanceUID": "derived-series",
        "ImageType": "DERIVED\\PRIMARY\\M\\SCT_DEEPSEG_SPINALCORD",
    }

    strip_source_parent_refs(meta)

    assert "MultiFrameSOPInstanceUID" not in meta
    assert "PSMultiFrameSOPInstanceUID" not in meta
    assert "PSSeriesInstanceUID" not in meta
    assert "MFInstanceNumber" not in meta
    assert "DicomEngineDimString" not in meta
    assert "CONTROL.MultiFrameSOPInstanceUID" not in meta
    assert "CONTROL.PSMultiFrameSOPInstanceUID" not in meta
    assert "CONTROL.PSSeriesInstanceUID" not in meta
    assert not any(key.startswith("ReferencedGSPS") for key in meta)
    assert not any("Referenced" in key and "UID" in key for key in meta)
    assert not any("Referenced" in key and "FrameNumber" in key for key in meta)
    assert meta["SeriesInstanceUID"] == "derived-series"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\SCT_DEEPSEG_SPINALCORD"


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
