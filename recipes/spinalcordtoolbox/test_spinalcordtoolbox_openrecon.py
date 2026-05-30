import ast
import base64
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
            self.matrix_size = [1, 1, 1]

    class FakeImage:
        def __init__(self, data):
            self.data = np.array(data, copy=True)
            self.data_type = 0
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
        DATATYPE_CXFLOAT = 8
        DATATYPE_CXDOUBLE = 9
        IMTYPE_COMPLEX = 2
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


def test_openrecon_does_not_expose_segmentation_postprocessing_toggle():
    label = json.loads(LABEL_PATH.read_text())
    parameter_ids = {parameter["id"] for parameter in label["parameters"]}
    assert "segmentpostprocessing" not in parameter_ids
    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert "segmentpostprocessing" not in defaults

    wrapper_source = WRAPPER_PATH.read_text()
    assert 'boolean_checker(\n        "segmentpostprocessing"' not in wrapper_source
    assert "segment_postprocessing" not in wrapper_source
    assert "source-image-header segmentation image(s)" in wrapper_source


def test_openrecon_exposes_debug_threshold_segment_toggle():
    parameter = _label_parameter("sctdebugthresholdsegment")
    assert parameter["type"] == "boolean"
    assert parameter["default"] is False

    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert defaults["sctdebugthresholdsegment"] is False

    wrapper_source = WRAPPER_PATH.read_text()
    assert 'boolean_checker(\n        "sctdebugthresholdsegment"' in wrapper_source
    assert "sctdebugthresholdsegment is enabled; skipping SCT model execution" in wrapper_source
    assert "_simple_threshold_segmentation_volume" in wrapper_source
    assert "_write_debug_threshold_sct_outputs" in wrapper_source


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
    assert "_validate_output_images" in function_names
    assert "SCT_OUTPUT_SERIES_CONTRACT" in wrapper_source
    assert "Invalid SCT output series contract before send" in wrapper_source
    assert "derived role {role} reuses input SeriesInstanceUID" in wrapper_source
    assert "output role {role} reuses input image_series_index" in wrapper_source
    assert "duplicates scanner storage key" in wrapper_source
    assert "derived role {role} has Meta/IceMiniHead SeriesInstanceUID mismatch" in wrapper_source
    assert 'derived_series_allocator.allocate(output_spec["series_suffix"])' in wrapper_source
    assert 'derived_series_allocator.allocate("ORIGINAL")' in wrapper_source
    assert 'derived_series_allocator.allocate("PASSTHROUGH")' in wrapper_source
    assert "_restamp_passthrough_images" in function_names
    assert "_log_and_validate_output_series_contract(\n                output_images" in wrapper_source


def test_process_image_allocates_original_before_segmentation_like_vesselboost():
    wrapper_source = WRAPPER_PATH.read_text()
    tree = ast.parse(wrapper_source)
    process_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "process_image"
    )
    process_source = ast.get_source_segment(wrapper_source, process_node)

    original_allocation = 'original_passthrough_index = derived_series_allocator.allocate("ORIGINAL")'
    segmentation_allocation = 'output_series_index = derived_series_allocator.allocate(output_spec["series_suffix"])'
    assert process_source.index(original_allocation) < process_source.index(segmentation_allocation)
    assert "imagesOut.extend(\n                _restamp_passthrough_images" in process_source
    assert "imagesOut = original_passthrough_images + imagesOut" not in process_source


def test_passthrough_restamp_uses_fresh_series_identity():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_passthrough_output_identity",
            "_clone_mrd_image",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_explicit_header_geometry_meta",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_meta_text",
            "_header_vector",
            "_json_log_default",
            "_log_json_event",
            "_meta_vector",
            "_patch_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_restamp_passthrough_images",
            "_sanitize_minihead_param_value",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.getHead().image_series_index = 1
    image.getHead().image_index = 9
    image.getHead().slice = 8
    image.getHead().read_dir = [1.0, 0.0, 0.0]
    image.getHead().phase_dir = [0.0, 1.0, 0.0]
    image.getHead().slice_dir = [0.0, 0.0, 1.0]
    image.getHead().position = [10.0, 20.0, 30.0]
    image.attribute_string = helpers["FakeMeta"]({
        "SeriesDescription": "source",
        "SeriesInstanceUID": "1.2.3",
        "SOPInstanceUID": "1.2.3.4.5",
        "SeriesNumberRangeNameUID": "source_group",
        "ImageTypeValue4": "M",
        "CONTROL.PSMultiFrameSOPInstanceUID": "source-parent",
        "IceMiniHead": _contract_minihead(
            sequence_description="source",
            series_grouping="source_group",
            series_uid="1.2.3",
            sop_uid="1.2.3.4.5",
            image_type_value4="M",
            image_type_value3="M",
            slice_index=8,
            image_index=9,
        ),
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
    assert output_meta["SOPInstanceUID"].startswith("2.25.")
    assert output_meta["SOPInstanceUID"] != "1.2.3.4.5"
    assert output_meta["SeriesNumberRangeNameUID"] == "source_group_original"
    assert output_meta["ImageTypeValue4"] == "ORIGINAL"
    assert output_meta["SequenceDescriptionAdditional"] == "or"
    assert output_meta["Actual3DImagePartNumber"] == "0"
    assert output_meta["AnatomicalPartitionNo"] == "0"
    assert output_meta["SliceNo"] == "0"
    assert "CONTROL.PSMultiFrameSOPInstanceUID" not in output_meta
    # Explicit per-slice geometry is stamped on originals (matches vesselboost),
    # so the scanner converter/DicomWriter can assemble the multi-frame ("concat")
    # series without closing the parent early and orphaning a frame.
    assert output_meta["ImageRowDir"] == helpers["_meta_vector"]([1.0, 0.0, 0.0])
    assert output_meta["ImageColumnDir"] == helpers["_meta_vector"]([0.0, 1.0, 0.0])
    assert output_meta["ImageSliceNormDir"] == helpers["_meta_vector"]([0.0, 0.0, 1.0])
    assert output_meta["SlicePosLightMarker"] == helpers["_meta_vector"]([10.0, 20.0, 30.0])
    # Meta and IceMiniHead identity must agree (matches vesselboost): the derived
    # SeriesDescription is re-stamped into BOTH, never left as the source value,
    # so the scanner cannot build the concat parent off a stale minihead identity.
    out_minihead = helpers["_decode_ice_minihead"](output_meta)
    assert out_minihead, "expected the restamped original to carry an IceMiniHead"
    assert output_meta["SeriesDescription"] == "source_original"
    assert (
        helpers["_extract_minihead_string_value"](out_minihead, "SeriesDescription")
        == output_meta["SeriesDescription"]
    )
    assert (
        helpers["_extract_minihead_string_value"](out_minihead, "SequenceDescription")
        == output_meta["SequenceDescription"]
    )
    assert (
        helpers["_extract_minihead_string_value"](out_minihead, "SeriesInstanceUID")
        == output_meta["SeriesInstanceUID"]
    )


def test_passthrough_non_original_role_skips_explicit_geometry():
    # _restamp_passthrough_images is also the PASSTHROUGH path for phase/unknown
    # images. Vesselboost stamps explicit geometry only on ORIGINALS, so this
    # path must not gain the new geometry tags (no scanner-visible regression).
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
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
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.getHead().image_series_index = 1
    image.getHead().image_index = 3
    image.getHead().slice = 2
    image.getHead().read_dir = [1.0, 0.0, 0.0]
    image.getHead().phase_dir = [0.0, 1.0, 0.0]
    image.getHead().slice_dir = [0.0, 0.0, 1.0]
    image.getHead().position = [10.0, 20.0, 30.0]
    image.attribute_string = helpers["FakeMeta"]({
        "SeriesDescription": "source",
        "SeriesInstanceUID": "1.2.3",
        "SOPInstanceUID": "1.2.3.4.5",
        "SeriesNumberRangeNameUID": "source_group",
        "ImageTypeValue4": "P",
    }).serialize()

    restamped = helpers["_restamp_passthrough_images"]([image], "PASSTHROUGH", 2)

    assert len(restamped) == 1
    output_meta = helpers["FakeMeta"].deserialize(restamped[0].attribute_string)
    assert restamped[0].getHead().image_series_index == 2
    assert output_meta["ImageTypeValue4"] == "PASSTHROUGH"
    # No explicit per-slice geometry tags on the non-original passthrough path.
    assert "ImageRowDir" not in output_meta
    assert "ImageColumnDir" not in output_meta
    assert "ImageSliceNormDir" not in output_meta
    assert "SlicePosLightMarker" not in output_meta


def test_sct_segment_outputs_source_geometry_2d_slices():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_sct_output_identity",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_get_meta_values",
            "_get_meta_text",
            "_patch_ice_minihead",
            "_patch_source_image_header_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_array_tokens",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_exam_data_role",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
            "_sct_output_to_source_geometry_images",
            "_source_geometry_header_image_index",
            "_source_geometry_header_slice",
            "_source_postprocessing_image_type_identity",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_spinalcord": {"series_suffix": "sct_deepseg_spinalcord"}
    }
    source_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid=f"1.2.3.4.{slice_index + 1}",
            slice_index=slice_index,
            source=True,
        )
        for slice_index in range(3)
    ]
    data = np.zeros((2, 2, 3), dtype=np.int16)
    data[:, :, 1] = 1

    outputs = helpers["_sct_output_to_source_geometry_images"](
        data,
        "sct_deepseg_spinalcord",
        source_images,
        2,
        1,
        segmentation_colormap=True,
        series_suffix="sct_deepseg_spinalcord",
    )

    assert len(outputs) == 3
    seen_sop_uids = set()
    for index, output in enumerate(outputs):
        head = output.getHead()
        meta = helpers["FakeMeta"].deserialize(output.attribute_string)
        assert head.image_series_index == 2
        assert head.image_index == index + 1
        assert head.slice == index
        assert meta["DataRole"] == "Image"
        assert meta["Keep_image_geometry"] == "1"
        assert meta["SegmentSourceGeometry"] == "1"
        assert meta["SegmentSourceImageHeader"] == "1"
        assert "SegmentPostProcessing" not in meta
        assert meta["SegmentPostProcessingChildRole"] == "2"
        assert meta["ExamDataRole"] == helpers["_format_exam_data_role_sequential_number"](2)
        assert meta["LUTFileName"] == "MicroDeltaHotMetal.pal"
        assert meta["ImageTypeValue4"] == ["NORM"]
        assert meta["ImageTypeValue3"] == "M"
        assert meta["ImageComment"] == "source_sct_deepseg_spinalcord"
        assert meta["ImageComments"] == "source_sct_deepseg_spinalcord"
        assert meta["SOPInstanceUID"].startswith("2.25.")
        assert meta["SOPInstanceUID"] not in seen_sop_uids
        seen_sop_uids.add(meta["SOPInstanceUID"])
        minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
        assert "SequentialNumber" in minihead
        assert "sct_deepseg_spinalcord" in minihead
        assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue4") == ["NORM"]


def test_sct_segment_source_header_outputs_allow_interleaved_source_slice_order():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_sct_output_identity",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_get_image_series_index",
            "_get_meta_values",
            "_get_meta_text",
            "_identity_values",
            "_image_minihead",
            "_meta_from_image",
            "_meta_int",
            "_minihead_long_value",
            "_patch_ice_minihead",
            "_patch_source_image_header_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_array_tokens",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_exam_data_role",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
            "_sct_output_to_source_geometry_images",
            "_series_contract_role",
            "_series_slice_limit",
            "_source_geometry_header_image_index",
            "_source_geometry_header_slice",
            "_source_postprocessing_image_type_identity",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_source_parent_refs",
            "_validate_identity_fields",
            "_validate_output_images",
            "_validate_storage_fields",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_spinalcord": {"series_suffix": "sct_deepseg_spinalcord"}
    }
    source_layout = [(0, 1), (2, 2), (1, 3)]
    source_images = []
    for ordinal, (slice_index, image_index) in enumerate(source_layout):
        image = _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid=f"1.2.3.4.{ordinal + 1}",
            slice_index=slice_index,
            source=True,
        )
        image.getHead().image_index = image_index
        source_images.append(image)

    outputs = helpers["_sct_output_to_source_geometry_images"](
        np.zeros((2, 2, 3), dtype=np.int16),
        "sct_deepseg_spinalcord",
        source_images,
        2,
        1,
        segmentation_colormap=False,
        series_suffix="sct_deepseg_spinalcord",
    )

    for output, (slice_index, image_index) in zip(outputs, source_layout):
        header = output.getHead()
        meta = helpers["FakeMeta"].deserialize(output.attribute_string)
        minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
        assert header.slice == slice_index
        assert header.image_index == image_index
        assert meta["SliceNo"] == str(slice_index)
        assert meta["ChronSliceNo"] == str(image_index - 1)
        assert helpers["_minihead_long_value"](minihead, "SliceNo") == slice_index
        assert helpers["_minihead_long_value"](minihead, "ChronSliceNo") == image_index - 1

    helpers["_validate_output_images"](outputs, source_images)


def test_output_series_contract_rejects_input_series_index_reuse():
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_non_empty_values",
            "_sct_derived_roles",
            "_summary_uid_values",
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


def _load_output_contract_validator_helpers():
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_non_empty_values",
            "_sct_derived_roles",
            "_summary_uid_values",
            "_validate_output_series_contract",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_spinalcord": {"series_suffix": "sct_deepseg_spinalcord"}
    }
    helpers["SCT_ANALYSIS_OUTPUTS"] = {}
    helpers["RESERVED_SCANNER_SERIES_INDICES"] = {99}
    return helpers


def _load_output_image_validator_helpers():
    return _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_decode_ice_minihead",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_image_series_index",
            "_get_meta_text",
            "_get_meta_values",
            "_identity_values",
            "_image_minihead",
            "_meta_from_image",
            "_meta_int",
            "_minihead_long_value",
            "_series_contract_role",
            "_series_slice_limit",
            "_validate_identity_fields",
            "_validate_output_images",
            "_validate_storage_fields",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
        ],
    )


def _contract_minihead(
    *,
    sequence_description,
    series_grouping,
    series_uid,
    sop_uid,
    image_type_value4,
    image_type_value3="M",
    slice_index=0,
    image_index=1,
):
    image_type_value3_param = (
        f'<ParamString."ImageTypeValue3">{{ "{image_type_value3}" }}\n'
        if image_type_value3 is not None
        else ""
    )
    minihead = f"""
<ParamString."SeriesDescription">{{ "{sequence_description}" }}
<ParamString."SequenceDescription">{{ "{sequence_description}" }}
<ParamString."ProtocolName">{{ "{sequence_description}" }}
<ParamString."SeriesNumberRangeNameUID">{{ "{series_grouping}" }}
<ParamString."SeriesInstanceUID">{{ "{series_uid}" }}
<ParamString."SOPInstanceUID">{{ "{sop_uid}" }}
{image_type_value3_param}<ParamArray."ImageTypeValue4">
{{
  {{ "{image_type_value4}" }}
}}
<ParamLong."Actual3DImagePartNumber">{{ 0 }}
<ParamLong."AnatomicalPartitionNo">{{ 0 }}
<ParamLong."AnatomicalSliceNo">{{ {slice_index} }}
<ParamLong."ChronSliceNo">{{ {slice_index} }}
<ParamLong."NumberInSeries">{{ {image_index} }}
<ParamLong."ProtocolSliceNumber">{{ {slice_index} }}
<ParamLong."SliceNo">{{ {slice_index} }}
"""
    return base64.b64encode(minihead.encode("utf-8")).decode("ascii")


def _contract_image(
    helpers,
    *,
    series_index,
    role,
    series_uid,
    sop_uid,
    slice_index,
    source=False,
):
    image = helpers["FakeImage"](np.zeros((1, 1, 2, 2), dtype=np.int16))
    head = image.getHead()
    head.image_series_index = series_index
    head.image_index = slice_index + 1
    head.slice = slice_index
    image_type_value4 = "NORM" if source else role.lower()
    sequence_description = "source" if source else f"source_{image_type_value4}"
    series_grouping = "source_group" if source else f"source_group_{image_type_value4}"
    image_type_value3 = "M" if source or role == "ORIGINAL" else None
    meta_fields = {
        "SeriesDescription": sequence_description,
        "SequenceDescription": sequence_description,
        "ProtocolName": sequence_description,
        "SeriesNumberRangeNameUID": series_grouping,
        "SeriesInstanceUID": series_uid,
        "SOPInstanceUID": sop_uid,
        "ImageTypeValue4": image_type_value4,
        "Keep_image_geometry": "1",
        "Actual3DImagePartNumber": "0",
        "AnatomicalPartitionNo": "0",
        "AnatomicalSliceNo": str(slice_index),
        "ChronSliceNo": str(slice_index),
        "NumberInSeries": str(slice_index + 1),
        "ProtocolSliceNumber": str(slice_index),
        "SliceNo": str(slice_index),
        "IsmrmrdSliceNo": str(slice_index),
    }
    if image_type_value3 is not None:
        meta_fields["ImageTypeValue3"] = image_type_value3
    meta = helpers["FakeMeta"](meta_fields)
    meta["IceMiniHead"] = _contract_minihead(
        sequence_description=sequence_description,
        series_grouping=series_grouping,
        series_uid=series_uid,
        sop_uid=sop_uid,
        image_type_value4=image_type_value4,
        image_type_value3=image_type_value3,
        slice_index=slice_index,
        image_index=slice_index + 1,
    )
    image.attribute_string = meta.serialize()
    return image


def _valid_sop_contract_input_summary():
    return [
        {
            "role": "M",
            "image_series_index": 1,
            "series_instance_uid": "1.2.3",
            "sop_instance_uids": ["1.2.3.4.1", "1.2.3.4.2"],
            "minihead_series_instance_uid": "1.2.3",
            "minihead_series_grouping": "source",
            "minihead_protocol_name": "source",
        }
    ]


def _valid_sop_contract_output_summary():
    return [
        {
            "role": "SCT_DEEPSEG_SPINALCORD",
            "image_series_index": 2,
            "series_instance_uid": "2.25.4",
            "meta_series_instance_uid": "2.25.4",
            "minihead_series_instance_uid": "2.25.4",
            "meta_sop_instance_uid": "2.25.4.1",
            "minihead_sop_instance_uid": "2.25.4.1",
            "meta_sop_instance_uids": ["2.25.4.1", "2.25.4.2"],
            "minihead_sop_instance_uids": ["2.25.4.1", "2.25.4.2"],
            "series_grouping": "source_sct_deepseg_spinalcord",
            "meta_series_grouping": "source_sct_deepseg_spinalcord",
            "minihead_series_grouping": "source_sct_deepseg_spinalcord",
            "meta_protocol_name": "source_sct_deepseg_spinalcord",
            "minihead_protocol_name": "source_sct_deepseg_spinalcord",
            "count": 2,
        }
    ]


def _assert_output_contract_rejects(output_summary, expected_error):
    helpers = _load_output_contract_validator_helpers()
    try:
        helpers["_validate_output_series_contract"](
            output_summary,
            _valid_sop_contract_input_summary(),
        )
    except ValueError as exc:
        assert expected_error in str(exc)
    else:
        raise AssertionError("Expected validator to reject invalid SOPInstanceUID contract")


def test_output_series_contract_accepts_representative_and_per_image_sop_uids():
    helpers = _load_output_contract_validator_helpers()

    helpers["_validate_output_series_contract"](
        _valid_sop_contract_output_summary(),
        _valid_sop_contract_input_summary(),
    )


def test_output_series_contract_accepts_explicit_volume_without_minihead():
    helpers = _load_output_contract_validator_helpers()
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0].update(
        {
            "count": 1,
            "meta_sop_instance_uid": "2.25.4.1",
            "meta_sop_instance_uids": ["2.25.4.1"],
            "minihead_series_instance_uid": "N/A",
            "minihead_sop_instance_uid": "N/A",
            "minihead_sop_instance_uids": [],
            "minihead_series_grouping": "N/A",
            "minihead_protocol_name": "N/A",
            "keep_image_geometry": 0,
        }
    )

    helpers["_validate_output_series_contract"](
        output_summary,
        _valid_sop_contract_input_summary(),
    )


def test_output_series_contract_rejects_missing_meta_sop_instance_uid():
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0]["meta_sop_instance_uids"] = []
    output_summary[0]["meta_sop_instance_uid"] = ""

    _assert_output_contract_rejects(
        output_summary,
        "derived role SCT_DEEPSEG_SPINALCORD is missing Meta SOPInstanceUID",
    )


def test_output_series_contract_rejects_missing_minihead_sop_instance_uid():
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0]["minihead_sop_instance_uids"] = []
    output_summary[0]["minihead_sop_instance_uid"] = ""

    _assert_output_contract_rejects(
        output_summary,
        "derived role SCT_DEEPSEG_SPINALCORD is missing IceMiniHead SOPInstanceUID",
    )


def test_output_series_contract_rejects_duplicate_sop_instance_uids():
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0]["meta_sop_instance_uids"] = ["2.25.4.1", "2.25.4.1"]
    output_summary[0]["minihead_sop_instance_uids"] = ["2.25.4.1", "2.25.4.1"]

    helpers = _load_output_contract_validator_helpers()
    try:
        helpers["_validate_output_series_contract"](
            output_summary,
            _valid_sop_contract_input_summary(),
        )
    except ValueError as exc:
        message = str(exc)
        assert (
            "derived role SCT_DEEPSEG_SPINALCORD has duplicate Meta SOPInstanceUID values"
            in message
        )
        assert (
            "derived role SCT_DEEPSEG_SPINALCORD has duplicate IceMiniHead SOPInstanceUID values"
            in message
        )
    else:
        raise AssertionError("Expected validator to reject duplicate SOPInstanceUID values")


def test_output_series_contract_rejects_meta_minihead_sop_instance_uid_mismatch():
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0]["minihead_sop_instance_uids"] = ["2.25.4.1", "2.25.4.3"]

    _assert_output_contract_rejects(
        output_summary,
        "derived role SCT_DEEPSEG_SPINALCORD has Meta/IceMiniHead SOPInstanceUID mismatch",
    )


def test_output_series_contract_rejects_input_sop_instance_uid_collision():
    output_summary = _valid_sop_contract_output_summary()
    output_summary[0]["meta_sop_instance_uids"] = ["1.2.3.4.1", "2.25.4.2"]
    output_summary[0]["minihead_sop_instance_uids"] = ["1.2.3.4.1", "2.25.4.2"]

    _assert_output_contract_rejects(
        output_summary,
        "derived role SCT_DEEPSEG_SPINALCORD reuses input SOPInstanceUID(s): ['1.2.3.4.1']",
    )


def test_output_image_validator_accepts_original_and_segmentation_slice_stacks():
    helpers = _load_output_image_validator_helpers()
    input_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid=f"1.2.3.4.{slice_index + 1}",
            slice_index=slice_index,
            source=True,
        )
        for slice_index in range(9)
    ]
    original_images = [
        _contract_image(
            helpers,
            series_index=2,
            role="ORIGINAL",
            series_uid="2.25.300",
            sop_uid=f"2.25.300.{slice_index + 1}",
            slice_index=slice_index,
        )
        for slice_index in range(9)
    ]
    segmentation_images = [
        _contract_image(
            helpers,
            series_index=3,
            role="SCT_DEEPSEG_SPINALCORD",
            series_uid="2.25.200",
            sop_uid=f"2.25.200.{slice_index + 1}",
            slice_index=slice_index,
        )
        for slice_index in range(9)
    ]

    helpers["_validate_output_images"](
        original_images + segmentation_images,
        input_images,
    )


def test_output_image_validator_rejects_descending_series_send_order():
    helpers = _load_output_image_validator_helpers()
    input_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid=f"1.2.3.4.{slice_index + 1}",
            slice_index=slice_index,
            source=True,
        )
        for slice_index in range(3)
    ]
    original_images = [
        _contract_image(
            helpers,
            series_index=3,
            role="ORIGINAL",
            series_uid="2.25.300",
            sop_uid=f"2.25.300.{slice_index + 1}",
            slice_index=slice_index,
        )
        for slice_index in range(3)
    ]
    segmentation_images = [
        _contract_image(
            helpers,
            series_index=2,
            role="SCT_DEEPSEG_SPINALCORD",
            series_uid="2.25.200",
            sop_uid=f"2.25.200.{slice_index + 1}",
            slice_index=slice_index,
        )
        for slice_index in range(3)
    ]

    try:
        helpers["_validate_output_images"](original_images + segmentation_images, input_images)
    except ValueError as exc:
        assert "image_series_index 2 after 3" in str(exc)
    else:
        raise AssertionError("descending output series order was accepted")


def test_output_image_validator_accepts_explicit_volume_without_minihead():
    helpers = _load_output_image_validator_helpers()
    input_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid=f"1.2.3.4.{slice_index + 1}",
            sop_uid=f"1.2.3.5.{slice_index + 1}",
            slice_index=slice_index,
            source=True,
        )
        for slice_index in range(9)
    ]
    output_image = _contract_image(
        helpers,
        series_index=2,
        role="SCT_DEEPSEG_SPINALCORD",
        series_uid="2.25.200",
        sop_uid="2.25.200.1",
        slice_index=0,
    )
    head = output_image.getHead()
    head.image_index = 1
    head.slice = 0
    head.matrix_size = [2, 2, 9]
    meta = helpers["FakeMeta"].deserialize(output_image.attribute_string)
    del meta["IceMiniHead"]
    meta["Keep_image_geometry"] = "0"
    meta["partition_count"] = "1"
    meta["slice_count"] = "9"
    meta["NumberOfSlices"] = "9"
    meta["ImagesInAcquisition"] = "9"
    output_image.attribute_string = meta.serialize()

    helpers["_validate_output_images"]([output_image], input_images)


def test_output_image_validator_rejects_derived_source_classifier_tokens():
    helpers = _load_output_image_validator_helpers()
    input_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid="1.2.3.4.1",
            slice_index=0,
            source=True,
        )
    ]
    output_image = _contract_image(
        helpers,
        series_index=2,
        role="SCT_DEEPSEG_SPINALCORD",
        series_uid="2.25.200",
        sop_uid="2.25.200.1",
        slice_index=0,
    )
    meta = helpers["FakeMeta"].deserialize(output_image.attribute_string)
    meta["ImageTypeValue3"] = "M"
    minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
    minihead = minihead.replace(
        '{ "sct_deepseg_spinalcord" }',
        '{ "sct_deepseg_spinalcord" }{ "DIS2D" }',
    )
    minihead = minihead + '\n<ParamString."ImageTypeValue3">{ "M" }\n'
    meta["IceMiniHead"] = base64.b64encode(minihead.encode("utf-8")).decode("ascii")
    output_image.attribute_string = meta.serialize()

    try:
        helpers["_validate_output_images"]([output_image], input_images)
    except ValueError as exc:
        message = str(exc)
        assert "expected ['sct_deepseg_spinalcord']" in message
        assert "unsafe scanner Meta ImageTypeValue3=M" in message
        assert "unsafe scanner IceMiniHead ImageTypeValue3=M" in message
    else:
        raise AssertionError("Expected validator to reject inherited derived classifier tokens")


def test_output_image_validator_rejects_duplicate_scanner_storage_key():
    helpers = _load_output_image_validator_helpers()
    input_images = [
        _contract_image(
            helpers,
            series_index=1,
            role="NORM",
            series_uid="1.2.3",
            sop_uid=f"1.2.3.4.{slice_index + 1}",
            slice_index=slice_index,
            source=True,
        )
        for slice_index in range(2)
    ]
    output_images = [
        _contract_image(
            helpers,
            series_index=2,
            role="SCT_DEEPSEG_SPINALCORD",
            series_uid="2.25.200",
            sop_uid="2.25.200.1",
            slice_index=0,
        ),
        _contract_image(
            helpers,
            series_index=2,
            role="SCT_DEEPSEG_SPINALCORD",
            series_uid="2.25.200",
            sop_uid="2.25.200.2",
            slice_index=0,
        ),
    ]

    try:
        helpers["_validate_output_images"](output_images, input_images)
    except ValueError as exc:
        assert "duplicates scanner storage key" in str(exc)
    else:
        raise AssertionError("Expected validator to reject duplicate scanner storage keys")


def test_minihead_string_extraction_prefers_writer_format_and_cleans_helper_prefix():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_string_value",
            "_first_non_empty_text",
        ],
    )

    class BrokenMrdHelper:
        @staticmethod
        def extract_minihead_string_param(minihead_text, name):
            return '{ "t2_tse_sag_spine_original'

    helpers["mrdhelper"] = BrokenMrdHelper
    minihead_text = '<ParamString."ProtocolName"> { "t2_tse_sag_}_spine_original" }'

    assert (
        helpers["_extract_minihead_string_value"](minihead_text, "ProtocolName")
        == "t2_tse_sag_}_spine_original"
    )
    assert (
        helpers["_extract_minihead_string_value"](
            "<XProtocol></XProtocol>",
            "ProtocolName",
        )
        == "t2_tse_sag_spine_original"
    )


def test_patch_minihead_replaces_sop_identity_and_keeps_partition_constant():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_patch_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
        ],
        assignments=["SCANNER_PARTITION_INDEX"],
    )
    minihead_text = """
<ParamString."SequenceDescription">{ "source" }
<ParamString."ProtocolName">{ "source" }
<ParamString."SeriesNumberRangeNameUID">{ "source_group" }
<ParamString."SeriesInstanceUID">{ "source-series" }
<ParamString."SOPInstanceUID">{ "source-sop" }
<ParamString."ImageType">{ "ORIGINAL\\PRIMARY\\M\\NORM" }
<ParamString."ImageTypeValue3">{ "M" }
<ParamArray."ImageTypeValue3">
{
  { "M" }
}
<ParamArray."ImageTypeValue4">
{
  { "NORM" }{ "DIS2D" }
}
<ParamLong."Actual3DImagePartNumber">{ 7 }
<ParamLong."AnatomicalPartitionNo">{ 7 }
<ParamLong."SliceNo">{ 7 }
"""

    patched, changed = helpers["_patch_ice_minihead"](
        minihead_text,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "NORM",
        "SCT_DEEPSEG_SPINALCORD",
        target_display_token="sct_deepseg_spinalcord",
        output_index=4,
    )

    assert changed is True
    assert helpers["_extract_minihead_string_value"](patched, "SOPInstanceUID") == "2.25.1.4"
    assert "source-sop" not in patched
    assert helpers["_extract_minihead_string_value"](
        patched,
        "ImageType",
    ) == "DERIVED\\PRIMARY\\M\\SCT_DEEPSEG_SPINALCORD"
    assert helpers["_extract_minihead_string_value"](patched, "ImageTypeValue3") == ""
    assert helpers["_extract_minihead_array_tokens"](patched, "ImageTypeValue3") == []
    assert helpers["_extract_minihead_array_tokens"](
        patched,
        "ImageTypeValue4",
    ) == ["sct_deepseg_spinalcord"]
    assert "DIS2D" not in patched
    assert '<ParamLong."Actual3DImagePartNumber">{ 0 }' in patched
    assert '<ParamLong."AnatomicalPartitionNo">{ 0 }' in patched
    assert '<ParamLong."SliceNo">{ 4 }' in patched


def test_replace_or_append_minihead_bool_param_handles_inputs():
    helpers = _load_runtime_helpers_for_test(
        ["_replace_or_append_minihead_bool_param"],
    )
    replace = helpers["_replace_or_append_minihead_bool_param"]

    # Existing "true" → false
    text = '<ParamBool."BIsSeriesEnd">{ "true" }\n'
    patched, changed = replace(text, "BIsSeriesEnd", False)
    assert changed is True
    assert '<ParamBool."BIsSeriesEnd">{ "false" }' in patched

    # Empty default → true (host treats empty as false)
    text = '<ParamBool."BIsSeriesEnd">{ }\n'
    patched, changed = replace(text, "BIsSeriesEnd", True)
    assert changed is True
    assert '<ParamBool."BIsSeriesEnd">{ "true" }' in patched

    # Already-matching → no-op
    text = '<ParamBool."BIsSeriesEnd">{ "true" }\n'
    patched, changed = replace(text, "BIsSeriesEnd", True)
    assert changed is False
    assert patched == text

    # Empty + want false → treated as already matching (default is false)
    text = '<ParamBool."BIsSeriesEnd">{ }\n'
    patched, changed = replace(text, "BIsSeriesEnd", False)
    assert changed is False
    assert patched == text

    # Missing param → append
    text = '<ParamString."SeriesDescription">{ "source" }\n'
    patched, changed = replace(text, "BIsSeriesEnd", True)
    assert changed is True
    assert '<ParamBool."BIsSeriesEnd">' in patched
    assert '"true"' in patched.split('<ParamBool."BIsSeriesEnd">')[1]


def test_patch_minihead_marks_only_last_frame_as_series_end():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_patch_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
        ],
        assignments=["SCANNER_PARTITION_INDEX"],
    )
    # Source minihead with end-of-series flags inherited from the source's
    # chronologically-last slice — i.e., the failure mode from sct.log.
    minihead_text = """
<ParamString."SequenceDescription">{ "source" }
<ParamString."ProtocolName">{ "source" }
<ParamString."SeriesNumberRangeNameUID">{ "source_group" }
<ParamString."SeriesInstanceUID">{ "source-series" }
<ParamString."SOPInstanceUID">{ "source-sop" }
<ParamString."ImageType">{ "ORIGINAL\\PRIMARY\\M\\NORM" }
<ParamArray."ImageTypeValue4">
{
  { "NORM" }
}
<ParamLong."AnatomicalSliceNo">{ 7 }
<ParamLong."SliceNo">{ 7 }
<ParamBool."BIsSeriesEnd">{ "true" }
<ParamBool."ConcatenationEnd">{ "true" }
"""

    # Frame 13 of 15 — NOT the last spatial frame. The inherited "true" flags
    # must be cleared, otherwise the host's MrDicomWriter closes the parent
    # multi-frame series early and rejects later frames (sct.log:5442-5453).
    patched_middle, _ = helpers["_patch_ice_minihead"](
        minihead_text,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "NORM",
        "SCT_DEEPSEG_SPINALCORD",
        target_display_token="sct_deepseg_spinalcord",
        output_index=13,
        is_last_in_series=False,
    )
    assert '<ParamBool."BIsSeriesEnd">{ "false" }' in patched_middle
    assert '<ParamBool."ConcatenationEnd">{ "false" }' in patched_middle

    # Last output frame keeps the end markers set.
    patched_last, _ = helpers["_patch_ice_minihead"](
        minihead_text,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "NORM",
        "SCT_DEEPSEG_SPINALCORD",
        target_display_token="sct_deepseg_spinalcord",
        output_index=14,
        is_last_in_series=True,
    )
    assert '<ParamBool."BIsSeriesEnd">{ "true" }' in patched_last
    assert '<ParamBool."ConcatenationEnd">{ "true" }' in patched_last

    # When the source minihead has no end-of-series params at all, the patcher
    # appends them so the host always sees an explicit close on the last frame.
    minihead_without_flags = """
<ParamString."SeriesDescription">{ "source" }
<ParamString."SeriesInstanceUID">{ "source-series" }
<ParamString."SOPInstanceUID">{ "source-sop" }
<ParamString."ImageType">{ "ORIGINAL\\PRIMARY\\M\\NORM" }
"""
    patched_append, _ = helpers["_patch_ice_minihead"](
        minihead_without_flags,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "NORM",
        "SCT_DEEPSEG_SPINALCORD",
        target_display_token="sct_deepseg_spinalcord",
        output_index=14,
        is_last_in_series=True,
    )
    assert '<ParamBool."BIsSeriesEnd">' in patched_append
    assert '<ParamBool."ConcatenationEnd">' in patched_append


def test_patch_source_image_header_minihead_marks_only_last_frame_as_series_end():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_patch_source_image_header_ice_minihead",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_array_tokens",
            "_replace_or_append_minihead_bool_param",
            "_replace_or_append_minihead_exam_data_role",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
        ],
        assignments=["SCANNER_PARTITION_INDEX"],
    )
    minihead_text = """
<ParamString."SeriesDescription">{ "source" }
<ParamString."SeriesInstanceUID">{ "source-series" }
<ParamString."SOPInstanceUID">{ "source-sop" }
<ParamString."ImageType">{ "ORIGINAL\\PRIMARY\\M\\NORM" }
<ParamArray."ImageTypeValue4">
{
  { "NORM" }
}
<ParamLong."AnatomicalSliceNo">{ 7 }
<ParamBool."BIsSeriesEnd">{ "true" }
<ParamBool."ConcatenationEnd">{ "true" }
"""
    exam_data_role = helpers["_format_exam_data_role_sequential_number"](3)

    patched_middle, _ = helpers["_patch_source_image_header_ice_minihead"](
        minihead_text,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "ORIGINAL\\PRIMARY\\M\\NORM",
        ["NORM"],
        exam_data_role,
        13,
        14,
        is_last_in_series=False,
    )
    assert '<ParamBool."BIsSeriesEnd">{ "false" }' in patched_middle
    assert '<ParamBool."ConcatenationEnd">{ "false" }' in patched_middle

    patched_last, _ = helpers["_patch_source_image_header_ice_minihead"](
        minihead_text,
        "source_sct_deepseg_spinalcord",
        "source_group_sct_deepseg_spinalcord",
        "2.25.1",
        "2.25.1.4",
        "ORIGINAL\\PRIMARY\\M\\NORM",
        ["NORM"],
        exam_data_role,
        14,
        15,
        is_last_in_series=True,
    )
    assert '<ParamBool."BIsSeriesEnd">{ "true" }' in patched_last
    assert '<ParamBool."ConcatenationEnd">{ "true" }' in patched_last


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


def test_debug_threshold_segmentation_keeps_largest_component_per_slice():
    helpers = _load_runtime_helpers_for_test(
        [
            "_simple_threshold_segmentation_volume",
            "_bright_foreground_threshold",
            "_largest_connected_component_per_plane",
            "_largest_connected_component_2d",
        ],
    )
    input_data = np.zeros((5, 5, 2), dtype=np.float32)
    input_data[1:3, 1:3, 0] = 10
    input_data[4, 4, 0] = 10

    output = helpers["_simple_threshold_segmentation_volume"](input_data, max_val=7)

    assert output.dtype == np.int16
    assert output.shape == input_data.shape
    assert np.all(output[1:3, 1:3, 0] == 7)
    assert output[4, 4, 0] == 0
    assert np.count_nonzero(output[:, :, 1]) == 0


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
    assert "_build_derived_sop_instance_uid" in function_names
    assert "_patch_ice_minihead" in function_names
    assert "_replace_or_append_minihead_long_param" in function_names
    assert 'tmpMeta["ProtocolName"] = output_identity["sequence_description"]' in wrapper_source
    assert 'tmpMeta["SeriesInstanceUID"] = output_identity["series_instance_uid"]' in wrapper_source
    assert 'tmpMeta["SOPInstanceUID"] = sop_instance_uid' in wrapper_source
    assert 'tmpMeta["SequenceDescriptionAdditional"] = "or"' in wrapper_source
    assert 'tmpMeta["IceMiniHead"] = _encode_ice_minihead(patched_minihead_text)' in wrapper_source
    assert '"ProtocolName", sequence_description' in wrapper_source
    assert '"SeriesInstanceUID", series_instance_uid' in wrapper_source
    assert '"SOPInstanceUID", sop_instance_uid' in wrapper_source
    assert 'oldHeader.image_index = _source_geometry_header_image_index(source_image, iImg)' in wrapper_source
    assert 'oldHeader.slice = _source_geometry_header_slice(source_image, iImg)' in wrapper_source
    assert "image_index=output_header_image_index" in wrapper_source
    assert "SCT segmentations are returned as source-geometry" in wrapper_source
    assert 'tmpMeta["Keep_image_geometry"] = 0' not in wrapper_source
    assert 'tmpMeta["Keep_image_geometry"] = "1"' in wrapper_source
    assert 'tmpMeta["SegmentSourceGeometry"] = "1"' in wrapper_source
    assert 'tmpMeta["SegmentSourceImageHeader"] = "1"' in wrapper_source
    assert 'tmpMeta["SegmentPostProcessingChildRole"] = str(int(output_series_index))' in wrapper_source
    assert 'tmpMeta["SegmentPostProcessing"] = 1' not in wrapper_source
    assert 'tmpMeta["partition_count"] = "1"' not in wrapper_source
    assert "Packed SCT output" not in wrapper_source
    assert "SCANNER_PARTITION_INDEX = 0" in wrapper_source
    assert '"Actual3DImagePartNumber", SCANNER_PARTITION_INDEX' in wrapper_source
    assert '"ChronSliceNo", output_index' in wrapper_source
    assert '"ProtocolSliceNumber", output_index' in wrapper_source
    assert 'tmpMeta["ImageSliceNormDir"]' not in wrapper_source
    assert 'text.replace("\\\\", "/")' not in wrapper_source
    # End-of-series remap (sct.log:5442-5453): every output frame must override
    # the inherited BIsSeriesEnd/ConcatenationEnd flags so the host closes the
    # multi-frame series at the actual last OUTPUT frame.
    assert "_replace_or_append_minihead_bool_param" in function_names
    assert '"BIsSeriesEnd", bool(is_last_in_series)' in wrapper_source
    assert '"ConcatenationEnd", bool(is_last_in_series)' in wrapper_source
    assert "is_last_in_series=(iImg == output_count - 1)" in wrapper_source
    assert "is_last_in_series=(iImg == len(source_images) - 1)" in wrapper_source


def test_wrapper_strips_source_parent_references_from_derived_meta():
    strip_source_parent_refs = _load_helper_for_test("_strip_source_parent_refs")
    prefixes = _module_assignment("SOURCE_PARENT_REFERENCE_META_PREFIXES")
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

    assert "ReferencedImageSequence" in prefixes

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


def test_wrapper_strips_scanner_unsafe_derived_meta():
    helpers = _load_runtime_helpers_for_test(
        ["_strip_scanner_write_unsafe_meta"],
        assignments=["SCANNER_WRITE_UNSAFE_META_KEYS"],
    )
    meta = {
        "ImageTypeValue3": "M",
        "DICOM.ImageTypeValue3": "M",
        "ImageTypeValue4": "sct_deepseg_spinalcord",
    }

    helpers["_strip_scanner_write_unsafe_meta"](meta)

    assert "ImageTypeValue3" not in meta
    assert "DICOM.ImageTypeValue3" not in meta
    assert meta["ImageTypeValue4"] == "sct_deepseg_spinalcord"


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
    assert '"sctdebugthresholdsegment": OPENRECON_DEFAULTS["sctdebugthresholdsegment"]' in wrapper_source
    assert "sct_deepseg_gm" in wrapper_source
