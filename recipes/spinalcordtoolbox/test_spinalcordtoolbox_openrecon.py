import ast
import base64
import copy
import importlib.util
import json
import subprocess
import zipfile
from pathlib import Path

import h5py
import nibabel as nib
import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "spinalcordtoolbox.py"
LABEL_PATH = RECIPE_DIR / "OpenReconLabel.json"
NIFTI2MRD_PATH = RECIPE_DIR / "nifti2mrd.py"


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
    "spine",
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
        "sct_deepseg_spine",
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
    "sct_deepseg_spine": (
        {
            "filename": "output_totalspineseg_discs.nii.gz",
            "series_suffix": "sct_deepseg_spine_totalspineseg_discs",
        },
        {
            "filename": "output_totalspineseg_all.nii.gz",
            "series_suffix": "sct_deepseg_spine_totalspineseg_all",
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


def _xml_escape(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _xlsx_column_name(index):
    index += 1
    letters = []
    while index:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


def _write_minimal_xlsx(path, rows):
    sheet_rows = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row):
            cell_ref = f"{_xlsx_column_name(column_index)}{row_index}"
            cells.append(
                f'<c r="{cell_ref}" t="inlineStr"><is><t>{_xml_escape(value)}</t></is></c>'
            )
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(sheet_rows)}</sheetData>'
        "</worksheet>"
    )
    with zipfile.ZipFile(path, "w") as xlsx_zip:
        xlsx_zip.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="xml" ContentType="application/xml"/>'
            "</Types>",
        )
        xlsx_zip.writestr("xl/worksheets/sheet1.xml", sheet_xml)


def _function_names():
    tree = ast.parse(WRAPPER_PATH.read_text())
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def _load_nifti2mrd_module():
    spec = importlib.util.spec_from_file_location("nifti2mrd_for_test", NIFTI2MRD_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
            self.field_of_view = [1.0, 1.0, 1.0]
            self.position = [0.0, 0.0, 0.0]
            self.read_dir = [1.0, 0.0, 0.0]
            self.phase_dir = [0.0, 1.0, 0.0]
            self.slice_dir = [0.0, 0.0, 1.0]

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
        "csv": __import__("csv"),
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
        "nib": nib,
        "np": np,
        "Path": Path,
        "re": __import__("re"),
        "subprocess": subprocess,
        "uuid": __import__("uuid"),
        "zipfile": zipfile,
        "ET": __import__("xml.etree.ElementTree", fromlist=["ElementTree"]),
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


def test_openrecon_exposes_segmentation_colourmap_lut_toggle():
    parameter = _label_parameter("segmentationcolormap")
    assert parameter["type"] == "boolean"
    assert parameter["default"] is False

    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert defaults["segmentationcolormap"] is False


def test_openrecon_does_not_expose_segmentation_postprocessing_toggle():
    label = json.loads(LABEL_PATH.read_text())
    parameter_ids = {parameter["id"] for parameter in label["parameters"]}
    assert "segmentpostprocessing" not in parameter_ids
    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert "segmentpostprocessing" not in defaults


def test_openrecon_exposes_debug_threshold_segment_toggle():
    parameter = _label_parameter("sctdebugthresholdsegment")
    assert parameter["type"] == "boolean"
    assert parameter["default"] is False

    defaults = _module_assignment("OPENRECON_DEFAULTS")
    assert defaults["sctdebugthresholdsegment"] is False


def test_repeated_slice_positions_are_split_into_sct_input_volumes():
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_sct_input_volumes",
            "_build_slice_geometry_records",
            "_format_vector",
            "_header_vector",
            "_infer_slice_axis",
            "_log_affine_slice_consistency",
            "_log_slice_geometry",
            "_log_slice_sort_mapping",
            "_normalize_vector",
            "_slice_slot_key",
            "_slice_sort_indices",
            "_split_source_images_by_volume",
        ],
    )
    images = []
    for slice_index in range(3):
        for volume_index in range(2):
            image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
            head = image.getHead()
            head.image_index = slice_index + 1
            head.slice = slice_index
            head.matrix_size = [2, 2, 3]
            head.field_of_view = [2.0, 2.0, 9.0]
            head.position = [0.0, 0.0, slice_index * 3.0]
            head.read_dir = [1.0, 0.0, 0.0]
            head.phase_dir = [0.0, 1.0, 0.0]
            head.slice_dir = [0.0, 0.0, 1.0]
            image.attribute_string = helpers["FakeMeta"]({
                "SeriesDescription": f"epi_vol_{volume_index + 1}",
            }).serialize()
            images.append(image)

    volumes = helpers["_build_sct_input_volumes"](images)

    assert len(volumes) == 2
    assert [volume["input_indices"] for volume in volumes] == [[0, 2, 4], [1, 3, 5]]
    assert [volume["series_label_suffix"] for volume in volumes] == ["vol001", "vol002"]
    assert [
        [image.getHead().slice for image in volume["images"]]
        for volume in volumes
    ] == [[0, 1, 2], [0, 1, 2]]


def test_repeated_positions_split_volumes_even_with_global_slice_numbers():
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_slice_geometry_records",
            "_header_vector",
            "_infer_slice_axis",
            "_normalize_vector",
            "_slice_slot_key",
            "_split_source_images_by_volume",
        ],
    )
    headers = []
    for volume_index in range(2):
        for slice_index in range(3):
            header = helpers["FakeHead"]()
            header.slice = volume_index * 3 + slice_index
            header.position = [0.0, 0.0, slice_index * 3.0]
            header.slice_dir = [0.0, 0.0, 1.0]
            headers.append(header)

    assert helpers["_split_source_images_by_volume"](headers) == [[0, 1, 2], [3, 4, 5]]


def test_volume_series_label_uniquifies_grouping_without_changing_role_token():
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_derived_series_instance_uid",
            "_build_passthrough_output_identity",
            "_build_sct_output_identity",
            "_decode_ice_minihead",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_meta_text",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_sc_epi": {"series_suffix": "sct_deepseg_sc_epi"}
    }
    source_meta = helpers["FakeMeta"]({
        "SeriesDescription": "rest_EPI",
        "SeriesNumberRangeNameUID": "rest_EPI_group",
        "SeriesInstanceUID": "1.2.3",
        "ImageTypeValue4": "M",
    })

    segmentation_identity = helpers["_build_sct_output_identity"](
        source_meta,
        "sct_deepseg_sc_epi",
        33,
        series_suffix="sct_deepseg_sc_epi",
        series_label_suffix="vol002",
    )
    original_identity = helpers["_build_passthrough_output_identity"](
        source_meta,
        "ORIGINAL",
        32,
        series_label_suffix="vol002",
    )

    assert segmentation_identity["series_description"] == "rest_EPI_sct_deepseg_sc_epi_vol002"
    assert segmentation_identity["grouping"] == "rest_EPI_group_sct_deepseg_sc_epi_vol002"
    assert segmentation_identity["type_token"] == "SCT_DEEPSEG_SC_EPI"
    assert segmentation_identity["display_token"] == "sct_deepseg_sc_epi"
    assert segmentation_identity["image_comment"] == "sct_deepseg_sc_epi_vol002"
    assert original_identity["series_description"] == "rest_EPI_original_vol002"
    assert original_identity["grouping"] == "rest_EPI_group_original_vol002"
    assert original_identity["type_token"] == "ORIGINAL"


def test_openrecon_exposes_spinalcord_area_analysis():
    choices = _analysis_choices()
    assert "sct_spinalcord_area" in choices

    helpers = _load_runtime_helpers_for_test(
        ["_sct_derived_roles", "_supported_analysis_ids"],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_BUNDLES",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
            "SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX",
        ],
    )
    assert "sct_spinalcord_area" in helpers["_supported_analysis_ids"]()
    assert helpers["SCT_ANALYSIS_REGISTRY"]["sct_spinalcord_area"] == {
        "kind": "spinalcord_area",
        "series_suffix": "sct_spinalcord_area",
    }
    assert "SCT_SPINALCORD_AREA_METRICS" in helpers["_sct_derived_roles"]()


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
            "_minihead_long_value",
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
            "_source_geometry_header_image_index",
            "_source_geometry_header_slice",
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
    assert output.getHead().image_index == 9
    assert output.getHead().slice == 8
    assert output_meta["SeriesInstanceUID"] != "1.2.3"
    assert output_meta["SOPInstanceUID"].startswith("2.25.")
    assert output_meta["SOPInstanceUID"] != "1.2.3.4.5"
    assert output_meta["SeriesNumberRangeNameUID"] == "source_group_original"
    assert output_meta["ImageTypeValue4"] == "ORIGINAL"
    assert output_meta["SequenceDescriptionAdditional"] == "or"
    assert output_meta["Actual3DImagePartNumber"] == "0"
    assert output_meta["AnatomicalPartitionNo"] == "0"
    assert output_meta["AnatomicalSliceNo"] == "8"
    assert output_meta["ChronSliceNo"] == "8"
    assert output_meta["NumberInSeries"] == "9"
    assert output_meta["ProtocolSliceNumber"] == "8"
    assert output_meta["SliceNo"] == "8"
    assert output_meta["IsmrmrdSliceNo"] == "8"
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
    assert helpers["_minihead_long_value"](out_minihead, "AnatomicalSliceNo") == 8
    assert helpers["_minihead_long_value"](out_minihead, "ChronSliceNo") == 8
    assert helpers["_minihead_long_value"](out_minihead, "NumberInSeries") == 9
    assert helpers["_minihead_long_value"](out_minihead, "ProtocolSliceNumber") == 8
    assert helpers["_minihead_long_value"](out_minihead, "SliceNo") == 8


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
            "_apply_sct_dicom_metrics",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_sct_lesion_analysis_metrics",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_estimate_slice_spacing",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_finite_values",
            "_first_non_empty_text",
            "_format_lesion_metrics_summary",
            "_format_optional_metric_number",
            "_format_exam_data_role_sequential_number",
            "_format_sct_metric_number",
            "_get_meta_values",
            "_get_meta_text",
            "_header_vector",
            "_infer_slice_axis",
            "_is_total_lesion_metric_row",
            "_lesion_metrics_text",
            "_metric_summary",
            "_normalize_metric_column_name",
            "_normalize_vector",
            "_patch_ice_minihead",
            "_patch_sct_metric_minihead",
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
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX",
            "SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
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
    for slice_index, image in enumerate(source_images):
        head = image.getHead()
        head.matrix_size = [2, 2, 3]
        head.field_of_view = [2.0, 2.0, 9.9]
        head.slice_dir = [1.0, 0.0, 0.0]
        head.position = [slice_index * 3.3, 0.0, 0.0]
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
        assert list(head.matrix_size) == [2, 2, 1]
        np.testing.assert_allclose(list(head.field_of_view), [2.0, 2.0, 3.3])
        assert meta["DataRole"] == "Segmentation"
        assert meta["Keep_image_geometry"] == "1"
        assert meta["SegmentSourceGeometry"] == "1"
        assert "SegmentSourceImageHeader" not in meta
        assert "SegmentPostProcessing" not in meta
        assert meta["SegmentPostProcessingChildRole"] == "2"
        assert meta["ExamDataRole"] == helpers["_format_exam_data_role_sequential_number"](2)
        assert meta["LUTFileName"] == "MicroDeltaHotMetal.pal"
        assert meta["ImageType"] == "DERIVED\\PRIMARY\\SEGMENTATION\\SCT_DEEPSEG_SPINALCORD"
        assert meta["DicomImageType"] == "DERIVED\\PRIMARY\\SEGMENTATION\\SCT_DEEPSEG_SPINALCORD"
        assert meta["ImageTypeValue4"] == ["SCT_DEEPSEG_SPINALCORD"]
        assert "ImageTypeValue3" not in meta
        assert meta["ImageComment"] == "source_sct_deepseg_spinalcord"
        assert meta["ImageComments"] == "source_sct_deepseg_spinalcord"
        assert meta["SOPInstanceUID"].startswith("2.25.")
        assert meta["SOPInstanceUID"] not in seen_sop_uids
        seen_sop_uids.add(meta["SOPInstanceUID"])
        minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
        assert "SequentialNumber" in minihead
        assert "sct_deepseg_spinalcord" in minihead
        assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue4") == [
            "SCT_DEEPSEG_SPINALCORD"
        ]
        assert helpers["_extract_minihead_string_value"](minihead, "ImageTypeValue3") == ""
        assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue3") == []


def test_sci_lesion_mask_is_embedded_as_segmentation_dicom_series():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_apply_sct_dicom_metrics",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_sct_lesion_analysis_metrics",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_estimate_slice_spacing",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_finite_values",
            "_first_non_empty_text",
            "_format_lesion_metrics_summary",
            "_format_exam_data_role_sequential_number",
            "_format_optional_metric_number",
            "_format_sct_metric_number",
            "_get_meta_values",
            "_get_meta_text",
            "_header_vector",
            "_infer_slice_axis",
            "_is_total_lesion_metric_row",
            "_lesion_metrics_text",
            "_metric_summary",
            "_normalize_metric_column_name",
            "_normalize_vector",
            "_patch_ice_minihead",
            "_patch_sct_metric_minihead",
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
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX",
            "SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_lesion_sci_t2": {
            "series_suffix": "sct_deepseg_lesion_sci_t2",
        }
    }
    source_images = [
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
    lesion_mask = np.ones((2, 2, 1), dtype=np.int16)
    metrics = helpers["_build_sct_lesion_analysis_metrics"](
        Path("/tmp/output_lesion_analysis.xlsx"),
        [
            {
                "lesion_id": "1",
                "metrics": {
                    "label": "1",
                    "volume [mm^3]": "10.5",
                    "length [mm]": "2.25",
                },
                "volume_mm3": 10.5,
                "volume_text": "10.5",
                "length_mm": 2.25,
                "length_text": "2.25",
                "max_equivalent_diameter_mm": 4.0,
                "max_equivalent_diameter_text": "4",
                "max_axial_damage_ratio": 0.42,
                "max_axial_damage_ratio_text": "0.42",
                "dorsal_bridge_width_mm": 1.1,
                "dorsal_bridge_width_text": "1.1",
                "ventral_bridge_width_mm": 1.2,
                "ventral_bridge_width_text": "1.2",
                "total_bridge_width_mm": 2.3,
                "total_bridge_width_text": "2.3",
            }
        ],
        label_path=Path("/tmp/output_lesion_label.nii.gz"),
    )

    outputs = helpers["_sct_output_to_source_geometry_images"](
        lesion_mask,
        "sct_deepseg_lesion_sci_t2",
        source_images,
        2,
        1,
        series_suffix="sct_deepseg_lesion_sci_t2_lesion_seg",
        dicom_metrics=metrics,
    )

    assert len(outputs) == 1
    meta = helpers["FakeMeta"].deserialize(outputs[0].attribute_string)
    assert meta["DataRole"] == "Segmentation"
    assert meta["Keep_image_geometry"] == "1"
    assert meta["SegmentSourceGeometry"] == "1"
    assert meta["ImageType"] == (
        "DERIVED\\PRIMARY\\SEGMENTATION\\SCT_DEEPSEG_LESION_SCI_T2_LESION_SEG"
    )
    assert meta["DicomImageType"] == (
        "DERIVED\\PRIMARY\\SEGMENTATION\\SCT_DEEPSEG_LESION_SCI_T2_LESION_SEG"
    )
    assert meta["ImageTypeValue4"] == ["SCT_DEEPSEG_LESION_SCI_T2_LESION_SEG"]
    assert "source_sct_deepseg_lesion_sci_t2_lesion_seg" in meta["ImageComment"]
    assert "SCI lesion metrics: lesions=1, total volume=10.5 mm3, max length=2.25 mm" in meta["ImageComment"]
    assert meta["SCTMetricName"] == "sci_lesion_analysis"
    assert meta["SCTMetricSource"] == "sct_analyze_lesion"
    assert meta["SCTAnalyzeLesionCount"] == "1"
    assert meta["SCTAnalyzeLesionTotalVolumeMm3"] == "10.5"
    assert meta["SCTAnalyzeLesionMaxLengthMm"] == "2.25"
    assert meta["SCTAnalyzeLesionMaxEquivalentDiameterMm"] == "4"
    assert meta["SCTAnalyzeLesionMaxAxialDamageRatio"] == "0.42"
    assert meta["SCTAnalyzeLesionMinDorsalBridgeWidthMm"] == "1.1"
    assert meta["SCTAnalyzeLesionMinVentralBridgeWidthMm"] == "1.2"
    assert meta["SCTAnalyzeLesionRows"] == "1:volume=10.5,length=2.25,max_damage=0.42"
    assert meta["SCTAnalyzeLesionXlsx"] == "/tmp/output_lesion_analysis.xlsx"
    assert meta["SCTAnalyzeLesionLabel"] == "/tmp/output_lesion_label.nii.gz"
    minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
    assert "sct_deepseg_lesion_sci_t2_lesion_seg" in minihead
    assert "SCI lesion metrics: lesions=1, total volume=10.5 mm3, max length=2.25 mm" in minihead
    assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue4") == [
        "SCT_DEEPSEG_LESION_SCI_T2_LESION_SEG"
    ]


def test_sct_segment_source_geometry_outputs_allow_interleaved_source_slice_order():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_apply_sct_dicom_metrics",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_estimate_slice_spacing",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_get_image_series_index",
            "_get_meta_values",
            "_get_meta_text",
            "_header_vector",
            "_identity_values",
            "_image_minihead",
            "_infer_slice_axis",
            "_meta_from_image",
            "_meta_int",
            "_minihead_long_value",
            "_metric_summary",
            "_normalize_vector",
            "_patch_ice_minihead",
            "_patch_sct_metric_minihead",
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
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
            "_validate_identity_fields",
            "_validate_output_images",
            "_validate_storage_fields",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY",
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


def test_nifti2mrd_preserves_sagittal_volume_geometry(tmp_path):
    nifti2mrd = _load_nifti2mrd_module()
    data = np.arange(4 * 8 * 8, dtype=np.float32).reshape((4, 8, 8))
    input_path = tmp_path / "PatSAG_Se7_Res1.0_1.0_Spac1.0.nii.gz"
    output_path = tmp_path / "sagittal.mrd.h5"
    img = nib.Nifti1Image(data, np.eye(4))
    img.set_qform(np.eye(4), code=1)
    img.set_sform(np.eye(4), code=1)
    nib.save(img, input_path)

    nifti2mrd.convert_nifti_to_ismrmrd(str(input_path), str(output_path))

    with h5py.File(output_path, "r") as h5_file:
        headers = h5_file["dataset/image_7/header"][:]
        mrd_data_shape = h5_file["dataset/image_7/data"].shape

    assert len(headers) == 4
    assert mrd_data_shape == (4, 1, 1, 8, 8)
    np.testing.assert_array_equal(headers["matrix_size"][0], [8, 8, 1])
    np.testing.assert_allclose(headers["field_of_view"][0], [8.0, 8.0, 1.0])
    np.testing.assert_array_equal(headers["slice"], np.arange(4, dtype=headers["slice"].dtype))


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


def test_read_sct_lesion_analysis_metrics_extracts_xlsx_rows(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_sct_lesion_analysis_metrics",
            "_finite_values",
            "_first_non_empty_text",
            "_format_lesion_metrics_summary",
            "_format_optional_metric_number",
            "_format_sct_metric_number",
            "_is_total_lesion_metric_row",
            "_lesion_metrics_text",
            "_metric_float_or_none",
            "_normalize_metric_column_name",
            "_read_sct_lesion_analysis_metrics",
            "_read_xlsx_table_rows",
            "_row_value_by_column_names",
            "_sct_lesion_metric_rows",
            "_table_rows_from_matrix",
            "_uniquify_table_headers",
            "_xlsx_cell_text",
            "_xlsx_column_index",
            "_xlsx_shared_strings",
            "_xlsx_sheet_matrix",
            "_xml_descendants",
            "_xml_local_name",
        ],
        assignments=[
            "SCT_ANALYZE_LESION_DORSAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_LENGTH_COLUMN",
            "SCT_ANALYZE_LESION_MAX_DAMAGE_RATIO_COLUMN",
            "SCT_ANALYZE_LESION_MAX_DIAMETER_COLUMN",
            "SCT_ANALYZE_LESION_TOTAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_VENTRAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_VOLUME_COLUMN",
            "SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX",
        ],
    )
    xlsx_path = tmp_path / "output_lesion_analysis.xlsx"
    label_path = tmp_path / "output_lesion_label.nii.gz"
    _write_minimal_xlsx(
        xlsx_path,
        [
            [
                "label",
                "volume [mm^3]",
                "length [mm]",
                "max_equivalent_diameter [mm]",
                "max_axial_damage_ratio []",
                "interpolated_dorsal_bridge_width [mm]",
                "interpolated_ventral_bridge_width [mm]",
            ],
            ["1", "10.5", "2.25", "4", "0.42", "1.1", "1.2"],
            ["2", "5", "1", "2", "0.1", "0.9", "1.5"],
        ],
    )

    metrics = helpers["_read_sct_lesion_analysis_metrics"](
        xlsx_path,
        label_path=label_path,
    )

    assert metrics["source"] == "sct_analyze_lesion"
    assert metrics["report_series_suffix"] == "sct_lesion_analysis_metrics"
    assert metrics["lesion_count"] == 2
    assert metrics["total_volume_text"] == "15.5"
    assert metrics["max_length_text"] == "2.25"
    assert metrics["max_equivalent_diameter_text"] == "4"
    assert metrics["max_axial_damage_ratio_text"] == "0.42"
    assert metrics["min_dorsal_bridge_width_text"] == "0.9"
    assert metrics["min_ventral_bridge_width_text"] == "1.2"
    assert metrics["summary"] == (
        "SCI lesion metrics: lesions=2, total volume=15.5 mm3, max length=2.25 mm"
    )
    assert metrics["lesion_metrics_text"] == (
        "1:volume=10.5,length=2.25,max_damage=0.42;"
        "2:volume=5,length=1,max_damage=0.1"
    )


def test_run_lesion_sci_t2_analysis_returns_lesion_and_cord_masks(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_attach_sci_lesion_analysis_metrics",
            "_build_sct_lesion_analysis_metrics",
            "_expected_sct_output_specs",
            "_find_sct_analyze_lesion_label",
            "_find_sct_analyze_lesion_xlsx",
            "_finite_values",
            "_first_non_empty_text",
            "_format_lesion_metrics_summary",
            "_format_optional_metric_number",
            "_format_sct_metric_number",
            "_is_total_lesion_metric_row",
            "_lesion_metrics_text",
            "_metric_float_or_none",
            "_normalize_metric_column_name",
            "_output_spec_with_series_suffix",
            "_read_sct_lesion_analysis_metrics",
            "_read_xlsx_table_rows",
            "_require_sct_output_specs",
            "_row_value_by_column_names",
            "_run_sct_analysis",
            "_sct_lesion_metric_rows",
            "_supported_analysis_ids",
            "_table_rows_from_matrix",
            "_uniquify_table_headers",
            "_xlsx_cell_text",
            "_xlsx_column_index",
            "_xlsx_shared_strings",
            "_xlsx_sheet_matrix",
            "_xml_descendants",
            "_xml_local_name",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_ANALYZE_LESION_DORSAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_LENGTH_COLUMN",
            "SCT_ANALYZE_LESION_MAX_DAMAGE_RATIO_COLUMN",
            "SCT_ANALYZE_LESION_MAX_DIAMETER_COLUMN",
            "SCT_ANALYZE_LESION_TOTAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_VENTRAL_BRIDGE_COLUMN",
            "SCT_ANALYZE_LESION_VOLUME_COLUMN",
            "SCT_DEEPSEG_TASKS",
            "SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_lesion_sci_t2": {
            "kind": "deepseg",
            "task": "lesion_sci_t2",
            "series_suffix": "sct_deepseg_lesion_sci_t2",
        }
    }
    commands = []

    def fake_run_command(command, cwd):
        commands.append((tuple(command), Path(cwd)))
        if command[0] == "sct_deepseg":
            assert command[0:2] == ["sct_deepseg", "lesion_sci_t2"]
            Path(cwd, "output_lesion_seg.nii.gz").write_bytes(b"lesion")
            Path(cwd, "output_sc_seg.nii.gz").write_bytes(b"cord")
        elif command[0] == "sct_analyze_lesion":
            analysis_dir = Path(command[command.index("-ofolder") + 1])
            _write_minimal_xlsx(
                analysis_dir / "output_lesion_analysis.xlsx",
                [
                    [
                        "label",
                        "volume [mm^3]",
                        "length [mm]",
                        "max_equivalent_diameter [mm]",
                        "max_axial_damage_ratio []",
                    ],
                    ["1", "10.5", "2.25", "4", "0.42"],
                    ["2", "5", "1", "2", "0.1"],
                ],
            )
            (analysis_dir / "output_lesion_label.nii.gz").write_bytes(b"label")
        else:
            raise AssertionError(f"Unexpected command: {command}")

    helpers["_run_command"] = fake_run_command
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    specs = helpers["_run_sct_analysis"](
        "sct_deepseg_lesion_sci_t2",
        tmp_path / "input.nii.gz",
        work_dir,
    )

    assert commands[0] == (
        (
            "sct_deepseg",
            "lesion_sci_t2",
            "-i",
            str(tmp_path / "input.nii.gz"),
            "-o",
            str(work_dir / "output.nii.gz"),
            "-qc",
            str(work_dir / "qc_singleSubj"),
        ),
        work_dir,
    )
    assert commands[1] == (
        (
            "sct_analyze_lesion",
            "-m",
            str(work_dir / "output_lesion_seg.nii.gz"),
            "-s",
            str(work_dir / "output_sc_seg.nii.gz"),
            "-ofolder",
            str(work_dir / "sct_analyze_lesion"),
            "-qc",
            str(work_dir / "qc_singleSubj"),
        ),
        work_dir,
    )
    assert [(spec["path"], spec["series_suffix"]) for spec in specs] == [
        (
            work_dir / "output_lesion_seg.nii.gz",
            "sct_deepseg_lesion_sci_t2_lesion_seg",
        ),
        (
            work_dir / "output_sc_seg.nii.gz",
            "sct_deepseg_lesion_sci_t2_sc_seg",
        ),
    ]
    assert specs[0]["dicom_metrics"]["summary"] == (
        "SCI lesion metrics: lesions=2, total volume=15.5 mm3, max length=2.25 mm"
    )
    assert specs[0]["dicom_metrics"]["xlsx_path"] == str(
        work_dir / "sct_analyze_lesion" / "output_lesion_analysis.xlsx"
    )
    assert "dicom_metrics" not in specs[1]


def test_label_vertebrae_output_path_follows_segmentation_filename():
    helpers = _load_runtime_helpers_for_test(
        [
            "_nifti_output_stem",
            "_sct_label_vertebrae_discs_output_path",
            "_sct_label_vertebrae_output_path",
        ],
    )
    output_path = helpers["_sct_label_vertebrae_output_path"]
    discs_output_path = helpers["_sct_label_vertebrae_discs_output_path"]

    assert output_path(
        Path("/tmp/openrecon/input_seg.nii.gz"),
        Path("/tmp/openrecon"),
    ) == Path("/tmp/openrecon/input_seg_labeled.nii.gz")
    assert discs_output_path(
        Path("/tmp/openrecon/input_seg.nii.gz"),
        Path("/tmp/openrecon"),
    ) == Path("/tmp/openrecon/input_seg_labeled_discs.nii.gz")
    assert output_path(
        Path("/tmp/openrecon/sct_deepseg_spinalcord/output.nii.gz"),
        Path("/tmp/openrecon/sct_label_vertebrae"),
    ) == Path("/tmp/openrecon/sct_label_vertebrae/output_labeled.nii.gz")
    assert discs_output_path(
        Path("/tmp/openrecon/sct_deepseg_spinalcord/output.nii.gz"),
        Path("/tmp/openrecon/sct_label_vertebrae"),
    ) == Path("/tmp/openrecon/sct_label_vertebrae/output_labeled_discs.nii.gz")


def test_read_sct_mean_area_extracts_process_segmentation_column(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_read_sct_metrics_csv",
            "_read_sct_mean_area",
        ],
        assignments=["SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN"],
    )
    csv_path = tmp_path / "csa.csv"
    csv_path.write_text("MEAN(area),STD(area)\n42.5,1.2\n", encoding="utf-8")

    assert helpers["_read_sct_mean_area"](csv_path) == 42.5


def test_read_sct_mean_area_rejects_missing_column(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_read_sct_metrics_csv",
            "_read_sct_mean_area",
        ],
        assignments=["SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN"],
    )
    csv_path = tmp_path / "csa.csv"
    csv_path.write_text("MEAN(other)\n42.5\n", encoding="utf-8")

    try:
        helpers["_read_sct_mean_area"](csv_path)
    except ValueError as exc:
        assert "MEAN(area)" in str(exc)
    else:
        raise AssertionError("Expected missing MEAN(area) column to fail")


def test_detected_vertebral_levels_are_read_from_label_volume(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        ["_read_detected_vertebral_levels"],
    )
    label_data = np.zeros((3, 3, 3), dtype=np.float32)
    label_data[1, 1, 0] = 4
    label_data[1, 1, 1] = 3
    label_data[1, 1, 2] = 3
    label_path = tmp_path / "output_labeled.nii.gz"
    nib.save(nib.Nifti1Image(label_data, np.eye(4)), str(label_path))

    assert helpers["_read_detected_vertebral_levels"](label_path) == [3, 4]


def test_format_vertebral_levels_for_sct_sorts_and_deduplicates():
    helpers = _load_runtime_helpers_for_test(
        ["_format_vertebral_levels_for_sct"],
    )

    assert helpers["_format_vertebral_levels_for_sct"]([4, 3, 3]) == "3,4"


def test_read_spinalcord_area_metrics_extracts_per_level_rows(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_spinalcord_area_rows",
            "_first_non_empty_text",
            "_format_sct_metric_number",
            "_read_sct_metrics_csv",
            "_read_spinalcord_area_metrics",
            "_sct_metric_row_value",
        ],
        assignments=[
            "SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN",
            "SCT_PROCESS_SEGMENTATION_VERT_LEVEL_COLUMN",
        ],
    )
    csv_path = tmp_path / "csa_perlevel.csv"
    csv_path.write_text(
        "VertLevel,MEAN(area),STD(area),Slice (I->S)\n"
        "3,42.5,1.2,1:2\n"
        "4,45,1,3:4\n",
        encoding="utf-8",
    )

    metrics = helpers["_read_spinalcord_area_metrics"](csv_path)

    assert metrics["rows"] == [
        {
            "level": "3",
            "mean_area_mm2": 42.5,
            "mean_area_text": "42.5",
            "slice": "1:2",
            "std_area": "1.2",
        },
        {
            "level": "4",
            "mean_area_mm2": 45.0,
            "mean_area_text": "45",
            "slice": "3:4",
            "std_area": "1",
        },
    ]


def test_run_spinalcord_area_analysis_segments_and_processes_csa(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_average_metric_row_area",
            "_attach_spinalcord_area_metrics",
            "_build_spinalcord_area_rows",
            "_build_spinalcord_area_metrics",
            "_expected_sct_output_specs",
            "_first_non_empty_text",
            "_format_spinalcord_area_summary",
            "_format_sct_metric_number",
            "_format_vertebral_levels_for_sct",
            "_level_area_text",
            "_nifti_output_stem",
            "_read_detected_vertebral_levels",
            "_read_sct_metrics_csv",
            "_read_sct_mean_area",
            "_read_spinalcord_area_metrics",
            "_require_sct_output_specs",
            "_run_sct_analysis",
            "_run_sct_label_vertebrae",
            "_sct_label_vertebrae_discs_output_path",
            "_sct_label_vertebrae_output_path",
            "_sct_metric_row_value",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
            "SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN",
            "SCT_PROCESS_SEGMENTATION_VERT_LEVEL_COLUMN",
            "SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_spinalcord_area": {
            "kind": "spinalcord_area",
            "series_suffix": "sct_spinalcord_area",
        }
    }
    commands = []

    def fake_run_command(command, cwd):
        commands.append((tuple(command), Path(cwd)))
        if command[0] == "sct_deepseg":
            Path(command[command.index("-o") + 1]).write_bytes(b"nii")
        elif command[0] == "sct_label_vertebrae":
            label_data = np.zeros((3, 3, 3), dtype=np.float32)
            label_data[1, 1, 0] = 3
            label_data[1, 1, 1] = 4
            nib.save(
                nib.Nifti1Image(label_data, np.eye(4)),
                str(Path(cwd) / "output_labeled.nii.gz"),
            )
            nib.save(
                nib.Nifti1Image(np.zeros((3, 3, 3), dtype=np.float32), np.eye(4)),
                str(Path(cwd) / "output_labeled_discs.nii.gz"),
            )
        elif command[0] == "sct_process_segmentation":
            Path(command[command.index("-o") + 1]).write_text(
                "VertLevel,MEAN(area),STD(area),Slice (I->S)\n"
                "3,42.5,1.2,1:2\n"
                "4,45,1,3:4\n",
                encoding="utf-8",
            )
        else:
            raise AssertionError(f"Unexpected command: {command}")

    helpers["_run_command"] = fake_run_command
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    specs = helpers["_run_sct_analysis"](
        "sct_spinalcord_area",
        tmp_path / "input.nii.gz",
        work_dir,
    )

    assert [command[0][0] for command in commands] == [
        "sct_deepseg",
        "sct_label_vertebrae",
        "sct_process_segmentation",
    ]
    assert commands[0][0][:4] == (
        "sct_deepseg",
        "spinalcord",
        "-i",
        str(tmp_path / "input.nii.gz"),
    )
    assert commands[1][0][:4] == (
        "sct_label_vertebrae",
        "-i",
        str(tmp_path / "input.nii.gz"),
        "-s",
    )
    assert commands[1][0][4] == str(work_dir / "output.nii.gz")
    assert commands[2][0][:3] == (
        "sct_process_segmentation",
        "-i",
        str(work_dir / "output.nii.gz"),
    )
    assert commands[2][0][3:] == (
        "-vert",
        "3,4",
        "-discfile",
        str(work_dir / "output_labeled_discs.nii.gz"),
        "-perlevel",
        "1",
        "-o",
        str(work_dir / "spinalcord_area.csv"),
    )
    assert len(specs) == 1
    assert specs[0]["path"] == work_dir / "output.nii.gz"
    assert specs[0]["series_suffix"] == "sct_spinalcord_area"
    assert specs[0]["dicom_metrics"]["mean_area_text"] == "43.75"
    assert specs[0]["dicom_metrics"]["level_count"] == 2
    assert specs[0]["dicom_metrics"]["level_area_text"] == "3:42.5;4:45"
    assert specs[0]["dicom_metrics"]["summary"] == (
        "Spinal cord area per level: 3=42.5, 4=45 mm2"
    )


def test_run_spinalcord_area_analysis_returns_segmentation_when_metrics_fail(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_attach_spinalcord_area_metrics",
            "_expected_sct_output_specs",
            "_format_vertebral_levels_for_sct",
            "_nifti_output_stem",
            "_require_sct_output_specs",
            "_run_sct_analysis",
            "_run_sct_label_vertebrae",
            "_sct_label_vertebrae_discs_output_path",
            "_sct_label_vertebrae_output_path",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_spinalcord_area": {
            "kind": "spinalcord_area",
            "series_suffix": "sct_spinalcord_area",
        }
    }
    commands = []

    def fake_run_command(command, cwd):
        commands.append((tuple(command), Path(cwd)))
        if command[0] == "sct_deepseg":
            Path(command[command.index("-o") + 1]).write_bytes(b"nii")
        elif command[0] == "sct_label_vertebrae":
            raise subprocess.CalledProcessError(
                1,
                command,
                output="",
                stderr="ValueError: not enough values to unpack (expected 3, got 0)",
            )
        else:
            raise AssertionError(f"Unexpected command: {command}")

    helpers["_run_command"] = fake_run_command
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    specs = helpers["_run_sct_analysis"](
        "sct_spinalcord_area",
        tmp_path / "input.nii.gz",
        work_dir,
    )

    assert [command[0][0] for command in commands] == [
        "sct_deepseg",
        "sct_label_vertebrae",
    ]
    assert len(specs) == 1
    assert specs[0]["path"] == work_dir / "output.nii.gz"
    assert specs[0]["series_suffix"] == "sct_spinalcord_area"
    assert "dicom_metrics" not in specs[0]


def test_run_rootlets_analysis_omits_qc_report(tmp_path):
    helpers = _load_runtime_helpers_for_test(
        [
            "_expected_sct_output_specs",
            "_require_sct_output_specs",
            "_run_sct_analysis",
        ],
        assignments=[
            "SCT_ANALYSIS_OUTPUTS",
            "SCT_ANALYSIS_REGISTRY",
            "SCT_DEEPSEG_TASKS",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_rootlets": {
            "kind": "deepseg",
            "task": "rootlets",
            "series_suffix": "sct_deepseg_rootlets",
        }
    }
    commands = []

    def fake_run_command(command, cwd):
        commands.append((tuple(command), Path(cwd)))
        Path(command[command.index("-o") + 1]).write_bytes(b"nii")

    helpers["_run_command"] = fake_run_command
    work_dir = tmp_path / "work"
    work_dir.mkdir()

    specs = helpers["_run_sct_analysis"](
        "sct_deepseg_rootlets",
        tmp_path / "input.nii.gz",
        work_dir,
    )

    assert commands == [
        (
            (
                "sct_deepseg",
                "rootlets",
                "-i",
                str(tmp_path / "input.nii.gz"),
                "-o",
                str(work_dir / "output.nii.gz"),
            ),
            work_dir,
        )
    ]
    assert specs == (
        {
            "path": work_dir / "output.nii.gz",
            "series_suffix": "sct_deepseg_rootlets",
        },
    )


def test_spinalcord_area_metrics_are_embedded_in_segmentation_meta():
    helpers = _load_runtime_helpers_for_test(
        [
            "_apply_sct_dicom_metrics",
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_build_spinalcord_area_metrics",
            "_collect_non_empty_texts",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_estimate_slice_spacing",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_format_spinalcord_area_summary",
            "_format_sct_metric_number",
            "_get_meta_values",
            "_get_meta_text",
            "_header_vector",
            "_infer_slice_axis",
            "_level_area_text",
            "_metric_summary",
            "_normalize_vector",
            "_patch_ice_minihead",
            "_patch_sct_metric_minihead",
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
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN",
            "SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX",
            "SCT_SEGMENT_POSTPROCESSING_CHILD_ROLE_META_KEY",
            "SCT_SEGMENT_POSTPROCESSING_META_KEY",
            "SCT_SEGMENT_SOURCE_GEOMETRY_META_KEY",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_spinalcord_area": {"series_suffix": "sct_spinalcord_area"}
    }
    source_images = [
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
    metrics = helpers["_build_spinalcord_area_metrics"](42.5, Path("/tmp/csa.csv"))

    outputs = helpers["_sct_output_to_source_geometry_images"](
        np.ones((2, 2, 1), dtype=np.int16),
        "sct_spinalcord_area",
        source_images,
        2,
        1,
        series_suffix="sct_spinalcord_area",
        dicom_metrics=metrics,
    )

    assert len(outputs) == 1
    meta = helpers["FakeMeta"].deserialize(outputs[0].attribute_string)
    assert meta["SCTMetricName"] == "spinal_cord_area"
    assert meta["SCTMetricSource"] == "sct_process_segmentation"
    assert meta["SCTProcessSegmentationColumn"] == "MEAN(area)"
    assert meta["SCTProcessSegmentationMeanAreaMm2"] == "42.5"
    assert meta["SCTProcessSegmentationMeanAreaUnits"] == "mm2"
    assert meta["SCTProcessSegmentationPerLevel"] == "0"
    assert meta["SCTProcessSegmentationLevelCount"] == "0"
    assert meta["SCTProcessSegmentationLevelAreasMm2"] == ""
    assert meta["DerivationDescription"] == "Spinal cord area MEAN(area)=42.5 mm2"
    assert "source_sct_spinalcord_area" in meta["ImageComment"]
    assert "Spinal cord area MEAN(area)=42.5 mm2" in meta["ImageComment"]
    minihead = base64.b64decode(meta["IceMiniHead"]).decode("utf-8")
    assert "Spinal cord area MEAN(area)=42.5 mm2" in minihead


def test_spinalcord_area_metrics_report_image_is_explicit_dicom_series():
    helpers = _load_runtime_helpers_for_test(
        [
            "_apply_sct_dicom_metrics",
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_build_sct_metrics_report_images",
            "_build_spinalcord_area_metrics",
            "_build_spinalcord_area_report_images",
            "_copy_meta",
            "_decode_ice_minihead",
            "_estimate_slice_spacing",
            "_explicit_header_geometry_meta",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_spinalcord_area_summary",
            "_format_sct_metric_number",
            "_get_image_series_index",
            "_get_meta_text",
            "_get_meta_values",
            "_header_vector",
            "_identity_values",
            "_image_minihead",
            "_infer_slice_axis",
            "_level_area_text",
            "_meta_from_image",
            "_meta_int",
            "_meta_vector",
            "_metric_summary",
            "_metrics_report_analysis",
            "_metrics_report_series_suffix",
            "_minihead_long_value",
            "_normalize_vector",
            "_orient_metrics_report_page",
            "_pil_text_size",
            "_read_sct_mean_area",
            "_render_sct_metrics_report_page",
            "_render_spinalcord_area_report_page",
            "_series_contract_role",
            "_series_slice_limit",
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
            "_truncate_text_to_width",
            "_validate_identity_fields",
            "_validate_output_images",
            "_validate_storage_fields",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_spinalcord_area": {"series_suffix": "sct_spinalcord_area"}
    }
    source_images = [
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
    metric_rows = [
        {
            "level": "3",
            "mean_area_mm2": 42.5,
            "mean_area_text": "42.5",
            "slice": "1:2",
            "std_area": "1.2",
        },
        {
            "level": "4",
            "mean_area_mm2": 45.0,
            "mean_area_text": "45",
            "slice": "3:4",
            "std_area": "1",
        },
    ]
    metrics = helpers["_build_spinalcord_area_metrics"](
        43.75,
        Path("/tmp/csa_perlevel.csv"),
        metric_rows=metric_rows,
        label_info={
            "labeled_path": Path("/tmp/output_labeled.nii.gz"),
            "discs_path": Path("/tmp/output_labeled_discs.nii.gz"),
        },
    )

    outputs = helpers["_build_spinalcord_area_report_images"](metrics, source_images, 3)

    assert len(outputs) == 1
    output = outputs[0]
    header = output.getHead()
    meta = helpers["FakeMeta"].deserialize(output.attribute_string)
    assert header.image_series_index == 3
    assert header.image_index == 1
    assert header.slice == 0
    assert list(header.matrix_size) == [768, 512, 1]
    np.testing.assert_allclose(list(header.field_of_view), [768.0, 512.0, 1.0])
    assert output.data.shape == (1, 512, 768)
    assert np.count_nonzero(output.data) > 0
    assert meta["DataRole"] == "Image"
    assert meta["Keep_image_geometry"] == "0"
    assert meta["partition_count"] == "1"
    assert meta["slice_count"] == "1"
    assert meta["NumberOfSlices"] == "1"
    assert meta["ImagesInAcquisition"] == "1"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\SCT_SPINALCORD_AREA_METRICS"
    assert meta["DicomImageType"] == "DERIVED\\PRIMARY\\M\\SCT_SPINALCORD_AREA_METRICS"
    assert meta["ImageTypeValue4"] == "SCT_SPINALCORD_AREA_METRICS"
    assert meta["SCTMetricReport"] == "1"
    assert meta["SCTMetricName"] == "spinal_cord_area"
    assert meta["SCTProcessSegmentationMeanAreaMm2"] == "43.75"
    assert meta["SCTProcessSegmentationPerLevel"] == "1"
    assert meta["SCTProcessSegmentationLevelCount"] == "2"
    assert meta["SCTProcessSegmentationLevels"] == "3,4"
    assert meta["SCTProcessSegmentationLevelAreasMm2"] == "3:42.5;4:45"
    assert meta["SCTProcessSegmentationVertfile"] == "/tmp/output_labeled.nii.gz"
    assert meta["SCTProcessSegmentationDiscfile"] == "/tmp/output_labeled_discs.nii.gz"
    assert meta["DerivationDescription"] == (
        "Spinal cord area per level: 3=42.5, 4=45 mm2"
    )
    assert "Spinal cord area per level: 3=42.5, 4=45 mm2" in meta["ImageComment"]
    assert "IceMiniHead" not in meta

    helpers["_validate_output_images"](outputs, source_images)


def test_sct_lesion_analysis_metrics_report_image_is_explicit_dicom_series():
    helpers = _load_runtime_helpers_for_test(
        [
            "_apply_sct_dicom_metrics",
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_slice_geometry_records",
            "_build_sct_output_identity",
            "_build_sct_metrics_report_images",
            "_copy_meta",
            "_decode_ice_minihead",
            "_estimate_slice_spacing",
            "_explicit_header_geometry_meta",
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_image_series_index",
            "_get_meta_text",
            "_get_meta_values",
            "_header_vector",
            "_identity_values",
            "_image_minihead",
            "_infer_slice_axis",
            "_meta_from_image",
            "_meta_int",
            "_meta_vector",
            "_metric_summary",
            "_metrics_report_analysis",
            "_metrics_report_series_suffix",
            "_minihead_long_value",
            "_normalize_vector",
            "_orient_metrics_report_page",
            "_pil_text_size",
            "_render_sct_lesion_analysis_report_page",
            "_render_sct_metrics_report_page",
            "_series_contract_role",
            "_series_slice_limit",
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_position_meta",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
            "_truncate_text_to_width",
            "_validate_identity_fields",
            "_validate_output_images",
            "_validate_storage_fields",
        ],
        assignments=[
            "SCANNER_PARTITION_INDEX",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
            "SCT_LESION_ANALYSIS_METRICS_SERIES_SUFFIX",
            "SCT_PROCESS_SEGMENTATION_MEAN_AREA_COLUMN",
            "SCT_SEGMENT_SOURCE_IMAGE_HEADER_META_KEY",
            "SCT_SPINALCORD_AREA_METRICS_SERIES_SUFFIX",
            "SOURCE_PARENT_REFERENCE_META_KEYS",
            "SOURCE_PARENT_REFERENCE_META_PREFIXES",
        ],
    )
    helpers["SCT_ANALYSIS_REGISTRY"] = {
        "sct_deepseg_lesion_sci_t2": {"series_suffix": "sct_deepseg_lesion_sci_t2"}
    }
    source_images = [
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
    metrics = {
        "name": "sci_lesion_analysis",
        "source": "sct_analyze_lesion",
        "analysis": "sct_deepseg_lesion_sci_t2",
        "report_kind": "sct_lesion_analysis",
        "report_series_suffix": "sct_lesion_analysis_metrics",
        "summary": "SCI lesion metrics: lesions=2, total volume=15.5 mm3, max length=2.25 mm",
        "lesion_count": 2,
        "total_volume_text": "15.5",
        "max_length_text": "2.25",
        "max_equivalent_diameter_text": "4",
        "max_axial_damage_ratio_text": "0.42",
        "min_dorsal_bridge_width_text": "0.9",
        "min_ventral_bridge_width_text": "1.2",
        "lesion_metrics_text": "1:volume=10.5,length=2.25,max_damage=0.42;2:volume=5,length=1,max_damage=0.1",
        "xlsx_path": "/tmp/output_lesion_analysis.xlsx",
        "label_path": "/tmp/output_lesion_label.nii.gz",
        "rows": [
            {
                "lesion_id": "1",
                "volume_text": "10.5",
                "length_text": "2.25",
                "max_axial_damage_ratio_text": "0.42",
                "dorsal_bridge_width_text": "1.1",
            },
            {
                "lesion_id": "2",
                "volume_text": "5",
                "length_text": "1",
                "max_axial_damage_ratio_text": "0.1",
                "dorsal_bridge_width_text": "0.9",
            },
        ],
    }

    outputs = helpers["_build_sct_metrics_report_images"](metrics, source_images, 4)

    assert len(outputs) == 1
    output = outputs[0]
    header = output.getHead()
    meta = helpers["FakeMeta"].deserialize(output.attribute_string)
    assert header.image_series_index == 4
    assert header.image_index == 1
    assert header.slice == 0
    assert list(header.matrix_size) == [768, 512, 1]
    assert output.data.shape == (1, 512, 768)
    assert np.count_nonzero(output.data) > 0
    assert meta["DataRole"] == "Image"
    assert meta["Keep_image_geometry"] == "0"
    assert meta["ImageType"] == "DERIVED\\PRIMARY\\M\\SCT_LESION_ANALYSIS_METRICS"
    assert meta["DicomImageType"] == "DERIVED\\PRIMARY\\M\\SCT_LESION_ANALYSIS_METRICS"
    assert meta["ImageTypeValue4"] == "SCT_LESION_ANALYSIS_METRICS"
    assert meta["SCTMetricReport"] == "1"
    assert meta["SCTMetricName"] == "sci_lesion_analysis"
    assert meta["SCTMetricSource"] == "sct_analyze_lesion"
    assert meta["SCTAnalyzeLesionCount"] == "2"
    assert meta["SCTAnalyzeLesionTotalVolumeMm3"] == "15.5"
    assert meta["SCTAnalyzeLesionMaxLengthMm"] == "2.25"
    assert meta["SCTAnalyzeLesionMaxAxialDamageRatio"] == "0.42"
    assert meta["SCTAnalyzeLesionRows"] == (
        "1:volume=10.5,length=2.25,max_damage=0.42;"
        "2:volume=5,length=1,max_damage=0.1"
    )
    assert meta["DerivationDescription"] == (
        "SCI lesion metrics: lesions=2, total volume=15.5 mm3, max length=2.25 mm"
    )
    assert "SCI lesion metrics: lesions=2" in meta["ImageComment"]
    assert "IceMiniHead" not in meta

    helpers["_validate_output_images"](outputs, source_images)


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
