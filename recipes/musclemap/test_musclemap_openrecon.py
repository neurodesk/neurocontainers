import ast
import base64
import copy
import json
import re
from pathlib import Path

import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "musclemap.py"
OPENRECON_LABEL_PATH = RECIPE_DIR / "OpenReconLabel.json"


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

    class FakeMrdHelper:
        @staticmethod
        def extract_minihead_string_param(minihead_text, name):
            return '{ "mrdhelper_artifact'

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
            self.matrix_size = [8, 8, 1]
            self.field_of_view = [8.0, 8.0, 1.0]
            self.position = [3.0, 4.0, 5.0]
            self.read_dir = [0.0, 1.0, 0.0]
            self.phase_dir = [1.0, 0.0, 0.0]
            self.slice_dir = [0.0, 0.0, -1.0]
            self.average = 3
            self.contrast = 0
            self.phase = 4
            self.repetition = 5
            self.set = 6
            self.image_type = 1

    class FakeImage:
        def __init__(self, data):
            self.data = np.array(data, copy=True)
            self.data_type = 2
            self._head = FakeHead()
            self.attribute_string = "{}"

        @staticmethod
        def from_array(data, transpose=False):
            return FakeImage(data)

        def setHead(self, head):
            self._head = copy.deepcopy(head)
            self.image_series_index = self._head.image_series_index

        def getHead(self):
            return self._head

    class FakeIsmrmrd:
        Image = FakeImage
        Meta = FakeMeta
        DATATYPE_CXFLOAT = 7
        DATATYPE_CXDOUBLE = 8
        IMTYPE_COMPLEX = 2
        IMTYPE_MAGNITUDE = 1

    namespace = {
        "base64": base64,
        "copy": copy,
        "ismrmrd": FakeIsmrmrd,
        "json": json,
        "logging": type(
            "Logger",
            (),
            {
                "INFO": 20,
                "info": staticmethod(lambda *args, **kwargs: None),
                "log": staticmethod(lambda *args, **kwargs: None),
                "warning": staticmethod(lambda *args, **kwargs: None),
            },
        ),
        "mrdhelper": FakeMrdHelper,
        "np": np,
        "os": __import__("os"),
        "re": re,
        "traceback": __import__("traceback"),
        "uuid": __import__("uuid"),
    }
    ast.fix_missing_locations(ast.Module(body=helper_nodes, type_ignores=[]))
    exec(
        compile(ast.Module(body=helper_nodes, type_ignores=[]), str(WRAPPER_PATH), "exec"),
        namespace,
    )
    namespace["FakeHead"] = FakeHead
    namespace["FakeImage"] = FakeImage
    namespace["FakeMeta"] = FakeMeta
    return namespace


def _encoded_minihead(
    series_name="source_W",
    series_uid="1.2.3",
    series_group="source_group",
    image_type_value4="WATER",
):
    minihead = f"""
<ParamMap."DICOM">
{{
  <ParamString."SeriesDescription"> {{ "{series_name}" }}
  <ParamString."SequenceDescription"> {{ "{series_name}" }}
  <ParamString."ProtocolName"> {{ "{series_name}" }}
  <ParamString."SeriesNumberRangeNameUID"> {{ "{series_group}" }}
  <ParamString."SeriesInstanceUID"> {{ "{series_uid}" }}
  <ParamString."SOPInstanceUID"> {{ "{series_uid}.5.8" }}
  <ParamString."ImageType"> {{ "ORIGINAL\\PRIMARY\\DIXON\\{image_type_value4}" }}
  <ParamString."ImageTypeValue3"> {{ "DIXON" }}
  <ParamArray."ImageTypeValue4">
  {{
    {{ "{image_type_value4}" }}
  }}
  <ParamLong."Actual3DImagePartNumber"> {{ 8 }}
  <ParamLong."AnatomicalPartitionNo"> {{ 8 }}
  <ParamLong."AnatomicalSliceNo"> {{ 8 }}
  <ParamLong."ChronSliceNo"> {{ 8 }}
  <ParamLong."NumberInSeries"> {{ 9 }}
  <ParamLong."ProtocolSliceNumber"> {{ 8 }}
  <ParamLong."SliceNo"> {{ 8 }}
}}
</ParamMap>
"""
    return base64.b64encode(minihead.encode("utf-8")).decode("ascii")


def test_openrecon_label_exposes_dixon_segmentation_checkboxes_with_opposed_default():
    label = json.loads(OPENRECON_LABEL_PATH.read_text())
    parameters = {parameter["id"]: parameter for parameter in label["parameters"]}

    assert len(label["parameters"]) <= 14

    expected_defaults = {
        "segmentwater": False,
        "segmentinphase": False,
        "segmentopposedphase": True,
        "segmentfat": False,
    }

    for parameter_id, default in expected_defaults.items():
        assert parameters[parameter_id]["type"] == "boolean"
        assert parameters[parameter_id]["default"] is default


def test_openrecon_label_collapses_metrics_outputs_into_one_choice():
    label = json.loads(OPENRECON_LABEL_PATH.read_text())
    parameters = {parameter["id"]: parameter for parameter in label["parameters"]}

    assert "computemetrics" not in parameters
    assert "metricsburnseries" not in parameters
    assert "metricsincomments" not in parameters
    assert "metricsinminihead" not in parameters

    metrics_output = parameters["metricsoutput"]
    assert metrics_output["type"] == "choice"
    assert metrics_output["default"] == "all"
    assert [value["id"] for value in metrics_output["values"]] == [
        "all",
        "dicom",
        "comments",
        "minihead",
        "metadata",
        "none",
    ]


def test_metrics_output_choice_maps_to_runtime_outputs_and_preserves_legacy_configs():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_config_bool",
            "_config_parameters",
            "_first_non_empty_text",
            "_get_config_bool",
            "_normalize_metrics_output_mode",
            "_resolve_metrics_output_config",
        ],
        assignments=[
            "metricsOutputModeAliases",
            "metricsOutputModeDefault",
            "metricsOutputModeFlags",
        ],
    )

    all_outputs = helpers["_resolve_metrics_output_config"](
        {"parameters": {"metricsoutput": "all"}}
    )
    assert all_outputs["compute_metrics"] is True
    assert all_outputs["metrics_burn_series"] is True
    assert all_outputs["metrics_in_comments"] is True
    assert all_outputs["metrics_in_minihead"] is True

    metadata_only = helpers["_resolve_metrics_output_config"](
        {"parameters": {"metricsoutput": "metadata"}}
    )
    assert metadata_only["compute_metrics"] is True
    assert metadata_only["metrics_burn_series"] is False
    assert metadata_only["metrics_in_comments"] is True
    assert metadata_only["metrics_in_minihead"] is True

    no_outputs = helpers["_resolve_metrics_output_config"](
        {"parameters": {"metricsoutput": "none"}}
    )
    assert no_outputs["compute_metrics"] is False
    assert no_outputs["metrics_burn_series"] is False
    assert no_outputs["metrics_in_comments"] is False
    assert no_outputs["metrics_in_minihead"] is False

    legacy_outputs = helpers["_resolve_metrics_output_config"](
        {
            "parameters": {
                "computemetrics": False,
                "metricsburnseries": False,
                "metricsincomments": True,
                "metricsinminihead": False,
            }
        }
    )
    assert legacy_outputs["mode"] == "legacy"
    assert legacy_outputs["compute_metrics"] is True
    assert legacy_outputs["metrics_burn_series"] is False
    assert legacy_outputs["metrics_in_comments"] is True
    assert legacy_outputs["metrics_in_minihead"] is False


def test_metrics_label_transform_restores_original_ids_for_anatomy_lookup():
    helpers = _load_runtime_helpers_for_test(
        [
            "_metrics_label_scale_name",
            "_metrics_segmentation_labels_for_lookup",
            "_restore_musclemap_label_values",
            "_transform_musclemap_label_values",
        ],
    )

    original_labels = np.array(
        [
            [0, 1101, 1102],
            [1120, 1121, 1122],
        ],
        dtype=np.int64,
    )

    transformed_labels = helpers["_transform_musclemap_label_values"](original_labels)
    np.testing.assert_array_equal(
        transformed_labels,
        np.array(
            [
                [0, 331, 332],
                [336, 337, 338],
            ],
            dtype=np.int64,
        ),
    )

    np.testing.assert_array_equal(
        helpers["_restore_musclemap_label_values"](transformed_labels),
        original_labels,
    )
    np.testing.assert_array_equal(
        helpers["_metrics_segmentation_labels_for_lookup"](transformed_labels, True),
        original_labels,
    )
    np.testing.assert_array_equal(
        helpers["_metrics_segmentation_labels_for_lookup"](original_labels, False),
        original_labels,
    )
    assert helpers["_metrics_label_scale_name"](True) == "original"
    assert helpers["_metrics_label_scale_name"](False) == "native"


def test_dixon_segmentation_selection_defaults_to_opposed_phase_and_supports_multi_select():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_config_bool",
            "_config_parameters",
            "_first_non_empty_text",
            "_format_selected_dixon_image_types",
            "_get_config_bool",
            "_resolve_selected_dixon_image_types",
        ],
        assignments=[
            "dixonImageTypeDefaultSelections",
            "dixonImageTypeDisplayOrder",
            "dixonImageTypeParameterIds",
        ],
    )

    assert helpers["_resolve_selected_dixon_image_types"]({}) == {"OPPOSED_PHASE"}
    assert helpers["_format_selected_dixon_image_types"]({"OPPOSED_PHASE"}) == "OPPOSED_PHASE"

    selected = helpers["_resolve_selected_dixon_image_types"](
        {
            "parameters": {
                "segmentwater": True,
                "segmentinphase": False,
                "segmentopposedphase": True,
                "segmentfat": True,
            }
        }
    )

    assert selected == {"WATER", "OPPOSED_PHASE", "FAT"}


def test_dixon_image_type_resolution_handles_siemens_opposed_phase_aliases():
    helpers = _load_runtime_helpers_for_test(
        [
            "_canonical_dixon_image_type",
            "_decode_ice_minihead",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_first_non_empty_text",
            "_get_dicom_image_type_value",
            "_get_meta_text",
            "_is_dixon_scan",
            "_meta_text_values",
            "_resolve_dixon_image_type",
        ],
        assignments=[
            "dixonImageTypeAliases",
        ],
    )

    for raw_value in ("OUT_PHASE", "OPP_PHASE", "Opposed Phase", "opposed-phase"):
        assert helpers["_canonical_dixon_image_type"](raw_value) == "OPPOSED_PHASE"
    assert helpers["_canonical_dixon_image_type"](["NORM", "DIS2D", "OUT_PHASE"]) == "OPPOSED_PHASE"

    meta = helpers["FakeMeta"](
        {
            "ImageTypeValue3": "DIXON",
            "ImageTypeValue4": "OUT_PHASE",
        }
    )

    assert helpers["_resolve_dixon_image_type"](meta) == "OPPOSED_PHASE"
    assert helpers["_is_dixon_scan"]("DIXON", "OPPOSED_PHASE") is True


def test_segmentation_display_label_uses_source_dixon_contrast_from_minihead():
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_segmentation_image_label",
            "_canonical_dixon_image_type",
            "_decode_ice_minihead",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_first_non_empty_text",
            "_format_dixon_image_label",
            "_get_dicom_image_type_value",
            "_get_meta_text",
            "_meta_text_values",
            "_resolve_dixon_image_type",
            "_resolve_source_dixon_image_type_token",
        ],
        assignments=[
            "dixonImageTypeAliases",
            "muscleMapDisplayLabel",
        ],
    )

    water_meta = helpers["FakeMeta"](
        {
            "IceMiniHead": _encoded_minihead(image_type_value4="WATER"),
        }
    )
    opposed_meta = helpers["FakeMeta"](
        {
            "IceMiniHead": _encoded_minihead(
                series_name="source_OPP",
                series_group="source_group",
                image_type_value4="OUT_PHASE",
            ),
        }
    )

    water_token = helpers["_resolve_source_dixon_image_type_token"](water_meta)
    opposed_token = helpers["_resolve_source_dixon_image_type_token"](opposed_meta)

    assert water_token == "WATER"
    assert helpers["_format_dixon_image_label"](water_token) == "Water"
    assert helpers["_build_segmentation_image_label"](water_token) == "Musclemap_Water"
    assert opposed_token == "OUT_PHASE"
    assert helpers["_format_dixon_image_label"](opposed_token) == "Opposed_Phase"
    assert (
        helpers["_build_segmentation_image_label"](opposed_token)
        == "Musclemap_Opposed_Phase"
    )


def test_segmentation_input_decision_uses_selected_dixon_checkboxes():
    helpers = _load_runtime_helpers_for_test(
        [
            "_canonical_dixon_image_type",
            "_decode_ice_minihead",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_first_non_empty_text",
            "_get_dicom_image_type_value",
            "_get_meta_text",
            "_is_dixon_scan",
            "_is_magnitude_image",
            "_meta_text_values",
            "_resolve_dixon_image_type",
            "_resolve_segmentation_input_decision",
        ],
        assignments=[
            "dixonImageTypeAliases",
        ],
    )

    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.image_type = helpers["ismrmrd"].IMTYPE_MAGNITUDE

    water_meta = helpers["FakeMeta"](
        {
            "DicomImageType": "ORIGINAL\\PRIMARY\\DIXON\\WATER",
            "ImageTypeValue3": "DIXON",
            "ImageTypeValue4": "WATER",
        }
    )
    opposed_meta = helpers["FakeMeta"](
        {
            "DicomImageType": "ORIGINAL\\PRIMARY\\DIXON\\OUT_PHASE",
            "ImageTypeValue3": "DIXON",
            "ImageTypeValue4": "OUT_PHASE",
        }
    )

    default_selection = {"OPPOSED_PHASE"}
    water_decision = helpers["_resolve_segmentation_input_decision"](
        image,
        water_meta,
        default_selection,
    )
    opposed_decision = helpers["_resolve_segmentation_input_decision"](
        image,
        opposed_meta,
        default_selection,
    )

    assert water_decision["dixon_image_type"] == "WATER"
    assert water_decision["should_process"] is False
    assert opposed_decision["dixon_image_type"] == "OPPOSED_PHASE"
    assert opposed_decision["should_process"] is True

    water_selected_decision = helpers["_resolve_segmentation_input_decision"](
        image,
        water_meta,
        {"WATER"},
    )

    assert water_selected_decision["should_process"] is True


def test_minihead_string_parser_prefers_literal_value_over_mrdhelper_artifact():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_string_value",
            "_first_non_empty_text",
        ],
    )
    minihead_text = '<ParamString."ProtocolName">\t{ "Musclemap" }\n'

    assert helpers["_extract_minihead_string_value"](minihead_text, "ProtocolName") == "Musclemap"


def test_patched_minihead_protocol_name_round_trips_for_output_contract():
    helpers = _load_runtime_helpers_for_test(
        [
            "_extract_minihead_array_tokens",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_patch_ice_minihead",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
            "_validate_output_series_contract",
        ],
        assignments=[
            "muscleMapImageTypeToken",
            "reservedScannerSeriesIndices",
            "segmentSourceGeometryImageTypeValue4",
        ],
    )
    minihead_text = """
<XProtocol>
<ParamMap."DICOM">
{
  <ParamString."SequenceDescription"> { "source_W" }
  <ParamString."SeriesNumberRangeNameUID"> { "source_W" }
  <ParamString."SeriesInstanceUID"> { "1.2.3" }
  <ParamString."ImageType"> { "DERIVED\\PRIMARY\\DIXON\\WATER" }
  <ParamArray."ImageTypeValue4">
  {
    { "NORM" }
    { "WATER" }
    { "DIS3D" }
    { "DIS2D" }
  }
}
</XProtocol>
"""

    patched_musclemap, changed_musclemap = helpers["_patch_ice_minihead"](
        minihead_text,
        parent_sequence="Musclemap",
        parent_grouping="source_Musclemap",
        series_instance_uid="2.25.1",
        source_type_token="WATER",
        target_type_token="MUSCLEMAP",
        target_display_token="MUSCLEMAP",
        target_image_type_value3="M",
    )
    patched_metrics, changed_metrics = helpers["_patch_ice_minihead"](
        minihead_text,
        parent_sequence="Musclemap_Metrics",
        parent_grouping="source_Musclemap_Metrics",
        series_instance_uid="2.25.2",
        source_type_token="WATER",
        target_type_token="METRICS",
        target_display_token="METRICS",
        target_image_type_value3="M",
    )

    assert changed_musclemap
    assert changed_metrics
    assert helpers["_extract_minihead_string_value"](patched_musclemap, "ProtocolName") == "Musclemap"
    assert helpers["_extract_minihead_string_value"](patched_metrics, "ProtocolName") == "Musclemap_Metrics"
    assert helpers["_extract_minihead_string_value"](
        patched_musclemap, "SeriesNumberRangeNameUID"
    ) == "source_Musclemap"
    assert helpers["_extract_minihead_string_value"](
        patched_metrics, "SeriesNumberRangeNameUID"
    ) == "source_Musclemap_Metrics"
    assert helpers["_extract_minihead_string_value"](patched_musclemap, "SeriesInstanceUID") == "2.25.1"
    assert helpers["_extract_minihead_string_value"](patched_metrics, "SeriesInstanceUID") == "2.25.2"
    assert helpers["_extract_minihead_string_value"](
        patched_musclemap,
        "ImageType",
    ) == "DERIVED\\PRIMARY\\M\\MUSCLEMAP"

    input_summary = [
        {
            "role": "WATER",
            "image_series_index": 1,
            "series_instance_uid": "1.2.3",
        }
    ]
    output_summary = [
        {
            "role": "MUSCLEMAP",
            "image_series_index": 2,
            "series_instance_uid": "2.25.1",
            "meta_series_instance_uid": "2.25.1",
            "minihead_series_instance_uid": helpers["_extract_minihead_string_value"](
                patched_musclemap, "SeriesInstanceUID"
            ),
            "series_grouping": "source_Musclemap",
            "meta_series_grouping": "source_Musclemap",
            "minihead_series_grouping": helpers["_extract_minihead_string_value"](
                patched_musclemap, "SeriesNumberRangeNameUID"
            ),
            "meta_protocol_name": "Musclemap",
            "minihead_protocol_name": helpers["_extract_minihead_string_value"](
                patched_musclemap, "ProtocolName"
            ),
        },
        {
            "role": "METRICS",
            "image_series_index": 3,
            "series_instance_uid": "2.25.2",
            "meta_series_instance_uid": "2.25.2",
            "minihead_series_instance_uid": helpers["_extract_minihead_string_value"](
                patched_metrics, "SeriesInstanceUID"
            ),
            "series_grouping": "source_Musclemap_Metrics",
            "meta_series_grouping": "source_Musclemap_Metrics",
            "minihead_series_grouping": helpers["_extract_minihead_string_value"](
                patched_metrics, "SeriesNumberRangeNameUID"
            ),
            "meta_protocol_name": "Musclemap_Metrics",
            "minihead_protocol_name": helpers["_extract_minihead_string_value"](
                patched_metrics, "ProtocolName"
            ),
        },
    ]

    helpers["_validate_output_series_contract"](output_summary, input_summary)


def test_original_restamp_preserves_source_native_geometry_and_processing_identity():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_image_volume_key",
            "_build_passthrough_output_identity",
            "_clone_mrd_image",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_extract_minihead_long_value",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_dicom_image_type_value",
            "_get_first_meta_int",
            "_get_meta_text",
            "_ensure_minihead_long_param",
            "_ensure_original_storage_meta",
            "_meta_text_values",
            "_normalize_original_meta_image_type_value3",
            "_normalize_original_minihead_image_type_value3",
            "_original_passthrough_meta",
            "_original_storage_fields",
            "_patch_ice_minihead",
            "_patch_original_ice_minihead",
            "_patch_output_minihead_storage",
            "_output_meta",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_string_param",
            "_resolve_source_series_identity",
            "_restamp_passthrough_images",
            "_sanitize_minihead_param_value",
            "_set_meta_scalar",
            "_set_output_storage_meta",
            "_stamp_original_image",
            "_stamp_output_header",
            "_stamp_output_image",
            "_strip_dixon_series_suffix",
            "_strip_source_parent_refs",
        ],
        assignments=[
            "originalImageTypeValue3",
            "scannerPartitionIndex",
            "singlePartitionScannerFields",
            "sourceParentReferenceMetaKeys",
            "sourceParentReferenceMetaPrefixes",
        ],
    )

    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.getHead().image_series_index = 1
    image.getHead().image_index = 9
    image.getHead().slice = 8
    image.attribute_string = helpers["FakeMeta"](
        {
            "SeriesDescription": "source_W",
            "SequenceDescription": "source_W",
            "ProtocolName": "source_W",
            "SeriesInstanceUID": "1.2.3",
            "SOPInstanceUID": "1.2.3.5.8",
            "SeriesNumberRangeNameUID": "source_group",
            "ImageTypeValue4": "WATER",
            "SequenceDescriptionAdditional": "source_extra",
            "PSSeriesInstanceUID": "source-parent",
            "ReferencedImageSequence.Item.SOPInstanceUID": "referenced-source",
            "IceMiniHead": _encoded_minihead(),
        }
    ).serialize()

    restamped = helpers["_restamp_passthrough_images"]([image], "ORIGINAL", 2)

    assert image.getHead().image_series_index == 1
    assert len(restamped) == 1
    output = restamped[0]
    output_meta = helpers["FakeMeta"].deserialize(output.attribute_string)
    minihead = helpers["_decode_ice_minihead"](output_meta)

    assert output.getHead().image_series_index == 2
    assert output.image_series_index == 2
    assert output.getHead().image_index == 9
    assert output.getHead().slice == 8
    assert output.getHead().contrast == 0
    assert output_meta["SeriesDescription"] == "source_W"
    assert output_meta["SequenceDescription"] == "source_W"
    assert output_meta["ProtocolName"] == "source_W"
    assert output_meta["SeriesNumberRangeNameUID"] == "source_group_original"
    assert output_meta["SeriesInstanceUID"].startswith("2.25.")
    assert output_meta["SeriesInstanceUID"] != "1.2.3"
    assert output_meta["SOPInstanceUID"].startswith("2.25.")
    assert output_meta["SOPInstanceUID"] != "1.2.3.5.8"
    assert "ImageType" not in output_meta
    assert output_meta["ImageTypeValue3"] == "M"
    assert output_meta["ImageTypeValue4"] == "WATER"
    assert "DicomImageType" not in output_meta
    assert output_meta["SequenceDescriptionAdditional"] == "source_extra"
    assert output_meta["Keep_image_geometry"] == 1
    assert output_meta["Actual3DImagePartNumber"] == "0"
    assert output_meta["AnatomicalPartitionNo"] == "0"
    assert output_meta["AnatomicalSliceNo"] == "8"
    assert output_meta["ChronSliceNo"] == "8"
    assert output_meta["NumberInSeries"] == "9"
    assert output_meta["ProtocolSliceNumber"] == "8"
    assert output_meta["SliceNo"] == "8"
    assert output_meta["IsmrmrdSliceNo"] == "8"
    assert "PSSeriesInstanceUID" not in output_meta
    assert "ReferencedImageSequence.Item.SOPInstanceUID" not in output_meta
    assert helpers["_extract_minihead_string_value"](minihead, "SeriesDescription") == "source_W"
    assert helpers["_extract_minihead_string_value"](minihead, "SequenceDescription") == "source_W"
    assert helpers["_extract_minihead_string_value"](minihead, "ProtocolName") == "source_W"
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "SeriesNumberRangeNameUID",
    ) == "source_group_original"
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "SeriesInstanceUID",
    ) == output_meta["SeriesInstanceUID"]
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "SOPInstanceUID",
    ) == output_meta["SOPInstanceUID"]
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "ImageType",
    ) == "ORIGINAL\\PRIMARY\\DIXON\\WATER"
    assert helpers["_extract_minihead_string_value"](minihead, "ImageTypeValue3") == "M"
    assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue4") == ["WATER"]
    assert helpers["_extract_minihead_long_value"](minihead, "Actual3DImagePartNumber") == 0
    assert helpers["_extract_minihead_long_value"](minihead, "AnatomicalPartitionNo") == 0
    assert helpers["_extract_minihead_long_value"](minihead, "SliceNo") == 8
    assert helpers["_extract_minihead_long_value"](minihead, "NumberInSeries") == 9


def test_segmentation_stamp_uses_2d_source_geometry_identity():
    helpers = _load_runtime_helpers_for_test(
        [
            "_build_derived_sop_instance_uid",
            "_copy_meta",
            "_decode_ice_minihead",
            "_encode_ice_minihead",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_extract_minihead_long_value",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_format_exam_data_role_sequential_number",
            "_get_dicom_image_type_value",
            "_get_meta_text",
            "_meta_text_values",
            "_minihead_string_literal",
            "_normalise_identity_tokens",
            "_patch_ice_minihead",
            "_patch_output_minihead_storage",
            "_patch_segment_postprocessing_ice_minihead",
            "_remove_minihead_array_param",
            "_remove_minihead_string_param",
            "_replace_minihead_array_token",
            "_replace_or_append_minihead_array_token",
            "_replace_or_append_minihead_array_tokens",
            "_replace_or_append_minihead_long_param",
            "_replace_or_append_minihead_raw_string_param",
            "_replace_or_append_minihead_string_param",
            "_sanitize_minihead_param_value",
            "_segment_postprocessing_child_role",
            "_set_meta_scalar",
            "_set_output_storage_meta",
            "_stamp_output_header",
            "_stamp_output_image",
            "_strip_scanner_write_unsafe_meta",
            "_strip_scanner_write_unsafe_minihead",
            "_strip_source_parent_refs",
            "_output_meta",
        ],
        assignments=[
            "muscleMapImageTypeToken",
            "scannerPartitionIndex",
            "scannerWriteUnsafeMetaKeys",
            "segmentPostProcessingChildRoleMetaKey",
            "segmentPostProcessingMetaKey",
            "segmentSourceGeometryImageType",
            "segmentSourceGeometryImageTypeValue4",
            "segmentSourceGeometryMetaKey",
            "sourceGroupHeaderFields",
            "sourceParentReferenceMetaKeys",
            "sourceParentReferenceMetaPrefixes",
        ],
    )

    output = helpers["FakeImage"](np.ones((1, 2, 2), dtype=np.int16))
    output_header = output.getHead()
    output_header.image_series_index = 1
    output_header.image_index = 9
    output_header.slice = 8
    output_header.average = 3
    output_header.contrast = 4
    output_header.phase = 5
    output_header.repetition = 6
    output_header.set = 7
    source_meta = helpers["FakeMeta"](
        {
            "SeriesDescription": "source_W",
            "SequenceDescription": "source_W",
            "ProtocolName": "source_W",
            "SeriesInstanceUID": "1.2.3",
            "SOPInstanceUID": "1.2.3.5.8",
            "SeriesNumberRangeNameUID": "source_group",
            "ImageType": "ORIGINAL\\PRIMARY\\DIXON\\WATER",
            "ImageTypeValue3": "DIXON",
            "ImageTypeValue4": "WATER",
            "DicomImageType": "ORIGINAL\\PRIMARY\\DIXON\\WATER",
            "IceMiniHead": _encoded_minihead(),
        }
    )
    output_identity = {
        "series_description": "Musclemap",
        "sequence_description": "Musclemap",
        "sequence_description_additional": "Water",
        "grouping": "source_group_Musclemap_Water",
        "series_instance_uid": "2.25.42",
    }

    helpers["_stamp_output_image"](
        output,
        output_header,
        source_meta,
        output_identity,
        2,
        0,
        helpers["muscleMapImageTypeToken"],
        ["OPENRECON", "MUSCLEMAP", "SEGMENT_SOURCE_GEOMETRY"],
        image_comment="Musclemap_Water",
        source_type_token="WATER",
        extra_meta={"Keep_image_geometry": "1"},
        source_geometry_segment=True,
    )

    meta = helpers["FakeMeta"].deserialize(output.attribute_string)
    minihead = helpers["_decode_ice_minihead"](meta)

    assert output.getHead().image_series_index == 2
    assert output.image_series_index == 2
    assert output.getHead().image_index == 1
    assert output.getHead().slice == 0
    assert output.getHead().average == 0
    assert output.getHead().contrast == 0
    assert output.getHead().phase == 0
    assert output.getHead().repetition == 0
    assert output.getHead().set == 0
    assert meta["DataRole"] == "Segmentation"
    assert meta["SeriesDescription"] == "Musclemap"
    assert meta["SequenceDescription"] == "Musclemap"
    assert meta["SequenceDescriptionAdditional"] == "Water"
    assert meta["SeriesNumberRangeNameUID"] == "source_group_Musclemap_Water"
    assert meta["ImageType"] == helpers["segmentSourceGeometryImageType"]
    assert meta["DicomImageType"] == helpers["segmentSourceGeometryImageType"]
    assert "ImageTypeValue3" not in meta
    assert meta["ImageTypeValue4"] == helpers["segmentSourceGeometryImageTypeValue4"]
    assert meta["Keep_image_geometry"] == "1"
    assert meta[helpers["segmentSourceGeometryMetaKey"]] == "1"
    assert helpers["segmentPostProcessingMetaKey"] not in meta
    assert meta[helpers["segmentPostProcessingChildRoleMetaKey"]] == "2"
    assert "<CategoryEntry>2</CategoryEntry>" in meta["ExamDataRole"]
    assert "DIXON" not in meta["ImageType"]
    assert "WATER" not in meta["ImageType"]
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "ImageType",
    ) == helpers["segmentSourceGeometryImageType"]
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "SequenceDescriptionAdditional",
    ) == "Water"
    assert helpers["_extract_minihead_string_value"](
        minihead,
        "SeriesNumberRangeNameUID",
    ) == "source_group_Musclemap_Water"
    assert '<ParamString."ImageTypeValue3">' not in minihead
    assert helpers["_extract_minihead_array_tokens"](minihead, "ImageTypeValue3") == []
    assert helpers["_extract_minihead_array_tokens"](
        minihead,
        "ImageTypeValue4",
    ) == [helpers["segmentSourceGeometryImageTypeValue4"]]
    assert "<CategoryEntry>2</CategoryEntry>" in minihead
    assert helpers["_extract_minihead_long_value"](minihead, "Actual3DImagePartNumber") == 0
    assert helpers["_extract_minihead_long_value"](minihead, "AnatomicalPartitionNo") == 0
    assert helpers["_extract_minihead_long_value"](minihead, "SliceNo") == 0
    assert helpers["_extract_minihead_long_value"](minihead, "NumberInSeries") == 1


def test_metrics_report_image_matches_openreconi2iexample_explicit_volume_contract():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_build_derived_series_identity",
            "_build_derived_series_instance_uid",
            "_build_derived_sop_instance_uid",
            "_build_metrics_column_groups",
            "_build_metrics_report_images",
            "_chunk_list",
            "_copy_meta",
            "_decode_ice_minihead",
            "_explicit_header_geometry_meta",
            "_extract_dicom_image_type_values",
            "_extract_minihead_array_tokens",
            "_extract_minihead_long_value",
            "_extract_minihead_string_value",
            "_find_metrics_label_field",
            "_first_non_empty_text",
            "_format_meta_vector",
            "_format_metric_value",
            "_get_dicom_image_type_value",
            "_get_first_meta_int",
            "_get_meta_text",
            "_header_geometry_meta",
            "_header_vector",
            "_meta_text_values",
            "_metrics_fieldnames",
            "_metrics_rows",
            "_orient_metrics_report_page",
            "_output_meta",
            "_pil_text_size",
            "_render_metrics_report_pages",
            "_set_header_sequence_field",
            "_set_meta_scalar",
            "_set_output_storage_meta",
            "_stamp_output_header",
            "_stamp_output_image",
            "_strip_scanner_write_unsafe_meta",
            "_strip_source_parent_refs",
            "_truncate_text_to_width",
            "_validate_output_series_contract",
            "_validate_output_storage_contract",
        ],
        assignments=[
            "mmMetricsMethod",
            "muscleMapImageTypeToken",
            "reservedScannerSeriesIndices",
            "scannerPartitionIndex",
            "scannerWriteUnsafeMetaKeys",
            "segmentSourceGeometryImageTypeValue4",
            "singlePartitionScannerFields",
            "sourceGroupHeaderFields",
            "sourceParentReferenceMetaKeys",
            "sourceParentReferenceMetaPrefixes",
        ],
    )
    source_header = helpers["FakeHead"]()
    source_meta = helpers["FakeMeta"](
        {
            "SeriesDescription": "source_W",
            "SequenceDescription": "source_W",
            "ProtocolName": "source_W",
            "SeriesNumberRangeNameUID": "source_group",
            "SeriesInstanceUID": "1.2.3",
            "SOPInstanceUID": "1.2.3.4",
            "ImageType": "ORIGINAL\\PRIMARY\\DIXON\\WATER",
            "ImageTypeValue4": "WATER",
            "IceMiniHead": _encoded_minihead(),
        }
    )
    metrics_result = {
        "fieldnames": ["label", "mean", "volume_ml"],
        "rows": [
            {"label": "muscle", "mean": "42.0", "volume_ml": "1.25"},
        ],
        "method": "average",
        "region": "wholebody",
        "csv_path": "/tmp/metrics.csv",
    }

    report_images = helpers["_build_metrics_report_images"](
        metrics_result=metrics_result,
        source_headers=[source_header],
        source_meta=[source_meta],
        metrics_series_index=7,
        source_series_identity={
            "series_description": "source_W",
            "parent_grouping": "source_group",
            "series_instance_uid": "1.2.3",
        },
        source_type_token="WATER",
        metrics_comment="MuscleMap metrics summary",
        metrics_minihead_text="minihead metrics should not be copied",
        include_minihead_metrics=True,
        report_spacing=2.5,
    )

    assert len(report_images) == 1
    report_image = report_images[0]
    report_header = report_image.getHead()
    report_meta = helpers["FakeMeta"].deserialize(report_image.attribute_string)

    assert report_image.image_series_index == 7
    assert report_header.image_series_index == 7
    assert report_header.image_index == 1
    assert report_header.slice == 0
    assert list(report_header.matrix_size) == [768, 768, 1]
    assert list(report_header.field_of_view) == [768.0, 768.0, 1.0]
    assert list(report_header.position) == [0.0, 0.0, 0.0]
    assert list(report_header.read_dir) == [1.0, 0.0, 0.0]
    assert list(report_header.phase_dir) == [0.0, 1.0, 0.0]
    assert list(report_header.slice_dir) == [0.0, 0.0, 1.0]
    assert report_meta["DataRole"] == "Segmentation"
    assert report_meta["ImageProcessingHistory"] == ["PYTHON", "OPENRECON_METRICS"]
    assert report_meta["SeriesDescription"] == "source_W_Musclemap_Metrics"
    assert report_meta["SequenceDescription"] == "source_W_Musclemap_Metrics"
    assert report_meta["ProtocolName"] == "source_W_Musclemap_Metrics"
    assert report_meta["SeriesNumberRangeNameUID"] == "source_group_Musclemap_Metrics"
    assert report_meta["SequenceDescriptionAdditional"] == "openrecon"
    assert report_meta["ImageType"] == "DERIVED\\PRIMARY\\M\\METRICS"
    assert report_meta["ImageTypeValue4"] == "METRICS"
    assert report_meta["Keep_image_geometry"] == "0"
    assert report_meta["partition_count"] == "1"
    assert report_meta["slice_count"] == "1"
    assert report_meta["NumberOfSlices"] == "1"
    assert report_meta["ImagesInAcquisition"] == "1"
    assert report_meta["MetricsRows"] == "1"
    assert report_meta["ImageRowDir"] == [
        "1.000000000000000000",
        "0.000000000000000000",
        "0.000000000000000000",
    ]
    assert report_meta["ImageColumnDir"] == [
        "0.000000000000000000",
        "1.000000000000000000",
        "0.000000000000000000",
    ]
    assert report_meta["ImageSliceNormDir"] == [
        "0.000000000000000000",
        "0.000000000000000000",
        "1.000000000000000000",
    ]
    assert report_meta["SlicePosLightMarker"] == [
        "0.000000000000000000",
        "0.000000000000000000",
        "0.000000000000000000",
    ]
    assert "IceMiniHead" not in report_meta
    assert int(np.max(np.asarray(report_image.data))) > 0
    orientation_probe = np.zeros((4, 5), dtype=np.uint16)
    orientation_probe[0, 0] = 1
    orientation_probe[1, 4] = 2
    oriented_probe = helpers["_orient_metrics_report_page"](orientation_probe)
    assert oriented_probe[-1, -1] == 1
    assert oriented_probe[-2, 0] == 2

    helpers["_validate_output_series_contract"](
        [
            {
                "role": "METRICS",
                "image_series_index": 7,
                "series_instance_uid": report_meta["SeriesInstanceUID"],
                "meta_series_instance_uid": report_meta["SeriesInstanceUID"],
                "minihead_series_instance_uid": "N/A",
                "series_grouping": report_meta["SeriesNumberRangeNameUID"],
                "meta_series_grouping": report_meta["SeriesNumberRangeNameUID"],
                "minihead_series_grouping": "N/A",
                "meta_protocol_name": report_meta["ProtocolName"],
                "minihead_protocol_name": "N/A",
            }
        ],
        [
            {
                "role": "WATER",
                "image_series_index": 1,
                "series_instance_uid": "1.2.3",
            }
        ],
    )
    helpers["_validate_output_storage_contract"]([report_image])


def test_metrics_series_allocator_prefers_openreconi2iexample_index_when_available():
    helpers = _load_runtime_helpers_for_test(
        [
            "ConnectionSeriesAllocator",
            "_first_non_empty_text",
            "_json_log_default",
            "_log_json_event",
        ],
        assignments=[
            "metricsSeriesIndex",
        ],
    )

    allocator = helpers["ConnectionSeriesAllocator"](
        observed_indices={1},
        reserved_indices={99},
    )
    assert allocator.allocate("MUSCLEMAP") == 2
    assert allocator.allocate(
        "METRICS",
        preferred_index=helpers["metricsSeriesIndex"],
    ) == helpers["metricsSeriesIndex"]
    assert allocator.allocate(
        "METRICS",
        preferred_index=helpers["metricsSeriesIndex"],
    ) == helpers["metricsSeriesIndex"] + 1

    collision_allocator = helpers["ConnectionSeriesAllocator"](
        observed_indices={helpers["metricsSeriesIndex"]},
        reserved_indices={99},
    )
    assert collision_allocator.allocate(
        "METRICS",
        preferred_index=helpers["metricsSeriesIndex"],
    ) == helpers["metricsSeriesIndex"] + 1


def test_output_series_contract_rejects_restamped_role_input_index_reuse():
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_validate_output_series_contract",
        ],
        assignments=[
            "muscleMapImageTypeToken",
            "reservedScannerSeriesIndices",
            "segmentSourceGeometryImageTypeValue4",
        ],
    )
    input_summary = [
        {
            "role": "WATER",
            "image_series_index": 1,
            "series_instance_uid": "1.2.3",
        }
    ]
    output_summary = [
        {
            "role": "WATER",
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
        assert "output role WATER reuses input image_series_index 1" in str(exc)
    else:
        raise AssertionError("Expected validator to reject input image_series_index reuse")


def test_output_storage_contract_rejects_duplicate_scanner_storage_key():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_decode_ice_minihead",
            "_extract_minihead_long_value",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_first_meta_int",
            "_get_meta_text",
            "_validate_output_storage_contract",
        ],
        assignments=[
            "scannerPartitionIndex",
            "singlePartitionScannerFields",
        ],
    )
    images = [
        helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16)),
        helpers["FakeImage"](np.ones((1, 2, 2), dtype=np.int16)),
    ]
    for image in images:
        image.attribute_string = helpers["FakeMeta"](
            {
                "SeriesInstanceUID": "2.25.4",
                "SOPInstanceUID": "2.25.4.1",
                "Actual3DImagePartNumber": "0",
                "AnatomicalPartitionNo": "0",
                "SliceNo": "8",
                "ChronSliceNo": "8",
                "NumberInSeries": "9",
            }
        ).serialize()

    try:
        helpers["_validate_output_storage_contract"](images)
    except ValueError as exc:
        assert "duplicates scanner storage key" in str(exc)
    else:
        raise AssertionError("Expected validator to reject duplicate scanner storage key")


def test_output_storage_contract_rejects_nonzero_partition_counters():
    helpers = _load_runtime_helpers_for_test(
        [
            "_as_image_list",
            "_decode_ice_minihead",
            "_extract_minihead_long_value",
            "_extract_minihead_string_value",
            "_first_non_empty_text",
            "_get_first_meta_int",
            "_get_meta_text",
            "_validate_output_storage_contract",
        ],
        assignments=[
            "scannerPartitionIndex",
            "singlePartitionScannerFields",
        ],
    )
    image = helpers["FakeImage"](np.zeros((1, 2, 2), dtype=np.int16))
    image.attribute_string = helpers["FakeMeta"](
        {
            "SeriesInstanceUID": "2.25.4",
            "SOPInstanceUID": "2.25.4.1",
            "Actual3DImagePartNumber": "8",
            "AnatomicalPartitionNo": "8",
            "SliceNo": "8",
            "ChronSliceNo": "8",
            "NumberInSeries": "9",
            "IceMiniHead": _encoded_minihead(),
        }
    ).serialize()

    try:
        helpers["_validate_output_storage_contract"]([image])
    except ValueError as exc:
        message = str(exc)
        assert "scanner partition Meta Actual3DImagePartNumber=8, expected 0" in message
        assert "scanner partition IceMiniHead Actual3DImagePartNumber=8, expected 0" in message
    else:
        raise AssertionError("Expected validator to reject nonzero scanner partition counters")
