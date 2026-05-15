import ast
import base64
import copy
import json
import re
from pathlib import Path

import numpy as np


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "musclemap.py"


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
            self.contrast = 0
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
        "re": re,
        "uuid": __import__("uuid"),
    }
    ast.fix_missing_locations(ast.Module(body=helper_nodes, type_ignores=[]))
# FIX: 移除exec，改用安全方式
# 
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
  <ParamString."ImageType"> {{ "ORIGINAL\\PRIMARY\\DIXON\\WATER" }}
  <ParamString."ImageTypeValue3"> {{ "DIXON" }}
  <ParamArray."ImageTypeValue4">
  {{
    {{ "WATER" }}
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
        parent_sequence="MuscleMap_Metrics",
        parent_grouping="source_MuscleMap_Metrics",
        series_instance_uid="2.25.2",
        source_type_token="WATER",
        target_type_token="METRICS",
        target_display_token="METRICS",
        target_image_type_value3="M",
    )

    assert changed_musclemap
    assert changed_metrics
    assert helpers["_extract_minihead_string_value"](patched_musclemap, "ProtocolName") == "Musclemap"
    assert helpers["_extract_minihead_string_value"](patched_metrics, "ProtocolName") == "MuscleMap_Metrics"
    assert helpers["_extract_minihead_string_value"](
        patched_musclemap, "SeriesNumberRangeNameUID"
    ) == "source_Musclemap"
    assert helpers["_extract_minihead_string_value"](
        patched_metrics, "SeriesNumberRangeNameUID"
    ) == "source_MuscleMap_Metrics"
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
            "series_grouping": "source_MuscleMap_Metrics",
            "meta_series_grouping": "source_MuscleMap_Metrics",
            "minihead_series_grouping": helpers["_extract_minihead_string_value"](
                patched_metrics, "SeriesNumberRangeNameUID"
            ),
            "meta_protocol_name": "MuscleMap_Metrics",
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


def test_output_series_contract_rejects_restamped_role_input_index_reuse():
    helpers = _load_runtime_helpers_for_test(
        [
            "_first_non_empty_text",
            "_validate_output_series_contract",
        ],
        assignments=[
            "muscleMapImageTypeToken",
            "reservedScannerSeriesIndices",
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
