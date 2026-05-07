import ast
import re
from pathlib import Path


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

    namespace = {
        "logging": type(
            "Logger",
            (),
            {"warning": staticmethod(lambda *args, **kwargs: None)},
        ),
        "mrdhelper": FakeMrdHelper,
        "re": re,
    }
    ast.fix_missing_locations(ast.Module(body=helper_nodes, type_ignores=[]))
    exec(compile(ast.Module(body=helper_nodes, type_ignores=[]), str(WRAPPER_PATH), "exec"), namespace)
    return namespace


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
