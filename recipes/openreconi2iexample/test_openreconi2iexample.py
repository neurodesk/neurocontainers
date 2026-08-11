"""Focused regression tests for the openreconi2iexample wrapper.

These tests AST-extract specific helpers from the wrapper and exercise them
against fake inputs, mirroring the pattern used by the musclemap and SCT
recipes. They are NOT a full functional test suite for the wrapper.
"""

import ast
import base64
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "openreconi2iexample.py"
DICOM_CONVERTER_PATH = RECIPE_DIR / "dicom2mrd.py"


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

    namespace = {
        "base64": __import__("base64"),
        "json": json,
        "re": re,
    }
    exec(
        compile(
            ast.Module(body=helper_nodes, type_ignores=[]),
            str(WRAPPER_PATH),
            "exec",
        ),
        namespace,
    )
    return namespace


def _load_dicom_converter_helper_for_test(function_name, namespace):
    tree = ast.parse(DICOM_CONVERTER_PATH.read_text())
    helper_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == function_name
    )
    exec(
        compile(
            ast.Module(body=[helper_node], type_ignores=[]),
            str(DICOM_CONVERTER_PATH),
            "exec",
        ),
        namespace,
    )
    return namespace[function_name]


def _meta_with_image_type_value3(value="M"):
    return {
        "ImageTypeValue3": value,
    }


def _minihead_with_image_type_value3_string(value):
    return base64.b64encode(
        f'<ParamString."ImageTypeValue3">{{ "{value}" }}\n'.encode("utf-8")
    ).decode("ascii")


def _minihead_with_image_type_value3_array(tokens):
    token_lines = "\n  ".join(f'{{ "{token}" }}' for token in tokens)
    text = (
        '<ParamArray."ImageTypeValue3">\n'
        '{\n'
        f'  {token_lines}\n'
        '}\n'
    )
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _decoded_minihead(encoded):
    return base64.b64decode(encoded).decode("utf-8")


def test_unsafe_field_helper_rejects_image_type_value3_in_meta():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _meta_with_image_type_value3("M"),
        "",
        0,
    )
    assert errors == ["image 0 has unsafe scanner Meta ImageTypeValue3"]


def test_unsafe_field_helper_rejects_image_type_value3_in_minihead():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    minihead = _decoded_minihead(_minihead_with_image_type_value3_string("M"))
    errors = helpers["_scanner_write_unsafe_field_errors"](
        {},
        minihead,
        0,
    )
    assert errors == ["image 0 has unsafe scanner IceMiniHead ImageTypeValue3"]

    minihead_array = _decoded_minihead(
        _minihead_with_image_type_value3_array(["M"])
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        {},
        minihead_array,
        0,
    )
    assert errors == ["image 0 has unsafe scanner IceMiniHead ImageTypeValue3"]


def test_inversion_defaults_on_and_can_be_explicitly_disabled():
    helpers = _load_runtime_helpers_for_test(
        ["_config_bool", "_config_bool_any", "_send_invert_enabled"],
    )

    send_invert_enabled = helpers["_send_invert_enabled"]
    assert send_invert_enabled("openreconi2iexample") is True
    assert send_invert_enabled({"parameters": {"invert": False}}) is False
    assert send_invert_enabled({"parameters": {"invert": True}}) is True


def test_dicom_conversion_replaces_existing_output_file(tmp_path):
    output_path = tmp_path / "input_data.h5"
    output_path.write_bytes(b"old conversion")

    class FakeDataset:
        def __init__(self, path, group):
            assert not Path(path).exists()
            self.path = path
            self.group = group

    create_output_dataset = _load_dicom_converter_helper_for_test(
        "_create_output_dataset",
        {
            "os": os,
            "ismrmrd": SimpleNamespace(Dataset=FakeDataset),
        },
    )

    dataset = create_output_dataset(str(output_path), "dataset")

    assert dataset.path == str(output_path)
    assert dataset.group == "dataset"
