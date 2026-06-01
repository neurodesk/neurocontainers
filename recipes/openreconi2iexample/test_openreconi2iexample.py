"""Focused regression tests for the openreconi2iexample wrapper.

These tests AST-extract specific helpers from the wrapper and exercise them
against fake inputs, mirroring the pattern used by the musclemap and SCT
recipes. They are NOT a full functional test suite for the wrapper.
"""

import ast
import base64
import json
import re
from pathlib import Path


RECIPE_DIR = Path(__file__).resolve().parent
WRAPPER_PATH = RECIPE_DIR / "openreconi2iexample.py"


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


def _dixon_composable_meta(itv3_value="M"):
    return {
        "SegmentDixonComposable": "1",
        "ImageTypeValue3": itv3_value,
    }


def _non_dixon_meta(itv3_value="M"):
    return {
        "ImageTypeValue3": itv3_value,
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


def test_dixon_composable_allows_image_type_value3_M_in_meta():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "ORIGINAL_IMAGE_TYPE_VALUE3",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _dixon_composable_meta(itv3_value="M"),
        "",
        True,
        0,
    )
    assert errors == []


def test_dixon_composable_allows_image_type_value3_M_in_minihead():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "ORIGINAL_IMAGE_TYPE_VALUE3",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    minihead = _decoded_minihead(_minihead_with_image_type_value3_string("M"))
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _dixon_composable_meta(itv3_value="M"),
        minihead,
        True,
        0,
    )
    assert errors == []

    minihead_array = _decoded_minihead(
        _minihead_with_image_type_value3_array(["M"])
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _dixon_composable_meta(itv3_value="M"),
        minihead_array,
        True,
        0,
    )
    assert errors == []


def test_dixon_composable_rejects_non_M_image_type_value3():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "ORIGINAL_IMAGE_TYPE_VALUE3",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _dixon_composable_meta(itv3_value="MAP"),
        "",
        True,
        7,
    )
    assert errors == ["image 7 has unsafe scanner Meta ImageTypeValue3"]

    minihead = _decoded_minihead(
        _minihead_with_image_type_value3_string("MAP")
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _dixon_composable_meta(itv3_value=""),
        minihead,
        True,
        9,
    )
    assert errors == ["image 9 has unsafe scanner IceMiniHead ImageTypeValue3"]


def test_non_dixon_output_still_rejects_image_type_value3():
    helpers = _load_runtime_helpers_for_test(
        [
            "_meta_text",
            "_meta_values",
            "_minihead_array_tokens",
            "_minihead_string_value",
            "_scanner_write_unsafe_field_errors",
        ],
        assignments=[
            "ORIGINAL_IMAGE_TYPE_VALUE3",
            "SCANNER_WRITE_UNSAFE_META_KEYS",
        ],
    )
    errors = helpers["_scanner_write_unsafe_field_errors"](
        _non_dixon_meta(itv3_value="M"),
        "",
        False,
        2,
    )
    assert errors == ["image 2 has unsafe scanner Meta ImageTypeValue3"]

    minihead = _decoded_minihead(_minihead_with_image_type_value3_string("M"))
    errors = helpers["_scanner_write_unsafe_field_errors"](
        {},
        minihead,
        False,
        3,
    )
    assert errors == ["image 3 has unsafe scanner IceMiniHead ImageTypeValue3"]


def test_validate_storage_fields_uses_unsafe_field_helper():
    wrapper_source = WRAPPER_PATH.read_text()
    # The validator must delegate to the helper so the Dixon-composable
    # exemption is shared between _validate_storage_fields and any future
    # callers (e.g. an inline preflight in process()).
    assert "_scanner_write_unsafe_field_errors(" in wrapper_source
    # Sanity-check: the SegmentDixonComposable detection sits beside the
    # other output-kind flags in _validate_storage_fields, not inside a
    # branch that the unsafe-field check could miss.
    assert "is_dixon_composable_output = (" in wrapper_source


def test_validate_output_images_calls_validate_storage_fields():
    # Ensures the new helper is reached on every output, not buried inside a
    # branch that the Dixon-composable path skips.
    wrapper_source = WRAPPER_PATH.read_text()
    assert "_validate_output_images" in wrapper_source
    assert "_validate_storage_fields" in wrapper_source
    # _validate_output_images iterates output_images and dispatches to
    # _validate_storage_fields per image — preserve that wiring.
    assert "for index, image in enumerate(output_images):" in wrapper_source
    assert "_validate_storage_fields(" in wrapper_source
