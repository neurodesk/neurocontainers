import json
import tempfile
import unittest
from pathlib import Path

import pytest

from workflows.validate_openrecon_labels import (
    SCHEMA_PATH,
    VERSION_PLACEHOLDER,
    find_labels,
    prepare_label_for_validation,
    validate_label,
)


class OpenReconLabelValidationTests(unittest.TestCase):
    def test_version_placeholder_is_normalized_without_mutating_source(self):
        label = {
            "general": {
                "version": VERSION_PLACEHOLDER,
                "regulatory_information": {
                    "production_identifier": VERSION_PLACEHOLDER,
                    "material_number": f"tool_{VERSION_PLACEHOLDER}",
                },
            }
        }

        prepared = prepare_label_for_validation(label)

        self.assertEqual(prepared["general"]["version"], "0.0.0")
        self.assertEqual(
            prepared["general"]["regulatory_information"],
            {
                "production_identifier": "0.0.0",
                "material_number": "tool_0.0.0",
            },
        )
        self.assertEqual(label["general"]["version"], VERSION_PLACEHOLDER)

    def test_all_openrecon_labels_match_packaging_schema(self):
        labels = find_labels()

        self.assertTrue(SCHEMA_PATH.is_file())
        self.assertTrue(labels)

        failures = {
            label.relative_to(SCHEMA_PATH.parents[1]): errors
            for label in labels
            if (errors := validate_label(label, experimental_raw_return=True))
        }
        self.assertFalse(
            failures,
            json.dumps(
                {str(path): errors for path, errors in failures.items()},
                indent=2,
            ),
        )

    def test_label_without_config_parameter_is_rejected(self):
        source_path = (
            SCHEMA_PATH.parents[1] / "recipes" / "b0map" / "OpenReconLabel.json"
        )
        label = json.loads(source_path.read_text(encoding="utf-8"))
        label["parameters"] = [
            parameter
            for parameter in label["parameters"]
            if parameter.get("id") != "config"
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            label_path = Path(temp_dir) / "OpenReconLabel.json"
            label_path.write_text(json.dumps(label), encoding="utf-8")

            errors = validate_label(label_path)

        self.assertIn(
            'parameters: must contain exactly one parameter with id "config"; found 0',
            errors,
        )


if __name__ == "__main__":
    unittest.main()


@pytest.mark.parametrize(
    "version", ["2.10.0-build20261005", "2.10.0-rc1", "2.10.0+build20261005"]
)
def test_scanner_rejects_semver_suffixes(tmp_path: Path, version: str) -> None:
    source = SCHEMA_PATH.parent / "b0map" / "OpenReconLabel.json"
    label = json.loads(source.read_text(encoding="utf-8"))
    label["general"]["version"] = version
    target = tmp_path / "OpenReconLabel.json"
    target.write_text(json.dumps(label), encoding="utf-8")

    assert "general.version: must be a numeric X.Y.Z scanner version" in validate_label(target)


@pytest.fixture
def research_label():
    label = json.loads((SCHEMA_PATH.parent / "b0map" / "OpenReconLabel.json").read_text())
    label["reconstruction"].update(
        emitter="raw", injector="raw", content_qualification_type="RESEARCH"
    )
    return label


def test_raw_return_requires_explicit_opt_in(tmp_path, research_label):
    target = tmp_path / "label.json"
    target.write_text(json.dumps(research_label))
    assert any("injector" in error for error in validate_label(target))
    original_schema = SCHEMA_PATH.read_bytes()
    assert validate_label(target, experimental_raw_return=True) == []
    assert SCHEMA_PATH.read_bytes() == original_schema


@pytest.mark.parametrize("field,value", [
    ("emitter", "image"),
    ("content_qualification_type", "PRODUCT"),
    ("injector", "raww"),
    ("port", "9002"),
    ("can_use_gpu", "false"),
])
def test_raw_extension_preserves_contract_and_types(tmp_path, research_label, field, value):
    research_label["reconstruction"][field] = value
    target = tmp_path / "label.json"
    target.write_text(json.dumps(research_label))
    assert validate_label(target, experimental_raw_return=True)


def test_research_extension_preserves_metadata_validation(tmp_path, research_label):
    research_label["parameters"] = []
    target = tmp_path / "label.json"
    target.write_text(json.dumps(research_label))
    assert validate_label(target, experimental_raw_return=True)


def test_research_flag_preserves_stock_image_validation(tmp_path):
    label = json.loads((SCHEMA_PATH.parent / "b0map" / "OpenReconLabel.json").read_text())
    target = tmp_path / "label.json"
    target.write_text(json.dumps(label))
    assert validate_label(target, experimental_raw_return=True) == validate_label(target) == []
