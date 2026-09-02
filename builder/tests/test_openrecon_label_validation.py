import json
import tempfile
import unittest
from pathlib import Path

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
            if (errors := validate_label(label))
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
