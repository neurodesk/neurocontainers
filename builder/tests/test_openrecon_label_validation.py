import json
import unittest

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


if __name__ == "__main__":
    unittest.main()
