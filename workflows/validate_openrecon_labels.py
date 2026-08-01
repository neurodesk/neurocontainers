#!/usr/bin/env python3
"""Validate Neurocontainers OpenRecon labels against the packaging schema."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft7Validator, ValidationError


REPO_ROOT = Path(__file__).resolve().parents[1]
RECIPES_DIR = REPO_ROOT / "recipes"
SCHEMA_PATH = RECIPES_DIR / "OpenReconSchema_1.1.0.json"
VERSION_PLACEHOLDER = "VERSION_WILL_BE_REPLACED_BY_SCRIPT"


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def prepare_label_for_validation(label: dict[str, Any]) -> dict[str, Any]:
    """Apply the version substitution performed by OpenRecon packaging."""
    prepared = copy.deepcopy(label)
    general = prepared.get("general", {})
    if general.get("version") == VERSION_PLACEHOLDER:
        general["version"] = "0.0.0"

    regulatory = general.get("regulatory_information", {})
    if regulatory.get("production_identifier") == VERSION_PLACEHOLDER:
        regulatory["production_identifier"] = "0.0.0"
    material_number = regulatory.get("material_number")
    if isinstance(material_number, str):
        regulatory["material_number"] = material_number.replace(
            VERSION_PLACEHOLDER,
            "0.0.0",
        )
    return prepared


def find_labels(recipes_dir: Path = RECIPES_DIR) -> list[Path]:
    return sorted(recipes_dir.glob("*/OpenReconLabel.json"))


def format_validation_error(error: ValidationError) -> str:
    path = ".".join(str(part) for part in error.path) or "<root>"
    if error.validator == "maxItems":
        return (
            f"{path}: has {len(error.instance)} items; "
            f"maximum is {error.validator_value}"
        )
    return f"{path}: {error.message}"


def validate_label(
    label_path: Path,
    schema_path: Path = SCHEMA_PATH,
) -> list[str]:
    schema = load_json(schema_path)
    Draft7Validator.check_schema(schema)
    label = prepare_label_for_validation(load_json(label_path))
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(label), key=lambda error: list(error.path))
    return [format_validation_error(error) for error in errors]


def validate_labels(
    label_paths: Iterable[Path],
    schema_path: Path = SCHEMA_PATH,
) -> dict[Path, list[str]]:
    return {
        path: errors
        for path in label_paths
        if (errors := validate_label(path, schema_path))
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate OpenReconLabel.json files against OpenRecon schema 1.1.0."
    )
    parser.add_argument(
        "labels",
        nargs="*",
        type=Path,
        help="Specific label files to validate; defaults to every recipe label.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=SCHEMA_PATH,
        help="OpenRecon JSON schema path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    labels = args.labels or find_labels()
    failures = validate_labels(labels, args.schema)
    if failures:
        for path, errors in failures.items():
            print(f"OpenRecon label validation failed: {path}")
            for error in errors:
                print(f"  - {error}")
        return 1

    print(f"Validated {len(labels)} OpenReconLabel.json file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
