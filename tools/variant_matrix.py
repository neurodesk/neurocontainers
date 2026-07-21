#!/usr/bin/env python3
"""Expand logical recipe names into concrete workflow build jobs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from builder.variants import variant_specs


def build_matrix(
    repo_root: Path,
    applications: list[str],
    default_runner: str,
    arm64_runner: str,
) -> list[dict[str, str]]:
    matrix: list[dict[str, str]] = []
    for application in applications:
        recipe_path = repo_root / "recipes" / application / "build.yaml"
        recipe = yaml.safe_load(recipe_path.read_text(encoding="utf-8"))
        for spec in variant_specs(recipe):
            architecture = spec["architecture"]
            matrix.append(
                {
                    "application": application,
                    "variant": spec["variant"],
                    "architecture": architecture,
                    "runner": arm64_runner if architecture == "aarch64" else default_runner,
                }
            )
    return matrix


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--applications", required=True, help="JSON array of recipe names")
    parser.add_argument("--default-runner", required=True, help="JSON-encoded runner payload")
    parser.add_argument("--arm64-runner", required=True, help="JSON-encoded ARM64 runner payload")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    applications = json.loads(args.applications)
    if not isinstance(applications, list) or not all(isinstance(item, str) for item in applications):
        raise ValueError("--applications must be a JSON array of strings")
    print(
        json.dumps(
            build_matrix(
                args.repo_root,
                applications,
                args.default_runner,
                args.arm64_runner,
            ),
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
