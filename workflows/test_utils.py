"""Reusable helpers for locating releases and test configurations."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional, Tuple

import yaml


def resolve_path(candidate: str | Path, *, repo_root: Path, cwd: Path | None = None) -> Path:
    """Resolve a user-supplied path against cwd and repo root."""
    path = Path(candidate)
    if path.is_absolute():
        return path

    search_roots = [Path(cwd or Path.cwd()), repo_root]
    for root in search_roots:
        resolved = (root / path).resolve()
        if resolved.exists():
            return resolved

    return path.resolve()


def find_latest_release_file(release_dir: str | Path) -> Tuple[Optional[Path], Optional[str]]:
    """Select the most recent release metadata file for a recipe."""

    release_path = Path(release_dir)
    if not release_path.is_dir():
        return None, None

    latest_path: Optional[Path] = None
    latest_build_date = ""
    latest_version = ""

    for entry in sorted(release_path.iterdir()):
        if entry.suffix != ".json":
            continue

        candidate_version = entry.stem
        build_date = ""

        try:
            data = json.loads(entry.read_text(encoding="utf-8"))
            apps = data.get("apps", {}) or {}
            if apps:
                first_value = next(iter(apps.values()))
                build_date = str(first_value.get("version", "")).strip()
        except Exception:
            build_date = ""

        if latest_path is None:
            latest_path = entry
            latest_build_date = build_date
            latest_version = candidate_version
            continue

        if build_date and (not latest_build_date or build_date > latest_build_date):
            latest_path = entry
            latest_build_date = build_date
            latest_version = candidate_version
        elif build_date == latest_build_date and candidate_version > latest_version:
            latest_path = entry
            latest_version = candidate_version

    if latest_path is None:
        return None, None

    return latest_path, latest_version


def discover_test_config(recipe_dir: str | Path) -> Optional[Path]:
    """Return the default test configuration file for a recipe, if available."""

    recipe_path = Path(recipe_dir)
    test_yaml = recipe_path / "test.yaml"
    if test_yaml.is_file():
        return test_yaml

    build_yaml = recipe_path / "build.yaml"
    if not build_yaml.is_file():
        return None

    try:
        data = yaml.safe_load(build_yaml.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}

    if data.get("tests"):
        return build_yaml

    directives = (data.get("build") or {}).get("directives")

    def contains_test(entry) -> bool:
        if isinstance(entry, dict):
            if "test" in entry:
                return True
            for key in ("group", "directives"):
                if key in entry and contains_test(entry[key]):
                    return True
            for value in entry.values():
                if contains_test(value):
                    return True
        elif isinstance(entry, list):
            return any(contains_test(item) for item in entry)
        return False

    if contains_test(directives):
        return build_yaml

    return None
