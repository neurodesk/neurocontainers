"""Reusable helpers for locating releases and test configurations."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from builder.release_artifact import (  # re-exported for existing callers
    BUILD_DATE_PATTERN,
    find_latest_release_file,
)

__all__ = [
    "BUILD_DATE_PATTERN",
    "discover_test_config",
    "find_latest_release_file",
    "resolve_path",
]


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


def discover_test_config(recipe_dir: str | Path) -> Optional[Path]:
    """Return the default test configuration file for a recipe, if available."""

    recipe_path = Path(recipe_dir)
    fulltest_yaml = recipe_path / "fulltest.yaml"
    if fulltest_yaml.is_file():
        return fulltest_yaml

    return None
