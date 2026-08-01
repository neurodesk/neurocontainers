"""Tests for the pure rewrite helpers in builder/check_version.py.

These cover the two ways an automated bump can corrupt a recipe: writing a
version that reloads as a different value, and moving a pinned revision that
belongs to some other project.
"""

import importlib.util
from pathlib import Path

import pytest
import yaml

MODULE_PATH = Path(__file__).resolve().parents[1] / "check_version.py"
spec = importlib.util.spec_from_file_location("check_version", MODULE_PATH)
check_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_version)


def test_rewrite_version_quotes_a_version_that_would_reload_as_a_float() -> None:
    """`version: 1.10` unquoted reloads as 1.1, silently mislabelling the image."""
    updated = check_version.rewrite_version("name: demo\nversion: 1.9\n", "1.10")

    assert str(yaml.safe_load(updated)["version"]) == "1.10"


def test_rewrite_version_preserves_existing_quoting() -> None:
    updated = check_version.rewrite_version('name: demo\nversion: "1.2.3"\n', "1.2.4")

    assert 'version: "1.2.4"' in updated
    assert yaml.safe_load(updated)["version"] == "1.2.4"


def test_rewrite_version_ignores_nested_version_keys() -> None:
    text = (
        "name: demo\n"
        "version: 1.2.3\n"
        "build:\n"
        "  directives:\n"
        "    - template:\n"
        "        name: ants\n"
        "        version: 2.4.3\n"
    )

    updated = check_version.rewrite_version(text, "1.2.4")
    reloaded = yaml.safe_load(updated)

    assert reloaded["version"] == "1.2.4"
    assert reloaded["build"]["directives"][0]["template"]["version"] == "2.4.3"


def test_rewrite_version_keeps_a_trailing_comment() -> None:
    updated = check_version.rewrite_version(
        "name: demo\nversion: 1.2.3  # keep in sync with the base image\n", "1.2.4"
    )

    assert "# keep in sync with the base image" in updated
    assert yaml.safe_load(updated)["version"] == "1.2.4"


RECIPE_WITH_TWO_PINS = """name: demo
version: 1.0.0
build:
  directives:
    - group:
        - variables:
            github_url: https://github.com/someoneelse/helper.git
            revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    - group:
        - variables:
            github_url: https://github.com/rordenlab/niimath.git
            revision: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
"""


def test_revision_rewrite_leaves_another_projects_pin_alone() -> None:
    """Proximity is not ownership: only the sha beside the upstream URL moves."""
    updated, changed = check_version.rewrite_revision(
        RECIPE_WITH_TWO_PINS, "rordenlab/niimath", "c" * 40
    )

    assert changed == [("b" * 40, "c" * 40)]
    assert "a" * 40 in updated
    assert "b" * 40 not in updated


def test_revision_rewrite_skips_a_recipe_that_pins_only_foreign_shas() -> None:
    assert check_version.revisions_owned_by(
        RECIPE_WITH_TWO_PINS, "unrelated/project"
    ) == set()

    _, changed = check_version.rewrite_revision(
        RECIPE_WITH_TWO_PINS, "unrelated/project", "c" * 40
    )
    assert changed == []


@pytest.mark.parametrize("repo", ["rordenlab/niimath", "RordenLab/NiiMath"])
def test_revisions_owned_by_matches_case_insensitively(repo: str) -> None:
    assert check_version.revisions_owned_by(RECIPE_WITH_TWO_PINS, repo) == {"b" * 40}
