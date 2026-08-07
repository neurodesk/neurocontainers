"""Tests for the pure rewrite helpers in builder/check_version.py.

These cover the two ways an automated bump can corrupt a recipe: writing a
version that reloads as a different value, and moving a pinned revision that
belongs to some other project.
"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

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


def test_rewrite_fulltest_version_preserves_variable_indirection() -> None:
    text = (
        "name: demo\n"
        'tool_version: "1.2.3"\n'
        'version: "${tool_version}"\n'
        "tests:\n"
        "  - command: demo --version\n"
        '    expected_output_contains: "${version}"\n'
    )

    updated = check_version.rewrite_fulltest_version(text, "1.2.4")

    assert 'tool_version: "1.2.4"' in updated
    assert 'version: "${tool_version}"' in updated
    assert 'expected_output_contains: "${version}"' in updated


def test_prepare_fulltest_bump_updates_the_sibling_suite(tmp_path: Path) -> None:
    build_yaml = tmp_path / "build.yaml"
    fulltest_yaml = tmp_path / "fulltest.yaml"
    fulltest_yaml.write_text(
        "name: demo\nversion: 1.2.3\ntests: []\n",
        encoding="utf-8",
    )

    prepared = check_version.prepare_fulltest_bump(str(build_yaml), "1.2.4")

    path, updated, previous = prepared
    assert path == str(fulltest_yaml)
    assert previous == "1.2.3"
    assert yaml.safe_load(updated)["version"] == "1.2.4"


def test_prepare_fulltest_bump_is_optional_for_legacy_recipes(tmp_path: Path) -> None:
    build_yaml = tmp_path / "build.yaml"

    assert check_version.prepare_fulltest_bump(str(build_yaml), "1.2.4") is None


def test_submit_bump_commits_recipe_and_fulltest_together(
    tmp_path: Path, monkeypatch
) -> None:
    recipe_dir = tmp_path / "recipes" / "demo"
    recipe_dir.mkdir(parents=True)
    build_yaml = recipe_dir / "build.yaml"
    fulltest_yaml = recipe_dir / "fulltest.yaml"
    build_yaml.write_text(
        "name: demo\nversion: 1.2.3\nbuild: {}\n",
        encoding="utf-8",
    )
    fulltest_yaml.write_text(
        "name: demo\nversion: 1.2.3\ntests: []\n",
        encoding="utf-8",
    )
    git_calls = []

    monkeypatch.setattr(check_version, "pull_request_exists", lambda branch: False)
    monkeypatch.setattr(check_version, "remote_branch_exists", lambda branch: False)
    monkeypatch.setattr(check_version, "find_stale_update_issues", lambda path: [])
    monkeypatch.setattr(
        check_version,
        "open_pull_request",
        lambda *args, **kwargs: {"number": 1},
    )

    def fake_git(*args, **kwargs):
        git_calls.append(args)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(check_version, "git", fake_git)

    result = check_version.submit_bump(
        str(build_yaml),
        "demo",
        "1.2.3",
        "1.2.4",
        "example/demo",
        "v1.2.4",
        "main",
        False,
    )

    assert result == "opened"
    assert yaml.safe_load(build_yaml.read_text())["version"] == "1.2.4"
    assert yaml.safe_load(fulltest_yaml.read_text())["version"] == "1.2.4"
    assert (
        "add",
        "--",
        str(build_yaml),
        str(fulltest_yaml),
    ) in git_calls


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
