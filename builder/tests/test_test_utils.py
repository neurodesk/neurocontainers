from __future__ import annotations

from pathlib import Path

from workflows.test_utils import discover_test_config, find_latest_release_file


def write_release(root: Path, version: str, build_date: str) -> Path:
    release_file = root / f"{version}.json"
    release_file.write_text(
        f'{{"apps": {{"tool": {{"version": "{build_date}"}}}}}}\n',
        encoding="utf-8",
    )
    return release_file


def test_find_latest_release_file_ignores_placeholder_latest_metadata(
    tmp_path: Path,
) -> None:
    release_dir = tmp_path / "releases" / "tool"
    release_dir.mkdir(parents=True)
    latest_release = write_release(release_dir, "1.2.3", "20260601")
    write_release(release_dir, "latest", "latest")

    path, version, build_date = find_latest_release_file(release_dir)

    assert path == latest_release
    assert version == "1.2.3"
    assert build_date == "20260601"


def test_discover_test_config_prefers_fulltest_yaml(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes" / "cat12"
    recipe_dir.mkdir(parents=True)
    fulltest_yaml = recipe_dir / "fulltest.yaml"
    test_yaml = recipe_dir / "test.yaml"
    fulltest_yaml.write_text("tests: []\n", encoding="utf-8")
    test_yaml.write_text("tests: []\n", encoding="utf-8")

    assert discover_test_config(recipe_dir) == fulltest_yaml


def test_discover_test_config_ignores_legacy_test_yaml(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipes" / "cat12"
    recipe_dir.mkdir(parents=True)
    test_yaml = recipe_dir / "test.yaml"
    test_yaml.write_text("tests: []\n", encoding="utf-8")

    assert discover_test_config(recipe_dir) is None
