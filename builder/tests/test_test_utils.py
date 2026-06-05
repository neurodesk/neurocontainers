from __future__ import annotations

from pathlib import Path

from workflows.test_utils import discover_test_config


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
