from __future__ import annotations

import json
from pathlib import Path

import pytest

from workflows.release_pr_changes import (
    ReleaseChangeError,
    detect_release_pr_changes,
    write_github_outputs,
)


def write_release(root: Path, recipe: str, version: str, build_date: str = "20260521") -> Path:
    release_dir = root / "releases" / recipe
    release_dir.mkdir(parents=True, exist_ok=True)
    release_file = release_dir / f"{version}.json"
    release_file.write_text(
        json.dumps({"apps": {recipe: {"version": build_date}}}),
        encoding="utf-8",
    )
    return release_file


def test_new_recipe_test_yaml_without_release_metadata_is_skipped(tmp_path: Path) -> None:
    result = detect_release_pr_changes(
        [
            "recipes/mede/build.yaml",
            "recipes/mede/test.yaml",
        ],
        repo_root=tmp_path,
    )

    assert result.entries == ()
    assert result.skipped_new_recipe_tests == ("mede",)
    assert result.has_changes is False
    assert result.matrix() == []


def test_existing_recipe_test_yaml_uses_latest_release_metadata(tmp_path: Path) -> None:
    write_release(tmp_path, "cat12", "26.0.rc2", build_date="20260520")
    latest_release = write_release(tmp_path, "cat12", "26.0.rc3", build_date="20260521")

    result = detect_release_pr_changes(["recipes/cat12/test.yaml"], repo_root=tmp_path)

    assert result.skipped_new_recipe_tests == ()
    assert result.has_changes is True
    assert result.matrix() == [
        {
            "name": "cat12",
            "version": "26.0.rc3",
            "file": latest_release.relative_to(tmp_path).as_posix(),
        }
    ]


def test_release_metadata_still_must_be_isolated_from_unrelated_files(tmp_path: Path) -> None:
    with pytest.raises(ReleaseChangeError) as exc_info:
        detect_release_pr_changes(
            [
                "releases/cat12/26.0.rc3.json",
                "recipes/cat12/build.yaml",
            ],
            repo_root=tmp_path,
        )

    message = str(exc_info.value)
    assert "Release metadata changes must be isolated from unrelated files." in message
    assert "Release files: releases/cat12/26.0.rc3.json" in message
    assert "Unrelated files: recipes/cat12/build.yaml" in message


def test_github_outputs_are_written_for_skipped_new_recipe(tmp_path: Path) -> None:
    result = detect_release_pr_changes(["recipes/mede/test.yaml"], repo_root=tmp_path)
    output_path = tmp_path / "github-output.txt"

    write_github_outputs(result, output_path)

    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "has-changes=false",
        "modified-releases=[]",
    ]
