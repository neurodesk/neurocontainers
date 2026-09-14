from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from workflows.release_pr_changes import (
    ReleaseChangeError,
    detect_release_pr_changes,
    get_changed_files,
)


def write_release(
    root: Path,
    recipe: str,
    version: str,
    build_date: str = "20260521",
    source_recipe: str | None = None,
) -> Path:
    release_dir = root / "releases" / recipe
    release_dir.mkdir(parents=True, exist_ok=True)
    release_file = release_dir / f"{version}.json"
    release_file.write_text(
        json.dumps(
            {
                "apps": {recipe: {"version": build_date}},
                **({"recipe": source_recipe} if source_recipe else {}),
            }
        ),
        encoding="utf-8",
    )
    return release_file


def test_existing_recipe_fulltest_yaml_ignores_placeholder_latest_metadata(
    tmp_path: Path,
) -> None:
    latest_release = write_release(
        tmp_path,
        "mrtrix3",
        "3.0.8",
        build_date="20260107",
    )
    write_release(tmp_path, "mrtrix3", "latest", build_date="latest")

    result = detect_release_pr_changes(["recipes/mrtrix3/fulltest.yaml"], repo_root=tmp_path)

    assert result.skipped_new_recipe_tests == ()
    assert result.matrix() == [
        {
            "name": "mrtrix3",
            "recipe": "mrtrix3",
            "version": "3.0.8",
            "architecture": "x86_64",
            "file": latest_release.relative_to(tmp_path).as_posix(),
        }
    ]


def test_existing_recipe_fulltest_yaml_uses_latest_release_metadata(tmp_path: Path) -> None:
    write_release(tmp_path, "cat12", "26.0.rc2", build_date="20260520")
    latest_release = write_release(tmp_path, "cat12", "26.0.rc3", build_date="20260521")

    result = detect_release_pr_changes(["recipes/cat12/fulltest.yaml"], repo_root=tmp_path)

    assert result.skipped_new_recipe_tests == ()
    assert result.has_changes is True
    assert result.matrix() == [
        {
            "name": "cat12",
            "recipe": "cat12",
            "version": "26.0.rc3",
            "architecture": "x86_64",
            "file": latest_release.relative_to(tmp_path).as_posix(),
        }
    ]


def test_test_config_change_prefers_x86_release_over_arm64_metadata(
    tmp_path: Path,
) -> None:
    latest_x86_release = write_release(
        tmp_path,
        "niimath",
        "1.0.20250804",
        build_date="20251016",
    )
    write_release(
        tmp_path,
        "niimath",
        "1.0.20250804-arm64",
        build_date="20251016",
    )

    result = detect_release_pr_changes(["recipes/niimath/fulltest.yaml"], repo_root=tmp_path)

    assert result.matrix() == [
        {
            "name": "niimath",
            "recipe": "niimath",
            "version": "1.0.20250804",
            "architecture": "x86_64",
            "file": latest_x86_release.relative_to(tmp_path).as_posix(),
        }
    ]


def test_existing_recipe_legacy_test_yaml_is_ignored(
    tmp_path: Path,
) -> None:
    write_release(tmp_path, "cat12", "26.0.rc3", build_date="20260521")

    result = detect_release_pr_changes(["recipes/cat12/test.yaml"], repo_root=tmp_path)

    assert result.skipped_new_recipe_tests == ()
    assert result.has_changes is False
    assert result.matrix() == []


def test_release_metadata_can_be_paired_with_fulltest_yaml(tmp_path: Path) -> None:
    write_release(tmp_path, "cat12", "26.0.rc3")
    result = detect_release_pr_changes(
        [
            "releases/cat12/26.0.rc3.json",
            "recipes/cat12/fulltest.yaml",
        ],
        repo_root=tmp_path,
    )

    assert result.skipped_new_recipe_tests == ()
    assert result.matrix() == [
        {
            "name": "cat12",
            "recipe": "cat12",
            "version": "26.0.rc3",
            "architecture": "x86_64",
            "file": "releases/cat12/26.0.rc3.json",
        }
    ]


def test_named_variant_release_uses_source_recipe_test_suite(tmp_path: Path) -> None:
    write_release(tmp_path, "spinalcordtoolbox", "7.3.1", build_date="20260520")
    release = write_release(
        tmp_path,
        "spinalcordtoolbox_gpu",
        "7.3.2",
        source_recipe="spinalcordtoolbox",
    )

    result = detect_release_pr_changes(
        [
            "releases/spinalcordtoolbox_gpu/7.3.2.json",
            "recipes/spinalcordtoolbox/fulltest.yaml",
        ],
        repo_root=tmp_path,
    )

    assert result.matrix() == [
        {
            "name": "spinalcordtoolbox_gpu",
            "recipe": "spinalcordtoolbox",
            "version": "7.3.2",
            "architecture": "x86_64",
            "file": release.relative_to(tmp_path).as_posix(),
        }
    ]


def test_candidate_build_owns_fulltest_and_avoids_legacy_pr_comments(
    tmp_path: Path,
) -> None:
    """A build PR tests its new image once in the candidate gate."""
    write_release(tmp_path, "cat12", "26.0.rc3", build_date="20260521")

    result = detect_release_pr_changes(
        [
            "recipes/cat12/build.yaml",
            "recipes/cat12/fulltest.yaml",
        ],
        repo_root=tmp_path,
    )

    assert result.has_changes is False
    assert result.matrix() == []


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


def test_retiring_a_release_does_not_queue_a_test_for_the_removed_version(
    tmp_path: Path,
) -> None:
    """Deleting releases/<recipe>/<version>.json retires that version.

    The matrix leg reads the release JSON to resolve the image, so a deleted
    file has nothing to test and must not reach detection at all.
    """
    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "test")
    write_release(tmp_path, "panoptica", "2.1.3", build_date="20260728")
    write_release(tmp_path, "panoptica", "2.1.6", build_date="20260819")
    git("add", "-A")
    git("commit", "-m", "releases")

    git("checkout", "-b", "retire")
    (tmp_path / "releases" / "panoptica" / "2.1.3.json").unlink()
    git("commit", "-am", "retire panoptica 2.1.3")

    changed = get_changed_files("main", "retire", repo_root=tmp_path)

    assert changed == []
    assert detect_release_pr_changes(changed, repo_root=tmp_path).matrix() == []


@pytest.mark.parametrize("metadata, version", [
    ({"architecture": "aarch64", "recipe": "niimath", "variant": "arm64"}, "1.0"),
    ({"architecture": "arm64"}, "1.0"),
    ({"apps": {"tool": {"version": "20260914", "architecture": "aarch64"}}}, "1.0"),
    ({}, "1.0-arm64"),
])
def test_arm_release_matrix_carries_resolved_architecture(
    tmp_path: Path, metadata: dict, version: str,
) -> None:
    release = write_release(tmp_path, "niimath_arm64", version)
    data = json.loads(release.read_text())
    data.update(metadata)
    release.write_text(json.dumps(data))
    result = detect_release_pr_changes([release.relative_to(tmp_path).as_posix()], repo_root=tmp_path)
    assert result.matrix()[0]["architecture"] == "aarch64"


@pytest.mark.parametrize("named_variant", [False, True])
def test_mixed_architecture_releases_keep_both_matrix_entries(
    tmp_path: Path, named_variant: bool,
) -> None:
    x86 = write_release(tmp_path, "niimath", "1.0")
    arm_name = "niimath_arm64" if named_variant else "niimath"
    arm_version = "1.0" if named_variant else "1.0-arm64"
    arm = write_release(tmp_path, arm_name, arm_version, source_recipe="niimath")
    data = json.loads(arm.read_text())
    data["architecture"] = "aarch64"
    arm.write_text(json.dumps(data))

    result = detect_release_pr_changes(
        [x86.relative_to(tmp_path).as_posix(), arm.relative_to(tmp_path).as_posix(),
         "recipes/niimath/fulltest.yaml"],
        repo_root=tmp_path,
    )

    assert [(entry.name, entry.version, entry.architecture) for entry in result.entries] == [
        ("niimath", "1.0", "x86_64"),
        (arm_name, arm_version, "aarch64"),
    ]
    assert all(entry.recipe == "niimath" for entry in result.entries)


def test_test_only_change_with_only_legacy_arm_release(tmp_path: Path) -> None:
    write_release(tmp_path, "niimath", "1.0-arm64")
    result = detect_release_pr_changes(["recipes/niimath/fulltest.yaml"], repo_root=tmp_path)
    assert result.matrix()[0]["architecture"] == "aarch64"


def test_unsupported_release_architecture_fails_detection(tmp_path: Path) -> None:
    release = write_release(tmp_path, "tool", "1.0")
    data = json.loads(release.read_text())
    data["architecture"] = "riscv64"
    release.write_text(json.dumps(data))
    with pytest.raises(ReleaseChangeError, match="Invalid architecture"):
        detect_release_pr_changes([release.relative_to(tmp_path).as_posix()], repo_root=tmp_path)
