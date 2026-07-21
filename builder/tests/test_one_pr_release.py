from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from builder.release import release_data
from tools import one_pr_release


def write_recipe(root: Path, name: str = "demo", version: str = "1.2.3") -> Path:
    recipe_dir = root / "recipes" / name
    recipe_dir.mkdir(parents=True)
    recipe = {
        "name": name,
        "version": version,
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        "categories": ["programming"],
    }
    (recipe_dir / "build.yaml").write_text(yaml.safe_dump(recipe), encoding="utf-8")
    (recipe_dir / "fulltest.yaml").write_text("tests: []\n", encoding="utf-8")
    return recipe_dir


def test_detect_recipes_accepts_recipe_only_change(tmp_path: Path, monkeypatch) -> None:
    write_recipe(tmp_path)
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        one_pr_release,
        "changed_files",
        lambda base, head: ["recipes/demo/build.yaml", "recipes/demo/fulltest.yaml"],
    )

    assert one_pr_release.detect_recipes("base", "head") == ["demo"]


def test_detect_recipes_rejects_mixed_pr(tmp_path: Path, monkeypatch) -> None:
    write_recipe(tmp_path)
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        one_pr_release,
        "changed_files",
        lambda base, head: ["recipes/demo/build.yaml", ".github/workflows/unsafe.yml"],
    )

    try:
        one_pr_release.detect_recipes("base", "head")
    except RuntimeError as error:
        assert "recipe-only PR" in str(error)
    else:
        raise AssertionError("mixed PR was accepted")


def test_detect_recipes_requires_fulltest(tmp_path: Path, monkeypatch) -> None:
    recipe_dir = write_recipe(tmp_path)
    (recipe_dir / "fulltest.yaml").unlink()
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        one_pr_release,
        "changed_files",
        lambda base, head: ["recipes/demo/build.yaml"],
    )

    try:
        one_pr_release.detect_recipes("base", "head")
    except RuntimeError as error:
        assert "fulltest.yaml is required" in str(error)
    else:
        raise AssertionError("recipe without fulltest was accepted")


def test_verify_candidate_binds_artifacts_to_pr_and_recipe(
    tmp_path: Path, monkeypatch
) -> None:
    recipe_dir = write_recipe(tmp_path)
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    candidate_dir = tmp_path / "bundle" / "demo"
    candidate_dir.mkdir(parents=True)
    docker_archive = candidate_dir / "demo_1.2.3_20260721.docker.tar"
    sif = candidate_dir / "demo_1.2.3_20260721.simg"
    docker_archive.write_bytes(b"docker-image")
    sif.write_bytes(b"sif-image")

    recipe = yaml.safe_load((recipe_dir / "build.yaml").read_text(encoding="utf-8"))
    release = release_data("demo", "1.2.3", recipe, "20260721", "x86_64")
    (candidate_dir / "1.2.3.json").write_text(json.dumps(release), encoding="utf-8")
    manifest = {
        "recipe": "demo",
        "version": "1.2.3",
        "build_date": "20260721",
        "pr_number": 42,
        "head_sha": "abc123",
        "recipe_fingerprint": one_pr_release.recipe_fingerprint("demo"),
        "docker_archive": docker_archive.name,
        "docker_sha256": hashlib.sha256(b"docker-image").hexdigest(),
        "sif": sif.name,
        "sif_sha256": hashlib.sha256(b"sif-image").hexdigest(),
        "release_json": "1.2.3.json",
    }
    (candidate_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    verified = one_pr_release.verify_candidate(candidate_dir, "abc123", 42)
    assert verified["recipe"] == "demo"

    try:
        one_pr_release.verify_candidate(candidate_dir, "abc123", 41)
    except RuntimeError as error:
        assert "PR number mismatch" in str(error)
    else:
        raise AssertionError("candidate from another PR was accepted")

    (recipe_dir / "extra.sh").write_text("changed\n", encoding="utf-8")
    try:
        one_pr_release.verify_candidate(candidate_dir, "abc123", 42)
    except RuntimeError as error:
        assert "differs from tested candidate" in str(error)
    else:
        raise AssertionError("changed recipe matched stale candidate")
