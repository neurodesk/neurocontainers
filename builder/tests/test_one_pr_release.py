from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

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


def test_run_git_reports_command_and_stderr(monkeypatch) -> None:
    def fail(*args, **kwargs) -> None:
        raise subprocess.CalledProcessError(
            128, ["git", "show", "missing"], stderr="fatal: bad revision"
        )

    monkeypatch.setattr(one_pr_release.subprocess, "run", fail)

    try:
        one_pr_release.run_git("show", "missing")
    except RuntimeError as error:
        assert "git show missing" in str(error)
        assert "fatal: bad revision" in str(error)
        assert isinstance(error.__cause__, subprocess.CalledProcessError)
    else:
        raise AssertionError("Git failure was not wrapped")


def test_load_recipe_wraps_read_and_yaml_errors(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)
    path = tmp_path / "recipes" / "demo" / "build.yaml"

    try:
        one_pr_release.load_recipe("demo")
    except RuntimeError as error:
        assert str(path) in str(error)
        assert isinstance(error.__cause__, OSError)
    else:
        raise AssertionError("missing recipe was not wrapped")

    path.parent.mkdir(parents=True)
    path.write_bytes(b"\xff")
    try:
        one_pr_release.load_recipe("demo")
    except RuntimeError as error:
        assert str(path) in str(error)
        assert isinstance(error.__cause__, UnicodeError)
    else:
        raise AssertionError("decode error was not wrapped")

    path.write_text("version: [unterminated", encoding="utf-8")
    try:
        one_pr_release.load_recipe("demo")
    except RuntimeError as error:
        assert str(path) in str(error)
        assert isinstance(error.__cause__, yaml.YAMLError)
    else:
        raise AssertionError("YAML error was not wrapped")


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


def test_inspect_recipe_rejects_missing_or_unsafe_version(
    tmp_path: Path, monkeypatch
) -> None:
    recipe_dir = write_recipe(tmp_path)
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)

    recipe = yaml.safe_load((recipe_dir / "build.yaml").read_text(encoding="utf-8"))
    recipe.pop("version")
    (recipe_dir / "build.yaml").write_text(yaml.safe_dump(recipe), encoding="utf-8")
    try:
        one_pr_release.inspect_recipe("demo", "abc123")
    except RuntimeError as error:
        assert "missing a version field" in str(error)
    else:
        raise AssertionError("missing version was accepted")

    recipe["version"] = "../../unsafe"
    (recipe_dir / "build.yaml").write_text(yaml.safe_dump(recipe), encoding="utf-8")
    try:
        one_pr_release.inspect_recipe("demo", "abc123")
    except RuntimeError as error:
        assert "invalid version" in str(error)
    else:
        raise AssertionError("unsafe version was accepted")


def test_verify_candidate_binds_artifacts_to_pr_and_recipe(
    tmp_path: Path, monkeypatch
) -> None:
    recipe_dir = write_recipe(tmp_path)
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path)

    def build_date_for_head(recipe: str, revision: str = "HEAD") -> str:
        assert recipe == "demo"
        assert revision == "abc123"
        return "20260721"

    monkeypatch.setattr(one_pr_release, "build_date", build_date_for_head)
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
        "candidate_tag": "nd-candidate-demo:abc123",
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

    manifest["candidate_tag"] = "attacker-controlled:latest"
    (candidate_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    try:
        one_pr_release.verify_candidate(candidate_dir, "abc123", 42)
    except RuntimeError as error:
        assert "candidate_tag mismatch" in str(error)
    else:
        raise AssertionError("tampered candidate tag was accepted")
    manifest["candidate_tag"] = "nd-candidate-demo:abc123"

    manifest["docker_archive"] = "../outside.docker.tar"
    (candidate_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    try:
        one_pr_release.verify_candidate(candidate_dir, "abc123", 42)
    except RuntimeError as error:
        assert "Invalid candidate docker archive" in str(error)
    else:
        raise AssertionError("traversing candidate path was accepted")
    manifest["docker_archive"] = docker_archive.name

    manifest["recipe"] = "../demo"
    (candidate_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    try:
        one_pr_release.verify_candidate(candidate_dir, "abc123", 42)
    except RuntimeError as error:
        assert "Invalid candidate recipe identifier" in str(error)
    else:
        raise AssertionError("unsafe recipe identifier was accepted")
    manifest["recipe"] = "demo"
    (candidate_dir / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

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


def test_materialize_rejects_unverified_release_path(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(one_pr_release, "REPO_ROOT", tmp_path / "repo")
    bundle = tmp_path / "bundle"
    manifests = tmp_path / "verified.json"
    manifests.write_text(
        json.dumps(
            [
                {
                    "recipe": "demo",
                    "version": "1.2.3",
                    "release_json": "../outside.json",
                }
            ]
        ),
        encoding="utf-8",
    )

    try:
        one_pr_release.command_materialize(
            SimpleNamespace(bundle=str(bundle), manifests=str(manifests))
        )
    except RuntimeError as error:
        assert "Invalid verified release JSON" in str(error)
    else:
        raise AssertionError("unverified materialize path was accepted")

    candidate_dir = bundle / "demo"
    candidate_dir.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    (candidate_dir / "1.2.3.json").symlink_to(outside)
    manifests.write_text(
        json.dumps(
            [
                {
                    "recipe": "demo",
                    "version": "1.2.3",
                    "release_json": "1.2.3.json",
                }
            ]
        ),
        encoding="utf-8",
    )
    try:
        one_pr_release.command_materialize(
            SimpleNamespace(bundle=str(bundle), manifests=str(manifests))
        )
    except RuntimeError as error:
        assert "escapes" in str(error)
    else:
        raise AssertionError("escaping release symlink was accepted")
