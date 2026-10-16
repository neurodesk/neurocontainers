"""Exercise staged promotion without downloading or publishing images."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tools import one_pr_release as release


@pytest.fixture
def staged(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    monkeypatch.setattr(release, "REPO_ROOT", root)
    git = release.run_git
    git("init", "-b", "main")
    git("config", "user.name", "test")
    git("config", "user.email", "test@example.com")
    names = ("qsmxt", "vesselboost", "synthseg")
    for name in names:
        directory = root / "recipes" / name
        directory.mkdir(parents=True)
        (directory / "build.yaml").write_text(yaml.safe_dump({
            "name": name, "version": "1.2.3", "architectures": ["x86_64"],
            "categories": ["programming"],
            "build": {"kind": "neurodocker", "base-image": "ubuntu:24.04",
                      "pkg-manager": "apt", "directives": []},
        }))
        (directory / "adapter.py").write_text("originals_before_failure = True\n")
    git("add", ".")
    git("commit", "-m", "Merge tested recipes")
    merge = git("rev-parse", "HEAD")
    bundle = tmp_path / "previews"
    manifests = []
    for name in names:
        directory = bundle / name
        directory.mkdir(parents=True)
        info = release.inspect_recipe(name, merge)
        for field in ("docker_archive", "sif"):
            (directory / info[field]).write_bytes(b"small candidate fixture")
        release.command_manifest(SimpleNamespace(
            recipe=name, variant="", head_sha=merge, pr_number=42,
            candidate_dir=str(directory),
        ))
        manifests.append(release.verify_candidate(directory, merge, 42, merge))
        for field in ("docker_archive", "sif"):
            (directory / info[field]).unlink()
    path = tmp_path / "published.json"
    path.write_text(json.dumps(manifests))
    return SimpleNamespace(root=root, bundle=bundle, path=path, manifests=manifests,
                           head=merge, merge=merge, git=git)


def supersede(staged, *names):
    for name in names:
        (staged.root / "recipes" / name / "adapter.py").write_text("new_image = True\n")
    staged.git("commit", "-am", "Merge later image changes")


def test_current_recipes_survive_a_sibling_supersession(staged):
    supersede(staged, "synthseg")
    manifests = release.verify_published_metadata(
        staged.bundle, staged.path, staged.head, 42, staged.merge
    )
    assert [manifest["container"] for manifest in manifests] == ["qsmxt", "vesselboost"]
