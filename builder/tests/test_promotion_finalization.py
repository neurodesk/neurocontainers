"""Exercise staged promotion without downloading or publishing images."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest
import yaml

from tools import one_pr_release as release


WORKFLOW = yaml.safe_load(
    Path(".github/workflows/promote-container-candidate.yml").read_text()
)


def workflow_step(name, job="finalize"):
    return next(
        step for step in WORKFLOW["jobs"][job]["steps"] if step.get("name") == name
    )


@pytest.fixture
def staged(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    monkeypatch.setattr(release, "REPO_ROOT", root)
    git = release.run_git
    git("init", "-b", "main")
    git("config", "user.name", "test")
    git("config", "user.email", "test@example.com")
    shared = root / "macros" / "common"
    shared.mkdir(parents=True)
    (shared / "neurodocker.yaml").write_text("[]\n")
    names = ("qsmxt", "vesselboost", "synthseg")
    for name in names:
        directory = root / "recipes" / name
        directory.mkdir(parents=True)
        (directory / "build.yaml").write_text(
            yaml.safe_dump(
                {
                    "name": name,
                    "version": "1.2.3",
                    "architectures": ["x86_64"],
                    "categories": ["programming"],
                    "build": {
                        "kind": "neurodocker",
                        "base-image": "ubuntu:24.04",
                        "pkg-manager": "apt",
                        "directives": [{"include": "common/neurodocker.yaml"}],
                    },
                }
            )
        )
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
        release.command_manifest(
            SimpleNamespace(
                recipe=name,
                variant="",
                head_sha=merge,
                pr_number=42,
                candidate_dir=str(directory),
            )
        )
        manifests.append(release.verify_candidate(directory, merge, 42, merge))
        for field in ("docker_archive", "sif"):
            (directory / info[field]).unlink()
    path = tmp_path / "published.json"
    path.write_text(json.dumps(manifests))
    return SimpleNamespace(
        root=root,
        bundle=bundle,
        path=path,
        manifests=manifests,
        head=merge,
        merge=merge,
        git=git,
    )


def supersede(staged, *names):
    for name in names:
        (staged.root / "recipes" / name / "adapter.py").write_text("new_image = True\n")
    staged.git("commit", "-am", "Merge later image changes")


def test_current_recipes_survive_a_sibling_supersession(staged):
    supersede(staged, "synthseg")
    manifests = release.verify_published_metadata(
        staged.bundle,
        staged.path,
        staged.head,
        42,
        staged.merge,
        exclude_superseded=True,
    )
    assert [manifest["container"] for manifest in manifests] == ["qsmxt", "vesselboost"]


def select(staged):
    output = staged.path.with_name("selected.json")
    release.command_select_current_metadata(
        SimpleNamespace(
            bundle=str(staged.bundle),
            manifests=str(staged.path),
            head_sha=staged.head,
            pr_number=42,
            merge_sha=staged.merge,
            output=str(output),
        )
    )
    return output


@pytest.mark.parametrize(
    "stale", [(), ("synthseg",), ("qsmxt", "vesselboost", "synthseg")]
)
def test_selection_materializes_only_current_releases(staged, stale):
    if stale:
        supersede(staged, *stale)
    output = select(staged)
    release.command_materialize(
        SimpleNamespace(bundle=str(staged.bundle), manifests=str(output))
    )
    assert sorted(
        path.parent.name for path in (staged.root / "releases").glob("*/*.json")
    ) == sorted({"qsmxt", "vesselboost", "synthseg"} - set(stale))


@pytest.mark.parametrize(
    "field,value",
    [
        ("head_sha", "b" * 40),
        ("pr_number", 99),
        ("container", "unrelated"),
        ("recipe_fingerprint", "f" * 64),
        ("variant", "invented"),
        ("architecture", "aarch64"),
        ("version", "9.9.9"),
        ("docker_sha256", "bad"),
        ("sif_sha256", None),
        ("release_json", "../1.2.3.json"),
    ],
)
def test_supersession_never_excuses_invalid_manifest(staged, field, value):
    supersede(staged, "synthseg")
    staged.manifests[-1][field] = value
    staged.path.write_text(json.dumps(staged.manifests))
    with pytest.raises(RuntimeError):
        select(staged)
    assert not staged.path.with_name("selected.json").exists()


@pytest.mark.parametrize("invalid", ["{}", "[]", "[null]", "[{}]", "not json"])
def test_selection_rejects_malformed_or_empty_input(staged, invalid):
    staged.path.write_text(invalid)
    with pytest.raises(RuntimeError):
        select(staged)


@pytest.mark.parametrize("fault", ["missing", "malformed", "wrong", "duplicate"])
def test_bad_superseded_preview_or_duplicate_aborts_entire_selection(staged, fault):
    supersede(staged, "synthseg")
    preview = staged.bundle / "synthseg" / "1.2.3.json"
    if fault == "missing":
        preview.unlink()
    elif fault == "malformed":
        preview.write_text("not json")
    elif fault == "wrong":
        preview.write_text("{}")
    else:
        staged.path.write_text(json.dumps(staged.manifests + staged.manifests[-1:]))
    with pytest.raises(RuntimeError):
        select(staged)


def test_source_only_changes_require_authentic_merge_fingerprint(staged):
    recipe = staged.root / "recipes" / "synthseg" / "build.yaml"
    data = yaml.safe_load(recipe.read_text())
    data["readme"] = "Updated documentation"
    recipe.write_text(yaml.safe_dump(data))
    staged.git("commit", "-am", "Update documentation")
    assert json.loads(select(staged).read_text()) == staged.manifests
    staged.manifests[-1]["recipe_fingerprint"] = "f" * 64
    staged.path.write_text(json.dumps(staged.manifests))
    with pytest.raises(RuntimeError, match="Trusted recipe differs"):
        select(staged)


def test_original_head_merge_mismatch_is_not_later_supersession(staged):
    (staged.root / "recipes" / "synthseg" / "fulltest.yaml").write_text(
        "tests: [{name: installed behavior, command: check-installed}]\n"
    )
    staged.git("add", "recipes")
    staged.git("commit", "-m", "Land candidate with additional main inputs")
    staged.merge = staged.git("rev-parse", "HEAD")
    supersede(staged, "synthseg")
    with pytest.raises(RuntimeError, match="Trusted recipe differs"):
        select(staged)
    assert not staged.path.with_name("selected.json").exists()


def test_later_version_is_excluded_using_the_original_identity(staged):
    recipe = staged.root / "recipes" / "synthseg" / "build.yaml"
    data = yaml.safe_load(recipe.read_text())
    data["version"] = "2.0.0"
    recipe.write_text(yaml.safe_dump(data))
    staged.git("commit", "-am", "Merge next version")
    assert [m["container"] for m in json.loads(select(staged).read_text())] == [
        "qsmxt",
        "vesselboost",
    ]


def test_intervening_merge_still_aborts_strict_revalidation(staged):
    selected = select(staged)
    supersede(staged, "synthseg")
    with pytest.raises(RuntimeError, match="Merged recipe differs"):
        release.verify_published_metadata(
            staged.bundle, selected, staged.head, 42, staged.merge
        )


def test_an_unrelated_release_commit_does_not_invalidate_selection(staged):
    selected = select(staged)
    (staged.root / "unrelated.json").write_text("{}")
    staged.git("add", ".")
    staged.git("commit", "-m", "Another finalizer completed")
    assert (
        release.verify_published_metadata(
            staged.bundle, selected, staged.head, 42, staged.merge
        )
        == staged.manifests
    )


def test_merge_must_be_an_ancestor_of_trusted_checkout(staged):
    staged.git("checkout", "--orphan", "unrelated")
    staged.git("commit", "-m", "Unrelated history")
    with pytest.raises(RuntimeError, match="merge-base"):
        select(staged)


def test_older_candidate_cannot_overwrite_a_newer_release(staged):
    destination = staged.root / "releases" / "synthseg" / "1.2.3.json"
    destination.parent.mkdir(parents=True)
    newer = {"apps": {"synthseg 1.2.3": {"version": "20990101"}}}
    destination.write_text(json.dumps(newer))
    staged.git("add", "releases")
    staged.git("commit", "-m", "Publish a newer build")
    selected = select(staged)
    assert [m["container"] for m in json.loads(selected.read_text())] == [
        "qsmxt",
        "vesselboost",
    ]
    release.command_materialize(
        SimpleNamespace(bundle=str(staged.bundle), manifests=str(selected))
    )
    assert json.loads(destination.read_text()) == newer
    with pytest.raises(RuntimeError, match="Newer release already exists"):
        release.verify_published_metadata(
            staged.bundle, staged.path, staged.head, 42, staged.merge
        )


def test_historical_fingerprint_preserves_binary_shared_inputs_and_overlap(staged):
    shared = staged.root / "macros" / "common"
    (shared / "data.bin").write_bytes(b"\xff\x00\r\n\n")
    path = staged.root / "recipes" / "qsmxt" / "build.yaml"
    recipe = yaml.safe_load(path.read_text())
    recipe["auto_update"] = {"local": ["macros/common", "macros/common/data.bin"]}
    path.write_text(yaml.safe_dump(recipe))
    staged.git("add", ".")
    staged.git("commit", "-m", "Share binary build inputs")
    assert release.recipe_fingerprint("qsmxt", "HEAD") == release.recipe_fingerprint(
        "qsmxt"
    )


def test_shared_input_change_supersedes_every_consumer(staged):
    (staged.root / "macros" / "common" / "neurodocker.yaml").write_text(
        "- run: changed\n"
    )
    staged.git("commit", "-am", "Change a shared image input")
    assert json.loads(select(staged).read_text()) == []


@pytest.fixture
def workflow_env(staged, tmp_path):
    commands = tmp_path / "bin"
    commands.mkdir()
    python = commands / "python"
    python.write_text(
        f"#!{sys.executable}\n"
        "import os, subprocess, sys\n"
        "if sys.argv[1] == 'tools/one_pr_release.py':\n"
        f"    command = [sys.executable, {str(Path(release.__file__).resolve())!r}, "
        f"'--repo-root', {str(staged.root)!r}, *sys.argv[2:]]\n"
        "    raise SystemExit(subprocess.call(command))\n"
        "if (sys.argv[1:4] != ['-m', 'tools.v2_registry', 'finalize'] and\n"
        "    sys.argv[1:3] != ['-m', 'tools.copy_swift_object'] and\n"
        "    sys.argv[1] != 'tools/sync_openrecon.py'):\n"
        "    raise RuntimeError('Unexpected Python command')\n"
        "with open(os.environ['EFFECTS'], 'a') as log:\n"
        "    log.write(' '.join(sys.argv[1:]) + '\\n')\n"
    )
    python.chmod(0o755)
    for tool in ("oras", "aws", "swift", "gh"):
        command = commands / tool
        command.write_text('#!/bin/bash\nprintf "%s\\n" "$0 $*" >> "$EFFECTS"\n')
        command.chmod(0o755)
    effects = tmp_path / "effects.log"
    env = {
        **os.environ,
        "PATH": f"{commands}:{os.environ['PATH']}",
        "EFFECTS": str(effects),
        "HEAD_SHA": staged.head,
        "MERGE_SHA": staged.merge,
        "PR_NUMBER": "42",
        "GH_REGISTRY": "example",
        "DOCKERHUB_ORG": "example",
        "SIGN_ARTIFACTS": "false",
        "RELEASE_TOKEN": "local-fixture",
        "GITHUB_OUTPUT": str(tmp_path / "outputs"),
    }
    shutil.copytree(
        staged.bundle, staged.root / "promotion-metadata" / "release-previews"
    )
    for manifest in staged.manifests:
        (
            staged.root
            / "promotion-metadata"
            / f"verified-manifest-{manifest['container']}.json"
        ).write_text(json.dumps([manifest]))
    env["ARTIFACTS"] = json.dumps(
        [
            {"name": f"candidate-{m['container']}-{staged.head}"}
            for m in staged.manifests
        ]
    )
    return env


def run_step(staged, env, name, job="finalize"):
    return subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", workflow_step(name, job)["run"]],
        cwd=staged.root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize(
    "stale", [(), ("synthseg",), ("qsmxt", "vesselboost", "synthseg")]
)
def test_workflow_public_tags_and_metadata_use_only_selected_records(
    staged, workflow_env, stale
):
    if stale:
        supersede(staged, *stale)
    selected = run_step(
        staged, workflow_env, "Revalidate and combine published manifests"
    )
    assert selected.returncode == 0, selected.stderr
    tagged = run_step(
        staged, workflow_env, "Finalize public Docker tags from staged manifests"
    )
    assert tagged.returncode == 0, tagged.stderr
    copied = run_step(
        staged, workflow_env, "Finalize public SIF object names with server-side copies"
    )
    assert copied.returncode == 0, copied.stderr
    effects = Path(workflow_env["EFFECTS"])
    text = effects.read_text() if effects.exists() else ""
    for name in ("qsmxt", "vesselboost", "synthseg"):
        assert (name in text) == (name not in stale)
    release.command_materialize(
        SimpleNamespace(
            bundle=str(staged.bundle),
            manifests=str(staged.root / "verified-manifests.json"),
        )
    )
    assert sorted(
        path.parent.name for path in (staged.root / "releases").glob("*/*.json")
    ) == sorted({"qsmxt", "vesselboost", "synthseg"} - set(stale))
    workflow_env["FINALIZED_CONTAINERS"] = json.dumps(
        [
            m["container"]
            for m in json.loads((staged.root / "verified-manifests.json").read_text())
        ]
    )
    synced = run_step(staged, workflow_env, "Sync OpenRecon metadata", "sync_openrecon")
    assert synced.returncode == 0, synced.stderr
    for name in ("qsmxt", "vesselboost", "synthseg"):
        assert (f"--recipe {name}" in effects.read_text()) == (name not in stale)


@pytest.mark.parametrize("same_recipe", [False, True])
def test_metadata_push_rebases_and_rechecks_a_concurrent_merge(
    staged, workflow_env, tmp_path, same_recipe
):
    remote = tmp_path / "remote.git"
    subprocess.run(
        ["git", "init", "--bare", "-b", "main", str(remote)],
        check=True,
        capture_output=True,
    )
    staged.git("remote", "add", "origin", str(remote))
    staged.git("push", "origin", "main")
    selected = run_step(
        staged, workflow_env, "Revalidate and combine published manifests"
    )
    assert selected.returncode == 0, selected.stderr
    writer = tmp_path / "writer"
    subprocess.run(
        ["git", "clone", str(remote), str(writer)], check=True, capture_output=True
    )

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=writer, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("config", "user.name", "other")
    git("config", "user.email", "other@example.com")
    path = (
        writer / "recipes" / "synthseg" / "adapter.py"
        if same_recipe
        else writer / "unrelated.txt"
    )
    path.write_text("concurrent update\n")
    git("add", ".")
    git("commit", "-m", "Concurrent merge")
    git("push", "origin", "main")
    result = run_step(
        staged, workflow_env, "Commit generated release metadata directly to main"
    )
    remote_releases = subprocess.run(
        [
            "git",
            "--git-dir",
            str(remote),
            "ls-tree",
            "-r",
            "--name-only",
            "main",
            "--",
            "releases",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if same_recipe:
        assert result.returncode != 0
        assert (
            "Merged recipe differs from published candidate: synthseg" in result.stderr
        )
        assert remote_releases == ""
    else:
        assert result.returncode == 0, result.stderr
        assert sorted(remote_releases.splitlines()) == [
            f"releases/{name}/1.2.3.json"
            for name in ("qsmxt", "synthseg", "vesselboost")
        ]
        published = staged.git("rev-parse", "origin/main")
        repeated = run_step(
            staged, workflow_env, "Commit generated release metadata directly to main"
        )
        assert repeated.returncode == 0, repeated.stderr
        assert staged.git("rev-parse", "origin/main") == published
