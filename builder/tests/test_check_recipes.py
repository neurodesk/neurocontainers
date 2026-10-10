from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from workflows.check_recipes import check_recipes, git_changes, main, select_recipes
from workflows.validate_openrecon_labels import SCHEMA_PATH


def write_recipe(root: Path, name: str = "tool", *, suite: bool = True, **values) -> Path:
    directory = root / "recipes" / name
    directory.mkdir(parents=True, exist_ok=True)
    recipe = {
        "name": name,
        "version": "1.0.0",
        "architectures": ["x86_64"],
        "categories": ["programming"],
        "icon": "data:image/png;base64,aWNvbg==",
        "readme": "Tool documentation",
        "auto_update": {
            "method": "sources", "container_version": False, "sources": [], "local": [],
        },
        "build": {
            "kind": "neurodocker", "base-image": "ubuntu:24.04", "pkg-manager": "apt",
            "directives": [{"run": "echo {{ arch }} {{ context.variant }}"}],
        },
        **values,
    }
    (directory / "build.yaml").write_text(yaml.safe_dump(recipe), encoding="utf-8")
    if suite:
        (directory / "fulltest.yaml").write_text(yaml.safe_dump({
            "name": name, "version": "1.0.0", "tests": [{"command": "command -v tool"}],
        }), encoding="utf-8")
    return directory


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, text=True, capture_output=True,
    ).stdout.strip()


def commit(root: Path) -> str:
    git(root, "add", ".")
    git(root, "-c", "user.name=Test", "-c", "user.email=test@example.com",
        "commit", "-m", "test snapshot")
    return git(root, "rev-parse", "HEAD")


def test_selection_covers_recipe_files_and_declared_shared_consumers(tmp_path):
    write_recipe(tmp_path, "direct")
    write_recipe(tmp_path, "consumer", auto_update={
        "local": ["macros/shared"],
    })
    write_recipe(tmp_path, "include", build={
        "directives": [{"group": [{"include": "shared/build.yaml"}]}],
    })
    write_recipe(tmp_path, "unrelated")
    assert select_recipes(tmp_path, changed_paths=[
        "builder/cli.py", "recipes/direct/asset.txt", "macros/shared/build.yaml",
        "recipes/removed/build.yaml",
    ]) == ["consumer", "direct", "include"]
    assert select_recipes(tmp_path, changed_paths=["macros/shared-other/build.yaml"]) == []
    assert select_recipes(tmp_path, changed_paths=["builder/cli.py"]) == []
    assert select_recipes(tmp_path, requested=["direct", "direct"]) == ["direct"]
    with pytest.raises(ValueError, match="Unknown recipes"):
        select_recipes(tmp_path, requested=["../direct"])


def test_macro_selection_does_not_silently_skip_unreadable_recipes(tmp_path):
    directory = write_recipe(tmp_path)
    (directory / "build.yaml").write_text("not: [valid YAML", encoding="utf-8")
    assert select_recipes(tmp_path, changed_paths=["macros/shared/file.yaml"]) == ["tool"]
    report = check_recipes(tmp_path, ["tool"])
    assert [failure.check for failure in report.failures] == ["load"]


def test_generates_real_dockerfiles_for_every_declared_architecture_and_variant(tmp_path):
    write_recipe(tmp_path, architectures=["aarch64", "x86_64"], variants={
        "gpu": {"architectures": ["x86_64", "aarch64"]},
    })
    write_recipe(tmp_path, "arm-only", architectures=["aarch64"])
    report = check_recipes(tmp_path, ["tool", "arm-only"])
    assert report.passed, report.failures
    assert [Path(path).parent.name for path in report.generated] == [
        "tool", "tool_arm64", "tool_gpu", "tool_gpu_arm64", "arm-only_arm64",
    ]
    for path in report.generated:
        dockerfile = Path(path).read_text()
        architecture = "aarch64" if "arm64" in Path(path).parent.name else "x86_64"
        assert f"echo {architecture}" in dockerfile


def test_disabled_default_only_generates_named_targets(tmp_path):
    write_recipe(tmp_path, build_default=False, architectures=["x86_64", "aarch64"],
                 variants={"lite": {"architecture": "aarch64"}})
    report = check_recipes(tmp_path, ["tool"])
    assert report.passed, report.failures
    assert [Path(path).parent.name for path in report.generated] == ["tool_lite_arm64"]


def test_failures_are_aggregated_across_checks_targets_and_recipes(tmp_path):
    write_recipe(tmp_path, "bad-metadata", icon="bad")
    directory = write_recipe(tmp_path, "bad-policy", auto_update={
        "method": "manual", "reason": "Locally maintained test example",
    })
    recipe = yaml.safe_load((directory / "build.yaml").read_text())
    recipe["architectures"] = ["x86_64", "aarch64"]
    recipe["variants"] = {"gpu": {"architecture": "x86_64"}}
    recipe["build"]["directives"].append({
        "run": '{{ get_file("missing") }}', "condition": 'arch == "aarch64"',
    })
    (directory / "build.yaml").write_text(yaml.safe_dump(recipe))
    write_recipe(tmp_path, "last-good")
    report = check_recipes(tmp_path, ["bad-metadata", "bad-policy", "last-good"])
    assert [(failure.recipe, failure.check) for failure in report.failures] == [
        ("bad-metadata", "validation"),
        ("bad-policy", "update-policy"),
        ("bad-policy", "generation:arm64/aarch64"),
    ]
    assert [Path(path).parent.name for path in report.generated] == [
        "bad-metadata", "bad-policy", "bad-policy_gpu", "last-good",
    ]
    assert not report.passed


def test_new_suite_policy_is_separate_from_existing_update_audit(tmp_path):
    write_recipe(tmp_path, "existing", suite=False)
    write_recipe(tmp_path, "new", suite=False)
    report = check_recipes(tmp_path, ["existing", "new"], new_recipes={"new"})
    assert [(failure.recipe, failure.check) for failure in report.failures] == [
        ("new", "new-recipe-fulltest"),
        ("existing", "update-policy"),
        ("new", "update-policy"),
    ]
    assert len(report.generated) == 2


def test_remote_readme_and_declared_downloads_are_not_fetched(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("generation checks must not download remote files")

    monkeypatch.setattr("builder.recipe._read_readme_url", forbidden)
    monkeypatch.setattr("builder.cache.HttpCache.get", forbidden)
    write_recipe(tmp_path, readme=None, readme_url="https://example.com/README.md", files=[{
        "name": "archive", "url": "https://example.com/source.tar.gz",
    }], build={
        "kind": "neurodocker", "base-image": "ubuntu:24.04", "pkg-manager": "apt",
        "directives": [{"run": 'tar -xf {{ get_file("archive") }}'}],
    })
    report = check_recipes(tmp_path, ["tool"])
    assert report.passed, report.failures
    assert "tar -xf" in Path(report.generated[0]).read_text()


def test_mixed_code_and_recipe_changes_enforce_new_suite_and_report_all_errors(tmp_path):
    git(tmp_path, "init")
    write_recipe(tmp_path, "existing", suite=False)
    base = commit(tmp_path)
    write_recipe(tmp_path, "new", suite=False)
    (tmp_path / "builder").mkdir()
    (tmp_path / "builder" / "example.py").write_text("# code change\n")
    head = commit(tmp_path)
    changed, new_recipes = git_changes(tmp_path, base, head)
    assert "builder/example.py" in changed
    assert new_recipes == {"new"}
    report_path = tmp_path / "report.json"
    assert main(["--root", str(tmp_path), "--base", base, "--head", head,
                 "--json", str(report_path)]) == 1
    report = json.loads(report_path.read_text())
    assert report["recipes"] == ["new"]
    assert report["failures"][0]["check"] == "new-recipe-fulltest"
    assert main(["--root", str(tmp_path), "--all", "--base", base,
                 "--json", str(report_path)]) == 1
    assert json.loads(report_path.read_text())["recipes"] == ["existing", "new"]
    # A targeted run must still validate every newly introduced recipe.
    assert main(["--root", str(tmp_path), "--recipes", "existing", "--base", base,
                 "--json", str(report_path)]) == 1
    assert json.loads(report_path.read_text())["recipes"] == ["existing", "new"]


def test_changed_suite_metadata_is_validated_even_without_recipe_build_change(tmp_path):
    directory = write_recipe(tmp_path)
    suite_path = directory / "fulltest.yaml"
    suite = yaml.safe_load(suite_path.read_text())
    suite["version"] = "0.9.0"
    suite_path.write_text(yaml.safe_dump(suite))
    selected = select_recipes(tmp_path, changed_paths=["recipes/tool/fulltest.yaml"])
    report = check_recipes(tmp_path, selected)
    assert len(report.failures) == 1
    assert "update both files" in report.failures[0].message


def test_divergent_base_deletion_is_not_a_new_recipe_on_merged_checkout(tmp_path):
    git(tmp_path, "init")
    write_recipe(tmp_path, "a")
    commit(tmp_path)
    git(tmp_path, "branch", "topic")
    git(tmp_path, "checkout", "-b", "upstream")
    git(tmp_path, "rm", "-r", "recipes/a")
    base = commit(tmp_path)
    git(tmp_path, "checkout", "topic")
    write_recipe(tmp_path, "b")
    head = commit(tmp_path)
    git(tmp_path, "-c", "user.name=Test", "-c", "user.email=test@example.com",
        "merge", "--no-edit", "upstream")
    assert not (tmp_path / "recipes" / "a").exists()
    changed, new_recipes = git_changes(tmp_path, base, head)
    assert new_recipes == {"b"}
    assert all(not path.startswith("recipes/a/") for path in changed)
    report_path = tmp_path / "report.json"
    assert main(["--root", str(tmp_path), "--base", base, "--head", head,
                 "--json", str(report_path)]) == 0
    report = json.loads(report_path.read_text())
    assert report["recipes"] == ["b"]
    assert report["failures"] == []


def write_label(root: Path, name: str, *, valid: bool = False) -> Path:
    recipes_dir = root / "recipes"
    shutil.copyfile(SCHEMA_PATH, recipes_dir / SCHEMA_PATH.name)
    directory = recipes_dir / name
    directory.mkdir(exist_ok=True)
    path = directory / "OpenReconLabel.json"
    if valid:
        shutil.copyfile(SCHEMA_PATH.parent / "b0map" / "OpenReconLabel.json", path)
    else:
        path.write_text("{}", encoding="utf-8")
    return path


def test_invalid_openrecon_label_joins_other_failures_without_stopping_generation(tmp_path):
    write_recipe(tmp_path, "invalid", icon="bad")
    write_recipe(tmp_path, "valid")
    invalid_label = write_label(tmp_path, "invalid")
    valid_label = write_label(tmp_path, "valid", valid=True)
    report = check_recipes(tmp_path, ["invalid", "valid"])
    assert report.checked_labels == [str(invalid_label), str(valid_label)]
    assert [(failure.recipe, failure.check) for failure in report.failures] == [
        ("invalid", "openrecon-label"), ("invalid", "validation"),
    ]
    assert "required property" in report.failures[0].message
    assert len(report.generated) == 2


def test_targeted_recipe_check_leaves_unaffected_labels_out(tmp_path):
    write_recipe(tmp_path, "tool")
    write_recipe(tmp_path, "unrelated")
    write_label(tmp_path, "unrelated")
    report_path = tmp_path / "report.json"
    assert main(["--root", str(tmp_path), "--recipes", "tool",
                 "--json", str(report_path)]) == 0
    assert json.loads(report_path.read_text())["checked_labels"] == []


def test_schema_only_change_validates_all_labels_without_generating_unaffected_recipes(tmp_path):
    git(tmp_path, "init")
    write_recipe(tmp_path)
    write_label(tmp_path, "tool")
    base = commit(tmp_path)
    schema_path = tmp_path / "recipes" / SCHEMA_PATH.name
    schema = json.loads(schema_path.read_text())
    schema["description"] = "Updated packaging schema"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")
    head = commit(tmp_path)
    report_path = tmp_path / "report.json"
    assert main(["--root", str(tmp_path), "--base", base, "--head", head,
                 "--json", str(report_path)]) == 1
    report = json.loads(report_path.read_text())
    assert report["recipes"] == []
    assert report["generated"] == []
    assert len(report["checked_labels"]) == 1
    assert report["failures"][0]["check"] == "openrecon-label"
