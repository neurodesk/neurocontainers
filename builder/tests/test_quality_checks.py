from __future__ import annotations

import copy
from pathlib import Path
import subprocess

import pytest
import yaml

from workflows.quality_checks import gate_errors, integration_selection, select


def successful_checks(selected: str = "true") -> dict:
    return {
        "detect-changes": {
            "result": "success",
            "outputs": {"buildkit": selected, "recipes": selected},
        },
        "python-quality": {"result": "success"},
        "test-builder": {"result": "success"},
        "test-go": {"result": "success"},
        "buildkit": {"result": "success" if selected == "true" else "skipped"},
        "recipes": {"result": "success" if selected == "true" else "skipped"},
    }


@pytest.mark.parametrize("selected", ["true", "false"])
def test_gate_accepts_passed_checks_and_deliberately_unselected_integrations(selected):
    assert gate_errors(successful_checks(selected)) == []


@pytest.mark.parametrize("job", list(successful_checks()))
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", None])
def test_gate_rejects_incomplete_or_failed_selected_checks(job, result):
    needs = successful_checks()
    needs[job]["result"] = result
    assert gate_errors(needs)


@pytest.mark.parametrize("flag", ["buildkit", "recipes"])
@pytest.mark.parametrize("value", [None, "", "maybe", True])
def test_gate_rejects_missing_or_malformed_selection(flag, value):
    needs = successful_checks()
    needs["detect-changes"]["outputs"][flag] = value
    assert gate_errors(needs)


def test_gate_does_not_accept_an_unselected_failed_job():
    needs = successful_checks("false")
    needs["buildkit"]["result"] = "failure"
    assert gate_errors(needs)


def test_gate_rejects_missing_job_results():
    for job in successful_checks():
        needs = copy.deepcopy(successful_checks())
        del needs[job]
        assert gate_errors(needs)


@pytest.mark.parametrize(
    "path",
    [
        "builder/recipe.py",
        "workflows/check_recipes.py",
        "tools/variant_matrix.py",
        "macros/openrecon/neurodocker.yaml",
        "requirements.txt",
        "pyproject.toml",
        "uv.lock",
        ".github/workflows/test-builder.yml",
        ".github/actionlint.yaml",
    ],
)
def test_shared_changes_select_both_integrations_and_all_recipes(path):
    assert integration_selection([path]) == {
        "buildkit": "true",
        "recipes": "true",
        "all-recipes": "true",
    }


def test_recipe_changes_select_generation_and_documentation_changes_skip_integrations():
    assert integration_selection(["recipes/tool/build.yaml"])["recipes"] == "true"
    assert integration_selection(["recipes/tool/build.yaml"])["buildkit"] == "false"
    assert integration_selection(["README.md"])["recipes"] == "false"


def test_git_selection_preserves_spaces_newlines_and_shell_characters(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    subprocess.run(["git", "init", "-q"], check=True)
    for setting, value in [("user.name", "Test"), ("user.email", "test@example.org")]:
        subprocess.run(["git", "config", setting, value], check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-qm", "base"], check=True)
    base = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    recipe = tmp_path / "recipes" / "spaces\n$(touch unexpected)" / "build.yaml"
    recipe.parent.mkdir(parents=True)
    recipe.write_text("name: test\n")
    subprocess.run(["git", "add", "."], check=True)
    subprocess.run(["git", "commit", "-qm", "recipe"], check=True)
    output = tmp_path / "outputs"
    select(base, "HEAD", output)
    assert "recipes=true\n" in output.read_text()
    assert "buildkit=false\n" in output.read_text()
    assert not (tmp_path / "unexpected").exists()


def test_invalid_comparison_fails_instead_of_skipping_checks(tmp_path):
    with pytest.raises(subprocess.CalledProcessError):
        select("invalid-reference", "HEAD", tmp_path / "outputs")
    assert not (tmp_path / "outputs").exists()


def test_required_gate_covers_every_workflow_job_and_runs_after_failures():
    definition = yaml.safe_load(Path(".github/workflows/test-builder.yml").read_text())
    jobs = definition["jobs"]
    gate = jobs["code-quality"]
    assert gate["name"] == "Code quality"
    assert gate["if"] == "always()"
    assert set(gate["needs"]) == set(jobs) - {"code-quality"}
    assert definition["permissions"] == {"contents": "read"}
    for job in jobs.values():
        assert job["runs-on"] == "ubuntu-22.04"
        assert not job.get("continue-on-error")


def test_recipe_workflow_treats_filenames_as_data():
    definition = yaml.safe_load(
        Path(".github/workflows/validate-recipes.yml").read_text()
    )
    step = next(
        step
        for step in definition["jobs"]["validate-recipes"]["steps"]
        if step.get("name") == "Validate changed recipes"
    )
    assert set(step["env"]) == {"BASE_SHA", "HEAD_SHA"}
    assert "git diff --name-only -z" in step["run"]
    assert 'for file in "${changed_files[@]}"' in step["run"]
    assert "${{" not in step["run"]
