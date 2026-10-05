from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize(
    "workflow",
    [
        "arm-readiness",
        "auto-update",
        "build-app",
        "build-matlab-standalone",
        "cold-build-validation",
        "manual-build",
        "on-opening-issue",
        "pr-container-candidate",
        "process-issues",
        "publish-dashboard",
        "report-container-candidate",
        "test-release-pr",
        "update-apps-json",
        "update-webapps-json",
    ],
)
def test_neurodesk_automation_requires_the_upstream_repository(workflow: str) -> None:
    """Every infrastructure job must skip forks, including always() reporters."""
    definition = yaml.safe_load(Path(f".github/workflows/{workflow}.yml").read_text())
    guard = "github.repository == 'neurodesk/neurocontainers'"
    for name, job in definition["jobs"].items():
        condition = " ".join(job.get("if", "").split())
        if condition.startswith("${{") and condition.endswith("}}"):
            condition = condition[3:-2].strip()
        assert condition == guard or (
            condition.startswith(f"{guard} && (") and condition.endswith(")")
        ), f"{workflow}/{name} must require the upstream repository"


@pytest.mark.parametrize(
    "workflow",
    [
        "full-container-test",
        "manual-container-security-scan",
        "recipes-ci",
        "run-fulltest",
        "test-builder",
        "validate-recipes",
    ],
)
def test_portable_checks_remain_available_in_forks(workflow: str) -> None:
    definition = yaml.safe_load(Path(f".github/workflows/{workflow}.yml").read_text())
    for name, job in definition["jobs"].items():
        assert "github.repository" not in job.get("if", ""), (
            f"{workflow}/{name} must remain available in forks"
        )
