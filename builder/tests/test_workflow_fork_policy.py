import re
from pathlib import Path

import pytest
import yaml


def _requires_upstream_repository(condition: str) -> bool:
    condition = " ".join(condition.split())
    if condition.startswith("${{") and condition.endswith("}}"):
        condition = condition[3:-2].strip()
    guard = "github.repository == 'neurodesk/neurocontainers'"
    if condition == guard:
        return True
    prefix = f"{guard} && ("
    if not condition.startswith(prefix) or not condition.endswith(")"):
        return False
    expression = condition[len(prefix) : -1]
    expression = re.sub(r"'(?:[^']|'')*'", "", expression)
    depth = 0
    for character in expression:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("always()", True),
        ("true || false", True),
        ("contains('a) and ''b(', 'x')", True),
        ("always()) || (true", False),
        ("true) || (github.event_name == 'schedule'", False),
    ],
)
def test_repository_guard_cannot_be_bypassed_by_an_outer_or(
    expression: str, expected: bool
) -> None:
    condition = f"github.repository == 'neurodesk/neurocontainers' && ({expression})"
    assert _requires_upstream_repository(condition) is expected


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
    for name, job in definition["jobs"].items():
        assert _requires_upstream_repository(job.get("if", "")), (
            f"{workflow}/{name} must require the upstream repository"
        )


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
