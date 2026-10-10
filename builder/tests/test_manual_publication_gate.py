from pathlib import Path
import re

import pytest
import yaml


@pytest.mark.parametrize(
    "repository", ["neurodesk/neurocontainers", "contributor/neurocontainers"]
)
@pytest.mark.parametrize(
    "suffix,quay,ghcr,s3,changed,forced,cancelled,expected",
    [
        ("", "success", "success", "success", "true", "false", False, True),
        ("_arm64", "skipped", "skipped", "success", "true", "false", False, True),
        ("", "failure", "success", "success", "true", "false", False, False),
        ("", "success", "failure", "success", "true", "false", False, False),
        ("", "skipped", "skipped", "success", "true", "false", False, False),
        ("_arm64", "skipped", "failure", "success", "true", "false", False, False),
        ("_arm64", "skipped", "skipped", "failure", "true", "false", False, False),
        ("_arm64", "skipped", "skipped", "skipped", "true", "false", False, False),
        ("_arm64", "skipped", "skipped", "success", "false", "true", False, True),
        ("_arm64", "skipped", "skipped", "success", "false", "false", False, False),
        ("_arm64", "skipped", "skipped", "success", "true", "false", True, False),
    ],
)
def test_release_pr_requires_publication_but_allows_intentional_arm_skips(
    suffix, quay, ghcr, s3, changed, forced, cancelled, expected, repository
):
    """Require upstream identity and successful publication before release PRs."""
    workflow = yaml.safe_load(Path(".github/workflows/build-app.yml").read_text())
    job = workflow["jobs"]["create-pr"]
    results = {name: "success" for name in job["needs"]}
    results.update({"push-quay": quay, "push-ghcr": ghcr, "upload-s3": s3})
    values = {f"needs.{name}.result": value for name, value in results.items()}
    values.update(
        {
            "needs.config.outputs.IMAGE_SUFFIX": suffix,
            "needs.build-image.outputs.IMGDIFFERS": changed,
            "inputs.force_create_pr": forced,
            "github.repository": repository,
        }
    )
    expression = job["if"].removeprefix("${{").removesuffix("}}").strip()
    has_status_check = bool(
        re.search(r"\b(?:cancelled|success|failure|always)\(", expression)
    )
    expression = re.sub(
        r"needs\.[\w.-]+|inputs\.[\w]+|github\.repository",
        lambda match: repr(values[match.group()]),
        expression,
    )
    expression = expression.replace("cancelled()", repr(cancelled))
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression)
    condition = eval(" ".join(expression.split()), {"__builtins__": {}}, {})
    implicit_success = has_status_check or all(
        result == "success" for result in results.values()
    )
    assert bool(condition and implicit_success) is (
        expected and repository == "neurodesk/neurocontainers"
    )
