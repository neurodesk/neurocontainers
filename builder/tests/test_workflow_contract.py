from __future__ import annotations

from pathlib import Path


def test_build_app_workflow_uses_staged_cache_context() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()

    assert '--build-context "neurocontainer-cache=./cache"' in workflow
    assert "neurocontainer-cache=$HOME/.cache/neurocontainers/build-context" not in workflow


def test_create_pr_job_generates_release_without_rebuilding() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    create_pr_job = workflow.split("  create-pr:", 1)[1]

    assert 'python3 -m builder release "$APPLICATION" --write --architecture "$ARCHITECTURE"' in create_pr_job
    assert (
        'python3 -m builder build "$APPLICATION" --recreate --generate-release --architecture "$ARCHITECTURE"'
        not in create_pr_job
    )
