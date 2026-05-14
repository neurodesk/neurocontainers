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


def test_nectar_mirrors_are_best_effort() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    push_nectar_job = workflow.split("  push-nectar-registry:", 1)[1].split("  build-simg:", 1)[0]
    upload_nectar_job = workflow.split("  upload-nectar:", 1)[1].split("  upload-s3:", 1)[0]

    assert "continue-on-error: true" in push_nectar_job
    assert "continue-on-error: true" in upload_nectar_job


def test_create_pr_job_does_not_wait_for_nectar_mirrors() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    create_pr_header = workflow.split("  create-pr:", 1)[1].split("    steps:", 1)[0]

    assert "push-dockerhub" in create_pr_header
    assert "upload-s3" in create_pr_header
    assert "push-nectar-registry" not in create_pr_header
    assert "upload-nectar" not in create_pr_header


def test_nectar_registry_username_is_explicit() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    push_nectar_job = workflow.split("  push-nectar-registry:", 1)[1].split("  build-simg:", 1)[0]

    assert "username: s.bollmann@uq.edu.au" in push_nectar_job
    assert "REGISTRY_RC_NECTAR_ORG_AU_USERNAME" not in workflow
