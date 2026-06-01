from __future__ import annotations

from pathlib import Path


def test_build_app_workflow_uses_staged_cache_context() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()

    assert '--build-context "neurocontainer-cache=./cache"' in workflow
    assert "neurocontainer-cache=$HOME/.cache/neurocontainers/build-context" not in workflow


def test_build_app_workflow_uses_version_stable_build_cache_ref() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()

    assert "CACHE_REF=ghcr.io/${GH_REGISTRY}/${APPLICATION}${IMAGE_SUFFIX}:buildcache" in workflow
    assert "CACHE_REF=ghcr.io/${GH_REGISTRY}/${IMAGENAME}:buildcache" not in workflow


def test_create_pr_job_generates_release_without_rebuilding() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    create_pr_job = workflow.split("  create-pr:", 1)[1]

    assert 'python3 -m builder release "$APPLICATION" --write --architecture "$ARCHITECTURE"' in create_pr_job
    assert (
        'python3 -m builder build "$APPLICATION" --recreate --generate-release --architecture "$ARCHITECTURE"'
        not in create_pr_job
    )


def test_build_app_workflow_stages_without_hidden_docker_builds() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    config_job = workflow.split("  config:", 1)[1].split("  build-image:", 1)[0]
    build_image_job = workflow.split("  build-image:", 1)[1].split("  push-dockerhub:", 1)[0]

    assert 'python3 -m builder stage "$APPLICATION" --recreate --architecture "$ARCHITECTURE"' in config_job
    assert 'python3 -m builder stage "$APPLICATION" --recreate --download --architecture "$ARCHITECTURE"' in build_image_job
    assert "python3 -m builder build" not in config_job
    assert "python3 -m builder build" not in build_image_job
    assert "docker buildx build" in build_image_job


def test_nectar_mirrors_are_best_effort() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    push_nectar_job = workflow.split("  push-nectar-registry:", 1)[1].split("  build-simg:", 1)[0]
    upload_nectar_job = workflow.split("  upload-nectar:", 1)[1].split("  upload-s3:", 1)[0]

    assert "continue-on-error: true" in push_nectar_job
    assert "continue-on-error: true" in upload_nectar_job


def test_simg_upload_jobs_are_skipped_when_simg_build_is_skipped() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    build_simg_header = workflow.split("  build-simg:", 1)[1].split("    runs-on:", 1)[0]
    upload_nectar_header = workflow.split("  upload-nectar:", 1)[1].split("    # Nectar", 1)[0]
    upload_s3_header = workflow.split("  upload-s3:", 1)[1].split("    runs-on:", 1)[0]

    assert "inputs.skip_simg_build != 'true'" in build_simg_header
    assert "inputs.skip_simg_build != 'true'" in upload_nectar_header
    assert "inputs.skip_simg_build != 'true'" in upload_s3_header


def test_nectar_registry_username_is_explicit() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    push_nectar_job = workflow.split("  push-nectar-registry:", 1)[1].split("  build-simg:", 1)[0]

    assert "username: s.bollmann@uq.edu.au" in push_nectar_job
    assert "REGISTRY_RC_NECTAR_ORG_AU_USERNAME" not in workflow
