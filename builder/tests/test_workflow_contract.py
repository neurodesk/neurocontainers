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


def test_build_app_workflow_strips_version_inline_comments() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    old_version_extractor = (
        "VERSION=$(sed -n 's/^version:[[:space:]]*//p' "
        '"recipes/${APPLICATION}/build.yaml" | head -1 | tr -d "\\\"\'")'
    )

    assert "sed 's/[[:space:]]#.*$//'" in workflow
    assert old_version_extractor not in workflow


def test_build_app_workflow_compares_image_config_not_only_rootfs() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    config_job = workflow.split("  config:", 1)[1].split("  build-image:", 1)[0]
    build_image_job = workflow.split("  build-image:", 1)[1].split("  push-dockerhub:", 1)[0]

    assert "IMAGE_FINGERPRINT_CACHE" in config_job
    assert "IMAGE_FINGERPRINT_NEW" in build_image_job
    assert "python3 builder/image_fingerprint.py" in config_job
    assert "python3 builder/image_fingerprint.py" in build_image_job
    assert "ROOTFS_CACHE" not in workflow
    assert "ROOTFS_NEW" not in workflow


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


def test_build_simg_uses_selected_runner_pool() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    build_simg_job = workflow.split("  build-simg:", 1)[1].split("  upload-nectar:", 1)[0]

    assert "runs-on: ${{ fromJSON(inputs.runner) }}" in build_simg_job
    assert (
        "contains(inputs.runner, 'arm') && 'blacksmith-8vcpu-ubuntu-2404-arm' || 'ubuntu-22.04'"
        not in build_simg_job
    )


def test_build_simg_sets_apptainer_paths_for_non_github_runners() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    build_simg_job = workflow.split("  build-simg:", 1)[1].split("  upload-nectar:", 1)[0]
    common_setup = build_simg_job.split("      - name: Set runner base path", 1)[1].split(
        "      - name: Configure GitHub-hosted runner",
        1,
    )[0]

    assert "elif [ -d /home/runner/_work ]; then" in common_setup
    assert 'BASE_PATH=/home/runner/_work' in common_setup
    assert '"$BASE_PATH/apptainer/cache" "$BASE_PATH/apptainer/tmp"' in common_setup
    assert 'sudo chown -R "$(id -u):$(id -g)" "$BASE_PATH/tmp" "$BASE_PATH/apptainer"' in common_setup
    assert 'sudo chmod -R u+rwX "$BASE_PATH/tmp" "$BASE_PATH/apptainer"' in common_setup
    assert 'APPTAINER_CACHEDIR="$BASE_PATH/apptainer/cache"' in common_setup
    assert 'APPTAINER_TMPDIR="$BASE_PATH/apptainer/tmp"' in common_setup
    assert 'SINGULARITY_CACHEDIR="$BASE_PATH/apptainer/cache"' in common_setup
    assert 'SINGULARITY_TMPDIR="$BASE_PATH/apptainer/tmp"' in common_setup


def test_setup_apptainer_updates_apt_before_local_deb_install() -> None:
    action = Path(".github/actions/setup-apptainer/action.yml").read_text()
    amd64_branch = action.split('else\n          if [[ ! -s "$deb_path" ]]', 1)[1].split(
        "        fi\n\n        echo",
        1,
    )[0]

    assert "sudo apt-get update" in amd64_branch
    assert amd64_branch.index("sudo apt-get update") < amd64_branch.index(
        'sudo apt-get install -y --no-install-recommends "$deb_path"'
    )


def test_nectar_registry_username_is_explicit() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    push_nectar_job = workflow.split("  push-nectar-registry:", 1)[1].split("  build-simg:", 1)[0]

    assert "username: s.bollmann@uq.edu.au" in push_nectar_job
    assert "REGISTRY_RC_NECTAR_ORG_AU_USERNAME" not in workflow


def test_update_apps_json_syncs_neurocommand_icons() -> None:
    workflow = Path(".github/workflows/update-apps-json.yml").read_text()

    assert "python -m pip install cairosvg" in workflow
    assert "python .github/workflows/scripts/sync_neurocontainer_icons.py" in workflow
    assert "--neurocontainers-path .." in workflow
    assert "git diff --quiet neurodesk/apps.json neurodesk/icons" in workflow
    assert "git add neurodesk/apps.json neurodesk/icons" in workflow


def test_update_apps_json_pushes_fixed_branch_without_per_release_pr() -> None:
    # apps.json updates flow through the fixed update-apps-json branch that
    # neurocommand's consolidation queue consumes; opening a PR per release
    # floods subscribers with notifications.
    workflow = Path(".github/workflows/update-apps-json.yml").read_text()

    assert 'BRANCH_NAME="update-apps-json"' in workflow
    assert 'git push --force origin "$BRANCH_NAME"' in workflow
    assert "gh pr create" not in workflow
    assert "group: update-apps-json" in workflow
