from __future__ import annotations

from pathlib import Path


def test_build_app_workflow_uses_staged_cache_context() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()

    assert '--build-context "neurocontainer-cache=./cache"' in workflow
    assert "neurocontainer-cache=$HOME/.cache/neurocontainers/build-context" not in workflow


def test_build_app_workflow_uses_version_stable_build_cache_ref() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()

    assert "CACHE_REF=ghcr.io/${GH_REGISTRY}/${CONTAINER_NAME}:buildcache" in workflow
    assert "CACHE_REF=ghcr.io/${GH_REGISTRY}/${IMAGENAME}:buildcache" not in workflow


def test_manual_workflow_expands_named_variants_and_passes_identity_to_builder() -> None:
    build_workflow = Path(".github/workflows/build-app.yml").read_text()
    manual_workflow = Path(".github/workflows/manual-build.yml").read_text()

    assert "variant: ${{ matrix.variant }}" in manual_workflow
    assert "architecture: ${{ matrix.architecture }}" in manual_workflow
    assert "-m tools.variant_matrix" in manual_workflow
    assert 'VARIANT_ARGS=(--variant "$VARIANT")' in build_workflow
    assert 'ARCHITECTURE="${{ inputs.architecture }}"' in build_workflow
    assert 'CONTAINER_NAME="${APPLICATION}_${VARIANT}"' in build_workflow


def test_candidate_workflow_builds_every_declared_variant() -> None:
    candidate_workflow = Path(".github/workflows/pr-container-candidate.yml").read_text()

    assert "include: ${{ fromJSON(needs.detect.outputs.targets) }}" in candidate_workflow
    assert (
        "matrix.container }} | ${{ matrix.version }} | ${{ matrix.architecture"
        in candidate_workflow
    )
    assert '--architecture "${ARCHITECTURE}" --variant "${VARIANT}"' in candidate_workflow
    # aarch64 candidates must not land on the x86 ARC pool.
    assert "matrix.architecture == 'aarch64'" in candidate_workflow
    assert "--architecture x86_64" not in candidate_workflow


def test_candidate_workflow_reports_every_premerge_check_in_one_comment() -> None:
    candidate_workflow = Path(".github/workflows/pr-container-candidate.yml").read_text()
    reporter = Path(".github/workflows/report-container-candidate.yml").read_text()
    validator = Path(".github/workflows/validate-recipes.yml").read_text()

    assert "Validate changed recipes and OpenRecon metadata" in candidate_workflow
    assert "candidate-report-${{ matrix.container }}" in candidate_workflow
    assert 'tee "candidate/${CONTAINER}/dive-report.txt"' in candidate_workflow
    assert '--dive-status "${DIVE_OUTCOME}"' in candidate_workflow
    assert "pattern: candidate-report-*" in reporter
    assert "passed — review and merge PR" in reporter
    assert "candidate failed after PR" in reporter
    assert "closed without merging" in reporter
    assert "Action required:" in reporter
    assert "### What was tested" in reporter
    assert "Deploy check:" in reporter
    assert "Runtime/fulltest:" in reporter
    assert "github.rest.pulls.get" in reporter
    assert "head: `${forkOwner}:${run.head_branch}`" in reporter
    assert "reportPrNumbers" in reporter
    assert "candidate.head.sha === run.head_sha" in reporter
    assert "pr.head.sha !== run.head_sha" in reporter
    assert "core.setFailed(`Expected one PR" in reporter
    assert "### Dive failures" in reporter
    assert "report.dive.failedChecks" in reporter
    assert "What automatic promotion publishes" in reporter
    assert "container-release-status:start" in reporter
    assert "container-release-identity:" in reporter
    assert "promote-container-candidate.yml" in reporter
    assert "promotionRun?.conclusion === 'success'" in reporter
    assert "--name ${candidate.artifactName} --dir ${candidateDir}" in reporter
    assert "--pattern 'candidate-*'" not in reporter
    assert "artifact.expires_at" in reporter
    assert "artifact.size_in_bytes" in reporter
    assert "report.tests.passedTests" in reporter
    assert "No recipe build candidates; no container approval comment is needed" in reporter
    assert "classified the recipe changes as source-only" in reporter
    assert "file.filename.match(/^recipes\\/([^/]+)\\//)" in reporter
    assert "container-release-outcome: ${outcome}" in reporter
    assert "container-release-transition: ${previousOutcome}-to-${outcome}" in reporter
    assert "previousOutcome && previousOutcome !== outcome" in reporter
    assert "alreadyNotified" in reporter
    assert "The previous failing candidate result no longer applies" in reporter
    assert "View the full lifecycle summary" in reporter
    assert reporter.count("github.rest.issues.createComment") == 2
    assert "github.rest.issues.updateComment" in reporter
    assert reporter.index("body: transitionBody") < reporter.index(
        "github.rest.issues.updateComment"
    )
    assert "createComment" not in validator
    assert "pull-requests: write" not in validator


def test_promotion_updates_the_existing_candidate_comment_in_place() -> None:
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    report_job = workflow.split("  report_promotion:", 1)[1]

    assert "needs: [resolve, await_candidate, promote]" in report_job
    assert "always()" in report_job
    assert "pull-requests: write" in report_job
    assert "container-release-status:start" in report_job
    assert "container-release-identity:" in report_job
    assert "github.rest.issues.updateComment" in report_job
    assert "github.rest.issues.createComment" not in report_job
    assert "candidate reporter will include this promotion state" in report_job


def test_candidate_artifacts_and_promotion_key_off_container_identity() -> None:
    candidate_workflow = Path(".github/workflows/pr-container-candidate.yml").read_text()
    promote_workflow = Path(".github/workflows/promote-container-candidate.yml").read_text()
    publish_steps = promote_workflow.split(
        "      - name: Publish the exact tested Docker archives", 1
    )[1].split("      - name: Commit generated release metadata directly to main", 1)[0]

    assert (
        "name: candidate-${{ matrix.container }}-${{ github.event.pull_request.head.sha }}"
        in candidate_workflow
    )
    assert "path: candidate/${{ matrix.container }}/" in candidate_workflow
    # Two variants of one recipe publish to different registry repositories.
    assert 'ghcr="ghcr.io/${GH_REGISTRY}/${container}"' in publish_steps
    assert 'quay="quay.io/neurodesk/${container}"' in publish_steps
    assert "${recipe}" not in publish_steps


def test_candidate_promotion_syncs_openrecon_from_verified_manifests() -> None:
    promote_workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    release_step = "      - name: Commit generated release metadata directly to main"
    sync_step = "      - name: Sync OpenRecon metadata"

    assert sync_step in promote_workflow
    assert promote_workflow.index(sync_step) > promote_workflow.index(release_step)
    sync_body = promote_workflow.split(sync_step, 1)[1]
    assert "NEURODESK_GITHUB_TOKEN_ISSUE_AUTOMATION" in sync_body
    assert 'select(.architecture == "x86_64" and .variant == "")' in sync_body
    assert 'recipe="$(echo "${manifest}" | jq -r \'.recipe\')"' in sync_body
    assert 'version="$(echo "${manifest}" | jq -r \'.version\')"' in sync_body
    assert 'python tools/sync_openrecon.py --recipe "${recipe}" --version "${version}"' in sync_body


def test_candidate_promotion_waits_for_exact_candidate_before_using_arc() -> None:
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    wait_job = workflow.split("  await_candidate:", 1)[1].split("  promote:", 1)[0]
    promote_job = workflow.split("  promote:", 1)[1]

    assert "runs-on: ubuntu-latest" in wait_job
    assert "timeout-minutes: 180" in wait_job
    assert "actions: read" in wait_job
    assert "while (Date.now() < deadline)" in wait_job
    assert "await new Promise(resolve => setTimeout(resolve, 30_000))" in wait_job
    assert "item.head_sha === headSha" in wait_job
    assert "run.status === 'completed'" in wait_job
    assert "No successful candidate run" not in promote_job
    assert "needs: [resolve, await_candidate]" in promote_job
    assert (
        "ARTIFACTS: ${{ needs.await_candidate.outputs.artifacts }}"
        in promote_job
    )


def test_release_paths_dispatch_unchanged_openrecon_recipes() -> None:
    build_workflow = Path(".github/workflows/build-app.yml").read_text()
    promote_workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    flag = "--dispatch-unchanged"
    assert build_workflow.count(flag) == 1
    assert promote_workflow.count(flag) == 1


def test_candidate_promotion_uses_trusted_main_oidc_identity() -> None:
    """Promotion resolves a main push to the tested PR head before publishing."""
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    assert "  push:\n    branches: [main]" in workflow
    assert "  workflow_dispatch:" in workflow
    assert "pull_request_target:" not in workflow
    assert "listPullRequestsAssociatedWithCommit" in workflow
    assert "item.merge_commit_sha === context.sha" in workflow
    assert "needs.resolve.outputs.should_promote == 'true' &&" in workflow
    assert "needs.resolve.outputs.recipes != '[]'" in workflow
    assert "HEAD_SHA: ${{ needs.resolve.outputs.head_sha }}" in workflow
    assert "PR_NUMBER: ${{ needs.resolve.outputs.pr_number }}" in workflow


def test_pr_and_postmerge_workflows_share_the_release_planner() -> None:
    candidate = Path(".github/workflows/pr-container-candidate.yml").read_text()
    promotion = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    planner = "python tools/one_pr_release.py --repo-root . detect"
    assert "../trusted/tools/one_pr_release.py --repo-root . detect" in candidate
    assert planner in promotion
    assert "steps.detect.outputs.changed_recipes != ''" in candidate
    assert 'steps.detect.outputs.changed_recipes != \'[]\'' in candidate
    assert 'RECIPES: ${{ steps.detect.outputs.changed_recipes }}' in candidate
    assert '- "recipes/**"' in promotion
    assert 'needs.resolve.outputs.recipes != \'[]\'' in promotion
    assert "permissions: {}" in promotion


def test_candidate_promotion_installs_aws_cli_before_s3_upload() -> None:
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    install_step = workflow.split("      - name: Install promotion dependencies", 1)[1]
    install_step = install_step.split(
        "      - name: Download candidate bundles without executing them", 1
    )[0]
    assert "awscli-exe-linux-x86_64.zip" in install_step
    assert "aws --version" in install_step


def test_candidate_promotion_preserves_optional_publish_behaviour() -> None:
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    assert "for attempt in 1 2 3" in workflow
    assert "Make Quay repositories public" in workflow
    assert "QUAY_API_TOKEN" in workflow
    assert "/changevisibility" in workflow
    assert workflow.count('org.opencontainers.image.title=${container}') == 2
    assert workflow.count('org.opencontainers.image.version=${version}_${build_date}') == 2


def test_candidate_promotion_refreshes_auth_after_long_publication() -> None:
    workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    checkout = workflow.split("      - name: Checkout trusted main", 1)[1].split(
        "      - uses: actions/setup-python@v6", 1
    )[0]
    token_step = "      - name: Create fresh release token for metadata push"
    metadata_step = "      - name: Commit generated release metadata directly to main"

    assert "persist-credentials: false" in checkout
    assert workflow.index("      - name: Attach tested SIFs to OCI images") < workflow.index(
        token_step
    )
    assert workflow.index(token_step) < workflow.index(metadata_step)
    metadata = workflow.split(metadata_step, 1)[1].split(
        "      - name: Sync OpenRecon metadata", 1
    )[0]
    assert "RELEASE_TOKEN: ${{ steps.metadata-token.outputs.token }}" in metadata
    assert metadata.count('http.https://github.com/.extraheader="${git_auth_header}"') == 2


def test_manual_and_candidate_release_paths_share_openrecon_sync_helper() -> None:
    build_workflow = Path(".github/workflows/build-app.yml").read_text()
    promote_workflow = Path(
        ".github/workflows/promote-container-candidate.yml"
    ).read_text()

    helper = "python tools/sync_openrecon.py"
    assert build_workflow.count(helper) == 1
    assert promote_workflow.count(helper) == 1


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


def test_config_job_fingerprints_latest_without_pulling_it() -> None:
    # `docker inspect` needs the image locally, so fingerprinting :latest used
    # to download every layer just to read the small config object.
    workflow = Path(".github/workflows/build-app.yml").read_text()
    config_job = workflow.split("  config:", 1)[1].split("  build-image:", 1)[0]

    assert "docker pull" not in config_job
    assert (
        "python3 builder/image_fingerprint.py \\\n"
        '            --remote --allow-missing --architecture "$ARCHITECTURE" "$IMAGE_REF"'
    ) in config_job


def test_build_image_job_fingerprints_the_new_image_locally() -> None:
    # The new image only exists in the local daemon at comparison time.
    workflow = Path(".github/workflows/build-app.yml").read_text()
    build_image_job = workflow.split("  build-image:", 1)[1].split("  push-dockerhub:", 1)[0]

    assert 'python3 builder/image_fingerprint.py "$IMAGE_REF"' in build_image_job
    assert "--remote" not in build_image_job


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


def test_dive_waste_check_is_reported_on_release_pr_without_opening_an_issue() -> None:
    build_workflow = Path(".github/workflows/build-app.yml").read_text()
    release_test_workflow = Path(".github/workflows/test-release-pr.yml").read_text()

    assert "Analyze image layer waste with Dive" not in build_workflow
    assert "Open issue for Dive wasted space" not in build_workflow
    assert "Analyze image layer waste with Dive" in release_test_workflow
    assert "Dive image layer waste analysis" in release_test_workflow
    assert "dive-status-${{ matrix.release.name }}.txt" in release_test_workflow
    assert "gh issue create" not in release_test_workflow


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


def test_update_apps_json_runs_for_release_file_pushes() -> None:
    # Release metadata can be removed directly from main as well as merged via
    # a PR. A main-branch push trigger covers both paths and avoids leaving the
    # fixed update branch with a stale release snapshot.
    workflow = Path(".github/workflows/update-apps-json.yml").read_text()

    assert "  push:\n    branches: [main]\n    paths:\n      - \"releases/**/*.json\"" in workflow
    assert "pull_request:" not in workflow
    assert "github.event.pull_request.merged" not in workflow
