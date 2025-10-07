# Container Testing Workflow

This document describes how container tests are defined, executed locally, and automated through the GitHub Actions workflows in this repository. The focus is on the tooling under `workflows/` and the CI entry points that exercise it.

## Test Definitions and Tooling

- **Test sources** live beside each recipe. The default location is `recipes/<name>/test.yaml`; when that file is absent, the tooling falls back to the `tests` block embedded in `recipes/<name>/build.yaml`. Each test entry generally specifies a shell script snippet and optional mounts or GPU requirements.
- **`workflows/test_runner.py`** provides the high-level `ContainerTestRunner` used by local commands, the full-matrix script, and GitHub Actions. Internally it relies on `workflows/container_tester.py`, which handles runtime selection, container discovery, and test execution. Key flags you will see in CI:
  - `--runtime apptainer` forces the Apptainer/Singularity backend.
  - `--location auto` searches CVMFS first, then local `./sifs`, then downloads via release metadata, and finally falls back to Docker tags.
  - `--release-file …` injects build-date information so downloads come from the correct storage path.
  - `--cleanup` deletes any downloaded artifacts after tests finish; `--output` writes a JSON summary used for reporting.
- **`workflows/generate_test_report.py`** turns the JSON summary into a PR-friendly Markdown report. GitHub Actions uploads both the JSON and Markdown outputs as artifacts and uses the Markdown for inline comments.
- **`workflows/format_test_results.py`** converts a single container’s JSON results into Markdown suitable for GitHub issue comments.
- **`workflows/pr_test_runner.py`** provides a higher-level interface that scans git diff to find updated recipes. It is useful for local validation (`test-containers.sh test-pr`) but is not wired into the current Actions workflows; instead, the workflows invoke `container_tester.py` directly.

## GitHub Actions Entry Points

### Release PR Testing – `.github/workflows/test-release-pr.yml`

This workflow runs automatically when a pull request targeting `master` or `main` modifies `releases/*/*.json`.

1. **`detect-changes` job** (Ubuntu runner) checks out the repository with full history and enumerates modified release descriptors. It emits a JSON matrix containing each `{name, version, file}` tuple and a `has-changes` flag.
2. **`test-containers` job** (self-hosted runner) fans out across the matrix when `has-changes` is true. Each matrix leg:
   - Checks out the repo and installs Python requirements.
   - Verifies container runtimes (`docker`, `apptainer` or `singularity`) so the self-hosted machine has the necessary binaries.
   - Locates the test definition by preferring `recipes/<name>/test.yaml` and falling back to `recipes/<name>/build.yaml` (`find-tests` step).
   - Runs the shared runner with `cleanup` enabled so downloaded artifacts are removed once tests finish. The `continue-on-error` flag lets subsequent steps gather logs even when tests fail.
   - Generates Markdown via `workflows/generate_test_report.py` and uploads both the JSON and Markdown artifacts as `test-results-<name>`.
   - Uses `actions/github-script` to post (or update) a PR comment containing the Markdown report for that specific container.
3. **`summarize-results` job** (Ubuntu runner) aggregates every `test-results-*.json` artifact, prints a count of passed/failed recipes, and updates a single “Container Test Summary” PR comment. If any container failed, this job calls `core.setFailed`, which fails the workflow and surfaces red status in the PR checks.

### Fleet Release Smoke Test – `.github/workflows/full-container-test.yml`

This manually triggered workflow validates every recipe that has a published release entry. When dispatching the workflow you can optionally supply a comma-separated `recipes` list to limit the matrix to specific containers:

1. **`prepare-matrix` job** builds a catalog of recipes, pairing each with the newest `releases/<recipe>/<version>.json` by comparing the embedded build date. Recipes without a release are kept in the matrix and marked for skip reporting. If the optional `recipes` filter is provided, the catalog is trimmed to those names; missing entries are called out in the tracking issue for visibility.
2. **`create-issue` job** opens a tracking GitHub Issue summarising how many recipes will run versus skip and captures the runner architecture from the dispatch input.
3. **`test-containers` job** fans out across the matrix. When a release exists, it downloads the published SIF via `container_tester.py --release-file … --cleanup`, executes the recipe’s tests, converts the JSON output to Markdown with `format_test_results.py`, and posts the result as a comment on the tracking issue. Recipes without tests or without releases generate skipped-result comments instead.
4. **`finalize` job** gathers every JSON artifact, posts an aggregated summary comment, and updates the issue body with pass/fail/skipped totals.

### On-Demand Recipe Matrix – `.github/workflows/recipes-ci.yml`

This manually triggered workflow supports large-scale or architecture-specific validation runs.

1. **Inputs** allow maintainers to choose architecture (`x86_64` or `arm64`), toggle a debug mode (limits to the `niimath` recipe), and decide whether to execute container tests (`run-tests` toggle).
2. **`build-builder-sif` job** builds the reusable `builder.sif` artifact:
   - Installs Apptainer on the GitHub-hosted runner.
  - Installs the repo as a Python package to expose `sf-*` entry points.
  - Runs `sf-make builder --ignore-architectures --architecture <arch> --use-docker` and uploads the resulting SIF under `sifs/`.
3. **`prepare-matrix` job** walks `recipes/*/build.yaml` (skipping the `builder` recipe) to produce the matrix of recipe names. In debug mode the list is reduced to speed iteration.
4. **`build-or-test-recipe` job** runs per recipe:
   - Downloads the cached `builder.sif`, prepares a writable `/mnt/tmp`, and uses Apptainer + `sf-make` inside the SIF to build the target recipe SIF.
   - When `run-tests` is true, the job invokes the shared runner so the same reporting logic generates JSON, Markdown comments, and detailed reports. Because the freshly built SIF resides in the workspace, the runner uses the `--location local` resolution path instead of downloading.

The workflow defaults to `debug=true` and `run-tests=false`, so full container testing is opt-in; maintainers enable testing when they need deeper validation across many recipes or architectures.

### Builder Linting – `.github/workflows/test-builder.yml`

While not a runtime test, this workflow protects the builder tooling whenever files under `builder/` change. It creates a Python virtual environment, installs `requirements.txt`, and runs `./workflows/test_all.sh`. The script validates every recipe (`builder/validation.py`) and performs a “check-only” build via `builder/build.py generate … --check-only`. Failures here usually indicate malformed recipe metadata that would prevent the container tests from running downstream.

## Reproducing CI Runs Locally

1. **Set up dependencies**: ensure Apptainer/Singularity (or Docker) is installed, create a virtual environment, and `pip install -r requirements.txt`.
2. **Single container or release**:
   ```bash
   sf-test-remote dcm2niix \
     --version v1.0.20240202 \
     --release-file releases/dcm2niix/v1.0.20240202.json \
     --runtime apptainer \
     --location auto \
     --cleanup
   ```
   This mirrors the invocation inside `test-release-pr.yml` and produces the same artifacts.
3. **Recipe-focused check** (simulates `recipes-ci` when `run-tests=true`):
   ```bash
   sf-test-remote <name> --version <version> --location local --runtime apptainer --cleanup
   ```
   Make sure the corresponding `sifs/<name>_<version>.sif` exists, e.g. by running `sf-make <name>` first.
4. **Convenience wrapper**: `./test-containers.sh` offers shortcuts such as `./test-containers.sh test <name:version>` and `./test-containers.sh test-pr`, which chains through `workflows/pr_test_runner.py` to exercise every recipe touched by your local branch.

## Outputs, Reporting, and Cleanup

- CI stores raw JSON results as `builder/test-results-<name>.json` and Markdown summaries as `builder/test-report-<name>.md`. These are uploaded as artifacts and embedded into PR comments.
- `container_tester.py` exits non-zero when any test fails. The GitHub Actions steps run with `continue-on-error: true`, but the final summarizing job converts failures into a failed workflow run.
- Release-driven tests run with `--cleanup`, which deletes downloaded SIFs after completion. Locally you can reuse cached downloads (`~/.cache/neurocontainers`) or purge everything with `sf-test-remote --cleanup-all`.
- When CVMFS is mounted, `--location auto` serves containers directly without downloads; otherwise the release metadata path is used.

By following the commands above and reviewing the referenced workflows, you can replicate, debug, and extend the automated container testing pipeline used by NeuroContainers.
