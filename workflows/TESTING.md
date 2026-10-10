# Container Testing Workflow

This document describes how container tests are defined, executed locally, and automated through the GitHub Actions workflows in this repository. The focus is on the tooling under `workflows/` and the CI entry points that exercise it.

## Test Definitions and Tooling

- **Test sources** live beside each recipe in `recipes/<name>/fulltest.yaml`. `test.yaml`, `test.sh`, and embedded `build.yaml` tests are no longer supported recipe test formats.
- **`builder/run_tests.py`** is the fulltest executor. Release PR testing invokes it through `workflows/release_test_runner.py`, which also runs the builtin deploy bins/path check.
- **`builder/release_artifact.py`** decides which SIF a fulltest runs against. See [Release Artifact Resolution](#release-artifact-resolution) below.
- **`workflows/container_tester.py`** still provides runtime selection, release container downloads, Docker-to-SIF conversion, and the builtin deploy check. It is not the fulltest YAML executor. Key flags you will see in CI:
  - `--runtime apptainer` forces the Apptainer/Singularity backend.
  - `--location auto` searches CVMFS first, then local `./sifs`, then downloads via release metadata. Release downloads try AWS S3 first and fall back to Nectar Object Storage. Docker tags are only used as the final fallback when the selected runtime is Docker.
  - `--docker-to-simg` bypasses the normal SIF lookup, pulls `ghcr.io/<registry>/<name>_<version>:<build_date>`, converts `docker save` output with `docker-save-to-simg`, and tests the generated `.simg` with Apptainer.
  - `--release-file …` injects build-date information so downloads come from the correct storage path.
  - `--cleanup` deletes any downloaded artifacts after tests finish; `--output` writes a JSON summary used for reporting.
- **`workflows/reporting.py`** turns JSON summaries into PR-friendly Markdown — `build_comment` for inline issue comments, `build_report` for the full report, and `build_aggregate_summary` for the matrix roll-up. It is the single source of report formatting; `test_runner.py`, `release_test_runner.py`, and `full_container_test.py` all call into it. GitHub Actions uploads both the JSON and Markdown outputs as artifacts.
- **`workflows/summarize_deploy_results.py`** parses the builtin `test_deploy.sh` output into a bins/path tree used by the same reports.

## Release test artifacts

Each `release_test_runner.py` invocation creates a fresh `fulltest-run-*` directory
under `--output-dir`. It retains the generated suite, raw JSON results, JSONL
records, log, and private `work/` directory after success or failure. Repeated runs
cannot reuse these files. Local candidates use their original container paths.
Downloaded and converted images use retained private hardlinks. The runner does
not copy image contents into the run directory or delete inputs or the shared cache.

`--results-path` and the report, comment, and status filenames under `--output-dir`
remain the latest published result. Concurrent callers that need independent
reports must select separate output directories and results paths. CI uploads the
diagnostics and excludes private work directories.

If fulltest execution fails before producing usable results, the published result
keeps completed deploy checks and adds one failed runner check with the original
error. Artifact references include only files created by that invocation. A missing
or malformed raw result does not erase the available log or JSONL references.
If report publication fails after saving results JSON, the runner leaves that JSON
intact, exits with failure, and reports failed status through `GITHUB_OUTPUT` when
it can write that file.

### Clean up old run artifacts

Preview cleanup, then delete eligible runs with the same retention period:

```bash
python -m workflows.test_run_artifacts --output-dir builder --older-than-days 7 --dry-run
python -m workflows.test_run_artifacts --output-dir builder --older-than-days 7
```

The default retention period is seven days since completion. Zero deletes eligible
superseded runs immediately. Negative and nonfinite ages are rejected. Each matching
entry prints its action and reason. Errors while accessing or deleting candidate
directories return a nonzero exit status. Dry-run takes the same locks and checks
references without changing files.

Cleanup considers only immediate `fulltest-run-*` directories with valid ownership
metadata. A directory lock protects execution and every publication step. Completion
means publication succeeded, even when tests failed. Each run records its absolute
results destination, so latest results at custom or external paths remain protected.
Publication holds a separate lock on the results destination's parent directory
through JSON writing, enrichment, and report generation. Cleanup takes this same
lock without waiting and holds it through reading the latest references and deletion.
A busy destination retains its candidates. This also coordinates runners using
separate output roots with a shared external destination. Results in the same parent
directory serialize publication, including skipped runs. These locks create no
sidecar files. A candidate that is itself the results parent is retained.

Cleanup retains active and unfinished runs, legacy directories without metadata,
invalid records, and runs whose latest results are missing, unreadable, or malformed.
Interrupted publication and forced termination leave unfinished runs, even if a child
process continues after its parent dies. These directories need manual review and
removal after all writers stop. A missing external results file can retain completed
runs indefinitely. Symlink candidates are skipped, and deletion never follows nested
symlinks. This command does not clean container images or the shared download cache.

### Image acquisition ownership

Release downloads and Docker conversion return `AcquiredImage` resources. Use
`with image:` and pass `str(image.path)` to runtime commands. The resource owns
a private directory on the configured cache filesystem. Its image is a hardlink
to the completed image, so retaining it does not copy the image contents.
`image.cache_path` identifies the shared cache entry for explicit eviction.

Each acquisition writes privately and atomically publishes only complete output.
A cache refresh or eviction cannot change an image retained by another caller.
Docker conversion also retains its converter binary until the resource closes.
Failed builds preserve the previous cache entry and remove their private files.

`ContainerTester` owns acquired images until `close()` or context exit. Runners
release these resources on success, errors, and early returns, including when
cache cleanup was not requested. `--cleanup` and `--auto-cleanup` additionally
evict shared cache entries. Local, candidate, and CVMFS inputs remain caller-owned.

A refresh can temporarily retain both old and new image data. Old image blocks
are freed when their last retained link closes. Each acquisition directory holds
an exclusive `flock` on itself while its owner is alive. Forced termination such
as SIGKILL leaves the directory behind but releases the lock. Every new
acquisition and `--cleanup-all` remove unlocked `.acquisition-*` directories in
the cache, so live acquisitions are never touched.


## Release Artifact Resolution

A fulltest always tests the container the recipe builds now. That is the rule everything below serves: `version:` must equal the recipe's `build.yaml` version, and no older release of the same recipe is ever substituted for it.

`fulltest.yaml` does not name the SIF it runs against. `releases/<recipe>/<version>.json` already records the authoritative build date, so `builder/release_artifact.py` derives the artifact from it and the recipe files stay untouched when a container is rebuilt.

Resolution order, highest precedence first:

1. **`--container <path>`** — the artifact the caller already downloaded or built. Both CI entry points use this: `workflows/release_test_runner.py` passes the release SIF (or `--candidate-container`) it just fetched, and `.github/workflows/run-fulltest.yml` passes the file it downloaded or converted. CI therefore never depends on a filename lookup.
2. **`container:` plus `pin_container: true`** — an explicit historical pin. The named file must exist; nothing is derived and nothing falls back.
3. **Release metadata** — `releases/<recipe>/<version>.json`, keyed on the suite's `name:` and `version:` (top-level variables in `version:` are expanded first). The match is exact. The expected filename is `<image>_<build_date>.simg`, or `<recipe>_<version>_<build_date>.simg` when the metadata carries no `image` field.
4. **Version-scoped lookup in `--containers-dir`** — used when that version has no release yet, which is normal between a version bump and its first build, and when the exact filename is absent so a locally built `sifs/<recipe>_<version>.sif` still works. The search is scoped to `<recipe>_<version>`, so it can select a different build date of the same release but never a different version; a lookup that would have to choose between versions fails as ambiguous.

There is deliberately no step that reaches for an older release. A recipe whose version has outrun its published releases resolves to the locally built container or fails saying nothing was found — it never quietly tests the predecessor.

Three things are checked by `builder/validation.py` and contract-tested in `builder/tests/test_release_artifact.py` because resolution depends on them: `name:` matches the recipe, `version:` is present and fully expanded, and `version:` equals the recipe's `build.yaml` version. Pull-request validation runs this cross-file check when either recipe file changes. Automated version-bump PRs update the sibling `fulltest.yaml` in the same commit; manual bumps must do the same. Where a test needs to assert the version — a `--version` string, an install path — write `${version}` rather than spelling it out, so the assertion follows the recipe instead of needing an edit at each bump.

Failures are reported rather than worked around. A `container:` that disagrees with release metadata names both the declared and the expected artifact — fix it by deleting the `container:` key, not by editing the filename, since that hand-maintenance loop is what this contract exists to remove. An unexpanded `${var}` in `version:` and a release file whose build date is not a `YYYYMMDD` date are both errors, not a quiet fallback to some other container.

`--releases-dir` points the resolver at a different metadata tree and `--no-release-metadata` disables it entirely for offline runs.

## GitHub Actions Entry Points

### Release PR Testing – `.github/workflows/test-release-pr.yml`

This workflow runs automatically when a pull request targeting `master` or `main` modifies `releases/*/*.json`.

1. **`detect-changes` job** (Ubuntu runner) checks out the repository with full history and enumerates modified release descriptors. It emits a JSON matrix containing each `{name, version, file}` tuple and a `has-changes` flag.
2. **`test-containers` job** (self-hosted runner) fans out across the matrix when `has-changes` is true. Each matrix leg:
   - Checks out the repo and installs Python requirements.
   - Verifies container runtimes (`docker`, `apptainer` or `singularity`) so the self-hosted machine has the necessary binaries.
   - Locates `recipes/<name>/fulltest.yaml`. Recipes without fulltests produce a skipped report instead of falling back to legacy test formats.
   - Runs Dive against the published Docker image using `.github/.dive-ci.yml`, then runs `workflows/release_test_runner.py`, which downloads or converts the release container, runs the deploy bins/path check, and executes `fulltest.yaml` through `builder/run_tests.py`. Dive findings and fulltest results are reported together on the release PR; wasted-space findings do not open separate issues.
   - The `continue-on-error` flag lets subsequent steps gather logs even when tests fail.
   - Generates Markdown in-process via `workflows/reporting.py` and uploads both the JSON and Markdown artifacts as `test-results-<name>`.
   - Uses `actions/github-script` to post (or update) a PR comment containing the Markdown report for that specific container.
3. **`summarize-results` job** (Ubuntu runner) aggregates every `test-results-*.json` artifact, prints a count of passed/failed recipes, and updates a single “Container Test Summary” PR comment. If any container failed, this job calls `core.setFailed`, which fails the workflow and surfaces red status in the PR checks.

### Fleet Release Smoke Test – `.github/workflows/full-container-test.yml`

This manually triggered workflow validates every recipe that has a published release entry. When dispatching the workflow you can optionally supply a comma-separated `recipes` list to limit the matrix to specific containers:

1. **`prepare-matrix` job** builds a catalog of recipes, pairing each with the newest `releases/<recipe>/<version>.json` by comparing the embedded build date. Recipes without a release are kept in the matrix and marked for skip reporting. If the optional `recipes` filter is provided, the catalog is trimmed to those names; missing entries are called out in the tracking issue for visibility.
2. **`create-issue` job** opens a tracking GitHub Issue summarising how many recipes will run versus skip and captures the runner architecture from the dispatch input.
3. **`test-containers` job** fans out across the matrix. When a release exists, it downloads the published SIF and executes the recipe’s `fulltest.yaml` through `builder/run_tests.py`; Docker-to-SIF conversion is available for matching GHCR images. Recipes without fulltests or without releases generate skipped-result comments instead.
4. **`finalize` job** gathers every JSON artifact, posts an aggregated summary comment, and updates the issue body with pass/fail/skipped totals.

### On-Demand Recipe Matrix – `.github/workflows/recipes-ci.yml`

This workflow builds recipes from the selected branch and runs their runtime tests against the new images. It has read-only repository permissions and retains test results without publishing containers.

- `recipes` selects comma-separated recipe names. When empty, `debug=true` selects `niimath` and `debug=false` selects every recipe.
- `architecture` accepts `x86_64`, `arm64`, or `all`. The matrix includes each selected recipe's declared architectures and variants.
- `run-tests` defaults to `true`. Setting it to `false` runs validation, staging, and Docker builds only.
- `x86-runner` selects the existing ARC or Blacksmith pool in the NeuroDesk repository. ARM builds use the Blacksmith ARM pool. Forks use GitHub-hosted runners.

The `prepare-matrix` job expands the selection with `workflows.recipe_ci`. Each `verify` job installs the builder from the checkout, stages declared downloads, and builds a Docker image. With runtime tests enabled, the job converts that image to a SIF and runs deploy checks plus `recipes/<name>/fulltest.yaml` through `workflows.release_test_runner`. A failed check fails the job. JSON results and diagnostic logs are retained for seven days.

For example, to build and test QSMxT and every declared DataLad variant on a branch:

```bash
gh workflow run recipes-ci.yml --ref <branch> \
  -f recipes=qsmxt,datalad -f architecture=all -f run-tests=true
```

A dispatch is limited to 256 concrete builds. For the full collection, dispatch each architecture separately or split the recipe list into batches.

### Code quality gate – `.github/workflows/test-builder.yml`

Every pull request runs this workflow, including documentation changes and forks.
Pushes to `main` and manual dispatches also run it. It uses GitHub-hosted runners,
read-only repository permissions, and no secrets. `Code quality` is the stable
required check for general code changes. `Container release gate` remains a
separate required check for the container release process.

The Python unit suite runs on Python 3.10 and 3.12. Static checks run on Python
3.12 with Ruff's E4, E7, E9, and F rules, Ruff formatting, scoped mypy checks,
and codespell. Go 1.25.1 runs gofmt, go vet, dashboard tests, and the standalone
Docker-to-SIMG converter tests. actionlint 1.7.12 validates every workflow and
uses ShellCheck 0.11.0 for inline shell. ShellCheck also checks shell scripts in
`workflows`, `tools`, `dashboard`, and `.github`.

To keep the formatting commit out of `git blame`, configure this checkout:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

ShellCheck warnings and errors block merges. Existing informational and style
findings do not yet block merges; this is the baseline severity, not a list of
disabled diagnostics. `.github/actionlint.yaml` declares the existing custom
runner label and excludes only actionlint's unsupported `concurrency.queue`
diagnostic in three existing workflows. GitHub supports that property. Remove
the diagnostic exception when actionlint supports it.

BuildKit integration and recipe generation run in separate jobs. Changes under
`builder`, `workflows`, `tools`, `macros`, or `.github`, and changes to
`requirements.txt`, `pyproject.toml`, or `uv.lock`, select both jobs. These shared
changes validate and generate every recipe. Recipe changes select affected
recipes and their declared architectures and variants. Shared macro consumers
are also checked. Unrelated documentation changes skip both integrations while
keeping the cheap checks. New branches and manual runs with no comparison base
run the full integrations.

The final `Code quality` job runs even if another job fails. It requires every
cheap check and the selection job to succeed. Each selected integration must
succeed, and each unselected integration must be skipped. Missing selection
outputs, failures, cancellations, and unexpected skips fail the gate.

`python -m workflows.check_recipes` audits update policies, validates recipes,
checks OpenRecon labels against the vendored schema, and generates Dockerfiles
for every declared architecture and variant. It aggregates failures so one
broken recipe does not hide failures in others. With a comparison base, newly
added recipes must have `fulltest.yaml`, including recipes in mixed code and
recipe pull requests. A future documented exception must also address the
update policy's fulltest contract; there is no flag that bypasses that rule.

Run the same checks locally after installing Go 1.25.1 and Python dependencies:

```bash
python -m pip install -r requirements.txt -e '.[dev,boutiques]'
python -m workflows.quality_checks python
python -m pytest builder/tests
python -m workflows.quality_checks go
python -m workflows.quality_checks install-workflow-tools "$PWD/.quality-tools"
PATH="$PWD/.quality-tools:$PATH" python -m workflows.quality_checks workflows
python -m workflows.check_recipes --base origin/main --head HEAD --json recipe-checks.json
```

The workflow tool installer downloads Linux x86_64 binaries, verifies their
pinned SHA256 digests, and writes only the two executables into the requested
directory. On other platforms, install those exact versions from their upstream
releases and place them on `PATH`. Python checker versions are pinned in the
`dev` extra in `pyproject.toml`. Use `--all --base origin/main --head HEAD` for a
full recipe sweep that still checks the new recipe requirement. The compatibility
command `./workflows/test_all.sh` runs the full sweep. To run the BuildKit tests,
first provide Docker with a Buildx builder, then run:

```bash
NEUROCONTAINERS_TEST_BUILDKIT=1 python -m pytest builder/tests/test_image_flatten.py
```

The independent `validate-recipes.yml` workflow installs the same dependencies
and validates recipe/fulltest contracts when either file changes. Changed
filenames move through NUL-delimited Git output and quoted shell arrays, so
spaces, newlines, and shell metacharacters are treated as filename data.

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
   This mirrors the release-container lookup path. Release PR fulltests are executed through `workflows/release_test_runner.py`.
   To exercise the Docker-to-SIMG conversion path locally, add:
   ```bash
   python workflows/full_container_test.py \
     --recipes dcm2niix \
     --docker-to-simg \
     --runtime apptainer \
     --verbose
   ```
3. **Recipe-focused check** (matches the Recipes CI runtime step):
   ```bash
   python -m workflows.release_test_runner \
     --recipe <name> --version <version> \
     --release-file releases/<name>/<version>.json \
     --candidate-container <path-to-newly-built.simg> \
     --test-config recipes/<name>/fulltest.yaml \
     --results-path check/<name>/test-results.json \
     --output-dir check/<name>/test-output --repo-root . --verbose
   ```
   Build the candidate first. The explicit candidate path allows testing before release metadata exists.
4. **Full local matrix**: `python workflows/full_container_test.py --recipes <name>[,<name>...]` reproduces the `full-container-test.yml` job through the shared `ContainerTestRunner` pipeline.

## Outputs, Reporting, and Cleanup

- CI stores raw JSON results as `builder/test-results-<name>.json` and Markdown summaries as `builder/test-report-<name>.md`. These are uploaded as artifacts and embedded into PR comments.
- `container_tester.py` exits non-zero when any test fails. The GitHub Actions steps run with `continue-on-error: true`, but the final summarizing job converts failures into a failed workflow run.
- Release-driven tests run with `--cleanup`, which deletes downloaded SIFs after completion. Locally you can reuse cached downloads (`~/.cache/neurocontainers`) or purge everything with `sf-test-remote --cleanup-all`.
- When CVMFS is mounted, `--location auto` serves containers directly without downloads; otherwise the release metadata path is used.

By following the commands above and reviewing the referenced workflows, you can replicate, debug, and extend the automated container testing pipeline used by NeuroContainers.
