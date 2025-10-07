# Local workflow runners

This directory contains helpers that mirror the GitHub Actions workflow logic so
you can run container tests without pushing commits. The main entry point is
`full_container_test.py` which reproduces the behaviour of
`.github/workflows/full-container-test.yml` using the shared
`ContainerTestRunner` pipeline.

## Usage

```bash
python workflows/full_container_test.py [options]
```

### Options

- `--recipes` — comma-separated list of recipe names to test. By default every
  recipe with a release is considered.
- `--runtime` — runtime passed to the shared runner. Defaults to
  `apptainer`.
- `--location` — container location argument forwarded to the runner. Defaults to `auto`.
- `--no-clean` — keep previous artifacts under `builder/` (legacy artifact location).
- `--no-cleanup` — keep downloaded containers instead of removing them at the end.

### What it does

1. Recreates the GitHub Actions discovery logic to enumerate recipes and locate
   their latest release metadata.
2. Uses `workflows/test_runner.py` to execute each container test and generate
   consistent artifacts under `builder/`.
3. Summarises builtin deploy checks via `workflows/summarize_deploy_results.py`
   before producing Markdown comments and detailed reports.
4. Writes an aggregated summary to `summary.md` in the repository root.

Ensure you have the project dependencies installed locally (e.g.
`pip install -r requirements.txt`) and access to the requested container runtime
before running the script.
