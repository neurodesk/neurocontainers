# Neurocontainers Dashboard

This package renders the Neurodesk container status dashboard as a static HTML
page. It reads the release metadata, recipes, and consolidated `apps.json`
information and optionally augments the output with the latest container test
run harvested from GitHub issues.

## Local build

1. Generate (or refresh) the aggregated `apps.json` file:
   ```bash
   python tools/generate_apps_json.py --output dashboard/apps.json
   ```
2. Render the static site (outputs to `dashboard/dist/` by default):
   ```bash
   ./go.sh run . \
     --out dist \
     --releases-dir ../releases \
     --recipes-dir ../recipes \
     --apps-json dashboard/apps.json
   ```

The binary fetches GitHub data unless you pass `--skip-github`. When running in
restricted environments you may also need `GOTOOLCHAIN=local` so that Go uses
its preinstalled toolchain.

Key flags:

- `--out` – directory for the generated site (defaults to `dist/`).
- `--apps-json` – path to the consolidated `apps.json` file.
- `--releases-dir` / `--recipes-dir` – locations of release JSON and recipe
  folders.
- `--skip-github` – skip fetching the latest test run via the GitHub API.

During development, `watch.sh` continuously regenerates the site into
`dashboard/local/site` (requires `watchexec`).

## Publishing

The `publish-dashboard.yml` GitHub Actions workflow builds the site on every
push to `main` (and via the manual trigger) and deploys it to GitHub Pages. The
workflow performs the following steps:

1. Generates `dashboard/apps.json` from the tracked releases.
2. Runs `go run ./dashboard` to emit a static site into `public/`.
3. Uploads the result as a Pages artifact and deploys it.

The workflow uses the default `GITHUB_TOKEN` to fetch the latest container test
run issue when available.
