# Neurocontainers Dashboard

This package renders the Neurodesk container status dashboard as a static HTML
page. It reads the release metadata, recipes, and domain catalogs from the
`apps.json` repository
information and optionally augments the output with the latest container test
run harvested from GitHub issues.

## Local build

1. Clone the `apps.json` repository beside `neurocontainers`.
2. Render the static site from inside `dashboard/` (outputs to `dashboard/dist/`
   by default):
   ```bash
   go run . \
     --out dist \
     --releases-dir ../releases \
     --recipes-dir ../recipes \
     --catalog-root ../../apps.json
   ```

The binary fetches GitHub data unless you pass `--skip-github`. When running in
restricted environments you may also need `GOTOOLCHAIN=local` so that Go uses
its preinstalled toolchain.

Key flags:

- `--out` – directory for the generated site (defaults to `dist/`).
- `--catalog-root` – path to the `apps.json` repository containing `domains.json`
  and each domain's `apps.json` file.
- `--releases-dir` / `--recipes-dir` – locations of release JSON and recipe
  folders.
- `--skip-github` – skip fetching the latest test run via the GitHub API.

## Tests

```bash
cd dashboard && go test ./...
```

`parser_test.go` round-trips the Markdown that `workflows/reporting.py` posts to
the test-run issue back through the dashboard's parser, using the committed
fixture in `testdata/`. Regenerate that fixture from `reporting.build_comment`
if the comment format changes.

## Publishing

The `publish-dashboard.yml` GitHub Actions workflow builds the site on every
push to `main` (and via the manual trigger) and deploys it to GitHub Pages. The
workflow performs the following steps:

1. Checks out the `apps.json` repository and reads every configured domain catalog.
2. Runs `go run ./dashboard` to emit a static site into `public/`.
3. Uploads the result as a Pages artifact and deploys it.

The workflow uses the default `GITHUB_TOKEN` to fetch the latest container test
run issue when available.
