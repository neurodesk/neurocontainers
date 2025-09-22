#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  download-run-logs.sh [-r owner/repo] [-o outdir] RUN_ID_OR_URL

Examples:
  download-run-logs.sh 1234567890
  download-run-logs.sh -r octo-org/octo-repo 1234567890
  download-run-logs.sh -o ./logs https://github.com/octo-org/octo-repo/actions/runs/1234567890
EOF
}

# --- deps check ---
need() { command -v "$1" >/dev/null 2>&1 || { echo "Error: '$1' not found in PATH" >&2; exit 1; }; }
need gh
need jq
need unzip

REPO=""
OUTDIR=""
while getopts ":r:o:h" opt; do
  case "$opt" in
    r) REPO="$OPTARG" ;;
    o) OUTDIR="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND-1))

if [ $# -lt 1 ]; then
  usage; exit 2
fi

INPUT="$1"

# Extract run ID if a URL was provided
if [[ "$INPUT" =~ /actions/runs/([0-9]+) ]]; then
  RUN_ID="${BASH_REMATCH[1]}"
else
  RUN_ID="$INPUT"
fi

# Determine repo if not provided
if [ -z "$REPO" ]; then
  # Uses the currently checked-out repo as default
  REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
fi

# Default output dir
if [ -z "$OUTDIR" ]; then
  OUTDIR="run-${RUN_ID}-logs"
fi

mkdir -p "$OUTDIR"

echo "Repo:   $REPO"
echo "Run ID: $RUN_ID"
echo "Out:    $OUTDIR"
echo

# -------- Download ALL logs (ZIP stream) --------
ZIP_PATH="$OUTDIR/run-${RUN_ID}-logs.zip"
echo "Downloading logs ZIP..."
gh api \
  -X GET \
  "repos/${REPO}/actions/runs/${RUN_ID}/logs" > $ZIP_PATH

echo "Unzipping logs..."
mkdir -p "$OUTDIR/logs"
unzip -q -o "$ZIP_PATH" -d "$OUTDIR/logs"
echo "Logs extracted to: $OUTDIR/logs"
echo

# -------- Jobs CSV (one row per job) --------
echo "Fetching jobs and writing CSV..."
CSV_PATH="$OUTDIR/jobs.csv"

# --paginate follows Link headers so we get all jobs if >100
gh api --paginate \
  "repos/${REPO}/actions/runs/${RUN_ID}/jobs?per_page=100" |
jq -r '
  # header
  (["job_id","job_name","status","conclusion","started_at","completed_at","run_attempt","html_url"] | @csv),
  # rows
  (.jobs[]? | [
      .id,
      .name,
      .status,
      (.conclusion // ""),
      .started_at,
      .completed_at,
      (.run_attempt // 1),
      .html_url
  ] | @csv)
' > "$CSV_PATH"

echo "CSV written to: $CSV_PATH"
echo "Done."

