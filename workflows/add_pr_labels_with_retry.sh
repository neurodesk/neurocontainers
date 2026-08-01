#!/usr/bin/env bash

set -euo pipefail

if (( $# < 2 )); then
  echo "Usage: $0 <pr-url> <label> [<label> ...]" >&2
  exit 2
fi

pr_url="$1"
shift

repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
pr_number="${pr_url##*/}"
max_attempts="${GH_API_MAX_ATTEMPTS:-5}"
retry_delay_seconds="${GH_API_RETRY_DELAY_SECONDS:-5}"

if [[ ! "$pr_number" =~ ^[0-9]+$ ]]; then
  echo "Could not extract a pull request number from: ${pr_url}" >&2
  exit 2
fi

if [[ ! "$max_attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "GH_API_MAX_ATTEMPTS must be a positive integer" >&2
  exit 2
fi

if [[ ! "$retry_delay_seconds" =~ ^[0-9]+$ ]]; then
  echo "GH_API_RETRY_DELAY_SECONDS must be a non-negative integer" >&2
  exit 2
fi

label_fields=()
for label in "$@"; do
  label_fields+=(--raw-field "labels[]=${label}")
done

for (( attempt = 1; attempt <= max_attempts; attempt++ )); do
  if gh api \
    --method POST \
    "repos/${repository}/issues/${pr_number}/labels" \
    "${label_fields[@]}" \
    >/dev/null; then
    echo "Added labels to pull request #${pr_number}."
    exit 0
  fi

  if (( attempt == max_attempts )); then
    echo "Failed to add PR labels after ${max_attempts} attempts." >&2
    exit 1
  fi

  delay=$((retry_delay_seconds * attempt))
  echo "Label update attempt ${attempt}/${max_attempts} failed; retrying in ${delay}s." >&2
  sleep "$delay"
done
