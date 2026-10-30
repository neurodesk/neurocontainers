#!/usr/bin/env bash
# Verify that every container in apps-catalog/apps.json is present in object
# storage and committed to signed public CVMFS. Exits 0 when all are published,
# non-zero otherwise.
set -euo pipefail

APPS_JSON="${APPS_JSON:-apps-catalog/apps.json}"
OBJECT_STORES="${CVMFS_METADATA_OBJECT_STORES:-https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk https://neurocontainers.s3.us-east-2.amazonaws.com}"
LAG_SECONDS="${CVMFS_METADATA_LAG_SECONDS:-86400}"
[[ "$LAG_SECONDS" =~ ^[0-9]+$ ]] || { echo "[ERROR] Invalid CVMFS_METADATA_LAG_SECONDS"; exit 1; }

start_time="$(date +%s)"

report() {
    echo "$1"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        echo "$1" >> "$GITHUB_STEP_SUMMARY"
    fi
}

publication_pending() {
    local age
    age=$(( $(date +%s) - start_time ))
    report "[PENDING] CVMFS publication deferred; elapsed ${age}s."
    if (( age >= LAG_SECONDS )); then
        report "[ERROR] Inventory publication lag exceeds ${LAG_SECONDS}s."
        exit 1
    fi
    exit 0
}

require_publication() {
    local status=0
    python3 cvmfs/check_publication.py --apps-json "$APPS_JSON" || status=$?
    case "$status" in
        0) ;;
        3) publication_pending ;;
        *) exit 1 ;;
    esac
}

# Print each inventory image missing from an object store; fail if any is.
# Checks run in parallel: the inventory needs over a thousand requests.
missing_images() {
    local stores missing
    read -ra stores <<< "$OBJECT_STORES"
    # shellcheck disable=SC2016 # $1 expands in the per-URL shell.
    mapfile -t missing < <(python3 -c "
import json, sys
entries = json.loads(open(sys.argv[1], encoding='utf-8').read())
for name, data in entries.items():
    for app, info in (data.get('apps') or {}).items():
        parts = app.rsplit(' ', 1)
        if '-' not in app or (len(parts) == 2 and parts[0] == name):
            print(app.replace(' ', '_') + '_' + str(info['version']))
" "$APPS_JSON" | while read -r entry; do
        for store in "${stores[@]}"; do
            echo "${store}/${entry}.simg"
        done
    done | xargs -P 16 -I{} sh -c \
        'curl --silent --fail --head --output /dev/null --connect-timeout 15 \
            --retry 3 --retry-delay 5 "$1" || echo "$1"' _ {})
    if [[ ${#missing[@]} -gt 0 ]]; then
        printf '[INFO] Not in object storage: %s\n' "${missing[@]}"
        return 1
    fi
}

missing_images
require_publication
echo "[INFO] All containers published."
