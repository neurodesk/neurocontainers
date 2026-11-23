#!/usr/bin/env bash
# Verify that every container in apps-catalog/apps.json is present in object
# storage and committed to signed public CVMFS. Exits 0 when all are published,
# non-zero otherwise.
set -euo pipefail

APPS_JSON="${APPS_JSON:-apps-catalog/apps.json}"
OBJECT_STORES="${CVMFS_METADATA_OBJECT_STORES:-https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk https://neurocontainers.s3.us-east-2.amazonaws.com}"
LAG_SECONDS="${CVMFS_METADATA_LAG_SECONDS:-86400}"
[[ "$LAG_SECONDS" =~ ^[0-9]+$ ]] || { echo "[ERROR] Invalid CVMFS_METADATA_LAG_SECONDS"; exit 1; }

catalog_root="${CATALOG_ROOT:-apps-catalog}"
start_time="$(git -C "$catalog_root" log --first-parent -1 --format=%ct -- domains.json ':(glob)*/apps.json')"
[[ "$start_time" =~ ^[0-9]+$ ]] || { echo "[ERROR] Missing catalog change time"; exit 1; }
pending_ids=()

report() {
    echo "$1"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        echo "$1" >> "$GITHUB_STEP_SUMMARY"
    fi
}

# Return "<epoch> [<oldest_id>]" for the earliest continuous introduction of
# any currently pending container across catalog_root's domain catalog history.
pending_since() {
    local now="$1"
    python3 - "$catalog_root" "$now" "$LAG_SECONDS" "$start_time" "${pending_ids[@]}" <<'PY'
import json
import subprocess
import sys

catalog_root = sys.argv[1]
now = int(sys.argv[2])
lag_seconds = int(sys.argv[3])
fallback_ts = int(sys.argv[4])
pending = {item for item in sys.argv[5:] if item}
cutoff = now - lag_seconds

if not pending:
    print(f"{fallback_ts}")
    raise SystemExit(0)


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", catalog_root, *args], text=True)


def is_primary(menu_name: str, app_name: str) -> bool:
    if "-" not in app_name:
        return True
    parts = app_name.rsplit(" ", 1)
    return len(parts) == 2 and parts[0] == menu_name


def ids_at_commit(rev: str) -> set[str]:
    try:
        domains_doc = json.loads(git("show", f"{rev}:domains.json"))
        domains = domains_doc.get("domains") or {}
    except Exception:
        return set()
    ids: set[str] = set()
    for domain in sorted(domains):
        try:
            catalog = json.loads(git("show", f"{rev}:{domain}/apps.json"))
        except Exception:
            continue
        if not isinstance(catalog, dict):
            continue
        for menu_name, menu_data in catalog.items():
            if not isinstance(menu_data, dict):
                continue
            for app_name, app_data in (menu_data.get("apps") or {}).items():
                if not isinstance(app_data, dict) or not is_primary(menu_name, app_name):
                    continue
                version = app_data.get("version")
                if version is not None:
                    ids.add(f"{app_name.replace(' ', '_')}_{version}")
    return ids


commits = [
    line.split()
    for line in git(
        "log", "--first-parent", "--format=%H %ct", "--", "domains.json", ":(glob)*/apps.json"
    ).splitlines()
    if line.strip()
]

remaining = set(pending)
introduced: dict[str, int] = {}

for commit_sha, ts_str in commits:
    ts = int(ts_str)
    present = ids_at_commit(commit_sha)
    for cid in list(remaining):
        if cid in present:
            introduced[cid] = min(introduced.get(cid, ts), ts)
        else:
            remaining.discard(cid)
    if not remaining:
        break
    if introduced and ts <= cutoff and min(introduced.values()) <= cutoff:
        break

if not introduced:
    print(f"{fallback_ts}")
else:
    oldest_id, oldest_ts = min(introduced.items(), key=lambda kv: (kv[1], kv[0]))
    print(f"{oldest_ts} {oldest_id}")
PY
}

publication_pending() {
    local now pending_info since oldest_id age detail=""
    now="$(date +%s)" || exit 1
    pending_info="$(pending_since "$now")" || exit 1
    read -r since oldest_id <<< "$pending_info"
    age=$(( now - since ))
    if [[ -n "$oldest_id" ]]; then
        detail=" (oldest pending ${oldest_id})"
    fi
    report "[PENDING] CVMFS publication deferred${detail}; elapsed ${age}s."
    if (( age >= LAG_SECONDS )); then
        report "[ERROR] Inventory publication lag exceeds ${LAG_SECONDS}s."
        exit 1
    fi
    exit 0
}

require_publication() {
    local object_missing="${1:-0}"
    local status=0 output line
    output="$(python3 cvmfs/check_publication.py --apps-json "$APPS_JSON")" || status=$?
    if [[ -n "$output" ]]; then
        printf '%s\n' "$output"
    fi
    case "$status" in
        0)
            if (( object_missing )); then
                publication_pending
            fi
            ;;
        3)
            if (( ! object_missing )); then
                pending_ids=()
            fi
            while IFS= read -r line; do
                if [[ "$line" == "[PENDING] Missing public container: "* ]]; then
                    pending_ids+=("${line#"[PENDING] Missing public container: "}")
                fi
            done <<< "$output"
            publication_pending
            ;;
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
    pending_ids=()
    if [[ ${#missing[@]} -gt 0 ]]; then
        printf '[INFO] Not in object storage: %s\n' "${missing[@]}"
        local url name
        for url in "${missing[@]}"; do
            name="${url##*/}"
            pending_ids+=("${name%.simg}")
        done
        return 1
    fi
}

object_missing=0
missing_images || object_missing=1
require_publication "$object_missing"
echo "[INFO] All containers published."
