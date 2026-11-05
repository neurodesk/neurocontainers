#!/usr/bin/env bash

# The caller supplies the transaction helpers from sync_containers_to_cvmfs.sh.
mirror_signature_sidecars() (
    local script_dir="$1" repo_root="$2" inventory="$3"
    local staging status=0
    local -a source_args=(--bucket "${CVMFS_SIGNATURE_BUCKET:-neurocontainers}")
    if [[ -n "${CVMFS_SIGNATURE_SOURCE:-}" ]]; then
        source_args=(--source "$CVMFS_SIGNATURE_SOURCE")
    fi
    staging=$(mktemp -d) || return 2
    trap 'rm -rf -- "$staging"' EXIT
    python3 "$script_dir/mirror_signature_sidecars.py" prepare \
        --repo-root "$repo_root" --log "$inventory" --staging "$staging" \
        "${source_args[@]}" || status=$?
    if [[ $status -eq 0 ]]; then
        echo "[INFO] Signature sidecars are unchanged; unsigned releases remain unsigned."
        return 0
    elif [[ $status -ne 1 ]]; then
        return 2
    fi
    open_cvmfs_transaction neurodesk.ardc.edu.au || return 2
    if ! python3 "$script_dir/mirror_signature_sidecars.py" apply \
        --repo-root "$repo_root" --staging "$staging"; then
        abort_cvmfs_transaction neurodesk.ardc.edu.au
        return 2
    fi
    publish_cvmfs_transaction neurodesk.ardc.edu.au "mirrored SIF signature sidecars"
)
