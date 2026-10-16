#!/usr/bin/env bash
set -euo pipefail

work_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/apptainer-smoke.XXXXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
export APPTAINER_CACHEDIR="$work_dir/cache"
export APPTAINER_TMPDIR="$work_dir/tmp"
export APPTAINER_MKSQUASHFS_PROCS=2
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR" "$work_dir/rootfs/bin"
cp /bin/busybox "$work_dir/rootfs/bin/busybox"
ln -s busybox "$work_dir/rootfs/bin/sh"
apptainer build "$work_dir/smoke.sif" "$work_dir/rootfs"
actual="$(apptainer exec --cleanenv "$work_dir/smoke.sif" /bin/busybox sh -c 'echo "apptainer exec $(/bin/busybox uname -m)"')"
expected="apptainer exec $(uname -m)"
if [[ "$actual" != "$expected" ]]; then
  printf 'Expected %s, got %s\n' "$expected" "$actual" >&2
  exit 1
fi
printf '%s\n' "$actual"
