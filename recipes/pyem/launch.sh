#!/bin/sh
set -eu

runtime_dir=$(mktemp -d "${TMPDIR:-/tmp}/pyem-$(id -u).XXXXXXXX")
trap 'rm -rf "$runtime_dir"' EXIT
export NUMBA_CACHE_DIR="${NUMBA_CACHE_DIR:-$runtime_dir/numba}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$runtime_dir/matplotlib}"
"/opt/pyem-venv/bin/$(basename "$0")" "$@"
