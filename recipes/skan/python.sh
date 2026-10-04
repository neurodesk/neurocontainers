#!/bin/sh
set -eu

runtime_dir=$(mktemp -d "${TMPDIR:-/tmp}/skan-$(id -u).XXXXXXXX")
trap 'rm -rf "$runtime_dir"' EXIT
export NUMBA_CACHE_DIR="${NUMBA_CACHE_DIR:-$runtime_dir/numba}"
/opt/miniconda/bin/python "$@"
