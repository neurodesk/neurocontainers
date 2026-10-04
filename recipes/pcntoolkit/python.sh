#!/bin/sh
set -eu

runtime_dir=$(mktemp -d "${TMPDIR:-/tmp}/pcntoolkit-$(id -u).XXXXXXXX")
trap 'rm -rf "$runtime_dir"' EXIT
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$runtime_dir/cache}"
/opt/miniconda/bin/python "$@"
