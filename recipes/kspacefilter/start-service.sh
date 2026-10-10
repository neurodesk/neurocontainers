#!/usr/bin/env bash
set -euo pipefail
exec docker run --rm --init -p "${KSPACEFILTER_PORT:-9002}:9002" \
  kspacefilter:0.1.0 kspacefilter "$@"
