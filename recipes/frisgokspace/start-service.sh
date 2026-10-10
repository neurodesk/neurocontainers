#!/usr/bin/env bash
set -euo pipefail
exec docker run --rm --init -p "${FRISGOKSPACE_PORT:-9002}:9002" \
  frisgokspace:0.1.0 frisgokspace "$@"
