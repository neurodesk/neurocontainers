#!/usr/bin/env bash

set -euo pipefail

# Keep the local sweep and CI on the same architecture and variant checks.
exec python3 -m workflows.check_recipes "$@"
