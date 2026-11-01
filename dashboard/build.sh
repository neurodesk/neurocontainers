#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
catalog_root="${APPS_CATALOG_ROOT:-$script_dir/../../apps.json}"
cd "$script_dir"

go run . \
  --out public \
  --releases-dir ../releases \
  --recipes-dir ../recipes \
  --catalog-root "$catalog_root"
