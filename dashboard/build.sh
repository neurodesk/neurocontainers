#!/usr/bin/env bash

set -ex

(cd .. && python tools/generate_apps_json.py --output dashboard/apps.json)

go run . \
  --out public \
  --releases-dir ../releases \
  --recipes-dir ../recipes \
  --apps-json apps.json
