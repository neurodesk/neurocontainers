#!/usr/bin/env python3
"""Add shared OpenRecon source policies; preview unless --apply is supplied."""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from builder.openrecon_updates import OPENRECON_PINS, OPENRECON_SOURCES, uses_openrecon
from migrate_source_policies import mapping_nodes, set_scalar, set_section, write_atomic


def migrate(path: Path, apply: bool) -> bool:
    original = path.read_text()
    recipe = yaml.safe_load(original)
    if not uses_openrecon(recipe):
        return False
    updated = original
    suite_path = path.with_name("fulltest.yaml")
    suite = suite_path.read_text()
    policy = copy.deepcopy(recipe["auto_update"])
    if policy["method"] != "sources":
        if recipe["name"] not in {"qsmxt", "meica"}:
            raise ValueError(f"{path}: migrate the primary source before its shared dependencies")
        version = str(recipe["version"])
        updated = set_scalar(updated, "variables", "upstream_version", version)
        nodes = mapping_nodes(updated)
        for name in ("files", "build", "copyright"):
            if name in nodes:
                node = nodes[name][1]
                start, end = node.start_mark.index, node.end_mark.index
                # Recompute marks after each replacement because lengths change.
                updated = updated[:start] + updated[start:end].replace("context.version", "context.upstream_version") + updated[end:]
                nodes = mapping_nodes(updated)
        updated = updated.replace("packages upstream QSMxT {{ context.version }}", "packages upstream QSMxT {{ context.upstream_version }}")
        source = {"id": recipe["name"], **policy,
                  "target": {"variable": "upstream_version", "value": "version", "fulltest_variable": "upstream_version"}}
        policy = {"method": "sources", "sources": [source]}
        suite = set_scalar(suite, None, "upstream_version", version)
        if recipe["name"] == "qsmxt":
            suite = suite.replace("      qsmxt --version\n", "      qsmxt --version | grep -F '${upstream_version}'\n")
        else:
            suite = suite.replace("      meica.py -h >", "      test \"${MEICA_SOURCE_TAG}\" = '${upstream_version}'\n      test \"${MEICA_RUNTIME_TAG}\" = '${upstream_version}'\n      meica.py -h >")
    for variable, value in OPENRECON_PINS.items():
        if variable not in (recipe.get("variables") or {}):
            updated = set_scalar(updated, "variables", variable, value)
    owned = {source.get("target", {}).get("variable") for source in policy.get("sources", [])}
    for source in OPENRECON_SOURCES:
        if source["target"]["variable"] not in owned:
            policy.setdefault("sources", []).append(copy.deepcopy(source))
    local = policy.setdefault("local", [])
    if "macros/openrecon" not in local:
        local.append("macros/openrecon")
    if recipe["auto_update"] != policy or original != updated:
        updated = set_section(updated, "auto_update", policy)
    if updated == original and suite == suite_path.read_text():
        return False
    yaml.safe_load(updated)
    if apply:
        write_atomic(suite_path, suite)
        write_atomic(path, updated)
    print(f"{recipe['name']}: four shared dependencies bound")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--exclude", action="append", default=[])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    changed = sum(migrate(path, args.apply) for path in sorted((root / "recipes").glob("*/build.yaml"))
                  if path.parent.name not in args.exclude)
    print(f"{'Migrated' if args.apply else 'Would migrate'} {changed} recipes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
