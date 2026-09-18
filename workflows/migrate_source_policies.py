#!/usr/bin/env python3
"""Replay the recorded migration of source-pinned recipe policies.

Preview by default. Use --apply to update only the recorded recipe cohort. Recipes
already using source policies are left alone, including their subsequently updated
pins. The JSON records preserve the observed baseline refs and deliberate changes.
"""
from __future__ import annotations

import argparse
import json
import re
import tempfile
from pathlib import Path

import yaml


def mapping_nodes(text: str) -> dict[str, tuple[yaml.Node, yaml.Node]]:
    root = yaml.compose(text)
    if not isinstance(root, yaml.MappingNode):
        raise ValueError("recipe must contain a YAML mapping")
    return {key.value: (key, value) for key, value in root.value}


def set_section(text: str, key: str, value: object) -> str:
    rendered = yaml.safe_dump({key: value}, sort_keys=False, width=100000)
    nodes = mapping_nodes(text)
    if key not in nodes:
        return text.rstrip() + "\n\n" + rendered
    key_node, value_node = nodes[key]
    return text[:key_node.start_mark.index] + rendered + "\n" + text[value_node.end_mark.index:]


def set_scalar(text: str, parent: str | None, key: str, value: str) -> str:
    nodes = mapping_nodes(text)
    if parent is None:
        if key not in nodes:
            return text.rstrip() + "\n\n" + yaml.safe_dump({key: value}, sort_keys=False)
        node = nodes[key][1]
    elif parent not in nodes:
        # Variables must precede expressions that reference them during rendering.
        return yaml.safe_dump({parent: {key: value}}, sort_keys=False) + "\n" + text
    else:
        node = nodes[parent][1]
        if not isinstance(node, yaml.MappingNode):
            raise ValueError(f"{parent} must be a mapping")
        match = next((v for k, v in node.value if k.value == key), None)
        if match is None:
            start = text.rfind("\n", 0, node.start_mark.index) + 1
            line = " " * node.start_mark.column + key + ": " + json.dumps(value) + "\n"
            return text[:start] + line + text[start:]
        node = match
    if not isinstance(node, yaml.ScalarNode):
        raise ValueError(f"{parent}.{key} must be a scalar")
    return text[:node.start_mark.index] + json.dumps(value) + text[node.end_mark.index:]


def remove_items(text: str, *, commands: list[str], files: list[str]) -> str:
    spans = []

    def walk(node: yaml.Node, role: str = "") -> None:
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                walk(value, key.value)
        elif isinstance(node, yaml.SequenceNode):
            for item in node.value:
                remove = role == "run" and isinstance(item, yaml.ScalarNode) and item.value in commands
                if role == "files" and isinstance(item, yaml.MappingNode):
                    remove = any(k.value == "name" and v.value in files for k, v in item.value)
                if remove:
                    start = text.rfind("\n", 0, item.start_mark.index) + 1
                    end = item.end_mark.index
                    if text[end:end + 1] == "\n":
                        end += 1
                    spans.append((start, end))
                else:
                    walk(item)

    walk(yaml.compose(text))
    for start, end in sorted(spans, reverse=True):
        text = text[:start] + text[end:]
    if yaml.safe_load(text).get("files", []) is None:
        text = re.sub(r"^files:\s*\n", "", text, flags=re.M)
    return text


def replacements(text: str, changes: list[list[str]]) -> str:
    for before, after in changes:
        parts = text.split(after)
        if any(before in part for part in parts):
            text = after.join(part.replace(before, after) for part in parts)
        elif len(parts) == 1:
            raise ValueError(f"expected migration input is missing: {before}")
    return text


def write_atomic(path: Path, contents: str) -> None:
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(contents)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def freeze_install_version(text: str) -> str:
    spans = []
    nodes = mapping_nodes(text)
    for key in ("build", "variables", "files", "deploy"):
        if key in nodes:
            node = nodes[key][1]
            spans.append((node.start_mark.index, node.end_mark.index))
    for start, end in sorted(spans, reverse=True):
        text = text[:start] + text[start:end].replace("context.version", "context.install_version") + text[end:]
    return text


def migrate(recipe_dir: Path, plan: dict, apply: bool) -> bool:
    recipe_path = recipe_dir / "build.yaml"
    original = recipe_path.read_text()
    if yaml.safe_load(original).get("auto_update", {}).get("method") == "sources":
        return False
    updated = original
    for key, value in plan["variables"].items():
        updated = set_scalar(updated, "variables", key, value)
    group = plan.get("replace_run_group")
    if group:
        document = yaml.safe_load(updated)
        for directive in document["build"]["directives"]:
            if group["contains"] in directive.get("run", []):
                old = directive["run"]
                updated = remove_items(updated, commands=old[1:], files=[])
                updated = replacements(updated, [[old[0], group["commands"][0]]])
                break
        else:
            raise ValueError("expected installation group is missing")
    updated = remove_items(updated, commands=plan.get("remove_commands", []), files=plan.get("remove_files", []))
    updated = replacements(updated, plan["replacements"])
    for file in plan["files"]:
        doc = yaml.safe_load(updated)
        updated = set_section(updated, "files", [*doc.get("files", []), file])
    if plan.get("freeze_install_version"):
        updated = freeze_install_version(updated)
    policy = {"method": "sources", "sources": plan["sources"]}
    if "local" in plan:
        policy["local"] = plan["local"]
    updated = set_section(updated, "auto_update", policy)
    if "container_version" in plan:
        updated = set_scalar(updated, None, "version", plan["container_version"])
    yaml.safe_load(updated)

    suite_path = recipe_dir / "fulltest.yaml"
    suite = suite_path.read_text() if suite_path.exists() else yaml.safe_dump(plan["new_suite"], sort_keys=False)
    suite = replacements(suite, plan["suite_replacements"])
    for key, value in plan["suite_scalars"].items():
        suite = set_scalar(suite, None, key, value)
    if "container_version" in plan:
        suite = set_scalar(suite, None, "version", plan["container_version"])
    yaml.safe_load(suite)
    if apply:
        # A retry after a partial write completes the recipe after its fulltest.
        write_atomic(suite_path, suite)
        write_atomic(recipe_path, updated)
    print(f"{recipe_dir.name}: {len(plan['sources'])} bound sources")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--recipe", action="append")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    plans = json.loads(Path(__file__).with_name("source_policy_migration.json").read_text())
    changed = 0
    for name, plan in plans.items():
        if args.recipe and name not in args.recipe:
            continue
        changed += migrate(root / "recipes" / name, plan, args.apply)
    print(f"{'Migrated' if args.apply else 'Would migrate'} {changed} recipes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
