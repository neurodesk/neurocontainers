#!/usr/bin/env python3
"""Summarise verbose results from test_deploy.sh into compact output."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Regular expressions used to extract filesystem paths from messages.
_PATH_PATTERNS: Tuple[re.Pattern[str], ...] = (
    re.compile(r"File ([^\s]+) does not exist"),
    re.compile(r"Directory ([^\s]+) does not exist"),
    re.compile(r"expected at ([^\s)]+)"),
    re.compile(r"Interpreter ([^\s]+) not found"),
    re.compile(r"Unable to read file header for ([^\s]+)"),
)

_FOUND_AT_PATTERN = re.compile(r"found at (.+)", re.IGNORECASE)


@dataclass
class BinarySummary:
    name: str
    status: str = "skipped"
    message: str = ""
    resolved_path: Optional[str] = None
    additional_messages: List[str] = field(default_factory=list)
    missing_libs: List[Tuple[str, Optional[str]]] = field(default_factory=list)

    def to_payload_entry(self) -> Dict[str, str]:
        lines: List[str] = []

        if self.status == "passed" and self.resolved_path:
            lines.append(f"Found at {self.resolved_path}")
        elif self.message.strip():
            lines.append(self.message.strip())

        if self.additional_messages:
            lines.extend(self.additional_messages)

        if self.missing_libs:
            lines.append("Missing libraries:")
            for lib_name, lib_path in self.missing_libs:
                if lib_path:
                    lines.append(f"- {lib_name} (expected at {lib_path})")
                else:
                    lines.append(f"- {lib_name}")

        message = "\n".join(line.strip() for line in lines if line.strip())
        return {
            "name": self.name,
            "status": self.status,
            "message": message,
        }


def _clean_path(raw: str) -> Optional[str]:
    path = raw.strip().rstrip(".)")
    if not path or path == "PATH":
        return None
    return path


def _extract_paths(message: str) -> Iterable[str]:
    for pattern in _PATH_PATTERNS:
        for match in pattern.findall(message):
            cleaned = _clean_path(match)
            if cleaned:
                yield cleaned


def _add_to_tree(tree: Dict[str, Dict], path: str) -> None:
    is_abs = path.startswith("/")
    parts = [part for part in path.strip("/").split("/") if part]
    if not parts:
        return

    root_label = "/" if is_abs else "."
    node = tree.setdefault(root_label, {})
    for part in parts:
        node = node.setdefault(part, {})


def _render_tree(tree: Dict[str, Dict]) -> List[str]:
    def render_node(node: Dict[str, Dict], prefix: str = "") -> List[str]:
        lines: List[str] = []
        items = sorted(node.items(), key=lambda pair: pair[0])
        for index, (name, child) in enumerate(items):
            last = index == len(items) - 1
            connector = "`--" if last else "|--"
            lines.append(f"{prefix}{connector} {name}")
            if child:
                extension = "    " if last else "|   "
                lines.extend(render_node(child, prefix + extension))
        return lines

    lines: List[str] = []
    for root, children in sorted(tree.items(), key=lambda pair: pair[0]):
        lines.append(root)
        if children:
            lines.extend(render_node(children))
    return lines


def _match_binary_name(path: str, binaries: Dict[str, BinarySummary]) -> Optional[str]:
    for name, binary in binaries.items():
        resolved = binary.resolved_path
        if not resolved:
            continue
        if path == resolved:
            return name
        try:
            if resolved and path.endswith("/" + Path(resolved).name):
                return name
        except ValueError:
            continue
    return None


def _summarise_builtin(payload: Dict) -> Tuple[Dict, bool]:
    tests = payload.get("tests")
    if not isinstance(tests, list):
        return payload, False

    # Avoid summarising twice.
    if payload.get("summarised"):
        return payload, False

    binaries: Dict[str, BinarySummary] = {}
    path_to_binary: Dict[str, str] = {}
    missing_paths: Set[str] = set()

    for entry in tests:
        name = entry.get("name", "")
        status = entry.get("status", "unknown")
        message = entry.get("message", "") or ""

        if name.startswith("deploy_bin:"):
            bin_name = name.split(":", 1)[1]
            binary = binaries.setdefault(bin_name, BinarySummary(name=bin_name))
            binary.status = status
            binary.message = message

            found_match = _FOUND_AT_PATTERN.search(message)
            if found_match:
                resolved = _clean_path(found_match.group(1))
                if resolved:
                    binary.resolved_path = resolved
                    path_to_binary.setdefault(resolved, bin_name)
            continue

        if name.startswith("ldd:"):
            # Format: ldd:<binary_path>:<lib_name>
            parts = name.split(":", 2)
            if len(parts) == 3:
                binary_path = parts[1]
                lib_name = parts[2]
                bin_name = path_to_binary.get(binary_path)
                if bin_name is None:
                    bin_name = _match_binary_name(binary_path, binaries)
                    if bin_name:
                        path_to_binary[binary_path] = bin_name
                if status == "failed":
                    paths = list(_extract_paths(message))
                    for path in paths:
                        missing_paths.add(path)
                    if bin_name:
                        binaries.setdefault(bin_name, BinarySummary(name=bin_name)).missing_libs.append(
                            (lib_name, paths[0] if paths else None)
                        )
            continue

        if name.startswith("file.exists:") or name.startswith("file.executable:"):
            file_path = name.split(":", 1)[1]
            target_binary = path_to_binary.get(file_path)
            if target_binary is None:
                target_binary = _match_binary_name(file_path, binaries)
                if target_binary:
                    path_to_binary[file_path] = target_binary
            if status == "failed":
                if target_binary:
                    binaries.setdefault(target_binary, BinarySummary(name=target_binary)).additional_messages.append(
                        message.strip()
                    )
                for path in _extract_paths(message):
                    missing_paths.add(path)
            continue

        if status == "failed":
            for path in _extract_paths(message):
                missing_paths.add(path)

    if not binaries and not missing_paths:
        return payload, False

    new_tests: List[Dict[str, str]] = []
    for bin_name in sorted(binaries):
        new_tests.append(binaries[bin_name].to_payload_entry())

    if missing_paths:
        tree: Dict[str, Dict] = {}
        for path in sorted(missing_paths):
            _add_to_tree(tree, path)
        tree_lines = _render_tree(tree)
        if tree_lines:
            new_tests.append(
                {
                    "name": "missing-paths",
                    "status": "failed",
                    "message": "Missing filesystem entries:\n" + "\n".join(tree_lines),
                }
            )

    payload["tests"] = new_tests
    payload["total"] = len(new_tests)
    payload["passed"] = sum(1 for entry in new_tests if entry["status"] == "passed")
    payload["failed"] = sum(1 for entry in new_tests if entry["status"] == "failed")
    payload["skipped"] = sum(1 for entry in new_tests if entry["status"] == "skipped")
    payload["summarised"] = True

    return payload, True


def summarise_results_file(path: Path) -> bool:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return False

    changed = False
    for test in data.get("test_results", []):
        stdout = test.get("stdout")
        if not stdout:
            continue
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            continue

        updated_payload, updated = _summarise_builtin(payload)
        if updated:
            test["stdout"] = json.dumps(updated_payload, indent=2)
            changed = True

    if changed:
        path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

    return changed


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Summarise test_deploy.sh results inside container tester outputs."
    )
    parser.add_argument(
        "results",
        nargs="+",
        help="Path(s) to test-results-*.json files to update in place.",
    )

    args = parser.parse_args(argv)

    any_changed = False
    for result_path in args.results:
        path = Path(result_path)
        if summarise_results_file(path):
            any_changed = True

    return 0 if any_changed else 0


if __name__ == "__main__":
    sys.exit(main())
