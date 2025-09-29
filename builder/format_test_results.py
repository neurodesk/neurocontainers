#!/usr/bin/env python3
"""Utilities for turning container test JSON results into Markdown comments."""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

STATUS_EMOJI = {
    "passed": "✅",
    "failed": "❌",
    "skipped": "⚠️",
}


def _emit_code_block(lines: List[str], content: str, indent: str = "  ") -> None:
    stripped = content.rstrip()
    if not stripped:
        return

    lines.append(f"{indent}```")
    for line in stripped.splitlines():
        lines.append(f"{indent}{line}")
    lines.append(f"{indent}```")


def _format_builtin(stdout: str, lines: List[str]) -> None:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        _emit_code_block(lines, stdout, indent="    ")
        return

    total = payload.get("total", len(payload.get("tests", [])))
    passed = payload.get("passed", 0)
    failed = payload.get("failed", 0)
    skipped = payload.get("skipped", 0)

    lines.append("  - Builtin checks:")
    lines.append(
        f"    Summary: {passed}/{total} passed (failed {failed}, skipped {skipped})"
    )

    for test in payload.get("tests", []):
        status = test.get("status", "unknown")
        emoji = STATUS_EMOJI.get(status, "❔")
        name = test.get("name", "unnamed")
        message = test.get("message", "")
        lines.append(f"    - {emoji} {name} — {status}")
        if message:
            for line in message.split("\\n"):
                if line:
                    lines.append(f"      {line}")


def build_comment(data: Dict, recipe: str, version: str) -> Tuple[str, str]:
    if not data:
        comment = [
            f"{STATUS_EMOJI['failed']} **{recipe}:{version}**",
            "",
            "_No test results were produced for this container._",
        ]
        return "\n".join(comment), "failed"

    total = data.get("total_tests", data.get("total", 0))
    passed = data.get("passed", 0)
    failed = data.get("failed", 0)
    skipped = data.get("skipped", 0)

    if failed > 0:
        status = "failed"
    elif total == 0 and skipped >= 0:
        status = "skipped"
    else:
        status = "passed"

    emoji = STATUS_EMOJI.get(status, "❔")

    comment_lines: List[str] = [
        f"{emoji} **{recipe}:{version}**",
        "",
        f"- Container: `{data.get('container', f'{recipe}:{version}')}`",
        f"- Runtime: `{data.get('runtime', 'unknown')}`",
        f"- Tests: {passed}/{total} passed (failed {failed}, skipped {skipped})",
    ]

    details = data.get("test_results", [])
    if details:
        comment_lines.append("")
        comment_lines.append("### Test Breakdown")

    for test in details:
        test_name = test.get("name", "unnamed")
        test_status = test.get("status", "unknown")
        test_emoji = STATUS_EMOJI.get(test_status, "❔")
        comment_lines.append(f"- {test_emoji} **{test_name}** — {test_status}")

        return_code = test.get("return_code")
        if return_code not in (None, 0):
            comment_lines.append(f"  - Return code: `{return_code}`")

        stdout = test.get("stdout", "").strip()
        stderr = test.get("stderr", "").strip()

        if stdout:
            comment_lines.append("  - stdout:")
            _format_builtin(stdout, comment_lines)

        if stderr:
            comment_lines.append("  - stderr:")
            _emit_code_block(comment_lines, stderr, indent="    ")

    if not details:
        comment_lines.append("")
        comment_lines.append("_No individual test cases were recorded._")

    return "\n".join(comment_lines), status


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert container test JSON to Markdown comment."
    )
    parser.add_argument("--results", required=True, help="Path to JSON results file")
    parser.add_argument("--recipe", required=True, help="Recipe/container name")
    parser.add_argument("--version", required=True, help="Container version")
    parser.add_argument("--output", required=True, help="Destination markdown file")
    parser.add_argument(
        "--status-output",
        help="Optional file to receive overall status (passed/failed/skipped)",
    )

    args = parser.parse_args()

    data: Dict = {}
    try:
        with open(args.results, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        data = {}
    except json.JSONDecodeError as exc:
        data = {
            "container": args.recipe,
            "runtime": "unknown",
            "total": 0,
            "passed": 0,
            "failed": 1,
            "skipped": 0,
            "test_results": [
                {
                    "name": "results-parsing",
                    "status": "failed",
                    "stdout": "",
                    "stderr": f"Invalid JSON: {exc}",
                    "return_code": 1,
                }
            ],
        }

    comment, status = build_comment(data, args.recipe, args.version)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(comment, encoding="utf-8")

    if args.status_output:
        Path(args.status_output).write_text(status, encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
