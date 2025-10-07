"""Shared utilities for rendering container test results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

STATUS_EMOJI = {
    "passed": "✅",
    "failed": "❌",
    "skipped": "⚠️",
    "unknown": "❔",
}

LOG_TAIL_LINES = 20


def _emit_code_block(
    lines: List[str],
    content: str,
    indent: str = "  ",
    limit_lines: int | None = None,
) -> None:
    stripped = content.rstrip()
    if not stripped:
        return

    display = stripped
    truncated = False
    total_lines = 0
    if limit_lines is not None and limit_lines > 0:
        parts = stripped.splitlines()
        total_lines = len(parts)
        if total_lines > limit_lines:
            display = "\n".join(parts[-limit_lines:])
            truncated = True

    lines.append(f"{indent}```")
    for line in display.splitlines():
        lines.append(f"{indent}{line}")
    lines.append(f"{indent}```")
    if truncated:
        lines.append(
            f"{indent}_... showing last {limit_lines} lines out of {total_lines}_"
        )


def _format_builtin(stdout: str, lines: List[str], indent: str) -> None:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        _emit_code_block(lines, stdout, indent=indent, limit_lines=LOG_TAIL_LINES)
        return

    total = payload.get("total", len(payload.get("tests", [])))
    passed = payload.get("passed", 0)
    failed = payload.get("failed", 0)
    skipped = payload.get("skipped", 0)

    lines.append(f"{indent}- Builtin checks:")
    lines.append(
        f"{indent}  Summary: {passed}/{total} passed (failed {failed}, skipped {skipped})"
    )

    for test in payload.get("tests", []):
        status = test.get("status", "unknown")
        emoji = STATUS_EMOJI.get(status, STATUS_EMOJI["unknown"])
        name = test.get("name", "unnamed")
        message = test.get("message", "")
        lines.append(f"{indent}  - {emoji} {name} — {status}")
        if message:
            for line in message.split("\n"):
                if line:
                    lines.append(f"{indent}    {line}")


def determine_status(data: Dict) -> str:
    total = data.get("total_tests", data.get("total", 0)) or 0
    failed = data.get("failed", 0) or 0
    skipped = data.get("skipped", 0) or 0

    if failed:
        return "failed"
    if total == 0:
        return "skipped" if skipped or data else "failed"
    return "passed"


def build_comment(
    data: Dict,
    recipe: str,
    version: str,
    *,
    log_tail_lines: int = LOG_TAIL_LINES,
) -> Tuple[str, str]:
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
    status = determine_status(data)
    emoji = STATUS_EMOJI.get(status, STATUS_EMOJI["unknown"])

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
        test_emoji = STATUS_EMOJI.get(test_status, STATUS_EMOJI["unknown"])
        comment_lines.append(f"- {test_emoji} **{test_name}** — {test_status}")

        return_code = test.get("return_code")
        if return_code not in (None, 0):
            comment_lines.append(f"  - Return code: `{return_code}`")

        stdout = test.get("stdout", "").strip()
        stderr = test.get("stderr", "").strip()

        if stdout:
            comment_lines.append("  - stdout:")
            _format_builtin(stdout, comment_lines, indent="    ")

        if stderr:
            comment_lines.append("  - stderr:")
            _emit_code_block(
                comment_lines,
                stderr,
                indent="    ",
                limit_lines=log_tail_lines,
            )

    if not details:
        comment_lines.append("")
        comment_lines.append("_No individual test cases were recorded._")

    return "\n".join(comment_lines), status


def build_report(data: Dict, recipe: str, version: str) -> str:
    status = determine_status(data)
    emoji = STATUS_EMOJI.get(status, STATUS_EMOJI["unknown"])
    passed = data.get("passed", 0)
    total = data.get("total_tests", data.get("total", 0))
    failed = data.get("failed", 0)

    header = [
        f"## Test Results for {recipe}:{version}",
        "",
        f"**Status:** {emoji} {status.upper()}",
        f"**Summary:** {passed}/{total} tests passed (failed {failed})",
        "",
    ]

    sections: List[str] = []
    failed_tests = [entry for entry in data.get("test_results", []) if entry.get("status") == "failed"]
    passed_tests = [entry for entry in data.get("test_results", []) if entry.get("status") == "passed"]

    if failed_tests:
        sections.append("### Failed Tests:")
        for test in failed_tests:
            sections.append(f"- ❌ {test.get('name', 'unnamed')}")
            stderr = test.get("stderr", "")
            if stderr:
                sections.append("  ```")
                sections.extend(f"  {line}" for line in stderr.rstrip().splitlines())
                sections.append("  ```")
        sections.append("")

    if passed_tests:
        sections.append("### Passed Tests:")
        for test in passed_tests:
            sections.append(f"- ✅ {test.get('name', 'unnamed')}")
        sections.append("")

    if not sections:
        sections.append("_No individual test results available._")

    return "\n".join(header + sections).rstrip() + "\n"


def build_aggregate_summary(entries: Iterable[Tuple[str, Dict]]) -> Tuple[str, Dict[str, int]]:
    lines: List[str] = ["## Aggregated Results", ""]
    totals = {"total": 0, "passed": 0, "failed": 0, "skipped": 0}

    for recipe, data in sorted(entries, key=lambda pair: pair[0]):
        status = determine_status(data)
        emoji = STATUS_EMOJI.get(status, STATUS_EMOJI["unknown"])
        if status == "passed":
            totals["passed"] += 1
        elif status == "failed":
            totals["failed"] += 1
        else:
            totals["skipped"] += 1

        totals["total"] += 1
        lines.append(f"- {emoji} `{recipe}` — {status}")

    if totals["total"] == 0:
        lines.append("- No artifacts were produced.")

    lines.append("")
    lines.append(
        f"**Totals:** {totals['passed']} passed, {totals['failed']} failed, {totals['skipped']} skipped (out of {totals['total']})"
    )

    return "\n".join(lines), totals


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
