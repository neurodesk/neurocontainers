#!/usr/bin/env python3
"""CLI wrapper for rendering container test JSON into Markdown comments."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workflows.reporting import build_comment, write_text


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert container test JSON to Markdown comment.",
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

    data = {}
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
    write_text(Path(args.output), comment)

    if args.status_output:
        Path(args.status_output).write_text(status, encoding="utf-8")

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
