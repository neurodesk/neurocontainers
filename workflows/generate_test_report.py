#!/usr/bin/env python3
"""CLI wrapper for building Markdown reports from container test results."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workflows.reporting import build_report, write_text


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate markdown test report from JSON results",
    )
    parser.add_argument("test_results_file", help="Path to JSON test results file")
    parser.add_argument("container_name", help="Container name")
    parser.add_argument("container_version", help="Container version")
    parser.add_argument("--output", "-o", help="Output markdown file path")

    args = parser.parse_args()

    try:
        with open(args.test_results_file, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        data = {}

    report = build_report(data, args.container_name, args.container_version)

    if args.output:
        write_text(Path(args.output), report)
    else:
        print(report)

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
