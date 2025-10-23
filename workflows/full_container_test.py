#!/usr/bin/env python3
"""Local runner for the full-container-test workflow."""

from __future__ import annotations

import argparse
import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from workflows.reporting import (
    build_aggregate_summary,
    build_comment,
    build_report,
    write_text,
)
from workflows.test_runner import (
    COMMENT_PREFIX,
    REPORT_PREFIX,
    RESULTS_PREFIX,
    STATUS_PREFIX,
    ContainerTestRunner,
    TestOutcome,
    TestRequest,
)
from workflows.test_utils import discover_test_config, find_latest_release_file

REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPES_DIR = REPO_ROOT / "recipes"
RELEASES_DIR = REPO_ROOT / "releases"
ARTIFACTS_DIR = REPO_ROOT / "builder"
SUMMARY_PATH = REPO_ROOT / "summary.md"


@dataclass
class ContainerSpec:
    recipe: str
    version: str
    release_file: Optional[Path]
    has_release: bool
    build_date: Optional[str]

    def __post_init__(self) -> None:
        if isinstance(self.release_file, str):
            stripped = self.release_file.strip()
            self.release_file = Path(stripped) if stripped else None

    @property
    def reference(self) -> str:
        return f"{self.recipe}:{self.version}" if self.version else self.recipe


@dataclass
class Classification:
    status: str
    message: str
    update_shared: bool


def discover_containers(requested: Sequence[str]) -> Tuple[List[ContainerSpec], List[str], int]:
    requested_clean = [entry.strip() for entry in requested if entry.strip()]
    requested_set = set(requested_clean)

    specs: List[ContainerSpec] = []
    available_names = set()

    if not RECIPES_DIR.exists():
        raise SystemExit(f"Recipes directory not found at {RECIPES_DIR}")

    for recipe_dir in sorted(RECIPES_DIR.iterdir()):
        if not recipe_dir.is_dir():
            continue
        recipe = recipe_dir.name
        if recipe == "builder":
            continue

        available_names.add(recipe)
        release_path, version, build_date = find_latest_release_file(RELEASES_DIR / recipe)
        has_release = release_path is not None
        specs.append(
            ContainerSpec(
                recipe=recipe,
                version=version or "",
                release_file=release_path,
                has_release=has_release,
                build_date=build_date,
            )
        )

    if requested_set:
        filtered = [spec for spec in specs if spec.recipe in requested_set]
    else:
        filtered = specs

    missing = sorted(requested_set - available_names) if requested_set else []
    return filtered, missing, len(specs)


def clean_previous_results() -> None:
    patterns = [
        "test-results-*.json",
        "comment-*.md",
        "status-*.txt",
        "test-report-*.md",
        "shared-update-*.txt",
    ]
    for pattern in patterns:
        for candidate in ARTIFACTS_DIR.glob(pattern):
            try:
                candidate.unlink()
            except FileNotFoundError:
                continue
    if SUMMARY_PATH.exists():
        SUMMARY_PATH.unlink()


def classify_outcome(outcome: TestOutcome) -> Classification:
    if outcome.status == "passed":
        icon = "✅"
        tag = "passed"
    elif outcome.status == "skipped":
        icon = "⚠️"
        tag = "skipped"
    else:
        icon = "❌"
        tag = "failed"

    reason = outcome.reason or ""
    reason = " ".join(reason.split()) if reason else ""
    if reason and len(reason) > 240:
        reason = reason[:237] + "..."

    message = f"{icon} `{outcome.recipe}:{outcome.version or 'unknown'}` {tag}"
    if reason:
        message += f" — {reason}"
    update_shared = outcome.status == "skipped"
    return Classification(status=outcome.status, message=message, update_shared=update_shared)


def headline_from_totals(totals: dict) -> str:
    failed = totals.get("failed", 0)
    skipped = totals.get("skipped", 0)
    if failed:
        return f"❌ {failed} container(s) failed"
    if skipped:
        return f"⚠️ {skipped} container(s) skipped"
    return "✅ All containers passed"


def determine_test_config(recipe: str) -> Tuple[bool, Optional[str]]:
    """Return whether a recipe has tests and the resolved config path."""

    recipe_dir = RECIPES_DIR / recipe
    if not recipe_dir.is_dir():
        return False, None

    config_path = discover_test_config(recipe_dir)
    if not config_path:
        return False, None

    return True, config_path.as_posix()


def _load_results(recipe: str, results_dir: Path = ARTIFACTS_DIR) -> Dict[str, object]:
    results_path = results_dir / f"{RESULTS_PREFIX}{recipe}.json"
    if not results_path.is_file():
        return {}
    try:
        return json.loads(results_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def summarize_result(spec: ContainerSpec) -> None:
    """Generate a markdown summary file for a container's test run."""

    data = _load_results(spec.recipe)
    version = spec.version or data.get("version") or "unknown"

    report_text = build_report(data, spec.recipe, str(version or "unknown"))
    report_path = ARTIFACTS_DIR / f"{REPORT_PREFIX}{spec.recipe}.md"
    write_text(report_path, report_text)


def format_comment(spec: ContainerSpec) -> None:
    """Ensure a Markdown comment and status file exist for the container."""

    data = _load_results(spec.recipe)
    version = spec.version or data.get("version") or "unknown"

    comment_text, status = build_comment(data, spec.recipe, str(version or "unknown"))

    comment_path = ARTIFACTS_DIR / f"{COMMENT_PREFIX}{spec.recipe}.md"
    write_text(comment_path, comment_text)

    status_path = ARTIFACTS_DIR / f"{STATUS_PREFIX}{spec.recipe}.txt"
    write_text(status_path, status + "\n")


def aggregate_summary(results_root: str | Path) -> Dict[str, int]:
    """Build an aggregate summary from downloaded test-result artifacts."""

    root = Path(results_root)
    entries: List[Tuple[str, Dict[str, object]]] = []

    if root.is_dir():
        pattern = f"{RESULTS_PREFIX}*.json"
        for path in sorted(root.rglob(pattern)):
            name = path.stem[len(RESULTS_PREFIX) :]
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, FileNotFoundError):
                payload = {}
            entries.append((name, payload))

    summary_markdown, totals = build_aggregate_summary(entries)
    write_text(SUMMARY_PATH, summary_markdown)
    return totals


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run container tests locally using the workflow logic.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """Examples:
  python workflows/full_container_test.py
  python workflows/full_container_test.py --recipes pytorch,tensorflow
  python workflows/full_container_test.py --runtime docker --location local
"""
        ),
    )
    parser.add_argument(
        "--recipes",
        type=str,
        default="",
        help="Comma-separated list of recipe names to test (defaults to all).",
    )
    parser.add_argument(
        "--runtime",
        type=str,
        default="apptainer",
        help="Container runtime to pass to the shared test runner (default: apptainer).",
    )
    parser.add_argument(
        "--location",
        type=str,
        default="auto",
        help="Container location (auto, cvmfs, local, release, docker).",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip removing previous results before running.",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep downloaded containers instead of removing them after tests.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose runner output.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    requested = args.recipes.split(",") if args.recipes else []

    if not args.no_clean:
        clean_previous_results()

    specs, missing, total_available = discover_containers(requested)
    if missing:
        print("Warning: requested recipes not found:")
        for name in missing:
            print(f"  - {name}")

    if not specs:
        print("No containers selected for testing.")
        return 1

    runner = ContainerTestRunner()
    outcomes: List[Tuple[ContainerSpec, TestOutcome]] = []

    print(
        f"Selected {len(specs)} container(s) (from {total_available} discovered). "
        f"Releases present for {sum(1 for s in specs if s.has_release)} recipe(s)."
    )

    for spec in specs:
        print(f"\n=== {spec.reference} ===")
        if not spec.has_release:
            print("No release metadata found; generating skipped result.")

        request = TestRequest(
            recipe=spec.recipe,
            version=spec.version or None,
            release_file=spec.release_file,
            runtime=args.runtime,
            location=args.location,
            cleanup=not args.no_cleanup,
            auto_cleanup=False,
            verbose=args.verbose,
            allow_missing_release=not spec.has_release,
            output_dir=ARTIFACTS_DIR,
        )

        outcome = runner.run(request)
        outcomes.append((spec, outcome))
        classification = classify_outcome(outcome)
        print(classification.message)

    summary_markdown, totals = build_aggregate_summary(
        (spec.recipe, outcome.results) for spec, outcome in outcomes
    )
    write_text(SUMMARY_PATH, summary_markdown)

    headline = headline_from_totals(totals)
    print("\n=== Summary ===")
    print(headline)
    print(SUMMARY_PATH.read_text(encoding="utf-8"))
    return 0 if totals.get("failed", 0) == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
