"""Shared execution pipeline for container tests."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from workflows.container_tester import ContainerTester
from workflows.reporting import build_comment, build_report, determine_status, write_text
from workflows.summarize_deploy_results import summarise_results_file
from workflows.test_utils import discover_test_config, find_latest_release_file, resolve_path

ARTIFACTS_DIR_NAME = "builder"
RESULTS_PREFIX = "test-results-"
COMMENT_PREFIX = "comment-"
STATUS_PREFIX = "status-"
REPORT_PREFIX = "test-report-"


@dataclass
class TestRequest:
    recipe: str
    version: Optional[str] = None
    release_file: Optional[str | Path] = None
    test_config: Optional[str | Path] = None
    runtime: Optional[str] = None
    location: str = "auto"
    gpu: bool = False
    cleanup: bool = False
    auto_cleanup: bool = False
    verbose: bool = False
    allow_missing_release: bool = False
    allow_missing_tests: bool = True
    output_dir: Optional[str | Path] = None
    results_path: Optional[str | Path] = None
    comment_filename: Optional[str] = None
    status_filename: Optional[str] = None
    report_filename: Optional[str] = None
    create_comment: bool = True
    create_report: bool = True


@dataclass
class TestOutcome:
    recipe: str
    version: str
    status: str
    results: Dict[str, object]
    results_path: Path
    release_file: Optional[Path] = None
    reason: Optional[str] = None
    comment_path: Optional[Path] = None
    comment: Optional[str] = None
    status_path: Optional[Path] = None
    report_path: Optional[Path] = None
    report: Optional[str] = None


class ContainerTestRunner:
    """Coordinate release-style container tests."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        artifacts_dir: Path | None = None,
    ) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parent.parent
        self.artifacts_dir = artifacts_dir or self.repo_root / ARTIFACTS_DIR_NAME
        self.tester = ContainerTester()

    # ------------------------------------------------------------------
    # Public API

    def run(self, request: TestRequest) -> TestOutcome:
        recipe_dir = self.repo_root / "recipes" / request.recipe
        if not recipe_dir.is_dir():
            raise FileNotFoundError(f"Recipe directory not found: {recipe_dir}")

        release_file, version, release_reason = self._resolve_release(
            request.recipe,
            request.version,
            request.release_file,
            request.allow_missing_release,
        )

        if release_reason and request.allow_missing_release:
            results = self._build_stub_result(
                request.recipe,
                version or request.version or "",
                status="skipped",
                message=release_reason,
            )
            return self._finalise(request, results, version or "", release_file, release_reason)

        legacy_script_path = self._discover_legacy_script(recipe_dir)

        test_config_path, test_reason = self._resolve_test_config(recipe_dir, request)
        using_implicit_tests = False

        if not request.test_config and test_config_path is None:
            using_implicit_tests = True
            test_reason = None  # We can synthesise a default test config

        if test_reason and request.allow_missing_tests and not using_implicit_tests:
            results = self._build_stub_result(
                request.recipe,
                version or request.version or "",
                status="skipped",
                message=test_reason,
            )
            return self._finalise(request, results, version or "", release_file, test_reason)

        if test_config_path is None and not using_implicit_tests:
            raise FileNotFoundError("Test configuration could not be resolved")
        if version is None:
            raise RuntimeError("Container version could not be determined")

        try:
            runtime = self.tester.select_runtime(request.runtime)
        except RuntimeError as exc:
            results = self._build_stub_result(
                request.recipe,
                version,
                status="failed",
                message=str(exc),
            )
            return self._finalise(request, results, version, release_file, str(exc))

        if request.verbose:
            print(f"Selected runtime: {runtime.name}")

        container_ref = self.tester.find_container(
            request.recipe,
            version,
            location=request.location,
            release_file=str(release_file) if release_file else None,
        )

        if not container_ref:
            message = (
                f"Unable to locate container {request.recipe}:{version} (location={request.location})"
            )
            results = self._build_stub_result(
                request.recipe,
                version,
                status="failed",
                message=message,
            )
            return self._finalise(request, results, version, release_file, message)

        if request.verbose:
            print(f"Resolved container reference: {container_ref}")

        target_version = version or request.version or "unknown"

        if using_implicit_tests:
            if request.verbose:
                print("No explicit test configuration; using builtin defaults")
            test_config = self.tester.test_extractor.default_test_config(
                request.recipe,
                target_version,
                legacy_script=legacy_script_path,
            )
        else:
            try:
                test_config = self.tester.test_extractor.extract_from_file(
                    str(test_config_path)
                )
            except Exception as exc:  # pragma: no cover - defensive
                message = f"Error loading test configuration: {exc}"
                results = self._build_stub_result(
                    request.recipe,
                    version,
                    status="failed",
                    message=message,
                )
                return self._finalise(request, results, version, release_file, message)

            if not test_config or not test_config.get("tests"):
                if request.verbose:
                    print("Test configuration empty; falling back to builtin defaults")
                test_config = self.tester.test_extractor.default_test_config(
                    request.recipe,
                    target_version,
                    legacy_script=legacy_script_path,
                )

        try:
            results = self.tester.run_test_suite(
                container_ref,
                test_config,
                gpu=request.gpu,
                verbose=request.verbose,
            )
        finally:
            if request.cleanup or request.auto_cleanup:
                self.tester.cleanup_downloaded_containers(verbose=request.verbose)

        return self._finalise(request, results, version, release_file)

    def cleanup_all(self, verbose: bool = False) -> int:
        return self.tester.cleanup_all_cached_containers(verbose)

    # ------------------------------------------------------------------
    # Internal helpers

    def _resolve_release(
        self,
        recipe: str,
        version: Optional[str],
        release_file: Optional[str | Path],
        allow_missing: bool,
    ) -> Tuple[Optional[Path], Optional[str], Optional[str]]:
        releases_dir = self.repo_root / "releases" / recipe

        if release_file:
            resolved = resolve_path(release_file, repo_root=self.repo_root)
            if not resolved.is_file():
                message = f"Release metadata not found at {resolved}"
                if allow_missing:
                    return None, version, message
                raise FileNotFoundError(message)
            detected_version = version or resolved.stem
            return resolved, detected_version, None

        if version:
            candidate = releases_dir / f"{version}.json"
            if candidate.is_file():
                return candidate, version, None
            message = f"Release metadata for version {version} not found"
            if allow_missing:
                return None, version, message
            raise FileNotFoundError(message)

        latest_path, latest_version, _ = find_latest_release_file(releases_dir)
        if latest_path and latest_version:
            return latest_path, latest_version, None

        message = "No release metadata found for recipe"
        if allow_missing:
            return None, None, message
        raise FileNotFoundError(message)

    def _resolve_test_config(
        self,
        recipe_dir: Path,
        request: TestRequest,
    ) -> Tuple[Optional[Path], Optional[str]]:
        if request.test_config:
            resolved = resolve_path(
                request.test_config,
                repo_root=self.repo_root,
            )
            if resolved.is_file():
                return resolved, None
            message = f"Test configuration not found at {resolved}"
            return None, message

        discovered = discover_test_config(recipe_dir)
        if discovered:
            return discovered, None
        return None, "No test configuration available"

    def _discover_legacy_script(self, recipe_dir: Path) -> Optional[Path]:
        """Locate legacy test.sh scripts used by shell-based recipes."""

        candidate = recipe_dir / "test.sh"
        if candidate.is_file():
            return candidate
        return None

    def _build_stub_result(
        self,
        recipe: str,
        version: str,
        *,
        status: str,
        message: str,
    ) -> Dict[str, object]:
        failed = 1 if status == "failed" else 0
        skipped = 1 if status != "failed" else 0
        return {
            "container": f"{recipe}:{version}" if version else recipe,
            "runtime": "unknown",
            "total_tests": 0,
            "passed": 0,
            "failed": failed,
            "skipped": skipped,
            "test_results": [
                {
                    "name": "container_tester",
                    "status": "failed" if status == "failed" else "skipped",
                    "stdout": "",
                    "stderr": message,
                    "return_code": 1 if status == "failed" else 0,
                }
            ],
        }

    def _finalise(
        self,
        request: TestRequest,
        results: Dict[str, object],
        version: str,
        release_file: Optional[Path],
        reason: Optional[str] = None,
    ) -> TestOutcome:
        output_dir = Path(request.output_dir or self.artifacts_dir)
        results_path = (
            Path(request.results_path)
            if request.results_path
            else output_dir / f"{RESULTS_PREFIX}{request.recipe}.json"
        )
        write_text(results_path, json.dumps(results, indent=2) + "\n")

        summarise_results_file(results_path)

        try:
            results_data: Dict[str, object] = json.loads(
                results_path.read_text(encoding="utf-8")
            )
        except Exception:
            results_data = results

        status = determine_status(results_data)

        comment_path: Optional[Path] = None
        comment_text: Optional[str] = None
        status_path: Optional[Path] = None
        if request.create_comment:
            comment_filename = (
                request.comment_filename
                if request.comment_filename is not None
                else f"{COMMENT_PREFIX}{request.recipe}.md"
            )
            comment_path = output_dir / comment_filename
            comment_text, derived_status = build_comment(
                results_data, request.recipe, version or "unknown"
            )
            write_text(comment_path, comment_text)

            status_filename = (
                request.status_filename
                if request.status_filename is not None
                else f"{STATUS_PREFIX}{request.recipe}.txt"
            )
            status_path = output_dir / status_filename
            write_text(status_path, derived_status)
            status = derived_status  # Align status with comment classification

        report_path: Optional[Path] = None
        report_text: Optional[str] = None
        if request.create_report:
            report_filename = (
                request.report_filename
                if request.report_filename is not None
                else f"{REPORT_PREFIX}{request.recipe}.md"
            )
            report_path = output_dir / report_filename
            report_text = build_report(
                results_data, request.recipe, version or "unknown"
            )
            write_text(report_path, report_text)

        return TestOutcome(
            recipe=request.recipe,
            version=version or "",
            status=status,
            results=results_data,
            results_path=results_path,
            release_file=release_file,
            reason=reason,
            comment_path=comment_path,
            comment=comment_text,
            status_path=status_path,
            report_path=report_path,
            report=report_text,
        )
