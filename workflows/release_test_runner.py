"""Run release PR container tests with fulltest-aware integration output."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from workflows.container_tester import ContainerTester
from workflows.reporting import build_comment, build_report, determine_status, write_text
from workflows.summarize_deploy_results import summarise_results_file


def _release_build_date(release_file: Path) -> str:
    data = json.loads(release_file.read_text(encoding="utf-8"))
    apps = data.get("apps", {}) or {}
    if not isinstance(apps, dict) or not apps:
        raise RuntimeError(f"No app entry in release file: {release_file}")
    first_value = next(iter(apps.values()))
    if isinstance(first_value, dict):
        raw_build_date = first_value.get("version", "")
    else:
        raw_build_date = first_value
    if isinstance(raw_build_date, float) and raw_build_date.is_integer():
        raw_build_date = int(raw_build_date)
    build_date = str(raw_build_date).strip()
    if not build_date:
        raise RuntimeError(f"Build date missing in release file: {release_file}")
    return build_date


def _normalise_run_tests_output(
    raw: dict[str, Any],
    *,
    recipe: str,
    version: str,
    container_ref: str,
    jsonl_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    summary = raw.get("summary", {}) or {}
    suites = raw.get("suites", []) or []
    record_lookup: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in jsonl_records or []:
        key = (str(record.get("suite", "")), str(record.get("test", "")))
        record_lookup.setdefault(key, []).append(record)

    test_results: list[dict[str, Any]] = []
    for suite in suites:
        suite_name = str(suite.get("name", ""))
        for test in suite.get("tests", []) or []:
            test_name = str(test.get("name", "unnamed"))
            records = record_lookup.get((suite_name, test_name), [])
            record = records.pop(0) if records else {}
            passed = bool(test.get("passed"))
            message = str(test.get("message", "") or "")
            stdout = str(record.get("stdout", "") or "")
            stderr = str(record.get("stderr", "") or "")
            if not passed and message and not stderr:
                stderr = message
            test_results.append(
                {
                    "name": test_name,
                    "status": "passed" if passed else "failed",
                    "stdout": stdout,
                    "stderr": stderr,
                    "return_code": int(record.get("exit_code", 0 if passed else 1) or 0),
                    "duration": test.get("duration", 0),
                    "message": message,
                }
            )

    total = int(summary.get("total_tests", len(test_results)) or 0)
    passed = int(summary.get("tests_passed", 0) or 0)
    failed = int(summary.get("tests_failed", 0) or 0)

    return {
        "container": container_ref,
        "runtime": "apptainer",
        "recipe": recipe,
        "version": version,
        "total_tests": total,
        "passed": passed,
        "failed": failed,
        "skipped": max(0, total - passed - failed),
        "test_results": test_results,
        "fulltest_summary": summary,
    }


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []

    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _combine_results(
    fulltest_results: dict[str, Any],
    deploy_results: dict[str, Any],
) -> dict[str, Any]:
    combined = dict(fulltest_results)
    combined["test_results"] = list(deploy_results.get("test_results", [])) + list(
        fulltest_results.get("test_results", [])
    )
    combined["total_tests"] = int(deploy_results.get("total_tests", 0) or 0) + int(
        fulltest_results.get("total_tests", 0) or 0
    )
    combined["passed"] = int(deploy_results.get("passed", 0) or 0) + int(
        fulltest_results.get("passed", 0) or 0
    )
    combined["failed"] = int(deploy_results.get("failed", 0) or 0) + int(
        fulltest_results.get("failed", 0) or 0
    )
    combined["skipped"] = int(deploy_results.get("skipped", 0) or 0) + int(
        fulltest_results.get("skipped", 0) or 0
    )
    return combined


def _write_integration_outputs(
    *,
    recipe: str,
    version: str,
    results: dict[str, Any],
    results_path: Path,
    output_dir: Path,
) -> str:
    write_text(results_path, json.dumps(results, indent=2) + "\n")
    summarise_results_file(results_path)

    data = json.loads(results_path.read_text(encoding="utf-8"))
    report = build_report(data, recipe, version)
    write_text(output_dir / f"test-report-{recipe}.md", report)

    comment, status = build_comment(data, recipe, version)
    write_text(output_dir / f"comment-{recipe}.md", comment)
    write_text(output_dir / f"status-{recipe}.txt", status + "\n")
    return status


def _failure_results(
    *,
    recipe: str,
    version: str,
    message: str,
    container_ref: str = "unresolved",
) -> dict[str, Any]:
    return {
        "container": container_ref,
        "runtime": "apptainer",
        "recipe": recipe,
        "version": version,
        "total_tests": 1,
        "passed": 0,
        "failed": 1,
        "skipped": 0,
        "test_results": [
            {
                "name": "release_test_runner",
                "status": "failed",
                "stdout": "",
                "stderr": message,
                "return_code": 1,
            }
        ],
    }


def _skipped_results(
    *,
    recipe: str,
    version: str,
    message: str,
) -> dict[str, Any]:
    return {
        "container": f"{recipe}:{version}",
        "runtime": "apptainer",
        "recipe": recipe,
        "version": version,
        "total_tests": 1,
        "passed": 0,
        "failed": 0,
        "skipped": 1,
        "test_results": [
            {
                "name": "fulltest discovery",
                "status": "skipped",
                "stdout": "",
                "stderr": message,
                "return_code": 0,
            }
        ],
    }


def run_fulltest_release(args: argparse.Namespace) -> str:
    """Run deploy and fulltest checks against a release or local candidate."""
    release_file = Path(args.release_file)
    test_config = Path(args.test_config)
    output_dir = Path(args.output_dir)
    results_path = Path(args.results_path)
    containers_dir = output_dir / "fulltest-containers"
    raw_results_path = output_dir / f"fulltest-raw-{args.recipe}.json"
    fulltest_log_path = output_dir / f"fulltest-{args.recipe}.log"
    fulltest_jsonl_path = output_dir / f"fulltest-{args.recipe}.jsonl"
    suite_path = output_dir / f"fulltest-suite-{args.recipe}.yaml"
    fulltest_work_dir = output_dir / f"fulltest-work-{args.recipe}"
    containers_dir.mkdir(parents=True, exist_ok=True)
    fulltest_work_dir.mkdir(parents=True, exist_ok=True)

    tester = ContainerTester()
    runtime = tester.select_runtime(args.runtime)
    if runtime.name != "apptainer":
        raise RuntimeError("fulltest.yaml release tests currently require Apptainer/Singularity")

    if getattr(args, "candidate_container", None):
        source = Path(args.candidate_container)
        if not source.is_file():
            raise RuntimeError(f"Candidate container not found: {source}")
        container_ref = str(source)
    elif args.docker_to_simg:
        container_ref = tester.convert_docker_image_to_simg(
            args.recipe,
            args.version,
            release_file=str(release_file),
            docker_registry=args.docker_registry,
            converter_source=args.docker_save_to_simg,
            verbose=args.verbose,
        )
    else:
        build_date = _release_build_date(release_file)
        image_basename = tester.release_downloader.extract_image_basename_from_release(
            str(release_file)
        )
        container_ref = tester.release_downloader.download_from_release(
            args.recipe,
            args.version,
            build_date,
            image_basename=image_basename,
            use_cache=False,
        )
        if not container_ref:
            try:
                container_ref = tester.convert_docker_image_to_simg(
                    args.recipe,
                    args.version,
                    release_file=str(release_file),
                    docker_registry=args.docker_registry,
                    converter_source=args.docker_save_to_simg,
                    verbose=args.verbose,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Unable to download release container {args.recipe}:{args.version}; "
                    f"Docker-to-SIMG fallback failed: {exc}"
                ) from exc
    source = Path(container_ref)
    target = containers_dir / source.name
    if source.resolve() != target.resolve():
        shutil.copy2(source, target)
    container_ref = str(target)

    suite = yaml.safe_load(test_config.read_text(encoding="utf-8")) or {}
    suite["name"] = suite.get("name") or args.recipe
    suite["version"] = args.version
    suite["container"] = Path(container_ref).name
    write_text(suite_path, yaml.safe_dump(suite, sort_keys=False))

    deploy_results = tester.run_test_suite(
        container_ref,
        {"tests": [{"name": "Simple Deploy Bins/Path Test", "builtin": "test_deploy.sh"}]},
        verbose=args.verbose,
    )

    command = [
        "uv",
        "run",
        "builder/run_tests.py",
        str(suite_path),
        "-c",
        str(containers_dir),
        "-o",
        str(raw_results_path),
        "--log",
        str(fulltest_log_path),
        "--jsonl",
        str(fulltest_jsonl_path),
        "--work-dir",
        str(fulltest_work_dir),
    ]
    proc = subprocess.run(command, cwd=args.repo_root, text=True, check=False)
    if proc.returncode != 0 and not raw_results_path.is_file():
        raise RuntimeError(f"run_tests.py failed before writing results: exit {proc.returncode}")

    raw = json.loads(raw_results_path.read_text(encoding="utf-8"))
    fulltest_results = _normalise_run_tests_output(
        raw,
        recipe=args.recipe,
        version=args.version,
        container_ref=container_ref,
        jsonl_records=_load_jsonl_records(fulltest_jsonl_path),
    )
    fulltest_results["fulltest_artifacts"] = {
        "raw_json": str(raw_results_path),
        "jsonl": str(fulltest_jsonl_path),
        "log": str(fulltest_log_path),
        "suite": str(suite_path),
    }
    results = _combine_results(fulltest_results, deploy_results)
    status = _write_integration_outputs(
        recipe=args.recipe,
        version=args.version,
        results=results,
        results_path=results_path,
        output_dir=output_dir,
    )
    if proc.returncode != 0:
        return "failed"
    return status


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse release-test integration command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipe", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--release-file", required=True)
    parser.add_argument("--test-config", required=True)
    parser.add_argument("--runtime", default="apptainer")
    parser.add_argument("--location", default="auto")
    parser.add_argument("--output-dir", default="builder")
    parser.add_argument("--results-path", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    parser.add_argument("--docker-to-simg", action="store_true")
    parser.add_argument(
        "--candidate-container",
        help="Test this local SIF instead of downloading a published release",
    )
    parser.add_argument("--docker-registry", default="neurodesk")
    parser.add_argument("--docker-save-to-simg", default="builder/docker-save-to-simg.go")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    test_config = Path(args.test_config)

    try:
        if not args.test_config or not test_config.is_file():
            message = "No fulltest.yaml test configuration available"
            status = _write_integration_outputs(
                recipe=args.recipe,
                version=args.version,
                results=_skipped_results(
                    recipe=args.recipe,
                    version=args.version,
                    message=message,
                ),
                results_path=Path(args.results_path),
                output_dir=Path(args.output_dir),
            )
        elif test_config.name != "fulltest.yaml":
            raise RuntimeError(
                f"Unsupported test configuration {test_config}; only fulltest.yaml is supported"
            )
        else:
            status = run_fulltest_release(args)
    except Exception as exc:
        message = str(exc)
        print(message, file=sys.stderr)
        try:
            _write_integration_outputs(
                recipe=args.recipe,
                version=args.version,
                results=_failure_results(
                    recipe=args.recipe,
                    version=args.version,
                    message=message,
                ),
                results_path=Path(args.results_path),
                output_dir=Path(args.output_dir),
            )
            if args.github_output:
                with Path(args.github_output).open("a", encoding="utf-8") as handle:
                    handle.write("status=failed\n")
                    handle.write(f"reason={message}\n")
        except Exception as write_exc:
            print(f"Unable to write failure report: {write_exc}", file=sys.stderr)
        return 1

    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"status={status}\n")

    print(f"Status: {status}")
    return 0 if status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
