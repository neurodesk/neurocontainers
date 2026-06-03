from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from workflows.release_test_runner import (
    _combine_results,
    _failure_results,
    _load_jsonl_records,
    _normalise_run_tests_output,
    _release_build_date,
    main,
)
from workflows.reporting import build_report


def test_release_build_date_reads_first_app_version(tmp_path: Path) -> None:
    release_file = tmp_path / "release.json"
    release_file.write_text(
        json.dumps({"apps": {"sample": {"version": "20260603"}}}),
        encoding="utf-8",
    )

    assert _release_build_date(release_file) == "20260603"


def test_normalise_run_tests_output_matches_github_reporting_schema() -> None:
    raw = {
        "summary": {
            "total_tests": 2,
            "tests_passed": 1,
            "tests_failed": 1,
        },
        "suites": [
            {
                "name": "sample",
                "tests": [
                    {"name": "help", "passed": True, "message": "OK"},
                    {"name": "import", "passed": False, "message": "Import failed"},
                ]
            }
        ],
    }

    result = _normalise_run_tests_output(
        raw,
        recipe="sample",
        version="1.0",
        container_ref="sample_1.0_20260603.simg",
        jsonl_records=[
            {
                "suite": "sample",
                "test": "help",
                "stdout": "usage\n",
                "stderr": "",
                "exit_code": 0,
            },
            {
                "suite": "sample",
                "test": "import",
                "stdout": "",
                "stderr": "traceback\n",
                "exit_code": 2,
            },
        ],
    )

    assert result["total_tests"] == 2
    assert result["passed"] == 1
    assert result["failed"] == 1
    assert result["test_results"] == [
        {
            "name": "help",
            "status": "passed",
            "stdout": "usage\n",
            "stderr": "",
            "return_code": 0,
            "duration": 0,
            "message": "OK",
        },
        {
            "name": "import",
            "status": "failed",
            "stdout": "",
            "stderr": "traceback\n",
            "return_code": 2,
            "duration": 0,
            "message": "Import failed",
        },
    ]


def test_load_jsonl_records_ignores_invalid_lines(tmp_path: Path) -> None:
    jsonl = tmp_path / "results.jsonl"
    jsonl.write_text('{"test": "help"}\nnot-json\n{"test": "import"}\n', encoding="utf-8")

    assert _load_jsonl_records(jsonl) == [{"test": "help"}, {"test": "import"}]


def test_combine_results_prepends_deploy_check_and_sums_counts() -> None:
    fulltest = {
        "total_tests": 2,
        "passed": 2,
        "failed": 0,
        "skipped": 0,
        "test_results": [{"name": "fulltest", "status": "passed"}],
    }
    deploy = {
        "total_tests": 1,
        "passed": 0,
        "failed": 1,
        "skipped": 0,
        "test_results": [{"name": "deploy", "status": "failed"}],
    }

    result = _combine_results(fulltest, deploy)

    assert result["total_tests"] == 3
    assert result["passed"] == 2
    assert result["failed"] == 1
    assert result["test_results"] == [
        {"name": "deploy", "status": "failed"},
        {"name": "fulltest", "status": "passed"},
    ]


def test_failure_results_are_reportable() -> None:
    result = _failure_results(
        recipe="niimath",
        version="1.0",
        message="Unable to download release container",
    )

    assert result["failed"] == 1
    assert result["test_results"][0]["name"] == "release_test_runner"
    assert "Unable to download" in result["test_results"][0]["stderr"]


def test_main_writes_failure_outputs_when_fulltest_adapter_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_fulltest(args: SimpleNamespace) -> str:
        raise RuntimeError("Unable to download release container")

    monkeypatch.setattr(
        "workflows.release_test_runner.run_fulltest_release",
        fail_fulltest,
    )
    test_config = tmp_path / "fulltest.yaml"
    test_config.write_text("tests: []\n", encoding="utf-8")
    release_file = tmp_path / "release.json"
    release_file.write_text("{}", encoding="utf-8")
    github_output = tmp_path / "github-output.txt"
    results_path = tmp_path / "builder" / "test-results-niimath.json"

    status = main(
        [
            "--recipe",
            "niimath",
            "--version",
            "1.0",
            "--release-file",
            str(release_file),
            "--test-config",
            str(test_config),
            "--results-path",
            str(results_path),
            "--output-dir",
            str(tmp_path / "builder"),
            "--github-output",
            str(github_output),
        ]
    )

    assert status == 1
    assert results_path.is_file()
    assert (tmp_path / "builder" / "test-report-niimath.md").is_file()
    assert "status=failed" in github_output.read_text(encoding="utf-8")


def test_build_report_includes_fulltest_summary_and_artifacts() -> None:
    report = build_report(
        {
            "total_tests": 1,
            "passed": 1,
            "failed": 0,
            "test_results": [],
            "fulltest_summary": {
                "total_suites": 1,
                "suites_passed": 1,
                "total_tests": 1,
                "tests_passed": 1,
                "duration": 2.5,
            },
            "fulltest_artifacts": {
                "raw_json": "builder/fulltest-raw-sample.json",
                "log": "builder/fulltest-sample.log",
            },
        },
        "sample",
        "1.0",
    )

    assert "### Fulltest Summary" in report
    assert "- Suites: 1/1 passed" in report
    assert "- raw_json: `builder/fulltest-raw-sample.json`" in report
