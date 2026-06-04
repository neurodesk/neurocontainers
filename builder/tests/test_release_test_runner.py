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
    run_fulltest_release,
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


def test_run_fulltest_release_uses_release_image_basename(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "cache" / "neurodesktop_20260428_arm64_20260519.simg"
    source.parent.mkdir()
    source.write_text("simg", encoding="utf-8")
    release_file = tmp_path / "20260428-arm64.json"
    release_file.write_text(
        json.dumps(
            {
                "apps": {
                    "neurodesktop 20260428 arm64": {
                        "version": "20260519",
                        "image": "neurodesktop_20260428_arm64",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    test_config = tmp_path / "fulltest.yaml"
    test_config.write_text("tests: []\n", encoding="utf-8")
    output_dir = tmp_path / "builder"

    calls: list[dict[str, object]] = []

    class FakeDownloader:
        def extract_image_basename_from_release(self, release_file: str) -> str:
            return "neurodesktop_20260428_arm64"

        def download_from_release(self, *args, **kwargs) -> str:
            calls.append({"args": args, "kwargs": kwargs})
            return str(source)

    class FakeTester:
        def __init__(self) -> None:
            self.release_downloader = FakeDownloader()

        def select_runtime(self, runtime: str) -> SimpleNamespace:
            return SimpleNamespace(name="apptainer")

        def run_test_suite(self, *args, **kwargs) -> dict[str, object]:
            return {
                "total_tests": 1,
                "passed": 1,
                "failed": 0,
                "skipped": 0,
                "test_results": [{"name": "deploy", "status": "passed"}],
            }

    def fake_run(command, **kwargs) -> SimpleNamespace:
        raw_path = Path(command[command.index("-o") + 1])
        log_path = Path(command[command.index("--log") + 1])
        jsonl_path = Path(command[command.index("--jsonl") + 1])
        raw_path.write_text(
            json.dumps(
                {
                    "summary": {
                        "total_tests": 0,
                        "tests_passed": 0,
                        "tests_failed": 0,
                    },
                    "suites": [],
                }
            ),
            encoding="utf-8",
        )
        log_path.write_text("", encoding="utf-8")
        jsonl_path.write_text("", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", FakeTester)
    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", fake_run)

    status = run_fulltest_release(
        SimpleNamespace(
            recipe="neurodesktop",
            version="20260428-arm64",
            release_file=str(release_file),
            test_config=str(test_config),
            output_dir=str(output_dir),
            results_path=str(output_dir / "results.json"),
            runtime="apptainer",
            docker_to_simg=False,
            docker_registry="neurodesk",
            docker_save_to_simg="builder/docker-save-to-simg.go",
            verbose=False,
            repo_root=str(tmp_path),
        )
    )

    assert status == "passed"
    assert calls == [
        {
            "args": ("neurodesktop", "20260428-arm64", "20260519"),
            "kwargs": {
                "image_basename": "neurodesktop_20260428_arm64",
                "use_cache": False,
            },
        }
    ]
    assert (
        "container: neurodesktop_20260428_arm64_20260519.simg"
        in (output_dir / "fulltest-suite-neurodesktop.yaml").read_text(
            encoding="utf-8"
        )
    )


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
