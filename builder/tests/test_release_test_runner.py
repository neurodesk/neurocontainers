from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from workflows.container_tester import AcquiredImage, ContainerTester
from workflows.release_test_runner import (
    _combine_results,
    _failure_results,
    _load_jsonl_records,
    _normalise_run_tests_output,
    _release_build_date,
    _skipped_results,
    main,
    run_fulltest_release,
)
from workflows.reporting import build_comment, build_report
from workflows.test_run_artifacts import cleanup_runs, managed_run


def _execute_fulltest(args: SimpleNamespace):
    with managed_run(Path(args.output_dir), results_path=Path(args.results_path)) as run:
        return run_fulltest_release(args, run_dir=run.path)


def test_release_build_date_reads_first_app_version(tmp_path: Path) -> None:
    release_file = tmp_path / "release.json"
    release_file.write_text(
        json.dumps({"apps": {"sample": {"version": "20260603"}}}),
        encoding="utf-8",
    )

    assert _release_build_date(release_file) == "20260603"


def test_release_build_date_accepts_scalar_app_version(tmp_path: Path) -> None:
    release_file = tmp_path / "release.json"
    release_file.write_text(
        json.dumps({"apps": {"sample": 20260603.0}}),
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


def test_skipped_results_are_reportable() -> None:
    result = _skipped_results(
        recipe="niimath",
        version="1.0",
        message="No fulltest.yaml test configuration available",
    )

    assert result["failed"] == 0
    assert result["skipped"] == 1
    assert result["test_results"][0]["name"] == "fulltest discovery"


def test_main_writes_skipped_outputs_when_fulltest_is_missing(
    tmp_path: Path,
) -> None:
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
            "",
            "--results-path",
            str(results_path),
            "--output-dir",
            str(tmp_path / "builder"),
            "--github-output",
            str(github_output),
        ]
    )

    assert status == 0
    assert results_path.is_file()
    results = json.loads(results_path.read_text(encoding="utf-8"))
    assert results["skipped"] == 1
    assert "status=skipped" in github_output.read_text(encoding="utf-8")


def test_main_writes_failure_outputs_when_fulltest_adapter_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fail_fulltest(args: SimpleNamespace, *, run_dir: Path) -> None:
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


@pytest.mark.parametrize("launch_fails", [False, True])
def test_run_fulltest_release_uses_release_image_basename(
    tmp_path: Path,
    monkeypatch,
    launch_fails: bool,
) -> None:
    """Release tests honor architecture-specific image basenames in metadata."""
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
    acquired: list[AcquiredImage] = []

    class FakeDownloader:
        def extract_image_basename_from_release(self, release_file: str) -> str:
            return "neurodesktop_20260428_arm64"

        def download_from_release(self, *args, **kwargs) -> AcquiredImage:
            calls.append({"args": args, "kwargs": kwargs})
            image = AcquiredImage(source)
            os.link(source, image.path)
            acquired.append(image)
            return image

    class FakeTester(ContainerTester):
        def __init__(self) -> None:
            super().__init__()
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

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", FakeTester)

    run_commands: list[list[str]] = []

    def fake_run(command, **kwargs) -> SimpleNamespace:
        run_commands.append(command)
        assert Path(command[command.index("--container") + 1]).read_text() == "simg"
        if launch_fails:
            raise OSError("cannot launch child")
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

    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", fake_run)

    outcome = _execute_fulltest(
        SimpleNamespace(
            recipe="neurodesktop",
            version="20260428-arm64",
            release_file=str(release_file),
            test_config=str(test_config),
            output_dir=str(output_dir),
            results_path=str(output_dir / "results.json"),
            runtime="apptainer",
            candidate_container=None,
            docker_to_simg=False,
            docker_registry="neurodesk",
            docker_save_to_simg="builder/docker-save-to-simg.go",
            verbose=False,
            repo_root=str(tmp_path),
        )
    )

    assert outcome.reason == ("cannot launch child" if launch_fails else None)
    assert outcome.results["failed"] == int(launch_fails)
    assert outcome.results["passed"] == 1
    assert calls == [
        {
            "args": ("neurodesktop", "20260428-arm64", "20260519"),
            "kwargs": {
                "image_basename": "neurodesktop_20260428_arm64",
                "use_cache": False,
            },
        }
    ]
    command = run_commands[0]
    suite_path = Path(command[3])
    run_dir = suite_path.parent
    assert "container:" not in suite_path.read_text(encoding="utf-8")
    assert run_dir.parent == output_dir
    assert run_commands == [
        [
            "uv", "run", str(tmp_path / "builder/run_tests.py"), str(suite_path),
            "--container", str(acquired[0].path),
            "-o", str(run_dir / "fulltest-raw-neurodesktop.json"),
            "--log", str(run_dir / "fulltest-neurodesktop.log"),
            "--jsonl", str(run_dir / "fulltest-neurodesktop.jsonl"),
            "--work-dir", str(run_dir / "work"),
        ]
    ]


    assert not acquired[0].path.exists()
    assert source.read_text() == "simg"


def test_run_fulltest_release_uses_local_candidate(tmp_path: Path, monkeypatch) -> None:
    """A local candidate bypasses published-release download and conversion."""
    source = tmp_path / "candidate.simg"
    source.write_text("simg", encoding="utf-8")
    test_config = tmp_path / "fulltest.yaml"
    test_config.write_text("tests: []\n", encoding="utf-8")
    output_dir = tmp_path / "builder"

    class FakeTester(ContainerTester):
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
        Path(command[command.index("-o") + 1]).write_text(
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
        Path(command[command.index("--log") + 1]).write_text("", encoding="utf-8")
        Path(command[command.index("--jsonl") + 1]).write_text("", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", FakeTester)
    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", fake_run)

    outcome = _execute_fulltest(
        SimpleNamespace(
            recipe="demo",
            version="1.2.3",
            release_file=str(tmp_path / "not-published.json"),
            candidate_container=str(source),
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

    assert outcome.reason is None
    assert outcome.results["failed"] == 0
    assert source.read_text(encoding="utf-8") == "simg"
    assert not list(output_dir.rglob("*.simg"))


def test_run_fulltest_release_falls_back_to_docker_conversion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "cache" / "bidsappbrainsuite_21a_20260629.docker.simg"
    source.parent.mkdir()
    source.write_text("simg", encoding="utf-8")
    release_file = tmp_path / "21a.json"
    release_file.write_text(
        json.dumps(
            {
                "apps": {
                    "bidsappbrainsuite 21a": {
                        "version": "20260629",
                        "exec": "",
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
        def extract_image_basename_from_release(self, release_file: str) -> None:
            return None

        def download_from_release(self, *args, **kwargs) -> None:
            calls.append({"method": "download", "args": args, "kwargs": kwargs})
            return None

    class FakeTester(ContainerTester):
        def __init__(self) -> None:
            super().__init__()
            self.release_downloader = FakeDownloader()

        def select_runtime(self, runtime: str) -> SimpleNamespace:
            return SimpleNamespace(name="apptainer")

        def convert_docker_image_to_simg(self, *args, **kwargs) -> str:
            calls.append({"method": "convert", "args": args, "kwargs": kwargs})
            return str(source)

        def run_test_suite(self, *args, **kwargs) -> dict[str, object]:
            return {
                "total_tests": 1,
                "passed": 1,
                "failed": 0,
                "skipped": 0,
                "test_results": [{"name": "deploy", "status": "passed"}],
            }

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", FakeTester)

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

    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", fake_run)

    outcome = _execute_fulltest(
        SimpleNamespace(
            recipe="bidsappbrainsuite",
            version="21a",
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

    assert outcome.reason is None
    assert outcome.results["failed"] == 0
    assert calls == [
        {
            "method": "download",
            "args": ("bidsappbrainsuite", "21a", "20260629"),
            "kwargs": {"image_basename": None, "use_cache": False},
        },
        {
            "method": "convert",
            "args": ("bidsappbrainsuite", "21a"),
            "kwargs": {
                "release_file": str(release_file),
                "docker_registry": "neurodesk",
                "converter_source": "builder/docker-save-to-simg.go",
                "verbose": False,
            },
        },
    ]


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


def test_build_comment_treats_json_scalar_stdout_as_plain_output() -> None:
    comment, status = build_comment(
        {
            "total_tests": 1,
            "passed": 1,
            "failed": 0,
            "skipped": 0,
            "test_results": [
                {
                    "name": "CUDA runtime pin",
                    "status": "passed",
                    "stdout": "11.8\n",
                    "stderr": "",
                    "return_code": 0,
                }
            ],
        },
        "musclemap",
        "1.3.10",
    )

    assert status == "passed"
    assert "11.8" in comment


@pytest.mark.parametrize(
    ("child_exit", "fulltest_passed", "deploy_passed", "total", "passed", "failed"),
    [
        (1, True, True, 3, 2, 1),
        (2, True, True, 3, 2, 1),
        (-15, True, True, 3, 2, 1),
        (0, True, True, 2, 2, 0),
        (1, False, True, 2, 1, 1),
        (0, True, False, 2, 1, 1),
        (1, True, False, 3, 1, 2),
    ],
)
def test_main_publishes_child_exit_with_completed_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    child_exit: int,
    fulltest_passed: bool,
    deploy_passed: bool,
    total: int,
    passed: int,
    failed: int,
) -> None:
    source = tmp_path / "candidate.simg"
    source.write_text("simg", encoding="utf-8")
    test_config = tmp_path / "fulltest.yaml"
    test_config.write_text("name: sample\ntests: []\n", encoding="utf-8")
    output_dir = tmp_path / "output"
    github_output = tmp_path / "github-output.txt"
    results_path = output_dir / "results.json"
    summary = {
        "total_tests": 1,
        "tests_passed": int(fulltest_passed),
        "tests_failed": int(not fulltest_passed),
    }
    test_message = "OK" if fulltest_passed else "Assertion failed"

    class FakeTester(ContainerTester):
        def select_runtime(self, runtime: str) -> SimpleNamespace:
            return SimpleNamespace(name="apptainer")

        def run_test_suite(self, *args, **kwargs) -> dict[str, object]:
            return {
                "total_tests": 1,
                "passed": int(deploy_passed),
                "failed": int(not deploy_passed),
                "skipped": 0,
                "test_results": [
                    {"name": "deploy", "status": "passed" if deploy_passed else "failed"}
                ],
            }

    def fake_run(command, **kwargs) -> SimpleNamespace:
        raw = {
            "summary": summary,
            "suites": [{"name": "sample", "tests": [{
                "name": "help", "passed": fulltest_passed,
                "message": test_message, "duration": 0.25,
            }]}],
        }
        Path(command[command.index("-o") + 1]).write_text(
            json.dumps(raw), encoding="utf-8"
        )
        Path(command[command.index("--jsonl") + 1]).write_text(
            json.dumps({
                "suite": "sample", "test": "help", "stdout": "usage\n",
                "stderr": "diagnostic\n", "exit_code": 0 if fulltest_passed else 1,
            }) + "\n", encoding="utf-8",
        )
        Path(command[command.index("--log") + 1]).write_text(
            "completed help\n", encoding="utf-8"
        )
        return SimpleNamespace(returncode=child_exit)

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", FakeTester)
    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", fake_run)
    exit_code = main([
        "--recipe", "sample", "--version", "1.0",
        "--release-file", str(tmp_path / "release.json"),
        "--candidate-container", str(source), "--test-config", str(test_config),
        "--output-dir", str(output_dir), "--results-path", str(results_path),
        "--github-output", str(github_output), "--repo-root", str(tmp_path),
    ])

    status = "failed" if failed else "passed"
    assert exit_code == int(bool(failed))
    assert f"Status: {status}" in capsys.readouterr().out
    assert github_output.read_text(encoding="utf-8") == f"status={status}\n"
    assert (output_dir / "status-sample.txt").read_text(encoding="utf-8") == f"{status}\n"
    results = json.loads(results_path.read_text(encoding="utf-8"))
    assert (results["total_tests"], results["passed"], results["failed"], results["skipped"]) == (
        total, passed, failed, 0,
    )
    assert len(results["test_results"]) == total
    assert results["fulltest_summary"] == summary
    assert results["test_results"][1] == {
        "name": "help", "status": "passed" if fulltest_passed else "failed",
        "stdout": "usage\n", "stderr": "diagnostic\n",
        "return_code": 0 if fulltest_passed else 1,
        "duration": 0.25, "message": test_message,
    }
    artifacts = results["fulltest_artifacts"]
    assert set(artifacts) == {"raw_json", "jsonl", "log", "suite"}
    assert all(Path(path).is_file() for path in artifacts.values())
    assert json.loads(Path(artifacts["raw_json"]).read_text())["summary"] == summary
    comment = (output_dir / "comment-sample.md").read_text(encoding="utf-8")
    report = (output_dir / "test-report-sample.md").read_text(encoding="utf-8")
    assert comment.startswith(f"{'❌' if failed else '✅'} **sample:1.0**")
    assert f"Tests: {passed}/{total} passed (failed {failed}, skipped 0)" in comment
    assert status.upper() in report
    assert f"{passed}/{total} tests passed (failed {failed})" in report
    if child_exit and fulltest_passed:
        execution = results["test_results"][2]
        assert execution["status"] == "failed"
        assert execution["return_code"] == child_exit
        assert "run_tests.py" in execution["stderr"]
        assert str(child_exit) in execution["stderr"]
        assert execution["stderr"] in comment
        assert execution["stderr"] in report


@pytest.fixture
def repeated_fulltest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    source = tmp_path / "candidate.simg"
    source.write_bytes(b"original image")
    config = tmp_path / "fulltest.yaml"
    config.write_text("name: sample\ntests: []\n")
    commands = []
    deploy_inputs = []
    behavior = {"raw": True, "jsonl": True, "exit": 0}

    class Tester(ContainerTester):
        def select_runtime(self, runtime: str) -> SimpleNamespace:
            return SimpleNamespace(name="apptainer")

        def run_test_suite(self, container: str, *args, **kwargs) -> dict[str, object]:
            deploy_inputs.append(container)
            return {"total_tests": 0, "passed": 0, "failed": 0,
                    "skipped": 0, "test_results": []}

    def child(command: list[str], **kwargs) -> SimpleNamespace:
        commands.append(command)
        if behavior["raw"]:
            Path(command[command.index("-o") + 1]).write_text(json.dumps({
                "summary": {"total_tests": 1, "tests_passed": 1, "tests_failed": 0},
                "suites": [{"name": "sample", "tests": [
                    {"name": "help", "passed": True}]}],
            }))
        if behavior["jsonl"]:
            Path(command[command.index("--jsonl") + 1]).write_text(json.dumps({
                "suite": "sample", "test": "help", "stdout": "old stdout",
                "stderr": "old stderr", "exit_code": 0,
            }) + "\n")
        Path(command[command.index("--log") + 1]).write_text("child log\n")
        return SimpleNamespace(returncode=behavior["exit"])

    monkeypatch.setattr("workflows.release_test_runner.ContainerTester", Tester)
    monkeypatch.setattr("workflows.release_test_runner.subprocess.run", child)
    output = tmp_path / "output"
    args = ["--recipe", "sample", "--version", "1.0", "--release-file", "missing.json",
            "--candidate-container", str(source), "--test-config", str(config),
            "--output-dir", str(output), "--results-path", str(output / "results.json"),
            "--repo-root", str(tmp_path), "--github-output", str(tmp_path / "github-output")]
    return SimpleNamespace(args=args, behavior=behavior, commands=commands,
                           deploy_inputs=deploy_inputs, output=output, source=source,
                           config=config)


@pytest.mark.parametrize("child_exit", [0, 2])
def test_repeated_fulltest_requires_current_results(
    repeated_fulltest: SimpleNamespace, child_exit: int,
) -> None:
    run = repeated_fulltest
    assert main(run.args) == 0
    run.behavior.update(raw=False, jsonl=False, exit=child_exit)
    assert main(run.args) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert "help" not in [test["name"] for test in results["test_results"]]
    assert f"exit {child_exit}" in results["test_results"][0]["stderr"]


def test_repeated_fulltest_does_not_borrow_jsonl(
    repeated_fulltest: SimpleNamespace,
) -> None:
    run = repeated_fulltest
    assert main(run.args) == 0
    run.behavior["jsonl"] = False
    assert main(run.args) == 0
    test = json.loads((run.output / "results.json").read_text())["test_results"][0]
    assert test["stdout"] == ""
    assert test["stderr"] == ""


def test_repeated_fulltest_retains_private_artifacts_and_workspace(
    repeated_fulltest: SimpleNamespace,
) -> None:
    run = repeated_fulltest
    assert main(run.args) == 0
    previous = json.loads((run.output / "results.json").read_text())["fulltest_artifacts"]
    contents = {path: Path(path).read_bytes() for path in previous.values()}
    old_work = Path(run.commands[-1][run.commands[-1].index("--work-dir") + 1])
    (old_work / "marker").write_text("previous work")
    run.config.write_text("name: changed\ntests: []\n")
    assert main(run.args) == 0
    current = json.loads((run.output / "results.json").read_text())["fulltest_artifacts"]
    assert set(current.values()).isdisjoint(previous.values())
    assert all(Path(path).read_bytes() == content for path, content in contents.items())
    work = Path(run.commands[-1][run.commands[-1].index("--work-dir") + 1])
    assert not (work / "marker").exists()
    assert (old_work / "marker").read_text() == "previous work"
    assert all(Path(path).parent == work.parent for path in current.values())


@pytest.mark.parametrize("location", ["legacy", "external", "symlink"])
def test_fulltest_uses_original_candidate_without_staging(
    repeated_fulltest: SimpleNamespace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    run = repeated_fulltest
    monkeypatch.chdir(tmp_path)
    source = (run.output / "fulltest-containers" if location == "legacy"
              else tmp_path / "first") / "candidate.simg"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"first image")
    second = tmp_path / "second" / source.name
    second.parent.mkdir()
    second.write_bytes(b"second image")
    if location == "symlink":
        target = source.with_name("actual-image.simg")
        source.rename(target)
        source.symlink_to(target)
        run.config.write_text("container: candidate.simg\ntests: []\n")
    run.args[run.args.index("--output-dir") + 1] = "output"
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    run.args[run.args.index("--repo-root") + 1] = str(checkout)
    for candidate in (source, second):
        run.args[run.args.index("--candidate-container") + 1] = str(candidate.relative_to(tmp_path))
        assert main(run.args) == 0
        command = run.commands[-1]
        assert Path(command[command.index("--container") + 1]) == candidate.resolve()
        assert run.deploy_inputs[-1] == str(candidate.resolve())
        assert "-c" not in command
        for flag in ("-o", "--log", "--jsonl", "--work-dir"):
            assert Path(command[command.index(flag) + 1]).is_absolute()
        assert Path(command[3]).is_absolute()
        assert Path(command[2]) == checkout / "builder/run_tests.py"
    assert source.read_bytes() == b"first image"
    assert second.read_bytes() == b"second image"
    assert list(run.output.rglob("*.simg")) == ([source] if location == "legacy" else [])


@pytest.mark.parametrize("mode,child_exit", [
    ("missing", 0), ("missing", 2), ("malformed", 2),
    ("invalid-shape", 2), ("launch", None),
])
def test_main_preserves_execution_failure_diagnostics(
    repeated_fulltest: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: str,
    child_exit: int | None,
) -> None:
    from workflows import release_test_runner as runner

    run = repeated_fulltest
    assert main(run.args) == 0
    previous = json.loads((run.output / "results.json").read_text())["fulltest_artifacts"]
    github_output = Path(run.args[run.args.index("--github-output") + 1])
    github_output.write_text("")
    capsys.readouterr()
    deploy = {"name": "deploy", "status": "failed", "stdout": "checked launchers",
              "stderr": "missing launcher", "return_code": 1}
    monkeypatch.setattr(runner.ContainerTester, "run_test_suite", lambda *a, **k: {
        "total_tests": 1, "passed": 0, "failed": 1, "skipped": 0,
        "test_results": [deploy],
    })
    current = {}

    def child(command: list[str], **kwargs) -> SimpleNamespace:
        current.update({
            "raw_json": Path(command[command.index("-o") + 1]),
            "jsonl": Path(command[command.index("--jsonl") + 1]),
            "log": Path(command[command.index("--log") + 1]),
            "suite": Path(command[3]),
        })
        if mode == "launch":
            raise OSError("cannot launch child")
        current["jsonl"].write_text('{"test":"partial","stdout":"current diagnostic"}\n')
        current["log"].write_text("current log diagnostic\n")
        if mode == "malformed":
            current["raw_json"].write_text("{malformed")
        elif mode == "invalid-shape":
            current["raw_json"].write_text("[]")
        return SimpleNamespace(returncode=child_exit)

    monkeypatch.setattr(runner.subprocess, "run", child)
    assert main(run.args) == 1
    captured = capsys.readouterr()
    results = json.loads((run.output / "results.json").read_text())
    assert results["container"] == str(run.source.resolve())
    assert (results["total_tests"], results["passed"], results["failed"], results["skipped"]) == (2, 0, 2, 0)
    assert results["test_results"][0] == deploy
    assert len(results["test_results"]) == 2
    failure = results["test_results"][1]
    assert failure["status"] == "failed"
    assert failure["return_code"] == (1 if child_exit is None else child_exit)
    assert failure["stderr"] in captured.err
    assert github_output.read_text() == f"status=failed\nreason={failure['stderr']}\n"
    artifacts = results["fulltest_artifacts"]
    assert artifacts == {key: str(path) for key, path in current.items() if path.is_file()}
    assert set(artifacts.values()).isdisjoint(previous.values())
    assert run.source.read_bytes() == b"original image"
    assert (run.output / "status-sample.txt").read_text() == "failed\n"
    report = (run.output / "test-report-sample.md").read_text()
    comment = (run.output / "comment-sample.md").read_text()
    assert "missing launcher" in report and "missing launcher" in comment
    assert failure["stderr"] in report and failure["stderr"] in comment
    assert all(path in report for path in artifacts.values())


def test_main_publication_failure_preserves_saved_results(
    repeated_fulltest: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from workflows import reporting

    run = repeated_fulltest
    write_text = reporting.write_text
    saved_results = []

    def fail_report(path: Path, text: str) -> None:
        if path.name == "test-report-sample.md":
            raise OSError("report disk failure")
        write_text(path, text)
        if path.name == "results.json":
            saved_results.append(json.loads(text))

    monkeypatch.setattr(reporting, "write_text", fail_report)
    assert main(run.args) == 1
    assert len(saved_results) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert results == saved_results[0]
    assert results["container"] == str(run.source.resolve())
    assert results["test_results"][0]["name"] == "help"
    assert results["total_tests"] == results["passed"] == 1
    assert results["failed"] == 0
    assert all(Path(path).is_file() for path in results["fulltest_artifacts"].values())
    assert "report disk failure" in capsys.readouterr().err
    github_output = Path(run.args[run.args.index("--github-output") + 1])
    assert github_output.read_text() == "status=failed\nreason=report disk failure\n"


def test_main_early_execution_failure_has_no_unwritten_artifacts(
    repeated_fulltest: SimpleNamespace,
) -> None:
    run = repeated_fulltest
    run.args[run.args.index("--candidate-container") + 1] = str(run.source.with_name("missing.simg"))
    assert main(run.args) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert results["container"] == "unresolved"
    assert results["fulltest_artifacts"] == {}
    assert results["total_tests"] == results["failed"] == 1
    assert results["passed"] == 0
    assert "Candidate container not found" in results["test_results"][0]["stderr"]
    assert not run.commands


@pytest.mark.parametrize("publication_fails", [False, True])
def test_main_preserves_multiline_execution_reason(
    repeated_fulltest: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    publication_fails: bool,
) -> None:
    from workflows import release_test_runner as runner
    from workflows import reporting

    run = repeated_fulltest
    reason = "cannot launch child\nFULLTEST_REASON\nstatus=injected"

    def fail_child(*args, **kwargs):
        raise OSError(reason)

    def fail_report(*args, **kwargs):
        raise OSError("report disk failure")

    monkeypatch.setattr(runner.subprocess, "run", fail_child)
    if publication_fails:
        monkeypatch.setattr(reporting, "build_report", fail_report)
    assert main(run.args) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert results["test_results"][0]["stderr"] == reason
    assert reason in capsys.readouterr().err
    github_output = Path(run.args[run.args.index("--github-output") + 1])
    lines = github_output.read_text().splitlines()
    assert lines[0] == "status=failed"
    assert lines[1].startswith("reason<<")
    delimiter = lines[1].removeprefix("reason<<")
    assert delimiter not in reason.splitlines()
    assert lines[-1] == delimiter
    assert "\n".join(lines[2:-1]) == reason


def test_main_github_output_failure_leaves_published_results(
    repeated_fulltest: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run = repeated_fulltest
    run.args[run.args.index("--github-output") + 1] = str(run.output)
    assert main(run.args) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert results["total_tests"] == results["passed"] == 1
    assert results["failed"] == 0
    assert results["test_results"][0]["name"] == "help"
    assert (run.output / "status-sample.txt").read_text() == "passed\n"
    assert "Unable to write GitHub output" in capsys.readouterr().err


def test_successfully_published_failed_execution_is_collectible(
    repeated_fulltest: SimpleNamespace,
) -> None:
    run = repeated_fulltest
    run.behavior["exit"] = 2
    assert main(run.args) == 1
    old = next(run.output.glob("fulltest-run-*"))
    run.behavior["exit"] = 0
    assert main(run.args) == 0
    actions = {entry.path: entry.action for entry in cleanup_runs(run.output, older_than_days=0)}
    assert actions[old] == "deleted"
    assert list(actions.values()).count("retained") == 1


def test_failed_publication_remains_unfinished_after_new_publication(
    repeated_fulltest: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from workflows import reporting

    run = repeated_fulltest
    original = reporting.build_report

    def fail_report(*args, **kwargs):
        raise OSError("report disk failure")

    monkeypatch.setattr(reporting, "build_report", fail_report)
    assert main(run.args) == 1
    old = next(run.output.glob("fulltest-run-*"))
    assert json.loads((run.output / "results.json").read_text())["fulltest_artifacts"]
    monkeypatch.setattr(reporting, "build_report", original)
    assert main(run.args) == 0
    entries = {entry.path: entry for entry in cleanup_runs(run.output, older_than_days=0)}
    assert entries[old].reason == "unfinished run"
    assert old.is_dir()


@pytest.mark.parametrize("execution_fails", [False, True])
def test_finalization_failure_preserves_publication_and_execution_reason(
    repeated_fulltest: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], execution_fails: bool,
) -> None:
    run = repeated_fulltest
    if execution_fails:
        run.behavior["raw"] = False
    original = Path.replace

    def fail_completion(path, target):
        if path.name == ".fulltest-run.json.tmp":
            raise OSError("completion disk failure")
        return original(path, target)

    monkeypatch.setattr(Path, "replace", fail_completion)
    assert main(run.args) == 1
    results = json.loads((run.output / "results.json").read_text())
    assert results["container"] == str(run.source.resolve())
    assert results["fulltest_artifacts"]
    assert results["failed"] == int(execution_fails)
    assert (run.output / "status-sample.txt").read_text() == ("failed\n" if execution_fails else "passed\n")
    github_output = Path(run.args[run.args.index("--github-output") + 1]).read_text()
    assert "status=failed" in github_output
    assert ("failed before writing results" if execution_fails else "completion disk failure") in github_output
    assert "Unable to finalize test run: completion disk failure" in capsys.readouterr().err
    entry, = cleanup_runs(run.output, older_than_days=0)
    assert entry.reason == "unfinished run"


def test_main_holds_run_lock_through_publication_in_another_process(
    repeated_fulltest: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import multiprocessing
    from workflows import release_test_runner as runner

    run = repeated_fulltest
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe()
    publish = runner.publish_test_results

    def paused_publication(**kwargs):
        child.send("executed")
        child.recv()
        return publish(**kwargs)

    monkeypatch.setattr(runner, "publish_test_results", paused_publication)
    process = context.Process(target=main, args=(run.args,))
    process.start()
    try:
        assert parent.poll(10), "runner did not reach publication"
        assert parent.recv() == "executed"
        entry, = cleanup_runs(run.output, older_than_days=0)
        assert entry.reason == "active run or another cleaner"
        assert list(entry.path.glob("*.log"))
        parent.send("publish")
        process.join(10)
        assert not process.is_alive()
        assert process.exitcode == 0
        entry, = cleanup_runs(run.output, older_than_days=0)
        assert entry.reason == "referenced by latest results"
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
        child.close()
