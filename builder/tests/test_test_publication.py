from __future__ import annotations

import json
from pathlib import Path

import pytest

from workflows import reporting
from workflows.reporting import (
    CommentOutput,
    PublicationPlan,
    ReportOutput,
    publish_test_results,
)
from workflows.test_runner import ContainerTestRunner, TestRequest as RunRequest


def _deploy_results() -> dict[str, object]:
    payload = {
        "total": 1,
        "passed": 1,
        "failed": 0,
        "skipped": 0,
        "tests": [
            {
                "name": "deploy_bin:tool",
                "status": "passed",
                "message": "Found at /opt/tool/bin/tool",
            }
        ],
    }
    return {
        "container": "sample.sif",
        "runtime": "apptainer",
        "total_tests": 1,
        "passed": 1,
        "failed": 0,
        "skipped": 0,
        "test_results": [
            {
                "name": "deploy",
                "status": "passed",
                "stdout": json.dumps(payload),
                "stderr": "",
                "return_code": 0,
            }
        ],
    }


@pytest.mark.parametrize("status_newline", [False, True])
def test_publication_renders_persisted_enriched_results(
    tmp_path: Path, status_newline: bool
) -> None:
    results = _deploy_results()
    results_path = tmp_path / "results.json"
    report_path = tmp_path / "report.md"
    comment_path = tmp_path / "comment.md"
    status_path = tmp_path / "status.txt"

    published = publish_test_results(
        results,
        recipe="sample",
        version="1.0",
        plan=PublicationPlan(
            results_path,
            (
                ReportOutput(report_path),
                CommentOutput(comment_path, status_path, status_newline),
            ),
        ),
    )

    assert published.results == json.loads(results_path.read_text())
    payload = json.loads(published.results["test_results"][0]["stdout"])
    assert payload["summarised"] is True
    assert payload["tests"][0]["name"] == "tool"
    assert "summarised" not in json.loads(results["test_results"][0]["stdout"])
    for path, content in (
        (comment_path, published.comment),
        (report_path, published.report),
    ):
        assert path.read_text() == content
        assert "deploy_bin:tool" not in content
    assert "tool — passed" in published.comment
    assert "PASSED" in published.report
    assert status_path.read_bytes() == (b"passed\n" if status_newline else b"passed")
    assert published.status == "passed"


@pytest.mark.parametrize("comment_first", [False, True])
def test_publication_failure_preserves_completed_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, comment_first: bool
) -> None:
    report_path = tmp_path / "report.md"
    comment_path = tmp_path / "comment.md"
    status_path = tmp_path / "status.txt"
    report = ReportOutput(report_path)
    comment = CommentOutput(comment_path, status_path)
    write_text = reporting.write_text

    def fail_report(path: Path, content: str) -> None:
        if path == report_path:
            raise PermissionError("report destination unavailable")
        write_text(path, content)

    monkeypatch.setattr(reporting, "write_text", fail_report)

    with pytest.raises(PermissionError, match="report destination unavailable"):
        publish_test_results(
            _deploy_results(),
            recipe="sample",
            version="1.0",
            plan=PublicationPlan(
                tmp_path / "results.json",
                (comment, report) if comment_first else (report, comment),
            ),
        )

    assert json.loads((tmp_path / "results.json").read_text())["passed"] == 1
    assert not report_path.exists()
    assert comment_path.exists() is comment_first
    assert status_path.exists() is comment_first


def test_publication_can_write_results_without_markdown(tmp_path: Path) -> None:
    published = publish_test_results(
        _deploy_results(),
        recipe="sample",
        version="1.0",
        plan=PublicationPlan(tmp_path / "results.json", ()),
    )

    assert published.status == "passed"
    assert published.comment is None
    assert published.report is None
    assert [path.name for path in tmp_path.iterdir()] == ["results.json"]


@pytest.mark.parametrize("fallback", [False, True])
def test_publication_reload_failure_obeys_caller_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fallback: bool
) -> None:
    def corrupt_results(path: Path) -> None:
        path.write_text("{broken", encoding="utf-8")

    monkeypatch.setattr(reporting, "summarise_results_file", corrupt_results)
    results = _deploy_results()
    plan = PublicationPlan(
        tmp_path / "results.json",
        (
            CommentOutput(tmp_path / "comment.md", tmp_path / "status.txt"),
        ),
        reload_fallback=fallback,
    )

    if fallback:
        published = publish_test_results(
            results, recipe="sample", version="1.0", plan=plan
        )
        assert published.results == results
        assert published.status == "passed"
        assert "deploy_bin:tool" in (tmp_path / "comment.md").read_text()
    else:
        with pytest.raises(json.JSONDecodeError):
            publish_test_results(
                results, recipe="sample", version="1.0", plan=plan
            )
        assert not (tmp_path / "comment.md").exists()
        assert not (tmp_path / "status.txt").exists()


@pytest.mark.parametrize(
    ("create_comment", "create_report"),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_matrix_runner_preserves_requested_artifact_paths(
    tmp_path: Path, create_comment: bool, create_report: bool
) -> None:
    (tmp_path / "recipes" / "sample").mkdir(parents=True)
    output_dir = tmp_path / "outputs"
    results_path = tmp_path / "custom" / "results.json"
    runner = ContainerTestRunner(repo_root=tmp_path)

    outcome = runner.run(
        RunRequest(
            recipe="sample",
            allow_missing_release=True,
            output_dir=output_dir,
            results_path=results_path,
            comment_filename="review.md",
            status_filename="verdict.txt",
            report_filename="summary.md",
            create_comment=create_comment,
            create_report=create_report,
        )
    )

    assert outcome.status == "skipped"
    assert outcome.version == ""
    assert outcome.results_path == results_path
    assert outcome.results == json.loads(results_path.read_text())
    assert (output_dir / "review.md").exists() is create_comment
    assert (output_dir / "verdict.txt").exists() is create_comment
    assert (output_dir / "summary.md").exists() is create_report
    if create_comment:
        assert outcome.comment_path == output_dir / "review.md"
        assert outcome.comment == outcome.comment_path.read_text()
        assert "sample:unknown" in outcome.comment
        assert outcome.status_path.read_bytes() == b"skipped"
    else:
        assert outcome.comment_path is None
        assert outcome.comment is None
        assert outcome.status_path is None
    if create_report:
        assert outcome.report_path == output_dir / "summary.md"
        assert outcome.report == outcome.report_path.read_text()
    else:
        assert outcome.report_path is None
        assert outcome.report is None
