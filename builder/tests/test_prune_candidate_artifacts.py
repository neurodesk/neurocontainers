from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from tools import prune_candidate_artifacts as prune

NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)
CUTOFF = NOW - timedelta(hours=6)
OLD = NOW - timedelta(days=3)
HEAD = "a" * 40
STALE = "b" * 40


def bundle(sha: str, run_id: int, created: datetime = OLD) -> prune.Bundle:
    return prune.Bundle(run_id * 10, f"candidate-qsmxt-{sha}", 2**30, created, run_id, sha)


def decide(bundles, runs, heads=frozenset({HEAD})) -> dict[str, list[int]]:
    plan = prune.plan_deletions(bundles, runs, set(heads), CUTOFF)
    return {
        "keep": [d.bundle.run_id for d in plan.keep],
        "delete": [d.bundle.run_id for d in plan.delete],
    }


def test_keeps_the_bundle_promotion_would_select() -> None:
    assert decide([bundle(HEAD, 1)], [prune.Run(1, HEAD, True)]) == {
        "keep": [1],
        "delete": [],
    }


def test_deletes_heads_of_no_open_or_merged_pr() -> None:
    """A superseded push or a PR closed without merging leaves a dead head."""
    assert decide([bundle(STALE, 1)], [prune.Run(1, STALE, True)]) == {
        "keep": [],
        "delete": [1],
    }


def test_deletes_older_runs_promotion_never_selects() -> None:
    """Promotion takes the newest candidate run for a head, even a live one."""
    runs = [prune.Run(1, HEAD, True), prune.Run(2, HEAD, True)]
    assert decide([bundle(HEAD, 1), bundle(HEAD, 2)], runs) == {
        "keep": [2],
        "delete": [1],
    }


def test_a_rebuild_after_reopening_supersedes_the_old_run() -> None:
    """Reopening or pushing back to an old SHA starts a newer run for it."""
    runs = [prune.Run(1, STALE, True), prune.Run(5, STALE, False)]
    assert decide([bundle(STALE, 1)], runs, heads={STALE}) == {
        "keep": [],
        "delete": [1],
    }


def test_keeps_a_dead_head_while_its_newest_run_is_in_progress() -> None:
    assert decide([bundle(STALE, 1)], [prune.Run(1, STALE, False)]) == {
        "keep": [1],
        "delete": [],
    }


def test_keeps_recent_bundles() -> None:
    fresh = bundle(STALE, 1, created=NOW - timedelta(hours=1))
    assert decide([fresh], [prune.Run(1, STALE, True)]) == {"keep": [1], "delete": []}


@pytest.mark.parametrize("runs", [[], [prune.Run(1, HEAD, True)]])
def test_keeps_bundles_it_cannot_tie_to_their_candidate_run(runs) -> None:
    """An unknown run, or a run for another head, is never guessed at."""
    assert decide([bundle(STALE, 1)], runs) == {"keep": [1], "delete": []}


def test_live_heads_are_open_heads_and_merged_final_heads() -> None:
    pulls = [
        {"state": "open", "head": {"sha": "1" * 40}, "merged_at": None},
        {"state": "closed", "head": {"sha": "2" * 40}, "merged_at": "2026-10-01T00:00:00Z"},
        {"state": "closed", "head": {"sha": "3" * 40}, "merged_at": None},
    ]
    assert prune.live_heads(pulls) == {"1" * 40, "2" * 40}


@pytest.mark.parametrize(
    "name, expected",
    [
        (f"candidate-qsmxt-{HEAD}", HEAD),
        (f"candidate-pydeface_arm64-{HEAD}", HEAD),
        (f"candidate-report-qsmxt-{HEAD}", None),
        (f"promotion-metadata-qsmxt-{HEAD}", None),
        ("candidate-qsmxt-main", None),
    ],
)
def test_only_candidate_bundles_are_considered(name: str, expected: str | None) -> None:
    item = {
        "id": 1,
        "name": name,
        "size_in_bytes": 1,
        "expired": False,
        "created_at": "2026-10-30T00:00:00Z",
        "workflow_run": {"id": 7},
    }
    parsed = prune.parse_bundle(item)
    assert (parsed.head_sha if parsed else None) == expected


class FakeGitHub:
    def __init__(self, bundles, runs, pulls):
        self.store = {b.id: b for b in bundles}
        self.runs = runs
        self.pull_list = pulls

    def bundles(self):
        return list(self.store.values())

    def candidate_runs(self, since):
        return self.runs

    def pulls(self):
        return self.pull_list

    def delete_artifact(self, artifact_id):
        del self.store[artifact_id]


OPEN_PR = [{"state": "open", "head": {"sha": HEAD}, "merged_at": None}]


@pytest.mark.parametrize("apply", [False, True])
def test_main_deletes_only_with_apply(monkeypatch, tmp_path, apply: bool) -> None:
    live, dead = bundle(HEAD, 1), bundle(STALE, 2)
    runs = [prune.Run(1, HEAD, True), prune.Run(2, STALE, True)]
    fake = FakeGitHub([live, dead], runs, OPEN_PR)
    monkeypatch.setattr(prune, "GitHub", lambda *args: fake)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    report = tmp_path / "plan.json"

    argv = ["--report", str(report)] + (["--apply"] if apply else [])
    assert prune.main(argv) == 0

    assert set(fake.store) == ({live.id} if apply else {live.id, dead.id})
    assert [row["id"] for row in json.loads(report.read_text())["delete"]] == [dead.id]


def test_refuses_to_plan_without_pull_requests(monkeypatch) -> None:
    """A failed PR listing would make every head look dead."""
    fake = FakeGitHub([bundle(HEAD, 1)], [prune.Run(1, HEAD, True)], [])
    monkeypatch.setattr(prune, "GitHub", lambda *args: fake)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    assert prune.main(["--apply"]) == 1
    assert fake.store


def test_apply_requires_a_token(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        prune.main(["--apply"])
