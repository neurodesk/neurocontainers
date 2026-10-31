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


def decide(bundles, runs=(), heads=frozenset({HEAD}), busy=frozenset()) -> dict:
    by_id = {run.id: run for run in runs}
    plan = prune.plan_deletions(bundles, set(heads), CUTOFF, by_id, set(busy))
    return {
        "keep": [d.bundle.run_id for d in plan.keep],
        "delete": [d.bundle.run_id for d in plan.delete],
    }


def test_keeps_every_bundle_of_a_live_head() -> None:
    """A running promotion may already have selected an older run's bundle."""
    runs = [prune.Run(1, HEAD), prune.Run(2, HEAD)]
    assert decide([bundle(HEAD, 1), bundle(HEAD, 2)], runs) == {
        "keep": [1, 2],
        "delete": [],
    }


def test_deletes_heads_of_no_open_or_merged_pr() -> None:
    """A superseded push or a PR closed without merging leaves a dead head."""
    assert decide([bundle(STALE, 1)], [prune.Run(1, STALE)]) == {
        "keep": [],
        "delete": [1],
    }


def test_keeps_a_dead_head_while_a_candidate_run_for_it_is_in_progress() -> None:
    runs = [prune.Run(1, STALE)]
    assert decide([bundle(STALE, 1)], runs, busy={STALE}) == {
        "keep": [1],
        "delete": [],
    }


def test_keeps_recent_bundles() -> None:
    fresh = bundle(STALE, 1, created=NOW - timedelta(hours=1))
    assert decide([fresh], [prune.Run(1, STALE)]) == {"keep": [1], "delete": []}


@pytest.mark.parametrize("runs", [[], [prune.Run(1, HEAD)]])
def test_keeps_bundles_it_cannot_tie_to_their_candidate_run(runs) -> None:
    """A missing, foreign or mismatched run is never guessed at."""
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
    def __init__(self, bundles, runs, pulls, busy=()):
        self.store = {b.id: b for b in bundles}
        self.runs = {run.id: run for run in runs}
        self.pull_list = pulls
        self.busy = set(busy)
        self.looked_up: list[int] = []

    def bundles(self):
        return list(self.store.values())

    def candidate_run(self, run_id):
        self.looked_up.append(run_id)
        return self.runs.get(run_id)

    def has_active_candidate_run(self, head_sha):
        return head_sha in self.busy

    def pulls(self):
        return self.pull_list

    def delete_artifact(self, artifact_id):
        del self.store[artifact_id]


OPEN_PR = [{"state": "open", "head": {"sha": HEAD}, "merged_at": None}]


@pytest.mark.parametrize("apply", [False, True])
def test_main_deletes_only_with_apply(monkeypatch, tmp_path, apply: bool) -> None:
    live, dead = bundle(HEAD, 1), bundle(STALE, 2)
    runs = [prune.Run(1, HEAD), prune.Run(2, STALE)]
    fake = FakeGitHub([live, dead], runs, OPEN_PR)
    monkeypatch.setattr(prune, "GitHub", lambda *args: fake)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    report = tmp_path / "plan.json"

    argv = ["--report", str(report)] + (["--apply"] if apply else [])
    assert prune.main(argv) == 0

    assert set(fake.store) == ({live.id} if apply else {live.id, dead.id})
    assert [row["id"] for row in json.loads(report.read_text())["delete"]] == [dead.id]
    assert fake.looked_up == [dead.run_id]


def test_republish_selection_survives_a_newer_run_for_the_same_head(monkeypatch) -> None:
    """A republish selects run 1; another PR with the same head starts run 2.

    The merged head stays live, so run 1's bundle survives until the queued
    publish job downloads it.
    """
    merged = [{"state": "closed", "head": {"sha": HEAD}, "merged_at": "2026-10-20T00:00:00Z"}]
    selected, newer = bundle(HEAD, 1), bundle(HEAD, 2)
    fake = FakeGitHub([selected, newer], [prune.Run(1, HEAD), prune.Run(2, HEAD)], merged)
    monkeypatch.setattr(prune, "GitHub", lambda *args: fake)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    assert prune.main(["--apply"]) == 0
    assert set(fake.store) == {selected.id, newer.id}


def test_refuses_to_plan_without_pull_requests(monkeypatch) -> None:
    """A failed PR listing would make every head look dead."""
    fake = FakeGitHub([bundle(HEAD, 1)], [prune.Run(1, HEAD)], [])
    monkeypatch.setattr(prune, "GitHub", lambda *args: fake)
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    assert prune.main(["--apply"]) == 1
    assert fake.store


def test_apply_requires_a_token(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        prune.main(["--apply"])


class FakeResponse:
    def __init__(self, body: object, link: str = ""):
        self.body = json.dumps(body).encode()
        self.headers = {"Link": link}

    def read(self) -> bytes:
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None


def serve(monkeypatch, responses: dict[str, FakeResponse]) -> None:
    def urlopen(request, timeout):
        path = request.full_url.removeprefix(f"{prune.API}/repos/{prune.REPOSITORY}")
        return responses[path]

    monkeypatch.setattr(prune.urllib.request, "urlopen", urlopen)


def test_client_follows_every_page_of_pull_requests(monkeypatch) -> None:
    second = f"{prune.API}/repos/{prune.REPOSITORY}/pulls?page=2"
    serve(
        monkeypatch,
        {
            "/pulls?state=all&per_page=100": FakeResponse(
                [{"number": 1}], f'<{second}>; rel="next"'
            ),
            "/pulls?page=2": FakeResponse([{"number": 2}]),
        },
    )
    assert [p["number"] for p in prune.GitHub("token").pulls()] == [1, 2]


@pytest.mark.parametrize(
    "event, path, expected",
    [
        ("pull_request", ".github/workflows/pr-container-candidate.yml", True),
        ("workflow_dispatch", ".github/workflows/pr-container-candidate.yml", False),
        ("pull_request", ".github/workflows/recipes-ci.yml", False),
    ],
)
def test_client_only_trusts_candidate_pull_request_runs(
    monkeypatch, event: str, path: str, expected: bool
) -> None:
    run = {"id": 7, "event": event, "path": path, "head_sha": STALE}
    serve(monkeypatch, {"/actions/runs/7": FakeResponse(run)})
    found = prune.GitHub("token").candidate_run(7)
    assert (found == prune.Run(7, STALE)) is expected
