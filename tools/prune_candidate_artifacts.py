"""Delete PR candidate bundles whose head no promotion can select.

``pr-container-candidate.yml`` uploads a ``candidate-<container>-<head sha>``
bundle (Docker archive, SIF and test output) for every PR push that needs a
build, and keeps it for 30 days. Promotion, including a ``pr_number``
republish, only ever selects bundles for a merged PR's final head SHA, and an
open PR can still become one. A head is live while it is an open PR's head, or
a merged PR's final head whose trusted push promotion has not finalized. Once
that ``finalize`` job succeeded, a republish refuses the merge and points at
``manual-build``, so its bundles are dead too. Bundles of dead heads are
deleted: finalized merges, superseded pushes and PRs closed without merging.
A later push back to a dead SHA, or reopening the PR, starts a new candidate
run, which promotion would select instead.

Every bundle of a live head is kept, including older runs for it, because a
running promotion may already have selected one. Bundles created within
``--min-age-hours``, bundles not owned by a ``pull_request`` run of the
candidate workflow for their head, and dead heads with a candidate run still in
progress are kept too. ``candidate-report-*`` and every other artifact are
never touched.

Only a running promotion downloads bundles. A PR reopened or pushed back to a
dead head and merged mid-sweep could let that promotion select an old run
before the new candidate run is visible. So ``--apply`` re-reads the PRs after
planning, and before every delete it stops if any promotion run is unfinished.
The next sweep picks up what was left. This is not a lock: a promotion that
starts between that check and the DELETE could still select the bundle. It
would then fail to download it, and rerunning the promotion selects the newer
candidate run that the reopen or push started.

Runs as a dry run unless ``--apply`` is given. Needs ``GITHUB_TOKEN`` with
``actions: write`` to delete, and read access to Actions and pull requests.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Collection, Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"
REPOSITORY = "neurodesk/neurocontainers"
CANDIDATE_WORKFLOW = "pr-container-candidate.yml"
PROMOTION_WORKFLOW = "promote-container-candidate.yml"
ACTIVE_STATUSES = ("queued", "in_progress", "waiting", "requested", "pending")
BUNDLE_NAME = re.compile(r"^candidate-(?!report-).+-(?P<sha>[0-9a-f]{40})$")


@dataclass(frozen=True)
class Bundle:
    id: int
    name: str
    size: int
    created: datetime
    run_id: int
    head_sha: str


@dataclass(frozen=True)
class Run:
    id: int
    head_sha: str


@dataclass(frozen=True)
class Decision:
    bundle: Bundle
    reason: str


@dataclass(frozen=True)
class Plan:
    keep: list[Decision]
    delete: list[Decision]


def plan_deletions(
    bundles: Iterable[Bundle],
    live_heads: set[str],
    cutoff: datetime,
    runs: Mapping[int, Run],
    busy_heads: set[str],
) -> Plan:
    """Split bundles into kept and deletable ones, with a reason for each.

    ``live_heads`` are open PR heads plus merged PR final heads. ``runs`` maps
    run IDs to candidate ``pull_request`` runs and ``busy_heads`` are heads
    with a candidate run in progress; both only need to cover dead heads.
    """
    keep: list[Decision] = []
    delete: list[Decision] = []
    for bundle in bundles:
        run = runs.get(bundle.run_id)
        if bundle.created >= cutoff:
            keep.append(Decision(bundle, "recent"))
        elif bundle.head_sha in live_heads:
            keep.append(Decision(bundle, "head of an open or unfinalized merged PR"))
        elif run is None or run.head_sha != bundle.head_sha:
            keep.append(Decision(bundle, "not from a known candidate run"))
        elif bundle.head_sha in busy_heads:
            keep.append(Decision(bundle, "candidate run in progress"))
        else:
            delete.append(Decision(bundle, "head of no open or unfinalized merged PR"))
    return Plan(keep, delete)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_bundle(item: dict) -> Bundle | None:
    match = BUNDLE_NAME.match(item["name"])
    run = item.get("workflow_run") or {}
    if item.get("expired") or not match or not run.get("id"):
        return None
    return Bundle(
        id=item["id"],
        name=item["name"],
        size=item["size_in_bytes"],
        created=parse_time(item["created_at"]),
        run_id=run["id"],
        head_sha=match["sha"],
    )


def live_heads(pulls: Iterable[dict], finalized: Collection[str] = ()) -> set[str]:
    """Open PR heads and the final heads of merges not in ``finalized``."""
    return {
        pull["head"]["sha"]
        for pull in pulls
        if pull["state"] == "open"
        or (pull.get("merged_at") and pull.get("merge_commit_sha") not in finalized)
    }


class GitHub:
    def __init__(self, token: str, repository: str = REPOSITORY, api: str = API):
        self.token = token
        self.repository = repository
        self.base = f"{api}/repos/{repository}"

    def _request(self, method: str, url: str) -> tuple[object, str]:
        request = urllib.request.Request(url, method=method)
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return (json.loads(body) if body else None), response.headers.get(
                "Link", ""
            )

    def _pages(self, path: str, key: str | None = None) -> Iterator[dict]:
        url: str | None = f"{self.base}{path}"
        while url:
            data, link = self._request("GET", url)
            yield from (data[key] if key else data)  # type: ignore[index]
            match = re.search(r'<([^>]+)>;\s*rel="next"', link)
            url = match[1] if match else None

    def bundles(self) -> list[Bundle]:
        items = self._pages("/actions/artifacts?per_page=100", "artifacts")
        return [bundle for item in items if (bundle := parse_bundle(item))]

    def candidate_run(self, run_id: int) -> Run | None:
        """Return the run if it is a pull_request run of the candidate workflow."""
        try:
            data, _ = self._request("GET", f"{self.base}/actions/runs/{run_id}")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None  # a deleted run proves nothing about its bundles
            raise
        assert isinstance(data, dict)
        if data["event"] != "pull_request" or not data["path"].endswith(
            f".github/workflows/{CANDIDATE_WORKFLOW}"
        ):
            return None
        return Run(data["id"], data["head_sha"])

    def has_active_candidate_run(self, head_sha: str) -> bool:
        query = urllib.parse.urlencode(
            {"event": "pull_request", "head_sha": head_sha, "per_page": 100}
        )
        runs = self._pages(
            f"/actions/workflows/{CANDIDATE_WORKFLOW}/runs?{query}", "workflow_runs"
        )
        return any(run["status"] != "completed" for run in runs)

    def push_promotion_finalized(self, merge_sha: str) -> bool:
        """Whether the newest trusted push promotion of a merge finalized."""
        query = urllib.parse.urlencode(
            {"event": "push", "branch": "main", "head_sha": merge_sha, "per_page": 100}
        )
        runs = [
            run
            for run in self._pages(
                f"/actions/workflows/{PROMOTION_WORKFLOW}/runs?{query}", "workflow_runs"
            )
            if run["head_sha"] == merge_sha
            and run["event"] == "push"
            and run["head_branch"] == "main"
            and run["path"].endswith(f".github/workflows/{PROMOTION_WORKFLOW}")
            and run["repository"]["full_name"] == self.repository
            and run["head_repository"]["full_name"] == self.repository
        ]
        if not runs:
            return False
        newest = max(runs, key=lambda run: run["id"])
        if newest["status"] != "completed":
            return False
        jobs = self._pages(
            f"/actions/runs/{newest['id']}/jobs?filter=latest&per_page=100", "jobs"
        )
        return any(
            job["name"] == "finalize" and job["conclusion"] == "success" for job in jobs
        )

    def promotion_active(self) -> bool:
        """Whether any promotion run, including a rerun of an old one, is unfinished."""
        for status in ACTIVE_STATUSES:
            query = urllib.parse.urlencode({"status": status, "per_page": 100})
            runs = self._pages(
                f"/actions/workflows/{PROMOTION_WORKFLOW}/runs?{query}", "workflow_runs"
            )
            if any(True for _ in runs):
                return True
        return False

    def pulls(self) -> list[dict]:
        return list(self._pages("/pulls?state=all&per_page=100"))

    def delete_artifact(self, artifact_id: int) -> None:
        self._request("DELETE", f"{self.base}/actions/artifacts/{artifact_id}")


def gib(decisions: Iterable[Decision]) -> float:
    return sum(decision.bundle.size for decision in decisions) / 2**30


def write_report(plan: Plan, path: str) -> None:
    def rows(decisions: list[Decision]) -> list[dict]:
        return [
            {
                **asdict(d.bundle),
                "created": d.bundle.created.isoformat(),
                "reason": d.reason,
            }
            for d in decisions
        ]

    with open(path, "w") as handle:
        json.dump(
            {"keep": rows(plan.keep), "delete": rows(plan.delete)}, handle, indent=2
        )


def summarize(plan: Plan) -> None:
    for label, decisions in (("Keep", plan.keep), ("Delete", plan.delete)):
        print(f"{label}: {len(decisions)} bundles, {gib(decisions):.1f} GiB")
        reasons = Counter(decision.reason for decision in decisions)
        for reason, count in reasons.most_common():
            size = gib(d for d in decisions if d.reason == reason)
            print(f"  {count:5d}  {size:8.1f} GiB  {reason}")


def non_negative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or more")
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=REPOSITORY)
    parser.add_argument("--min-age-hours", type=non_negative_int, default=6)
    parser.add_argument("--report", help="Write the full plan as JSON")
    parser.add_argument("--apply", action="store_true", help="Delete bundles")
    args = parser.parse_args(argv)

    token = os.environ.get("GITHUB_TOKEN", "")
    if args.apply and not token:
        parser.error("--apply needs GITHUB_TOKEN")

    started = datetime.now(timezone.utc)
    github = GitHub(token, args.repository)
    bundles = github.bundles()
    pulls = github.pulls()
    if not pulls:
        # Without PRs every head looks dead; never act on a failed listing.
        print("::error::The repository returned no pull requests; refusing to plan.")
        return 1
    # Only merges that still have bundles need their promotion checked.
    bundle_heads = {b.head_sha for b in bundles}
    finalized = {
        pull["merge_commit_sha"]
        for pull in pulls
        if pull.get("merged_at")
        and pull.get("merge_commit_sha")
        and pull["head"]["sha"] in bundle_heads
        and github.push_promotion_finalized(pull["merge_commit_sha"])
    }
    heads = live_heads(pulls, finalized)
    cutoff = started - timedelta(hours=args.min_age_hours)
    # Only dead heads can be deleted, so only their runs need checking.
    dead = [b for b in bundles if b.created < cutoff and b.head_sha not in heads]
    runs = {
        run_id: run
        for run_id in {b.run_id for b in dead}
        if (run := github.candidate_run(run_id))
    }
    busy = {
        sha
        for sha in {b.head_sha for b in dead}
        if github.has_active_candidate_run(sha)
    }
    plan = plan_deletions(bundles, heads, cutoff, runs, busy)
    if args.report:
        write_report(plan, args.report)
    summarize(plan)

    if not args.apply:
        print("Dry run: nothing deleted. Pass --apply to delete.")
        return 0
    # A head may have come back to life while the plan was being made.
    revived = live_heads(github.pulls(), finalized)
    deleted = failures = 0
    for decision in plan.delete:
        if decision.bundle.head_sha in revived:
            print(f"Skipping {decision.bundle.name}: its head is live again")
            continue
        if github.promotion_active():
            print("A promotion is running; leaving the rest for the next sweep.")
            break
        try:
            github.delete_artifact(decision.bundle.id)
            deleted += 1
        except urllib.error.HTTPError as error:
            if error.code == 404:
                continue  # expired or deleted since planning
            failures += 1
            print(f"::warning::{decision.bundle.name}: HTTP {error.code}")
    print(f"Deleted {deleted} of {len(plan.delete)} planned bundles; {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
