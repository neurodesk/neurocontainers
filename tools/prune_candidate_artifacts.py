"""Delete PR candidate bundles that no promotion can select.

``pr-container-candidate.yml`` uploads a ``candidate-<container>-<head sha>``
bundle (Docker archive, SIF and test output) for every PR push that needs a
build, and keeps it for 30 days. Promotion, including a ``pr_number``
republish, selects bundles by head SHA alone: it takes the newest
``pull_request`` run of the candidate workflow for that SHA and downloads that
run's bundles. A bundle can therefore only be selected if

* its run is the newest candidate run for its head SHA, and
* that SHA is still the head of an open PR or the final head of a merged PR.

Every other bundle is deleted: older runs for the same head, heads superseded
by a later push, and heads of PRs closed without merging. A later push back to
an old SHA, or reopening a PR, starts a newer candidate run, which is the only
one promotion would select. Merged final heads are kept until they expire, so
recovery and republishing keep working.

Bundles whose run is not a known candidate run, whose selected run is still in
progress, or that were created within ``--min-age-hours`` are kept.
``candidate-report-*`` and every other artifact are never touched.

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
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"
REPOSITORY = "neurodesk/neurocontainers"
CANDIDATE_WORKFLOW = "pr-container-candidate.yml"
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
    completed: bool


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
    runs: Iterable[Run],
    live_heads: set[str],
    cutoff: datetime,
) -> Plan:
    """Split bundles into kept and deletable ones, with a reason for each.

    ``runs`` are the ``pull_request`` runs of the candidate workflow and
    ``live_heads`` the head SHAs of open PRs plus final heads of merged PRs.
    """
    runs_by_id = {run.id: run for run in runs}
    newest: dict[str, Run] = {}
    for run in runs_by_id.values():
        if run.head_sha not in newest or run.id > newest[run.head_sha].id:
            newest[run.head_sha] = run

    keep: list[Decision] = []
    delete: list[Decision] = []
    for bundle in bundles:
        run = runs_by_id.get(bundle.run_id)
        if run is None or run.head_sha != bundle.head_sha:
            keep.append(Decision(bundle, "not from a known candidate run"))
        elif bundle.created >= cutoff:
            keep.append(Decision(bundle, "recent"))
        elif newest[run.head_sha].id != run.id:
            delete.append(Decision(bundle, "a newer candidate run exists for this head"))
        elif not run.completed:
            keep.append(Decision(bundle, "candidate run in progress"))
        elif run.head_sha in live_heads:
            keep.append(Decision(bundle, "head of an open or merged PR"))
        else:
            delete.append(Decision(bundle, "head of no open or merged PR"))
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


def live_heads(pulls: Iterable[dict]) -> set[str]:
    return {
        pull["head"]["sha"]
        for pull in pulls
        if pull["state"] == "open" or pull.get("merged_at")
    }


class GitHub:
    def __init__(self, token: str, repository: str = REPOSITORY, api: str = API):
        self.token = token
        self.base = f"{api}/repos/{repository}"

    def _request(self, method: str, url: str) -> tuple[object, str]:
        request = urllib.request.Request(url, method=method)
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return (json.loads(body) if body else None), response.headers.get("Link", "")

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

    def candidate_runs(self, since: datetime) -> list[Run]:
        query = urllib.parse.urlencode(
            {
                "event": "pull_request",
                "created": f">={since.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                "per_page": 100,
            }
        )
        items = self._pages(
            f"/actions/workflows/{CANDIDATE_WORKFLOW}/runs?{query}", "workflow_runs"
        )
        return [
            Run(item["id"], item["head_sha"], item["status"] == "completed")
            for item in items
        ]

    def pulls(self) -> list[dict]:
        return list(self._pages("/pulls?state=all&per_page=100"))

    def delete_artifact(self, artifact_id: int) -> None:
        self._request("DELETE", f"{self.base}/actions/artifacts/{artifact_id}")


def gib(decisions: Iterable[Decision]) -> float:
    return sum(decision.bundle.size for decision in decisions) / 2**30


def write_report(plan: Plan, path: str) -> None:
    def rows(decisions: list[Decision]) -> list[dict]:
        return [
            {**asdict(d.bundle), "created": d.bundle.created.isoformat(), "reason": d.reason}
            for d in decisions
        ]

    with open(path, "w") as handle:
        json.dump({"keep": rows(plan.keep), "delete": rows(plan.delete)}, handle, indent=2)


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
    # Bundles are uploaded by their run, so no run older than the oldest
    # bundle matters. A day of slack covers runs that started before midnight.
    since = min((b.created for b in bundles), default=started) - timedelta(days=1)
    runs = github.candidate_runs(since)
    plan = plan_deletions(
        bundles, runs, live_heads(pulls), started - timedelta(hours=args.min_age_hours)
    )
    if args.report:
        write_report(plan, args.report)
    summarize(plan)

    if not args.apply:
        print("Dry run: nothing deleted. Pass --apply to delete.")
        return 0
    failures = 0
    for decision in plan.delete:
        try:
            github.delete_artifact(decision.bundle.id)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                continue  # expired or deleted since planning
            failures += 1
            print(f"::warning::{decision.bundle.name}: HTTP {error.code}")
    print(f"Deleted {len(plan.delete) - failures} bundles; {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
