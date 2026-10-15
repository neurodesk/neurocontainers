"""Delete Nectar registry images that apps.json no longer publishes.

The Nectar Harbor project mirrors every container build as
``<name>_<version>:<build date>`` plus ``latest``. Neurodesk only serves the
builds listed in neurocommand's apps.json, so every other artifact only
consumes the project's storage quota. Artifacts pushed within ``--min-age-days``
are kept so an in-flight build or promotion staging tag is never removed.

Runs as a dry run unless ``--apply`` is given. Deleting needs
``NECTAR_REGISTRY_USERNAME`` and ``NECTAR_REGISTRY_PASSWORD``.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

HARBOR_API = "https://registry.rc.nectar.org.au/api/v2.0"
PROJECT = "ssdsorg"
APPS_JSON_URL = (
    "https://raw.githubusercontent.com/neurodesk/neurocommand/main/neurodesk/apps.json"
)
PAGE_SIZE = 100
# A truncated or empty apps.json would mark nearly every image for deletion.
MAX_DELETE_FRACTION = 0.5


@dataclass(frozen=True)
class Artifact:
    repository: str
    digest: str
    tags: tuple[str, ...]
    size: int
    pushed: datetime


@dataclass(frozen=True)
class Plan:
    keep: list[Artifact]
    recent: list[Artifact]
    delete: list[Artifact]
    published_missing: list[tuple[str, str]]


def published_images(apps: dict) -> set[tuple[str, str]]:
    """Return the (repository, tag) pairs that apps.json serves.

    The top-level key names the image and each app key ends with its version;
    GUI launchers such as "napari GUI-napari 0.8.0" share the image's build.
    """
    images = set()
    for image, entry in apps.items():
        for app, details in (entry.get("apps") or {}).items():
            version = app.rsplit(" ", 1)[-1]
            if " " not in app or not details.get("version"):
                raise ValueError(f"Unexpected apps.json entry: {app!r}")
            images.add((f"{image}_{version}".lower(), str(details["version"])))
    return images


def plan_deletions(
    artifacts: Iterable[Artifact],
    published: set[tuple[str, str]],
    cutoff: datetime,
) -> Plan:
    """Split artifacts into kept, too recent to judge, and deletable."""
    keep, recent, delete = [], [], []
    present = set()
    for artifact in artifacts:
        refs = {(artifact.repository, tag) for tag in artifact.tags}
        present |= refs
        if refs & published:
            keep.append(artifact)
        elif artifact.pushed > cutoff:
            recent.append(artifact)
        else:
            delete.append(artifact)
    repositories = {artifact.repository for artifact in keep + recent + delete}
    missing = sorted(ref for ref in published - present if ref[0] in repositories)
    return Plan(keep, recent, delete, missing)


class Harbor:
    def __init__(self, api: str, project: str, username: str = "", password: str = ""):
        self.api = api.rstrip("/")
        self.project = project
        self.auth = (
            "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
            if username
            else None
        )

    def _request(self, method: str, path: str) -> tuple[object, dict]:
        request = urllib.request.Request(f"{self.api}{path}", method=method)
        request.add_header("Accept", "application/json")
        if self.auth:
            request.add_header("Authorization", self.auth)
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return (json.loads(body) if body else None), dict(response.headers)

    def _pages(self, path: str) -> Iterator[dict]:
        page = 1
        while True:
            separator = "&" if "?" in path else "?"
            items, _ = self._request(
                "GET", f"{path}{separator}page={page}&page_size={PAGE_SIZE}"
            )
            yield from items or []
            if not items or len(items) < PAGE_SIZE:
                return
            page += 1

    def _repository_path(self, repository: str) -> str:
        # Harbor needs a slash inside a repository name double-encoded.
        name = urllib.parse.quote(urllib.parse.quote(repository, safe=""), safe="")
        return f"/projects/{self.project}/repositories/{name}"

    def repositories(self) -> list[str]:
        prefix = f"{self.project}/"
        return [
            item["name"].removeprefix(prefix)
            for item in self._pages(f"/projects/{self.project}/repositories")
        ]

    def artifacts(self, repository: str) -> list[Artifact]:
        path = f"{self._repository_path(repository)}/artifacts?with_tag=true"
        return [
            Artifact(
                repository=repository,
                digest=item["digest"],
                tags=tuple(tag["name"] for tag in item.get("tags") or []),
                size=int(item.get("size") or 0),
                pushed=datetime.fromisoformat(item["push_time"].replace("Z", "+00:00")),
            )
            for item in self._pages(path)
        ]

    def delete_artifact(self, artifact: Artifact) -> None:
        self._request(
            "DELETE",
            f"{self._repository_path(artifact.repository)}/artifacts/{artifact.digest}",
        )

    def delete_repository_if_empty(self, repository: str) -> bool:
        if self.artifacts(repository):
            return False
        self._request("DELETE", self._repository_path(repository))
        return True


def load_apps(source: str) -> dict:
    if source.startswith(("http://", "https://")):
        with urllib.request.urlopen(source, timeout=60) as response:
            return json.load(response)
    with open(source, encoding="utf-8") as handle:
        return json.load(handle)


def gib(artifacts: Iterable[Artifact]) -> float:
    return sum(artifact.size for artifact in artifacts) / 1024**3


def write_report(plan: Plan, path: str) -> None:
    def rows(artifacts: list[Artifact]) -> list[dict]:
        return [
            {
                "repository": a.repository,
                "digest": a.digest,
                "tags": list(a.tags),
                "size": a.size,
                "pushed": a.pushed.isoformat(),
            }
            for a in sorted(artifacts, key=lambda a: (a.repository, a.pushed))
        ]

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "keep": rows(plan.keep),
                "recent": rows(plan.recent),
                "delete": rows(plan.delete),
                "published_missing": [list(ref) for ref in plan.published_missing],
            },
            handle,
            indent=1,
        )


def non_negative_int(value: str) -> int:
    days = int(value)
    if days < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return days


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apps-json", default=APPS_JSON_URL)
    parser.add_argument("--api", default=HARBOR_API)
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--min-age-days", type=non_negative_int, default=14)
    parser.add_argument("--report", help="Write the full plan as JSON")
    parser.add_argument("--apply", action="store_true", help="Delete artifacts")
    args = parser.parse_args(argv)

    username = os.environ.get("NECTAR_REGISTRY_USERNAME", "")
    password = os.environ.get("NECTAR_REGISTRY_PASSWORD", "")
    if args.apply and not (username and password):
        parser.error(
            "--apply needs NECTAR_REGISTRY_USERNAME and NECTAR_REGISTRY_PASSWORD"
        )

    published = published_images(load_apps(args.apps_json))
    harbor = Harbor(args.api, args.project, username, password)
    artifacts = [
        artifact
        for repository in harbor.repositories()
        for artifact in harbor.artifacts(repository)
    ]
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.min_age_days)
    plan = plan_deletions(artifacts, published, cutoff)
    if args.report:
        write_report(plan, args.report)

    print(f"apps.json publishes {len(published)} images")
    print(
        f"Keep:   {len(plan.keep):5d} artifacts, {gib(plan.keep):8.1f} GiB (in apps.json)"
    )
    print(
        f"Recent: {len(plan.recent):5d} artifacts, {gib(plan.recent):8.1f} GiB "
        f"(pushed in the last {args.min_age_days} days)"
    )
    print(f"Delete: {len(plan.delete):5d} artifacts, {gib(plan.delete):8.1f} GiB")
    if plan.published_missing:
        print(f"{len(plan.published_missing)} published builds are missing from Nectar")

    if not args.apply:
        print("Dry run: nothing deleted. Pass --apply to delete.")
        return 0
    if not published or len(plan.delete) > MAX_DELETE_FRACTION * len(artifacts):
        print(
            f"::error::Refusing to delete {len(plan.delete)} of {len(artifacts)} "
            f"artifacts: more than {MAX_DELETE_FRACTION:.0%}, or apps.json lists "
            "no images. Check apps.json before pruning."
        )
        return 1

    failures = 0
    for artifact in plan.delete:
        try:
            harbor.delete_artifact(artifact)
        except urllib.error.HTTPError as error:
            failures += 1
            print(
                f"::warning::{artifact.repository}@{artifact.digest}: HTTP {error.code}"
            )
    emptied = 0
    for repository in sorted({artifact.repository for artifact in plan.delete}):
        try:
            emptied += harbor.delete_repository_if_empty(repository)
        except urllib.error.HTTPError as error:
            print(f"::warning::repository {repository}: HTTP {error.code}")
    print(
        f"Deleted {len(plan.delete) - failures} artifacts and {emptied} empty "
        f"repositories; {failures} deletions failed"
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
