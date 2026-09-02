"""Detect release PR container-test targets from a pull request diff."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

class ReleaseChangeError(RuntimeError):
    """Raised when release metadata is mixed with unrelated PR changes."""


@dataclass(frozen=True)
class ReleaseEntry:
    name: str
    recipe: str
    version: str
    file: str

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "recipe": self.recipe,
            "version": self.version,
            "file": self.file,
        }


@dataclass(frozen=True)
class DetectionResult:
    entries: tuple[ReleaseEntry, ...]
    skipped_new_recipe_tests: tuple[str, ...]

    @property
    def has_changes(self) -> bool:
        return bool(self.entries)

    def matrix(self) -> list[dict[str, str]]:
        return [entry.as_dict() for entry in self.entries]


RELEASE_PATTERN = re.compile(r"^releases/([^/]+)/([^/]+)\.json$")
TEST_CONFIG_PATTERN = re.compile(r"^recipes/([^/]+)/fulltest\.yaml$")
BUILD_RECIPE_PATTERN = re.compile(r"^recipes/([^/]+)/build\.yaml$")
BUILD_DATE_PATTERN = re.compile(r"^\d{8}$")
RECIPE_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


def _relative_posix(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _release_sort_key(version: str, build_date: str, *, prefer_x86_64: bool) -> tuple[int, str, str]:
    architecture_priority = 1
    if prefer_x86_64 and version.endswith("-arm64"):
        architecture_priority = 0
    return architecture_priority, build_date, version


def _release_build_date(release_file: Path) -> str:
    try:
        data = json.loads(release_file.read_text(encoding="utf-8"))
        apps = data.get("apps", {}) or {}
        if apps:
            first_value = next(iter(apps.values()))
            return str(first_value.get("version", "")).strip()
    except Exception:
        pass
    return ""


def _release_source_recipe(release_file: Path, fallback: str) -> str:
    try:
        data = json.loads(release_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback

    recipe = str(data.get("recipe", fallback)).strip()
    if not RECIPE_NAME_PATTERN.fullmatch(recipe):
        raise ReleaseChangeError(
            f"Invalid source recipe {recipe!r} in {release_file.as_posix()}"
        )
    return recipe


def find_latest_release_file(
    release_dir: str | Path,
    *,
    prefer_x86_64: bool = False,
) -> tuple[Path | None, str | None]:
    release_path = Path(release_dir)
    if not release_path.is_dir():
        return None, None

    latest_path: Path | None = None
    latest_key: tuple[int, str, str] | None = None
    latest_version = ""

    for entry in sorted(release_path.iterdir()):
        if entry.suffix != ".json":
            continue

        candidate_version = entry.stem
        build_date = _release_build_date(entry)
        if not BUILD_DATE_PATTERN.match(build_date):
            continue

        if latest_path is None:
            latest_path = entry
            latest_key = _release_sort_key(
                candidate_version,
                build_date,
                prefer_x86_64=prefer_x86_64,
            )
            latest_version = candidate_version
            continue

        candidate_key = _release_sort_key(
            candidate_version,
            build_date,
            prefer_x86_64=prefer_x86_64,
        )
        if latest_key is None or candidate_key > latest_key:
            latest_path = entry
            latest_key = candidate_key
            latest_version = candidate_version

    if latest_path is None:
        return None, None

    return latest_path, latest_version


def detect_release_pr_changes(
    changed_files: Iterable[str],
    *,
    repo_root: str | Path = ".",
) -> DetectionResult:
    """Return released container test targets for changed release/test files.

    A new recipe can add ``recipes/<name>/fulltest.yaml`` before release
    metadata exists. That should not fail this workflow because there is no
    published container for the release-test job to download yet.
    """

    root = Path(repo_root)
    paths = [line.strip() for line in changed_files if line.strip()]

    release_files = [path for path in paths if RELEASE_PATTERN.match(path)]
    unrelated_to_release = [
        path
        for path in paths
        if not RELEASE_PATTERN.match(path) and not TEST_CONFIG_PATTERN.match(path)
    ]

    if release_files and unrelated_to_release:
        raise ReleaseChangeError(
            "Release metadata changes must be isolated from unrelated files. "
            "Move the release JSON change to its own generated release PR or "
            "remove it from this branch.\n"
            f"Release files: {', '.join(release_files)}\n"
            f"Unrelated files: {', '.join(unrelated_to_release)}"
        )

    entries: dict[str, ReleaseEntry] = {}
    for path in paths:
        match = RELEASE_PATTERN.match(path)
        if not match:
            continue

        container, version = match.groups()
        source_recipe = _release_source_recipe(root / path, container)
        entries[container] = ReleaseEntry(
            name=container,
            recipe=source_recipe,
            version=version,
            file=path,
        )

    candidate_recipes = {
        match.group(1)
        for path in paths
        if (match := BUILD_RECIPE_PATTERN.match(path))
    }
    skipped_new_recipe_tests: list[str] = []
    skipped_seen: set[str] = set()
    for path in paths:
        match = TEST_CONFIG_PATTERN.match(path)
        if not match:
            continue

        recipe = match.group(1)
        if any(entry.recipe == recipe for entry in entries.values()):
            continue
        # A build.yaml change is tested against the exact newly built candidate
        # by PR container candidate. Retesting the previous published image here
        # is both misleading and creates extra per-container PR comments.
        if recipe in candidate_recipes:
            continue

        release_file, version = find_latest_release_file(
            root / "releases" / recipe,
            prefer_x86_64=True,
        )
        if release_file and version:
            entries[recipe] = ReleaseEntry(
                name=recipe,
                recipe=_release_source_recipe(release_file, recipe),
                version=version,
                file=_relative_posix(release_file, root),
            )
        elif recipe not in skipped_seen:
            skipped_new_recipe_tests.append(recipe)
            skipped_seen.add(recipe)

    return DetectionResult(
        entries=tuple(sorted(entries.values(), key=lambda item: item.name)),
        skipped_new_recipe_tests=tuple(sorted(skipped_new_recipe_tests)),
    )


def get_changed_files(base_ref: str, head_ref: str, *, repo_root: str | Path = ".") -> list[str]:
    proc = subprocess.run(
        # --diff-filter=d drops deletions. A retired release (its JSON removed)
        # has no container left to download, so queueing a test for it would
        # fail on the missing metadata file the matrix leg tries to read.
        ["git", "diff", "--name-only", "--diff-filter=d", f"{base_ref}...{head_ref}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        message = proc.stderr.strip() or "git diff failed"
        raise ReleaseChangeError(message)

    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def write_github_outputs(result: DetectionResult, output_path: str | Path) -> None:
    with Path(output_path).open("a", encoding="utf-8") as handle:
        handle.write(f"has-changes={'true' if result.has_changes else 'false'}\n")
        handle.write(f"modified-releases={json.dumps(result.matrix())}\n")


def _escape_notice(text: str) -> str:
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def print_skipped_notices(result: DetectionResult) -> None:
    for recipe in result.skipped_new_recipe_tests:
        message = (
            f"recipes/{recipe}/fulltest.yaml changed, but "
            f"releases/{recipe}/*.json does not exist. "
            "Skipping release-container tests for this new or unreleased recipe; "
            "this workflow only retests already released containers. "
            "Let the release workflow generate metadata first."
        )
        print(f"::notice::{_escape_notice(message)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", default="HEAD")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args(argv)

    try:
        changed_files = get_changed_files(args.base_ref, args.head_ref, repo_root=args.repo_root)
        result = detect_release_pr_changes(changed_files, repo_root=args.repo_root)
        if args.output:
            write_github_outputs(result, args.output)
        else:
            print(f"has-changes={'true' if result.has_changes else 'false'}")
            print(f"modified-releases={json.dumps(result.matrix())}")

        print_skipped_notices(result)
        if result.has_changes:
            names = ", ".join(entry.name for entry in result.entries)
            print(f"Release-container tests selected for: {names}")
        else:
            print("No release-container tests selected.")
    except ReleaseChangeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
