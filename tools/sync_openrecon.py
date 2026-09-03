#!/usr/bin/env python3
"""Synchronize released Neurocontainers metadata into an OpenRecon PR."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

SCRIPT_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_REPO_ROOT))

import yaml

from builder.variants import concrete_variant_specs

RECIPE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
VERSION_ASSIGNMENT_PATTERN = re.compile(
    r"^(?P<prefix>[ \t]*(?:export[ \t]+)?(?:version|VERSION)=).*$",
    re.MULTILINE,
)
OPENRECON_VERSION_ASSIGNMENT_PATTERN = re.compile(
    r"^(?P<prefix>[ \t]*(?:export[ \t]+)?openrecon_version=).*$",
    re.MULTILINE,
)
OPENRECON_SEMVER_PATTERN = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
TWO_PART_VERSION_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)$")


@dataclass(frozen=True)
class PreparedSync:
    """Files and PR notes produced while preparing one OpenRecon recipe."""

    paths: tuple[str, ...]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class OpenReconTarget:
    source_recipe: str
    variant: str
    container: str
    label: Path


def validate_recipe(recipe: str) -> str:
    """Return a safe recipe name or reject path and branch injection."""
    if not RECIPE_PATTERN.fullmatch(recipe):
        raise ValueError(f"Invalid recipe name: {recipe!r}")
    return recipe


def validate_version(version: str) -> str:
    """Return a safe release version or reject branch and shell metacharacters."""
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError(f"Invalid version: {version!r}")
    return version


def openrecon_version(version: str) -> str:
    """Return a schema-valid OpenRecon version for a container release."""
    validate_version(version)
    if OPENRECON_SEMVER_PATTERN.fullmatch(version):
        return version
    if TWO_PART_VERSION_PATTERN.fullmatch(version):
        return f"{version}.0"
    raise ValueError(
        f"OpenRecon requires a semantic version; cannot normalize {version!r}"
    )


def resolve_openrecon_target(
    source_root: Path,
    recipe: str,
    variant: str = "",
) -> OpenReconTarget:
    recipe = validate_recipe(recipe)
    if variant:
        validate_recipe(variant)

    source_recipe = source_root / "recipes" / recipe
    container = recipe
    if variant:
        build_file = source_recipe / "build.yaml"
        if not build_file.is_file():
            raise ValueError(f"Recipe {recipe!r} has no build.yaml")
        recipe_data = yaml.safe_load(build_file.read_text(encoding="utf-8"))
        matches = [
            spec
            for spec in concrete_variant_specs(recipe_data)
            if spec["recipe_variant"] == variant and spec["architecture"] == "x86_64"
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Recipe {recipe!r} has no unique x86_64 variant {variant!r}"
            )
        container = str(matches[0]["name"])

    label_name = (
        "OpenReconLabel.json" if not variant else f"OpenReconLabel.{variant}.json"
    )
    return OpenReconTarget(
        source_recipe=recipe,
        variant=variant,
        container=container,
        label=source_recipe / label_name,
    )


def update_params_version(contents: str, version: str) -> str:
    """Update the first supported version assignment in params.sh."""
    validate_version(version)
    if not VERSION_ASSIGNMENT_PATTERN.search(contents):
        raise RuntimeError("params.sh has no supported version assignment")
    updated = VERSION_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{version}",
        contents,
        count=1,
    )
    metadata_version = openrecon_version(version)
    if metadata_version == version:
        return OPENRECON_VERSION_ASSIGNMENT_PATTERN.sub("", updated, count=1)
    if OPENRECON_VERSION_ASSIGNMENT_PATTERN.search(updated):
        return OPENRECON_VERSION_ASSIGNMENT_PATTERN.sub(
            lambda match: f"{match.group('prefix')}{metadata_version}",
            updated,
            count=1,
        )

    version_assignment = VERSION_ASSIGNMENT_PATTERN.search(updated)
    assert version_assignment is not None
    insertion = f"\nexport openrecon_version={metadata_version}"
    return (
        updated[: version_assignment.end()]
        + insertion
        + updated[version_assignment.end() :]
    )


def prepare_recipe(
    source_root: Path,
    openrecon_root: Path,
    recipe: str,
    version: str,
    *,
    variant: str = "",
) -> PreparedSync | None:
    """Copy one recipe's OpenRecon metadata into an existing checkout."""
    version = validate_version(version)
    openrecon_target = resolve_openrecon_target(source_root, recipe, variant)
    source_recipe = source_root / "recipes" / openrecon_target.source_recipe
    if not openrecon_target.label.is_file():
        return None

    relative_recipe = Path("recipes") / openrecon_target.container
    target_recipe = openrecon_root / relative_recipe
    target_label = target_recipe / "OpenReconLabel.json"
    target_params = target_recipe / "params.sh"
    target_readme = target_recipe / "README.md"
    bootstrapped = not target_recipe.is_dir()
    target_recipe.mkdir(parents=True, exist_ok=True)

    notes: list[str] = []
    if bootstrapped:
        notes.append(
            f"- Create `{relative_recipe.as_posix()}` in OpenRecon because it did not exist yet"
        )

    if not target_params.is_file():
        target_params.write_text(
            "#!/bin/bash\n"
            "# Auto-generated by neurocontainers CI.\n"
            f"export toolName={openrecon_target.container}\n"
            f"export version={version}\n"
            "export baseDockerImage=vnmd/${toolName}_${version}\n",
            encoding="utf-8",
        )

    params = target_params.read_text(encoding="utf-8")
    updated_params = update_params_version(params, version)
    if updated_params != params:
        target_params.write_text(updated_params, encoding="utf-8")

    shutil.copyfile(openrecon_target.label, target_label)
    paths = [
        (relative_recipe / "OpenReconLabel.json").as_posix(),
        (relative_recipe / "params.sh").as_posix(),
    ]

    source_readme = source_recipe / "OpenReconREADME.md"
    if source_readme.is_file():
        shutil.copyfile(source_readme, target_readme)
        paths.append((relative_recipe / "README.md").as_posix())
        notes.append(
            f"- Copy `recipes/{openrecon_target.source_recipe}/OpenReconREADME.md` "
            "from neurocontainers "
            f"to `{relative_recipe.as_posix()}/README.md` for OpenRecon PDF generation"
        )

    return PreparedSync(paths=tuple(paths), notes=tuple(notes))


def run_command(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    capture_output: bool = False,
) -> str:
    """Run a subprocess and return stripped stdout when requested."""
    result = subprocess.run(
        list(command),
        cwd=cwd,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    return result.stdout.strip() if capture_output else ""


def existing_pull_request(repository: str, title: str) -> str | None:
    """Return the URL of an exact-title open PR, if one already exists."""
    output = run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            repository,
            "--state",
            "open",
            "--limit",
            "100",
            "--json",
            "title,url",
        ],
        capture_output=True,
    )
    pull_requests = json.loads(output)
    return next(
        (item["url"] for item in pull_requests if item.get("title") == title),
        None,
    )


def dispatch_openrecon_build(repository: str, recipe: str) -> None:
    """Build an unchanged OpenRecon recipe against the newly published image."""
    recipe = validate_recipe(recipe)
    applications = json.dumps([recipe], separators=(",", ":"))
    run_command(
        [
            "gh",
            "workflow",
            "run",
            "build-apps.yml",
            "--repo",
            repository,
            "--ref",
            "main",
            "-f",
            f"applications={applications}",
        ]
    )
    print(f"OpenRecon rebuild dispatched for unchanged {recipe} metadata.")


def sync_recipe(
    source_root: Path,
    recipe: str,
    version: str,
    repository: str,
    *,
    variant: str = "",
    dispatch_unchanged: bool = False,
) -> str | None:
    """Create or reuse the OpenRecon metadata PR for one released recipe."""
    version = validate_version(version)
    openrecon_target = resolve_openrecon_target(source_root, recipe, variant)
    container = openrecon_target.container
    if not openrecon_target.label.is_file():
        print(f"No OpenRecon label found for {container}; skipping OpenRecon PR.")
        return None

    title = f"Update {container} OpenRecon metadata to {version}"
    existing = existing_pull_request(repository, title)
    if existing:
        print(f"Open OpenRecon PR already exists for {container} {version}: {existing}")
        return existing

    with tempfile.TemporaryDirectory(prefix="openrecon-sync-") as temp_dir:
        openrecon_root = Path(temp_dir) / "openrecon"
        run_command(["gh", "repo", "clone", repository, str(openrecon_root)])
        run_command(["git", "config", "user.name", "neurocontainers-bot"], cwd=openrecon_root)
        run_command(
            [
                "git",
                "config",
                "user.email",
                "neurocontainers-bot@neurodesk.github.io",
            ],
            cwd=openrecon_root,
        )

        prepared = prepare_recipe(
            source_root,
            openrecon_root,
            recipe,
            version,
            variant=variant,
        )
        if prepared is None:
            return None
        status = run_command(
            ["git", "status", "--porcelain", "--", *prepared.paths],
            cwd=openrecon_root,
            capture_output=True,
        )
        if not status:
            print(f"No OpenRecon metadata changes detected for {container}.")
            if dispatch_unchanged:
                dispatch_openrecon_build(repository, container)
            else:
                print("Skipping OpenRecon PR and rebuild dispatch.")
            return None

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        branch = f"sync-openrecon/{container}-{version}-{timestamp}"
        run_command(["git", "switch", "-c", branch], cwd=openrecon_root)
        run_command(["git", "add", "--", *prepared.paths], cwd=openrecon_root)
        run_command(
            [
                "git",
                "commit",
                "-m",
                f"Update {container} OpenRecon metadata to {version}\n\n"
                "Auto-generated from neurocontainers CI.",
            ],
            cwd=openrecon_root,
        )
        run_command(["git", "push", "origin", branch], cwd=openrecon_root)

        changes = [
            f"- Update `recipes/{container}/OpenReconLabel.json` from neurocontainers",
            f"- Set `recipes/{container}/params.sh` version to `{version}`",
            *prepared.notes,
        ]
        body = (
            "## Summary\n\n"
            f"This PR updates OpenRecon metadata for **{container}** from the latest "
            "successful neurocontainers build.\n\n"
            "## Changes\n\n"
            + "\n".join(changes)
            + "\n\n🤖 Generated by neurocontainers CI"
        )
        url = run_command(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                repository,
                "--title",
                title,
                "--body",
                body,
                "--head",
                branch,
                "--base",
                "main",
            ],
            cwd=openrecon_root,
            capture_output=True,
        )
        print(f"OpenRecon pull request created for {container} {version}: {url}")
        return url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipe", required=True)
    parser.add_argument("--variant", default="")
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path.cwd(),
        help="Neurocontainers checkout (default: current directory)",
    )
    parser.add_argument(
        "--repository",
        default="neurodesk/openrecon",
        help="OpenRecon GitHub repository",
    )
    parser.add_argument(
        "--dispatch-unchanged",
        action="store_true",
        help=(
            "Dispatch build-apps.yml when metadata is unchanged so a rebuilt "
            "same-version container still propagates to OpenRecon"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sync_recipe(
        source_root=args.source_root.resolve(),
        recipe=args.recipe,
        version=args.version,
        repository=args.repository,
        variant=args.variant,
        dispatch_unchanged=args.dispatch_unchanged,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
