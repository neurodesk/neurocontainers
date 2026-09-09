#!/usr/bin/env python3
"""Bind unprivileged PR container candidates to trusted promotion."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

SCRIPT_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_REPO_ROOT))

from builder.release import release_data
from builder.release_plan import (
    ReleasePlan,
    plan_recipe_changes,
    recipe_names_from_paths,
)
from builder.variants import concrete_variant_specs

REPO_ROOT = SCRIPT_REPO_ROOT
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
BUILD_DATE_PATTERN = re.compile(r"^[0-9]{8}$")
RECIPE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
CANDIDATE_MANIFEST_FIELDS = (
    "recipe",
    "container",
    "variant",
    "architecture",
    "version",
    "build_date",
    "image_name",
    "candidate_tag",
    "docker_archive",
    "docker_sha256",
    "sif",
    "sif_sha256",
    "release_json",
    "pr_number",
    "head_sha",
    "recipe_fingerprint",
)
CANDIDATE_REPORT_SCHEMA_VERSION = 3
DIVE_STATUSES = {"success", "failure", "cancelled", "skipped", "unknown"}
LAUNCHER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/+:-]*$")
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
DIVE_EFFICIENCY_PATTERN = re.compile(
    r"\befficiency:\s*([0-9]+(?:\.[0-9]+)?)\s*%"
)
DIVE_WASTED_BYTES_PATTERN = re.compile(
    r"\bwastedBytes:\s*([0-9]+)\s+bytes\s+\(([^)]+)\)"
)
DIVE_USER_WASTED_PATTERN = re.compile(
    r"\buserWastedPercent:\s*([0-9]+(?:\.[0-9]+)?)\s*%"
)
DIVE_PERCENT_FAILURE_PATTERN = re.compile(
    r"FAIL:\s*(highestUserWastedPercent|lowestEfficiency):.*?"
    r"\([^=]+=([0-9]+(?:\.[0-9]+)?)\s*[<>]\s*"
    r"threshold=([0-9]+(?:\.[0-9]+)?)\)"
)
DIVE_INEFFICIENT_FILE_PATTERN = re.compile(
    r"^\s*(\d+)\s+([0-9]+(?:\.[0-9]+)?\s+[kKMGT]?B)\s+(/\S.*)$"
)


def run_git(*args: str) -> str:
    """Run Git in the selected repository and return stripped stdout."""
    command = ["git", *args]
    try:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "").strip() or "no stderr output"
        raise RuntimeError(
            f"Git command failed: {shlex.join(command)}: {stderr}"
        ) from error
    return result.stdout.strip()


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of a file without loading it into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def recipe_fingerprint(recipe: str) -> str:
    """Hash every file and relative path in a recipe directory."""
    recipe_dir = REPO_ROOT / "recipes" / recipe
    if not (recipe_dir / "build.yaml").is_file():
        raise RuntimeError(f"Missing recipe: {recipe_dir / 'build.yaml'}")

    digest = hashlib.sha256()
    for path in sorted(item for item in recipe_dir.rglob("*") if item.is_file()):
        relative = path.relative_to(recipe_dir).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def changed_files(base: str, head: str) -> list[str]:
    """List paths changed between a base commit and PR head."""
    output = run_git("diff", "--name-only", f"{base}...{head}")
    return [line for line in output.splitlines() if line]


def load_recipe_at(revision: str, recipe: str) -> dict[str, Any] | None:
    """Load build.yaml from a Git tree without rendering PR-authored templates."""
    relative = f"recipes/{recipe}/build.yaml"
    present = run_git("ls-tree", "--name-only", revision, "--", relative)
    if present != relative:
        return None
    try:
        data = yaml.safe_load(run_git("show", f"{revision}:{relative}"))
    except yaml.YAMLError as error:
        raise RuntimeError(
            f"Unable to parse {relative} at {revision}: {error}"
        ) from error
    if not isinstance(data, dict):
        raise RuntimeError(f"Recipe {relative} at {revision} must be a YAML mapping")
    return data


def release_plan(base: str, head: str) -> ReleasePlan:
    """Plan recipe work from Git trees using trusted, non-rendering policy."""
    paths = changed_files(base, head)
    names = recipe_names_from_paths(paths)
    plan = plan_recipe_changes(
        paths,
        {recipe: load_recipe_at(base, recipe) for recipe in names},
        {recipe: load_recipe_at(head, recipe) for recipe in names},
    )
    if not plan.candidate_recipes:
        return plan

    allowed = tuple(f"recipes/{recipe}/" for recipe in plan.changed_recipes)
    unrelated = [path for path in paths if not path.startswith(allowed)]
    if unrelated:
        raise RuntimeError(
            "Automated releases require a recipe-only PR. Unrelated paths: "
            + ", ".join(unrelated)
        )
    for recipe in plan.candidate_recipes:
        if not (REPO_ROOT / "recipes" / recipe / "fulltest.yaml").is_file():
            raise RuntimeError(f"recipes/{recipe}/fulltest.yaml is required")
    return plan


def detect_recipes(base: str, head: str) -> list[str]:
    """Return only recipes whose planned change requires a candidate build."""
    return release_plan(base, head).candidate_recipes


def build_date(recipe: str, revision: str = "HEAD") -> str:
    """Return the last build.yaml commit date in release-tag format."""
    value = run_git(
        "log",
        "-1",
        "--format=%ad",
        "--date=format:%Y%m%d",
        revision,
        "--",
        f"recipes/{recipe}/build.yaml",
    )
    if not value:
        raise RuntimeError(f"Could not determine build date for {recipe}")
    return value


def load_recipe(recipe: str) -> dict[str, Any]:
    """Load a recipe build file and require a mapping at its root."""
    path = REPO_ROOT / "recipes" / recipe / "build.yaml"
    try:
        contents = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise RuntimeError(f"Unable to read recipe YAML {path}: {error}") from error
    try:
        data = yaml.safe_load(contents)
    except yaml.YAMLError as error:
        raise RuntimeError(f"Unable to parse recipe YAML {path}: {error}") from error
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid recipe YAML: {path}")
    return data


def resolve_variant(recipe: str, variant: str) -> dict[str, Any]:
    """Return the declared concrete identity for a recipe variant selector.

    The selector reaches promotion inside an untrusted candidate manifest, so it
    is always resolved against the merged recipe rather than trusted as a name.
    """
    data = load_recipe(recipe)
    for spec in concrete_variant_specs(data):
        if str(spec["variant"]) == variant:
            return spec
    declared = ", ".join(str(spec["variant"]) or "default" for spec in concrete_variant_specs(data))
    raise RuntimeError(
        f"Recipe {recipe} does not declare variant {variant or 'default'!r}; declared: {declared}"
    )


def inspect_recipe(
    recipe: str,
    head_sha: str,
    variant: str = "",
    candidate_build_date: str | None = None,
) -> dict[str, str]:
    """Derive safe candidate names and release identifiers for a recipe variant."""
    data = load_recipe(recipe)
    if "version" not in data:
        raise RuntimeError(f"Recipe {recipe} build.yaml is missing a version field")
    version = str(data["version"])
    if not VERSION_PATTERN.fullmatch(version):
        raise RuntimeError(
            f"Recipe {recipe} has an invalid version {version!r}; "
            "use only letters, numbers, dots, underscores, and hyphens"
        )
    spec = resolve_variant(recipe, variant)
    container = validate_recipe_identifier(str(spec["name"]))
    date = (
        candidate_build_date
        if candidate_build_date is not None
        else build_date(recipe, head_sha)
    )
    if not isinstance(date, str) or not BUILD_DATE_PATTERN.fullmatch(date):
        raise RuntimeError(f"Invalid build date for {container}: {date!r}")
    image_name = f"{container}_{version}"
    return {
        "recipe": recipe,
        "container": container,
        "variant": variant,
        "architecture": str(spec["architecture"]),
        "version": version,
        "build_date": date,
        "image_name": image_name,
        "candidate_tag": f"nd-candidate-{container}:{head_sha[:12]}",
        "docker_archive": f"{image_name}_{date}.docker.tar",
        "sif": f"{image_name}_{date}.simg",
    }


def write_output(values: dict[str, str]) -> None:
    """Write values as GitHub step outputs or print them for local use."""
    output = os.environ.get("GITHUB_OUTPUT")
    if not output:
        for key, value in values.items():
            print(f"{key}={value}")
        return
    with Path(output).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def detect_targets(recipes: list[str]) -> list[dict[str, str]]:
    """Expand changed recipes into the concrete containers they now declare."""
    targets = []
    for recipe in recipes:
        data = load_recipe(recipe)
        version = str(data.get("version", ""))
        if not VERSION_PATTERN.fullmatch(version):
            raise RuntimeError(f"Recipe {recipe} has an invalid version {version!r}")
        for spec in concrete_variant_specs(data):
            targets.append(
                {
                    "recipe": recipe,
                    "variant": str(spec["variant"]),
                    "architecture": str(spec["architecture"]),
                    "version": version,
                    # The identity becomes an artifact name and a build path, so
                    # it is checked here rather than only at promotion time.
                    "container": validate_recipe_identifier(str(spec["name"])),
                }
            )
    return targets


def command_detect(args: argparse.Namespace) -> None:
    """Implement the detect CLI command."""
    plan = release_plan(args.base, args.head)
    recipes = plan.candidate_recipes
    write_output(
        {
            "recipes": json.dumps(recipes),
            "targets": json.dumps(detect_targets(recipes)),
            "changed_recipes": json.dumps(plan.changed_recipes),
            "source_only_recipes": json.dumps(plan.source_only_recipes),
            "release_plan": json.dumps(plan.as_dict(), separators=(",", ":")),
        }
    )


def command_inspect(args: argparse.Namespace) -> None:
    """Implement the inspect CLI command."""
    write_output(inspect_recipe(args.recipe, args.head_sha, args.variant))


def command_manifest(args: argparse.Namespace) -> None:
    """Create the release preview and provenance manifest for a candidate."""
    info = inspect_recipe(args.recipe, args.head_sha, args.variant)
    candidate_dir = Path(args.candidate_dir)
    docker_archive = candidate_dir / info["docker_archive"]
    sif = candidate_dir / info["sif"]
    if not docker_archive.is_file() or not sif.is_file():
        raise RuntimeError("Candidate Docker archive or SIF is missing")

    recipe = load_recipe(args.recipe)
    metadata = release_data(
        info["container"],
        info["version"],
        recipe,
        info["build_date"],
        info["architecture"],
        info["variant"],
        source_recipe=args.recipe,
    )
    release_path = candidate_dir / f"{info['version']}.json"
    release_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    manifest = {
        **info,
        "pr_number": args.pr_number,
        "head_sha": args.head_sha,
        "recipe_fingerprint": recipe_fingerprint(args.recipe),
        "docker_sha256": sha256_file(docker_archive),
        "sif_sha256": sha256_file(sif),
        "release_json": release_path.name,
    }
    (candidate_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def _result_count(data: dict[str, Any], field: str) -> int:
    """Read a non-negative integer count from test result JSON."""
    value = data.get(field, 0)
    if isinstance(value, bool):
        raise RuntimeError(f"Invalid candidate test result {field}: {value!r}")
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise RuntimeError(
            f"Invalid candidate test result {field}: {value!r}"
        ) from error
    if result < 0:
        raise RuntimeError(f"Invalid candidate test result {field}: {value!r}")
    return result


def build_dive_summary(report_path: Path, status: str) -> dict[str, Any]:
    """Extract bounded, data-only Dive findings for the trusted PR reporter."""
    if status not in DIVE_STATUSES:
        raise RuntimeError(f"Invalid Dive status: {status!r}")

    summary: dict[str, Any] = {
        "status": status,
        "failed_checks": [],
        "inefficient_files": [],
    }
    if not report_path.is_file():
        return summary

    try:
        text = report_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise RuntimeError(
            f"Unable to read Dive report {report_path}: {error}"
        ) from error
    text = ANSI_ESCAPE_PATTERN.sub("", text)

    if match := DIVE_EFFICIENCY_PATTERN.search(text):
        summary["efficiency_percent"] = float(match.group(1))
    if match := DIVE_WASTED_BYTES_PATTERN.search(text):
        summary["wasted_bytes"] = int(match.group(1))
        summary["wasted_bytes_display"] = match.group(2).strip()
    if match := DIVE_USER_WASTED_PATTERN.search(text):
        summary["user_wasted_percent"] = float(match.group(1))

    for match in DIVE_PERCENT_FAILURE_PATTERN.finditer(text):
        summary["failed_checks"].append(
            {
                "name": match.group(1),
                "actual_percent": float(match.group(2)) * 100,
                "threshold_percent": float(match.group(3)) * 100,
            }
        )

    for line in text.splitlines():
        match = DIVE_INEFFICIENT_FILE_PATTERN.match(line)
        if not match:
            continue
        path = match.group(3).strip()
        if len(path) > 512 or any(ord(character) < 32 for character in path):
            continue
        summary["inefficient_files"].append(
            {
                "count": int(match.group(1)),
                "wasted_space": match.group(2),
                "path": path,
            }
        )
        if len(summary["inefficient_files"]) == 5:
            break

    return summary


def build_candidate_report(
    candidate_dir: Path, *, dive_status: str = "unknown"
) -> dict[str, Any]:
    """Build the compact, data-only report consumed by the trusted PR reporter."""
    if dive_status not in DIVE_STATUSES:
        raise RuntimeError(f"Invalid Dive status: {dive_status!r}")
    manifest = load_candidate_manifest(candidate_dir)
    results_path = candidate_dir / "test-results.json"
    try:
        results = json.loads(results_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"Unable to read candidate test results {results_path}: {error}"
        ) from error
    if not isinstance(results, dict):
        raise RuntimeError(f"Candidate test results must be a JSON object: {results_path}")

    total = _result_count(results, "total_tests")
    passed = _result_count(results, "passed")
    failed = _result_count(results, "failed")
    skipped = _result_count(results, "skipped")
    if passed + failed + skipped != total:
        raise RuntimeError(
            "Candidate test result counts do not add up: "
            f"passed {passed}, failed {failed}, skipped {skipped}, total {total}"
        )

    fulltest = results.get("fulltest_summary", {}) or {}
    if not isinstance(fulltest, dict):
        raise RuntimeError("Candidate fulltest_summary must be a JSON object")

    failed_tests = []
    passed_tests = []
    test_results = results.get("test_results", []) or []
    if not isinstance(test_results, list):
        raise RuntimeError("Candidate test_results must be a JSON array")
    for test in test_results:
        if not isinstance(test, dict):
            continue
        name = str(test.get("name", "unnamed"))[:200]
        if test.get("status") == "failed":
            failed_tests.append(name)
        elif test.get("status") == "passed":
            passed_tests.append(name)

    recipe = load_recipe(validate_recipe_identifier(manifest["recipe"]))
    deploy = recipe.get("deploy") or {}
    bins = deploy.get("bins") if isinstance(deploy, dict) else None
    launcher = None
    if isinstance(bins, list):
        launcher = next(
            (
                item
                for item in bins
                if isinstance(item, str) and LAUNCHER_PATTERN.fullmatch(item)
            ),
            None,
        )

    return {
        "schema_version": CANDIDATE_REPORT_SCHEMA_VERSION,
        "recipe": manifest["recipe"],
        "container": manifest["container"],
        "variant": manifest["variant"],
        "architecture": manifest["architecture"],
        "version": manifest["version"],
        "build_date": manifest["build_date"],
        "docker_archive": manifest["docker_archive"],
        "sif": manifest["sif"],
        "candidate_tag": manifest["candidate_tag"],
        "launcher": launcher,
        "pr_number": manifest["pr_number"],
        "head_sha": manifest["head_sha"],
        "tests": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "fulltest_total": _result_count(fulltest, "total_tests"),
            "fulltest_passed": _result_count(fulltest, "tests_passed"),
            "fulltest_failed": _result_count(fulltest, "tests_failed"),
            "suites_total": _result_count(fulltest, "total_suites"),
            "suites_passed": _result_count(fulltest, "suites_passed"),
            "passed_tests": passed_tests[:20],
            "failed_tests": failed_tests,
        },
        "dive": build_dive_summary(candidate_dir / "dive-report.txt", dive_status),
    }


def command_report(args: argparse.Namespace) -> None:
    """Write a compact candidate report without copying test logs into a PR comment."""
    candidate_dir = Path(args.candidate_dir)
    report = build_candidate_report(candidate_dir, dive_status=args.dive_status)
    (candidate_dir / "candidate-report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )


def validate_recipe_identifier(recipe: Any) -> str:
    """Return a safe recipe identifier or reject manifest path injection."""
    if not isinstance(recipe, str) or not RECIPE_PATTERN.fullmatch(recipe):
        raise RuntimeError(f"Invalid candidate recipe identifier: {recipe!r}")
    return recipe


def candidate_file(candidate_dir: Path, value: Any, field: str) -> Path:
    """Resolve a manifest filename while confining it to the candidate directory."""
    if not isinstance(value, str) or not value or "\\" in value:
        raise RuntimeError(f"Invalid candidate {field}: {value!r}")
    relative = Path(value)
    if relative.is_absolute() or relative.name != value:
        raise RuntimeError(f"Invalid candidate {field}: {value!r}")
    root = candidate_dir.resolve()
    path = (root / relative).resolve()
    if path.parent != root:
        raise RuntimeError(f"Candidate {field} escapes {candidate_dir}: {value!r}")
    return path


def load_candidate_manifest(candidate_dir: Path) -> dict[str, Any]:
    """Load a candidate manifest and require its complete object schema."""
    path = candidate_dir / "manifest.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Unable to read candidate manifest {path}: {error}") from error
    if not isinstance(manifest, dict):
        raise RuntimeError(f"Candidate manifest must be a JSON object: {path}")
    missing = [field for field in CANDIDATE_MANIFEST_FIELDS if field not in manifest]
    if missing:
        raise RuntimeError(
            f"Candidate manifest {path} is missing fields: {', '.join(missing)}"
        )
    return manifest


def verify_candidate(
    candidate_dir: Path, expected_head_sha: str, expected_pr_number: int | None = None
) -> dict[str, Any]:
    """Verify a candidate against its PR identity and the merged recipe."""
    manifest = load_candidate_manifest(candidate_dir)
    recipe = validate_recipe_identifier(manifest.get("recipe"))
    variant = manifest.get("variant")
    if not isinstance(variant, str):
        raise RuntimeError(f"Invalid candidate variant for {recipe}: {variant!r}")
    # Resolving the selector against the merged recipe is what stops a candidate
    # from inventing a container identity and promoting itself into an
    # unrelated releases/ directory.
    # A squash merge deliberately leaves the PR head outside main's history.
    # The candidate job derived this date from that exact head before packaging;
    # promotion binds the manifest back to the event head, PR, merged recipe
    # fingerprint, artifact names, release JSON, and artifact checksums below.
    # Reusing the validated candidate date avoids fetching PR-authored Git
    # objects into this privileged pull_request_target job.
    expected_info = inspect_recipe(
        recipe,
        expected_head_sha,
        variant,
        manifest.get("build_date"),
    )
    container = expected_info["container"]
    if candidate_dir.name != container:
        raise RuntimeError(f"Candidate directory does not match container {container}")
    if manifest.get("container") != container:
        raise RuntimeError(f"Candidate container mismatch for {recipe}")
    if manifest["head_sha"] != expected_head_sha:
        raise RuntimeError(f"Candidate head SHA mismatch for {container}")
    if expected_pr_number is not None and manifest["pr_number"] != expected_pr_number:
        raise RuntimeError(f"Candidate PR number mismatch for {container}")
    if manifest["recipe_fingerprint"] != recipe_fingerprint(recipe):
        raise RuntimeError(f"Merged recipe differs from tested candidate: {container}")

    expected_release_json = f"{expected_info['version']}.json"
    paths = {
        "docker_archive": candidate_file(
            candidate_dir, manifest.get("docker_archive"), "docker archive"
        ),
        "sif": candidate_file(candidate_dir, manifest.get("sif"), "SIF"),
        "release_json": candidate_file(
            candidate_dir, manifest.get("release_json"), "release JSON"
        ),
    }
    expected_values = {
        "container": container,
        "variant": variant,
        "architecture": expected_info["architecture"],
        "version": expected_info["version"],
        "build_date": expected_info["build_date"],
        "image_name": expected_info["image_name"],
        "candidate_tag": expected_info["candidate_tag"],
        "docker_archive": expected_info["docker_archive"],
        "sif": expected_info["sif"],
        "release_json": expected_release_json,
    }
    for field, expected in expected_values.items():
        if manifest.get(field) != expected:
            raise RuntimeError(
                f"Candidate {field} mismatch for {container}: "
                f"expected {expected!r}, got {manifest.get(field)!r}"
            )
    for filename_key, digest_key in (
        ("docker_archive", "docker_sha256"),
        ("sif", "sif_sha256"),
    ):
        path = paths[filename_key]
        if not path.is_file() or sha256_file(path) != manifest[digest_key]:
            raise RuntimeError(f"Checksum mismatch: {path}")

    expected_release = release_data(
        container,
        expected_info["version"],
        load_recipe(recipe),
        expected_info["build_date"],
        expected_info["architecture"],
        variant,
        source_recipe=recipe,
    )
    actual_release = json.loads(paths["release_json"].read_text(encoding="utf-8"))
    if actual_release != expected_release:
        raise RuntimeError(f"Release JSON mismatch for {container}")
    return {**manifest, **expected_values, "recipe": recipe}


def command_verify(args: argparse.Namespace) -> None:
    """Verify all candidate directories and write their trusted manifests."""
    manifests = [
        verify_candidate(path.parent, args.head_sha, args.pr_number)
        for path in sorted(Path(args.bundle).glob("*/manifest.json"))
    ]
    if not manifests:
        raise RuntimeError(f"No candidate manifests found under {args.bundle}")
    Path(args.output).write_text(
        json.dumps(manifests, indent=2) + "\n", encoding="utf-8"
    )


def verify_published_metadata(
    bundle: Path,
    manifests_path: Path,
    expected_head_sha: str,
    expected_pr_number: int,
) -> list[dict[str, Any]]:
    """Revalidate small promotion metadata after large artifacts are published."""
    try:
        manifests = json.loads(manifests_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"Unable to read published manifests {manifests_path}: {error}"
        ) from error
    if not isinstance(manifests, list) or not manifests:
        raise RuntimeError("Published manifests must be a non-empty JSON array")

    containers: set[str] = set()
    for manifest in manifests:
        if not isinstance(manifest, dict):
            raise RuntimeError("Each published manifest must be a JSON object")
        missing = [
            field for field in CANDIDATE_MANIFEST_FIELDS if field not in manifest
        ]
        if missing:
            raise RuntimeError(
                "Published manifest is missing fields: " + ", ".join(missing)
            )

        recipe = validate_recipe_identifier(manifest.get("recipe"))
        container = validate_recipe_identifier(manifest.get("container"))
        if container in containers:
            raise RuntimeError(f"Duplicate published container manifest: {container}")
        containers.add(container)

        variant = manifest.get("variant")
        if not isinstance(variant, str):
            raise RuntimeError(f"Invalid candidate variant for {recipe}: {variant!r}")
        expected_info = inspect_recipe(
            recipe,
            expected_head_sha,
            variant,
            manifest.get("build_date"),
        )
        if manifest.get("head_sha") != expected_head_sha:
            raise RuntimeError(f"Published head SHA mismatch for {container}")
        if manifest.get("pr_number") != expected_pr_number:
            raise RuntimeError(f"Published PR number mismatch for {container}")
        if manifest.get("recipe_fingerprint") != recipe_fingerprint(recipe):
            raise RuntimeError(
                f"Merged recipe differs from published candidate: {container}"
            )

        expected_release_json = f"{expected_info['version']}.json"
        expected_values = {
            "recipe": recipe,
            "container": expected_info["container"],
            "variant": variant,
            "architecture": expected_info["architecture"],
            "version": expected_info["version"],
            "build_date": expected_info["build_date"],
            "image_name": expected_info["image_name"],
            "candidate_tag": expected_info["candidate_tag"],
            "docker_archive": expected_info["docker_archive"],
            "sif": expected_info["sif"],
            "release_json": expected_release_json,
        }
        for field, expected in expected_values.items():
            if manifest.get(field) != expected:
                raise RuntimeError(
                    f"Published {field} mismatch for {container}: "
                    f"expected {expected!r}, got {manifest.get(field)!r}"
                )

        release_path = candidate_file(
            bundle / container,
            manifest.get("release_json"),
            "release JSON",
        )
        try:
            actual_release = json.loads(release_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"Unable to read published release JSON {release_path}: {error}"
            ) from error
        expected_release = release_data(
            container,
            expected_info["version"],
            load_recipe(recipe),
            expected_info["build_date"],
            expected_info["architecture"],
            variant,
            source_recipe=recipe,
        )
        if actual_release != expected_release:
            raise RuntimeError(f"Published release JSON mismatch for {container}")
    return manifests


def command_verify_metadata(args: argparse.Namespace) -> None:
    """Verify staged metadata without downloading Docker or SIF artifacts."""
    verify_published_metadata(
        Path(args.bundle),
        Path(args.manifests),
        args.head_sha,
        args.pr_number,
    )


def command_materialize(args: argparse.Namespace) -> None:
    """Copy verified release previews into the repository release tree."""
    bundle = Path(args.bundle)
    manifests = json.loads(Path(args.manifests).read_text(encoding="utf-8"))
    for manifest in manifests:
        recipe = validate_recipe_identifier(manifest.get("recipe"))
        container = validate_recipe_identifier(manifest.get("container"))
        version = manifest.get("version")
        if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
            raise RuntimeError(f"Invalid verified version for {container}: {version!r}")
        expected_release_json = f"{version}.json"
        if manifest.get("release_json") != expected_release_json:
            raise RuntimeError(f"Invalid verified release JSON for {container}")
        source = candidate_file(
            bundle / container, manifest["release_json"], "release JSON"
        )
        destination = REPO_ROOT / "releases" / container / expected_release_json
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    result = argparse.ArgumentParser()
    result.add_argument("--repo-root", default=str(REPO_ROOT))
    subparsers = result.add_subparsers(dest="command", required=True)

    detect = subparsers.add_parser("detect")
    detect.add_argument("--base", required=True)
    detect.add_argument("--head", required=True)
    detect.set_defaults(func=command_detect)

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--recipe", required=True)
    inspect.add_argument("--head-sha", required=True)
    inspect.add_argument("--variant", default="")
    inspect.set_defaults(func=command_inspect)

    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("--recipe", required=True)
    manifest.add_argument("--head-sha", required=True)
    manifest.add_argument("--variant", default="")
    manifest.add_argument("--pr-number", required=True, type=int)
    manifest.add_argument("--candidate-dir", required=True)
    manifest.set_defaults(func=command_manifest)

    report = subparsers.add_parser("report")
    report.add_argument("--candidate-dir", required=True)
    report.add_argument(
        "--dive-status", choices=sorted(DIVE_STATUSES), default="unknown"
    )
    report.set_defaults(func=command_report)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--bundle", required=True)
    verify.add_argument("--head-sha", required=True)
    verify.add_argument("--pr-number", required=True, type=int)
    verify.add_argument("--output", required=True)
    verify.set_defaults(func=command_verify)

    verify_metadata = subparsers.add_parser("verify-metadata")
    verify_metadata.add_argument("--bundle", required=True)
    verify_metadata.add_argument("--manifests", required=True)
    verify_metadata.add_argument("--head-sha", required=True)
    verify_metadata.add_argument("--pr-number", required=True, type=int)
    verify_metadata.set_defaults(func=command_verify_metadata)

    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("--bundle", required=True)
    materialize.add_argument("--manifests", required=True)
    materialize.set_defaults(func=command_materialize)
    return result


def main() -> int:
    """Run the selected command against the requested repository root."""
    global REPO_ROOT
    args = parser().parse_args()
    REPO_ROOT = Path(args.repo_root).resolve()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
