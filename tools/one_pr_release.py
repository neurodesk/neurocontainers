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

REPO_ROOT = SCRIPT_REPO_ROOT
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
RECIPE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
CANDIDATE_MANIFEST_FIELDS = (
    "recipe",
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


def detect_recipes(base: str, head: str) -> list[str]:
    """Return changed recipes after enforcing the recipe-only PR boundary."""
    paths = changed_files(base, head)
    recipes = {
        parts[1]
        for path in paths
        if len(parts := Path(path).parts) >= 3
        and parts[0] == "recipes"
        and parts[2] == "build.yaml"
    }
    if not recipes:
        raise RuntimeError("No recipes/*/build.yaml change found")

    allowed = tuple(f"recipes/{recipe}/" for recipe in recipes)
    unrelated = [path for path in paths if not path.startswith(allowed)]
    if unrelated:
        raise RuntimeError(
            "Automated releases require a recipe-only PR. Unrelated paths: "
            + ", ".join(unrelated)
        )
    for recipe in recipes:
        if not (REPO_ROOT / "recipes" / recipe / "fulltest.yaml").is_file():
            raise RuntimeError(f"recipes/{recipe}/fulltest.yaml is required")
    return sorted(recipes)


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


def inspect_recipe(recipe: str, head_sha: str) -> dict[str, str]:
    """Derive safe candidate names and release identifiers for a recipe."""
    data = load_recipe(recipe)
    if "version" not in data:
        raise RuntimeError(f"Recipe {recipe} build.yaml is missing a version field")
    version = str(data["version"])
    if not VERSION_PATTERN.fullmatch(version):
        raise RuntimeError(
            f"Recipe {recipe} has an invalid version {version!r}; "
            "use only letters, numbers, dots, underscores, and hyphens"
        )
    date = build_date(recipe, head_sha)
    image_name = f"{recipe}_{version}"
    return {
        "recipe": recipe,
        "version": version,
        "build_date": date,
        "image_name": image_name,
        "candidate_tag": f"nd-candidate-{recipe}:{head_sha[:12]}",
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


def command_detect(args: argparse.Namespace) -> None:
    """Implement the detect CLI command."""
    write_output({"recipes": json.dumps(detect_recipes(args.base, args.head))})


def command_inspect(args: argparse.Namespace) -> None:
    """Implement the inspect CLI command."""
    write_output(inspect_recipe(args.recipe, args.head_sha))


def command_manifest(args: argparse.Namespace) -> None:
    """Create the release preview and provenance manifest for a candidate."""
    info = inspect_recipe(args.recipe, args.head_sha)
    candidate_dir = Path(args.candidate_dir)
    docker_archive = candidate_dir / info["docker_archive"]
    sif = candidate_dir / info["sif"]
    if not docker_archive.is_file() or not sif.is_file():
        raise RuntimeError("Candidate Docker archive or SIF is missing")

    recipe = load_recipe(args.recipe)
    metadata = release_data(
        args.recipe, info["version"], recipe, info["build_date"], "x86_64"
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
    if candidate_dir.name != recipe:
        raise RuntimeError(f"Candidate directory does not match recipe {recipe}")
    if manifest["head_sha"] != expected_head_sha:
        raise RuntimeError(f"Candidate head SHA mismatch for {recipe}")
    if expected_pr_number is not None and manifest["pr_number"] != expected_pr_number:
        raise RuntimeError(f"Candidate PR number mismatch for {recipe}")
    if manifest["recipe_fingerprint"] != recipe_fingerprint(recipe):
        raise RuntimeError(f"Merged recipe differs from tested candidate: {recipe}")

    expected_info = inspect_recipe(recipe, expected_head_sha)
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
                f"Candidate {field} mismatch for {recipe}: "
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
        recipe,
        expected_info["version"],
        load_recipe(recipe),
        expected_info["build_date"],
        "x86_64",
    )
    actual_release = json.loads(paths["release_json"].read_text(encoding="utf-8"))
    if actual_release != expected_release:
        raise RuntimeError(f"Release JSON mismatch for {recipe}")
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


def command_materialize(args: argparse.Namespace) -> None:
    """Copy verified release previews into the repository release tree."""
    bundle = Path(args.bundle)
    manifests = json.loads(Path(args.manifests).read_text(encoding="utf-8"))
    for manifest in manifests:
        recipe = validate_recipe_identifier(manifest.get("recipe"))
        version = manifest.get("version")
        if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
            raise RuntimeError(f"Invalid verified version for {recipe}: {version!r}")
        expected_release_json = f"{version}.json"
        if manifest.get("release_json") != expected_release_json:
            raise RuntimeError(f"Invalid verified release JSON for {recipe}")
        source = candidate_file(
            bundle / recipe, manifest["release_json"], "release JSON"
        )
        destination = REPO_ROOT / "releases" / recipe / expected_release_json
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
    inspect.set_defaults(func=command_inspect)

    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("--recipe", required=True)
    manifest.add_argument("--head-sha", required=True)
    manifest.add_argument("--pr-number", required=True, type=int)
    manifest.add_argument("--candidate-dir", required=True)
    manifest.set_defaults(func=command_manifest)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--bundle", required=True)
    verify.add_argument("--head-sha", required=True)
    verify.add_argument("--pr-number", required=True, type=int)
    verify.add_argument("--output", required=True)
    verify.set_defaults(func=command_verify)

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
