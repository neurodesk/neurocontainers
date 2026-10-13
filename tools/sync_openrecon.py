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
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

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
OPENRECON_VERSION_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*)){2}")
TWO_PART_VERSION_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
POST_RELEASE_VERSION_PATTERN = re.compile(
    r"(?P<base>(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*)){1,2})\.post(?:0|[1-9][0-9]*)"
)
IMAGE_ASSIGNMENT_PATTERN = re.compile(
    r"^(?P<prefix>[ \t]*(?:export[ \t]+)?baseDockerImage=)"
    r"(?P<quote>[\"']?)(?P<image>[^\s\"'#]*)(?P=quote)(?:[ \t]+#.*)?[ \t]*$",
    re.MULTILINE,
)
BUILD_DATE_PATTERN = re.compile(r"^\d{8}$")
DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
LITERAL_ASSIGNMENT_PATTERN = re.compile(
    r"^[ \t]*(?:export[ \t]+)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)="
    r"(?:'(?P<single>[^'\n]*)'|\"(?P<double>[^\"`\\\n]*)\"|(?P<bare>[^\s'\"`\\;&|<>()]*))"
    r"(?:[ \t]+#.*)?[ \t]*$",
    re.MULTILINE,
)
SHELL_REFERENCE_PATTERN = re.compile(
    r"\$(?:\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}|(?P<bare>[A-Za-z_][A-Za-z0-9_]*))"
)
MANIFEST_MEDIA_TYPES = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
REGISTRY_TIMEOUT_SECONDS = 30
ANY_ASSIGNMENT_PATTERNS = {
    "version": re.compile(
        r"^[ \t]*(?:export[ \t]+)?(?:version|VERSION)=", re.MULTILINE
    ),
    "openrecon_version": re.compile(
        r"^[ \t]*(?:export[ \t]+)?openrecon_version=", re.MULTILINE
    ),
    "baseDockerImage": re.compile(
        r"^[ \t]*(?:export[ \t]+)?baseDockerImage=", re.MULTILINE
    ),
}


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
    """Project a source-container version onto the scanner's numeric triplet."""
    validate_version(version)
    post_release = POST_RELEASE_VERSION_PATTERN.fullmatch(version)
    if post_release:
        version = post_release.group("base")
    if OPENRECON_VERSION_PATTERN.fullmatch(version):
        return version
    if TWO_PART_VERSION_PATTERN.fullmatch(version):
        return f"{version}.0"
    raise ValueError(
        f"OpenRecon requires a numeric X.Y.Z scanner version; cannot normalize {version!r}"
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


def literal_assignments(contents: str) -> dict[str, str]:
    """Expand literal params.sh assignments in order, without executing shell."""
    values: dict[str, str] = {}
    for match in LITERAL_ASSIGNMENT_PATTERN.finditer(contents):
        if match.group("single") is not None:
            values[match.group("name")] = match.group("single")
            continue
        raw = match.group("double")
        if raw is None:
            raw = match.group("bare")
        values[match.group("name")] = SHELL_REFERENCE_PATTERN.sub(
            lambda reference: values.get(
                reference.group("braced") or reference.group("bare"),
                reference.group(0),
            ),
            raw,
        )
    return values


def reject_duplicate_assignments(contents: str) -> None:
    """Refuse params.sh files whose effective value the sync cannot rewrite."""
    for name, pattern in ANY_ASSIGNMENT_PATTERNS.items():
        if len(pattern.findall(contents)) > 1:
            raise RuntimeError(f"params.sh assigns {name} more than once")
    if ANY_ASSIGNMENT_PATTERNS["baseDockerImage"].search(
        contents
    ) and not IMAGE_ASSIGNMENT_PATTERN.search(contents):
        raise RuntimeError("params.sh baseDockerImage assignment is not supported")


def scanner_version(contents: str) -> str | None:
    """Return the numeric scanner version params.sh currently publishes."""
    values = literal_assignments(contents)
    version = values.get("openrecon_version", values.get("version"))
    if version is None or not OPENRECON_VERSION_PATTERN.fullmatch(version):
        return None
    return version


def version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def update_params_version(contents: str, version: str) -> str:
    """Update the first supported version assignment in params.sh.

    The scanner version never regresses: a maintainer may release scanner
    packages ahead of the container version, and a rebuild keeps that release.
    """
    validate_version(version)
    if not VERSION_ASSIGNMENT_PATTERN.search(contents):
        raise RuntimeError("params.sh has no supported version assignment")
    reject_duplicate_assignments(contents)
    metadata_version = openrecon_version(version)
    current_version = scanner_version(contents)
    if current_version is not None and version_key(current_version) > version_key(
        metadata_version
    ):
        metadata_version = current_version
    updated = VERSION_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{version}",
        contents,
        count=1,
    )
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


def released_build_date(source_root: Path, container: str, version: str) -> str | None:
    """Return the build date the release metadata published for this version."""
    validate_version(version)
    releases = source_root / "releases"
    release = releases / container / f"{version}.json"
    if not release.resolve().is_relative_to(releases.resolve()):
        raise ValueError(f"Invalid container name: {container!r}")
    if not release.is_file():
        return None
    try:
        apps = json.loads(release.read_text(encoding="utf-8"))["apps"]
        build_date = str(apps[f"{container} {version}"]["version"])
    except (KeyError, TypeError, ValueError):
        return None
    return build_date if BUILD_DATE_PATTERN.fullmatch(build_date) else None


def split_image_digest(image: str) -> tuple[str, str | None]:
    """Split an image reference into its name/tag and an optional digest pin."""
    reference, separator, digest = image.rpartition("@")
    if not separator:
        return image, None
    if not DIGEST_PATTERN.fullmatch(digest):
        raise RuntimeError(f"Unsupported image digest in params.sh: {image!r}")
    return reference, digest


def dated_image_tag(contents: str) -> str | None:
    """Return the date pinned by a date-only or version-prefixed image tag."""
    match = IMAGE_ASSIGNMENT_PATTERN.search(contents)
    if match is None:
        return None
    reference, _ = split_image_digest(match.group("image"))
    _, separator, tag = reference.rpartition(":")
    build_date = tag.rsplit("_", 1)[-1]
    if not separator or "/" in tag or not BUILD_DATE_PATTERN.fullmatch(build_date):
        return None
    return build_date


def update_params_image_tag(contents: str, build_date: str) -> str:
    """Point a dated image reference at the build published for this release."""
    if not BUILD_DATE_PATTERN.fullmatch(build_date):
        raise ValueError(f"Invalid build date: {build_date!r}")
    pinned_date = dated_image_tag(contents)
    if pinned_date is None:
        return contents
    match = IMAGE_ASSIGNMENT_PATTERN.search(contents)
    assert match is not None
    reference, _ = split_image_digest(match.group("image"))
    reference_end = match.start("image") + len(reference)
    return (
        contents[: reference_end - len(pinned_date)]
        + build_date
        + contents[reference_end:]
    )


def pinned_image_digest(contents: str) -> str | None:
    """Return the digest params.sh pins its base image to, if any."""
    match = IMAGE_ASSIGNMENT_PATTERN.search(contents)
    if match is None:
        return None
    return split_image_digest(match.group("image"))[1]


def update_params_image_digest(contents: str, digest: str) -> str:
    """Replace the digest pinned by the base image reference."""
    if not DIGEST_PATTERN.fullmatch(digest):
        raise ValueError(f"Invalid image digest: {digest!r}")
    match = IMAGE_ASSIGNMENT_PATTERN.search(contents)
    if match is None or pinned_image_digest(contents) is None:
        return contents
    return (
        contents[: match.end("image") - len(digest)]
        + digest
        + contents[match.end("image") :]
    )


def pinned_image_reference(contents: str) -> str:
    """Return the expanded name:tag of a digest-pinned base image."""
    image = literal_assignments(contents).get("baseDockerImage")
    if image is None:
        raise RuntimeError("params.sh baseDockerImage must be a literal assignment")
    reference, _ = split_image_digest(image)
    name, separator, tag = reference.rpartition(":")
    if "$" in reference or not separator or "/" in tag:
        raise RuntimeError(
            f"Cannot refresh the digest of {image!r}; pin a fully resolvable name:tag"
        )
    return reference


class SameOriginHTTPSRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow HTTPS redirects only, and never carry credentials across hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlsplit(newurl)
        if target.scheme != "https":
            raise RuntimeError(f"Refusing non-HTTPS registry redirect to {newurl}")
        request = super().redirect_request(req, fp, code, msg, headers, newurl)
        if request is not None and target.netloc != urllib.parse.urlsplit(
            req.full_url
        ).netloc:
            request.remove_header("Authorization")
        return request


REGISTRY_OPENER = urllib.request.build_opener(SameOriginHTTPSRedirectHandler)


def parse_bearer_challenge(header: str) -> dict[str, str]:
    scheme, _, parameters = header.partition(" ")
    if scheme.lower() != "bearer":
        raise RuntimeError(f"Unsupported registry authentication: {header!r}")
    return dict(re.findall(r'(\w+)="([^"]*)"', parameters))


def resolve_image_digest(reference: str) -> str:
    """Resolve the manifest digest a registry serves for a public name:tag."""
    name, _, tag = reference.rpartition(":")
    registry, separator, repository = name.partition("/")
    if not separator or not ("." in registry or ":" in registry or registry == "localhost"):
        registry, repository = "docker.io", name
    if registry == "index.docker.io":
        registry = "docker.io"
    if registry == "docker.io":
        registry = "registry-1.docker.io"
        if "/" not in repository:
            repository = f"library/{repository}"
    url = f"https://{registry}/v2/{repository}/manifests/{tag}"
    headers = {"Accept": MANIFEST_MEDIA_TYPES}

    def head() -> str:
        request = urllib.request.Request(url, headers=headers, method="HEAD")
        with REGISTRY_OPENER.open(request, timeout=REGISTRY_TIMEOUT_SECONDS) as response:
            return response.headers.get("Docker-Content-Digest", "")

    try:
        try:
            digest = head()
        except urllib.error.HTTPError as error:
            if error.code != 401:
                raise
            challenge = parse_bearer_challenge(error.headers.get("WWW-Authenticate", ""))
            realm = urllib.parse.urlsplit(challenge.get("realm", ""))
            if realm.scheme != "https" or not realm.hostname or realm.username:
                raise RuntimeError(
                    f"Refusing registry token realm {challenge.get('realm')!r}"
                )
            query = urllib.parse.urlencode(
                {key: value for key, value in challenge.items() if key != "realm"}
            )
            separator = "&" if realm.query else "?"
            with REGISTRY_OPENER.open(
                f"{challenge['realm']}{separator}{query}",
                timeout=REGISTRY_TIMEOUT_SECONDS,
            ) as response:
                token = json.load(response)
            headers["Authorization"] = (
                f"Bearer {token.get('token') or token['access_token']}"
            )
            digest = head()
    except (urllib.error.URLError, KeyError, ValueError, OSError) as error:
        raise RuntimeError(f"Cannot resolve the digest of {reference}: {error}") from error
    if not DIGEST_PATTERN.fullmatch(digest):
        raise RuntimeError(f"Registry returned no usable digest for {reference}")
    return digest


def is_raw_return_label(label_path: Path) -> bool:
    label = json.loads(label_path.read_text(encoding="utf-8"))
    return label.get("reconstruction", {}).get("injector") == "raw"


def prepare_recipe(
    source_root: Path,
    openrecon_root: Path,
    recipe: str,
    version: str,
    *,
    variant: str = "",
    experimental_raw_return: bool = False,
    resolve_digest: Callable[[str], str] = resolve_image_digest,
) -> PreparedSync | None:
    """Copy one recipe's OpenRecon metadata into an existing checkout."""
    version = validate_version(version)
    openrecon_target = resolve_openrecon_target(source_root, recipe, variant)
    source_recipe = source_root / "recipes" / openrecon_target.source_recipe
    if not openrecon_target.label.is_file():
        return None
    raw_return = is_raw_return_label(openrecon_target.label)
    if raw_return and not experimental_raw_return:
        print(
            "Experimental raw-return label requires explicit local research staging; "
            "skipping stock sync."
        )
        return None
    if raw_return:
        from workflows.validate_openrecon_labels import validate_label

        errors = validate_label(openrecon_target.label, experimental_raw_return=True)
        if errors:
            raise ValueError("Invalid research label: " + "; ".join(errors))

    relative_recipe = Path("recipes") / openrecon_target.container
    target_recipe = openrecon_root / relative_recipe
    target_label = target_recipe / "OpenReconLabel.json"
    target_params = target_recipe / "params.sh"
    target_readme = target_recipe / "README.md"
    bootstrapped = not target_recipe.is_dir()

    notes: list[str] = []
    if bootstrapped:
        notes.append(
            f"- Create `{relative_recipe.as_posix()}` in OpenRecon because it did not exist yet"
        )

    params = (
        target_params.read_text(encoding="utf-8")
        if target_params.is_file()
        else (
            "#!/bin/bash\n"
            "# Auto-generated by neurocontainers CI.\n"
            f"export toolName={openrecon_target.container}\n"
            f"export version={version}\n"
            "export baseDockerImage=vnmd/${toolName}_${version}\n"
        )
    )
    updated_params = update_params_version(params, version)
    previous_scanner_version = scanner_version(params)
    if (
        previous_scanner_version is not None
        and scanner_version(updated_params) == previous_scanner_version
        and openrecon_version(version) != previous_scanner_version
    ):
        notes.append(
            f"- Keep scanner version `{previous_scanner_version}` in "
            f"`{(relative_recipe / 'params.sh').as_posix()}` because it is ahead of "
            f"the container's `{openrecon_version(version)}`; bump it if this "
            "release should install as a new scanner package"
        )
    pinned_tag = dated_image_tag(updated_params)
    if pinned_tag is not None:
        build_date = released_build_date(
            source_root, openrecon_target.container, version
        )
        if build_date is None and pinned_image_digest(updated_params) is not None:
            raise RuntimeError(
                f"No release metadata for {openrecon_target.container} {version}; "
                "cannot advance the digest-pinned base image"
            )
        if build_date is None:
            notes.append(
                f"- Leave the `{pinned_tag}` image tag in "
                f"`{(relative_recipe / 'params.sh').as_posix()}` because no release "
                f"metadata was found for {openrecon_target.container} {version}; "
                "confirm that image exists before merging"
            )
        else:
            updated_params = update_params_image_tag(updated_params, build_date)
            if build_date != pinned_tag:
                notes.append(
                    f"- Repoint `{(relative_recipe / 'params.sh').as_posix()}` from "
                    f"image tag `{pinned_tag}` to the published `{build_date}`"
                )
    pinned_digest = pinned_image_digest(updated_params)
    if pinned_digest is not None:
        reference = pinned_image_reference(updated_params)
        digest = resolve_digest(reference)
        updated_params = update_params_image_digest(updated_params, digest)
        if digest != pinned_digest:
            notes.append(
                f"- Pin `{(relative_recipe / 'params.sh').as_posix()}` to the "
                f"digest `{digest}` that `{reference}` resolves to"
            )
    target_recipe.mkdir(parents=True, exist_ok=True)
    if not target_params.is_file() or updated_params != params:
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

    if experimental_raw_return:
        for config_override in sorted(source_recipe.glob("wip_070_fire_*.json")):
            shutil.copyfile(config_override, target_recipe / config_override.name)
            paths.append((relative_recipe / config_override.name).as_posix())

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

    if is_raw_return_label(openrecon_target.label):
        print(
            f"Experimental raw-return label for {container}; skipping stock OpenRecon PR."
        )
        return None

    title = f"Update {container} OpenRecon metadata to {version}"
    existing = existing_pull_request(repository, title)
    if existing:
        print(f"Open OpenRecon PR already exists for {container} {version}: {existing}")
        return existing

    with tempfile.TemporaryDirectory(prefix="openrecon-sync-") as temp_dir:
        openrecon_root = Path(temp_dir) / "openrecon"
        run_command(["gh", "repo", "clone", repository, str(openrecon_root)])
        run_command(
            ["git", "config", "user.name", "neurocontainers-bot"], cwd=openrecon_root
        )
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
