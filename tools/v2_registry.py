"""Resolve SIF referrers and finalize chronological v2 registry tags."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from builder.image_fingerprint import (
    MANIFEST_ACCEPT,
    RegistryClient,
    RegistryError,
    parse_image_reference,
    resolve_credentials,
)

SIF_MEDIA_TYPE = "application/vnd.sylabs.sif.layer.v1.sif"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class SifReference:
    image: str
    subject_digest: str
    artifact_digest: str
    layer_digest: str

    @property
    def pull_uri(self) -> str:
        ref = parse_image_reference(self.image)
        return f"oras://{ref.registry}/{ref.repository}@{self.artifact_digest}"


def dated_tag(tag: str) -> tuple[str, str] | None:
    version, separator, date = tag.rpartition("_")
    if not separator or not version or not re.fullmatch(r"\d{8}", date):
        return None
    try:
        datetime.strptime(date, "%Y%m%d")
    except ValueError:
        return None
    return version, date


def floating_tags(tags: list[str], version: str) -> dict[str, str]:
    releases = [
        (parsed[1], tag, parsed[0]) for tag in tags if (parsed := dated_tag(tag))
    ]
    matching = [release for release in releases if release[2] == version]
    if not matching:
        raise ValueError(f"No dated release tags for version {version}")
    return {version: max(matching)[1], "latest": max(releases)[1]}


def registry_pages(
    client: RegistryClient, path: str, fallback: str | None = None
) -> list[dict]:
    pages = []
    visited = set()
    while path:
        if path in visited:
            raise RegistryError("Registry pagination repeated a page")
        visited.add(path)
        response = client.get(path, MANIFEST_ACCEPT)
        if response.status_code == 404 and fallback and not pages:
            path, fallback = fallback, None
            continue
        if response.status_code != 200:
            raise RegistryError(
                f"Registry read failed for {path}: HTTP {response.status_code}"
            )
        pages.append(response.json())
        next_link = re.search(r'<([^>]+)>;\s*rel="?next"?', response.header("Link"))
        path = ""
        if next_link:
            url = urlsplit(next_link[1])
            if url.netloc and url.netloc != client.registry:
                raise RegistryError("Registry pagination changed host")
            if not url.path.startswith("/v2/"):
                raise RegistryError("Invalid registry pagination path")
            path = url.path + (f"?{url.query}" if url.query else "")
    return pages


def read_manifest(
    client: RegistryClient, repository: str, reference: str
) -> tuple[str, dict]:
    response = client.get(f"/v2/{repository}/manifests/{reference}", MANIFEST_ACCEPT)
    if response.status_code != 200:
        raise RegistryError(
            f"Manifest {repository}:{reference}: HTTP {response.status_code}"
        )
    digest = "sha256:" + hashlib.sha256(response.body).hexdigest()
    advertised = response.header("Docker-Content-Digest")
    if advertised and advertised != digest:
        raise RegistryError("Registry manifest digest does not match its bytes")
    if reference.startswith("sha256:") and reference != digest:
        raise RegistryError("Registry returned the wrong manifest digest")
    return digest, response.json()


def reader_client(registry: str) -> RegistryClient:
    # Quay is the anonymous public mirror. Most GHCR packages predate public
    # defaults and GitHub has no API to change package visibility, so GHCR
    # publication is verified with the workflow's registry login.
    credentials = resolve_credentials(registry) if registry == "ghcr.io" else None
    return RegistryClient(registry, credentials=credentials)


def resolve_sif(image: str, client: RegistryClient | None = None) -> SifReference:
    ref = parse_image_reference(image)
    client = client or reader_client(ref.registry)
    subject_digest, _ = read_manifest(client, ref.repository, ref.reference)
    pages = registry_pages(
        client,
        f"/v2/{ref.repository}/referrers/{subject_digest}",
        f"/v2/{ref.repository}/manifests/{subject_digest.replace(':', '-')}",
    )
    candidates = []
    for page in pages:
        for descriptor in page.get("manifests", []):
            if descriptor.get("artifactType") != SIF_MEDIA_TYPE:
                continue
            artifact_digest = descriptor.get("digest", "")
            if not DIGEST.fullmatch(artifact_digest):
                raise RegistryError("Invalid SIF manifest digest")
            _, manifest = read_manifest(client, ref.repository, artifact_digest)
            layers = manifest.get("layers", [])
            if (
                manifest.get("subject", {}).get("digest") != subject_digest
                or manifest.get("artifactType") != SIF_MEDIA_TYPE
                or len(layers) != 1
                or layers[0].get("mediaType") != SIF_MEDIA_TYPE
                or not DIGEST.fullmatch(layers[0].get("digest", ""))
            ):
                raise RegistryError("Invalid SIF referrer subject or layer")
            candidates.append(
                SifReference(
                    image, subject_digest, artifact_digest, layers[0]["digest"]
                )
            )
    if not candidates:
        raise RegistryError(f"No SIF referrer for {image}")
    if len({candidate.layer_digest for candidate in candidates}) != 1:
        raise RegistryError(f"Ambiguous SIF contents for {image}")
    return min(candidates, key=lambda candidate: candidate.artifact_digest)


def finalize(
    repository: str, version: str, build_date: str, *, apply: bool
) -> dict[str, str]:
    ref = parse_image_reference(repository)
    client = RegistryClient(ref.registry, credentials=resolve_credentials(ref.registry))
    candidate = f"{version}_{build_date}"
    if dated_tag(candidate) != (version, build_date):
        raise ValueError("Invalid release version or build date")
    tags = [
        tag
        for page in registry_pages(client, f"/v2/{ref.repository}/tags/list?n=100")
        for tag in page.get("tags", []) or []
    ]
    if candidate not in tags:
        raise RegistryError(f"Dated candidate tag is missing: {candidate}")
    plan = floating_tags(tags, version)
    identities = {}
    for tag in {candidate, *plan.values()}:
        # Reader resolution proves the complete publication is pullable.
        identities[tag] = resolve_sif(f"{repository}:{tag}")
    if apply:
        for alias, tag in plan.items():
            source = f"{repository}@{identities[tag].subject_digest}"
            subprocess.run(["oras", "tag", source, alias], check=True)
            actual = resolve_sif(f"{repository}:{alias}")
            if actual.subject_digest != identities[tag].subject_digest:
                raise RegistryError(
                    f"Floating tag verification failed: {repository}:{alias}"
                )
    return plan


def audit_releases(releases: Path, registries: list[str]) -> tuple[list[dict], bool]:
    results = []
    failed = False
    paths = sorted(releases.glob("*/*.json"))
    if not paths:
        raise ValueError(f"No release metadata found under {releases}")
    for path in paths:
        metadata = json.loads(path.read_text())
        dates = {str(app["version"]) for app in metadata.get("apps", {}).values()}
        if not dates:
            failed = True
            results.append({"release": str(path), "error": "No published applications"})
        for date in sorted(dates):
            row = {"release": str(path), "tag": f"{path.stem}_{date}", "registries": {}}
            layers = set()
            for registry in registries:
                image = f"{registry.rstrip('/')}/{path.parent.name}:{row['tag']}"
                try:
                    identity = resolve_sif(image)
                    row["registries"][registry] = asdict(identity)
                    layers.add(identity.layer_digest)
                except (RegistryError, ValueError, OSError) as error:
                    failed = True
                    row["registries"][registry] = {"error": str(error)}
            if len(layers) > 1:
                failed = True
                row["error"] = "Mirrors publish different SIF bytes"
            results.append(row)
    return results, failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolve = commands.add_parser("resolve", help="Print a verified SIF artifact URI")
    resolve.add_argument("image")
    resolve.add_argument("--expected-sha256", help="Require this SIF layer SHA-256")
    finish = commands.add_parser("finalize", help="Select newest-date floating tags")
    finish.add_argument("repository")
    finish.add_argument("version")
    finish.add_argument("build_date")
    finish.add_argument("--apply", action="store_true")
    audit = commands.add_parser(
        "audit", help="Report publication gaps without modifying registries"
    )
    audit.add_argument("--releases", type=Path, default=Path("releases"))
    audit.add_argument("--registry", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        if args.command == "resolve":
            identity = resolve_sif(args.image)
            if (
                args.expected_sha256
                and identity.layer_digest != "sha256:" + args.expected_sha256
            ):
                raise RegistryError("SIF referrer does not match the tested artifact")
            print(identity.pull_uri)
        elif args.command == "finalize":
            print(
                json.dumps(
                    finalize(
                        args.repository, args.version, args.build_date, apply=args.apply
                    )
                )
            )
        else:
            rows, failed = audit_releases(
                args.releases,
                args.registry or ["quay.io/neurodesk", "ghcr.io/neurodesk"],
            )
            print(json.dumps(rows, indent=2))
            return int(failed)
    except (RegistryError, ValueError, OSError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
