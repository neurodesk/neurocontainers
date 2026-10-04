"""Sign accepted candidate bytes without claiming trusted-build provenance."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from tools.v2_registry import resolve_sif


WORKFLOW = "neurodesk/neurocontainers/.github/workflows/promote-container-candidate.yml@refs/heads/main"
IDENTITY = "https://github.com/" + WORKFLOW
ISSUER = "https://token.actions.githubusercontent.com"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verification_arguments() -> list[str]:
    return ["--certificate-identity", IDENTITY, "--certificate-oidc-issuer", ISSUER]


def run(*arguments: str) -> None:
    subprocess.run(["cosign", *arguments], check=True)


def sign(candidate: Path, manifest: dict, merge_sha: str, image: str) -> list[Path]:
    if os.environ.get("GITHUB_WORKFLOW_REF") != WORKFLOW or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("Signing requires the official main-branch promotion workflow")
    if not re.fullmatch(r"[0-9a-f]{40}", merge_sha):
        raise ValueError("Merge commit must be a full SHA")
    if not re.fullmatch(r"ghcr\.io/[^@]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Signing requires an immutable GHCR image digest")
    filename = manifest["sif"]
    if not isinstance(filename, str) or Path(filename).name != filename:
        raise ValueError("SIF must be a basename inside the verified candidate")
    sif = candidate / filename
    if sha256(sif) != manifest["sif_sha256"]:
        raise ValueError("SIF differs from the verified candidate")
    resolved = resolve_sif(image)
    if resolved.layer_digest != "sha256:" + manifest["sif_sha256"]:
        raise ValueError("Published SIF differs from the verified candidate")
    results = candidate / "test-results.json"
    if not results.is_file():
        raise ValueError("Candidate test results are required for the signed statement")
    statement = candidate / (filename + ".promotion.json")
    statement.write_text(json.dumps({
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [
            {"name": image.split("@")[0], "digest": {"sha256": resolved.subject_digest.removeprefix("sha256:")}},
            {"name": filename, "digest": {"sha256": manifest["sif_sha256"]}},
        ],
        "predicateType": "https://neurodesk.org/attestations/promotion/v1",
        "predicate": {
            "acceptance": "Merged recipe PR and successful candidate workflow accepted for publication",
            "build_provenance": "Candidate supplied by the PR workflow; not a trusted-build attestation",
            "merge_sha": merge_sha,
            "candidate": manifest,
            "test_results_sha256": sha256(results),
            "workflow_identity": IDENTITY,
        },
    }, sort_keys=True, indent=2) + "\n")
    sif_reference = resolved.pull_uri.removeprefix("oras://")
    for reference in (image, sif_reference):
        run("sign", "--yes", reference)
        run("verify", *verification_arguments(), reference)
    sif_bundle = candidate / (filename + ".sigstore.json")
    statement_bundle = candidate / (filename + ".promotion.sigstore.json")
    for artifact, bundle in ((sif, sif_bundle), (statement, statement_bundle)):
        run("sign-blob", "--yes", "--bundle", str(bundle), str(artifact))
        run("verify-blob", *verification_arguments(), "--bundle", str(bundle), str(artifact))
    return [sif_bundle, statement, statement_bundle]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--manifests", type=Path, required=True)
    parser.add_argument("--merge-sha", required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    manifests = json.loads(args.manifests.read_text())
    if not isinstance(manifests, list) or len(manifests) != 1:
        raise ValueError("Signing expects exactly one verified candidate")
    paths = sign(args.candidate, manifests[0], args.merge_sha, args.image)
    print(json.dumps([str(path) for path in paths]))


if __name__ == "__main__":
    main()
