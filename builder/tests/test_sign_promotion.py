import hashlib
import json
import subprocess

import pytest

from tools import sign_promotion as signing
from tools.v2_registry import SifReference


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    data = b"tested candidate bytes"
    (tmp_path / "demo.simg").write_bytes(data)
    (tmp_path / "test-results.json").write_text('{"passed": 2, "failed": 0}')
    digest = hashlib.sha256(data).hexdigest()
    image = "ghcr.io/neurodesk/demo@sha256:" + "a" * 64
    reference = SifReference(
        image, "sha256:" + "a" * 64, "sha256:" + "b" * 64, "sha256:" + digest
    )
    monkeypatch.setattr(signing, "resolve_sif", lambda image: reference)
    monkeypatch.setenv("GITHUB_WORKFLOW_REF", signing.WORKFLOW)
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    calls = []
    monkeypatch.setattr(signing, "run", lambda *args: calls.append(args))
    return (
        tmp_path,
        {"sif": "demo.simg", "sif_sha256": digest, "head_sha": "c" * 40},
        image,
        calls,
    )


def test_signs_exact_bytes_and_binds_acceptance_without_claiming_build_provenance(
    candidate,
):
    path, manifest, image, calls = candidate
    artifacts = signing.sign(path, manifest, "d" * 40, image)
    assert len(artifacts) == 3
    statement = json.loads((path / "demo.simg.promotion.json").read_text())
    assert statement["predicate"]["candidate"]["head_sha"] == "c" * 40
    assert statement["predicate"]["merge_sha"] == "d" * 40
    assert statement["predicate"]["test_results_sha256"] == signing.sha256(
        path / "test-results.json"
    )
    assert statement["subject"][1]["digest"]["sha256"] == manifest["sif_sha256"]
    assert "not a trusted-build" in statement["predicate"]["build_provenance"]
    assert calls[0] == ("sign", "--yes", image)
    for call in calls:
        if call[0].startswith("verify"):
            assert signing.IDENTITY in call and signing.ISSUER in call
            assert not any("regexp" in argument for argument in call)


@pytest.mark.parametrize(
    "fault",
    [
        "fork",
        "branch",
        "changed-bytes",
        "published-bytes",
        "missing-results",
        "mutable-tag",
    ],
)
def test_refuses_to_sign_invalid_or_untrusted_candidate(candidate, monkeypatch, fault):
    path, manifest, image, calls = candidate
    if fault == "fork":
        monkeypatch.setenv(
            "GITHUB_WORKFLOW_REF", signing.WORKFLOW.replace("neurodesk/", "fork/")
        )
    elif fault == "branch":
        monkeypatch.setenv("GITHUB_REF", "refs/pull/123/merge")
    elif fault == "changed-bytes":
        (path / "demo.simg").write_bytes(b"changed")
    elif fault == "published-bytes":
        monkeypatch.setattr(
            signing,
            "resolve_sif",
            lambda image: SifReference(
                image, "subject", "artifact", "sha256:" + "e" * 64
            ),
        )
    elif fault == "missing-results":
        (path / "test-results.json").unlink()
    else:
        image = "ghcr.io/neurodesk/demo:latest"
    with pytest.raises(ValueError):
        signing.sign(path, manifest, "d" * 40, image)
    assert not calls


def test_failed_signature_verification_stops_before_sidecars(candidate, monkeypatch):
    path, manifest, image, _ = candidate
    calls = []

    def run(*args):
        calls.append(args)
        if args[0] == "verify":
            raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(signing, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        signing.sign(path, manifest, "d" * 40, image)
    assert [call[0] for call in calls] == ["sign", "verify"]
    assert not (path / "demo.simg.sigstore.json").exists()
