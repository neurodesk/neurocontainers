import hashlib
import json

import pytest

from builder.image_fingerprint import HttpResponse, RegistryClient, RegistryError
from tools import v2_registry


def response(payload, status=200, headers=None):
    return HttpResponse(status, headers or {}, json.dumps(payload).encode())


def registry(image="example.test/tools/demo:1.0_20261003", layer="a" * 64):
    subject = response({"schemaVersion": 2, "layers": []})
    subject_digest = "sha256:" + hashlib.sha256(subject.body).hexdigest()
    artifact = response(
        {
            "artifactType": v2_registry.SIF_MEDIA_TYPE,
            "subject": {"digest": subject_digest},
            "layers": [
                {"mediaType": v2_registry.SIF_MEDIA_TYPE, "digest": "sha256:" + layer}
            ],
        }
    )
    artifact_digest = "sha256:" + hashlib.sha256(artifact.body).hexdigest()
    paths = {
        "/v2/tools/demo/manifests/1.0_20261003": subject,
        f"/v2/tools/demo/manifests/{artifact_digest}": artifact,
        f"/v2/tools/demo/referrers/{subject_digest}": response(
            {
                "manifests": [
                    {
                        "artifactType": v2_registry.SIF_MEDIA_TYPE,
                        "digest": artifact_digest,
                    },
                ]
            }
        ),
    }
    client = RegistryClient(
        "example.test",
        transport=lambda method, url, *args: paths[
            url.removeprefix("https://example.test")
        ],
    )
    return image, client, paths, subject_digest, artifact_digest


def test_floating_tags_follow_dates_instead_of_version_order():
    tags = [
        "2.0_20261001",
        "1.0_20261003",
        "1.0_20260901",
        "latest",
        "candidate-abc",
        "1.0_20260230",
    ]
    assert v2_registry.floating_tags(tags, "1.0") == {
        "1.0": "1.0_20261003",
        "latest": "1.0_20261003",
    }
    assert v2_registry.floating_tags(tags, "2.0") == {
        "2.0": "2.0_20261001",
        "latest": "1.0_20261003",
    }
    with pytest.raises(ValueError, match="No dated"):
        v2_registry.floating_tags(tags, "3.0")


def test_resolves_exact_sif_and_checks_its_subject_and_bytes():
    image, client, _, subject, artifact = registry()
    resolved = v2_registry.resolve_sif(image, client)
    assert resolved.subject_digest == subject
    assert resolved.layer_digest == "sha256:" + "a" * 64
    assert resolved.pull_uri == f"oras://example.test/tools/demo@{artifact}"


def test_supports_referrer_tag_fallback():
    image, client, paths, subject, _ = registry()
    path = f"/v2/tools/demo/referrers/{subject}"
    paths[f"/v2/tools/demo/manifests/{subject.replace(':', '-')}"] = paths[path]
    paths[path] = response({}, 404)
    assert v2_registry.resolve_sif(image, client).layer_digest == "sha256:" + "a" * 64


@pytest.mark.parametrize(
    "registry_host, expected",
    [
        ("ghcr.io", ("actor", "token")),
        ("quay.io", None),
    ],
)
def test_only_ghcr_publication_is_verified_with_credentials(
    monkeypatch, registry_host, expected
):
    monkeypatch.setattr(
        v2_registry, "resolve_credentials", lambda registry: ("actor", "token")
    )
    assert v2_registry.reader_client(registry_host).credentials == expected


@pytest.mark.parametrize(
    "fault", ["missing", "wrong-subject", "wrong-layer", "ambiguous", "digest"]
)
def test_refuses_incomplete_or_inconsistent_publication(fault):
    image, client, paths, subject, artifact = registry()
    index_path = f"/v2/tools/demo/referrers/{subject}"
    manifest_path = f"/v2/tools/demo/manifests/{artifact}"
    if fault == "missing":
        paths[index_path] = response({"manifests": []})
    elif fault == "digest":
        paths[manifest_path].body += b" "
    else:
        manifest = paths[manifest_path].json()
        if fault == "wrong-subject":
            manifest["subject"]["digest"] = "sha256:" + "b" * 64
        else:
            manifest["layers"][0]["digest"] = "sha256:" + "b" * 64
            if fault == "wrong-layer":
                manifest["layers"][0]["mediaType"] = "application/tar"
        modified = response(manifest)
        digest = "sha256:" + hashlib.sha256(modified.body).hexdigest()
        paths[f"/v2/tools/demo/manifests/{digest}"] = modified
        descriptors = (
            paths[index_path].json()["manifests"] if fault == "ambiguous" else []
        )
        descriptors.append(
            {"artifactType": v2_registry.SIF_MEDIA_TYPE, "digest": digest}
        )
        paths[index_path] = response({"manifests": descriptors})
    with pytest.raises(RegistryError):
        v2_registry.resolve_sif(image, client)


def test_pagination_keeps_later_tags_and_rejects_foreign_hosts():
    calls = []

    def transport(method, url, *args):
        calls.append(url)
        if url.endswith("?n=1"):
            return response(
                {"tags": ["1.0_20260101"]},
                headers={"Link": '</v2/tools/demo/tags/list?n=1&last=old>; rel="next"'},
            )
        return response({"tags": ["1.0_20261003"]})

    client = RegistryClient("example.test", transport=transport)
    pages = v2_registry.registry_pages(client, "/v2/tools/demo/tags/list?n=1")
    assert [tag for page in pages for tag in page["tags"]] == [
        "1.0_20260101",
        "1.0_20261003",
    ]
    assert len(calls) == 2
    foreign = RegistryClient(
        "example.test",
        transport=lambda *args: response(
            {}, headers={"Link": '<https://other.test/v2/list>; rel="next"'}
        ),
    )
    with pytest.raises(RegistryError, match="changed host"):
        v2_registry.registry_pages(foreign, "/v2/list")


def test_old_recovery_selects_newer_release_and_dry_run_never_writes(monkeypatch):
    monkeypatch.setattr(
        v2_registry,
        "registry_pages",
        lambda *args: [{"tags": ["1.0_20260101", "1.0_20261003", "2.0_20261002"]}],
    )
    monkeypatch.setattr(v2_registry, "resolve_credentials", lambda *args: None)
    calls = []
    aliases = {}

    def resolve(image):
        tag = image.rpartition(":")[2]
        digest = aliases.get(
            tag, "sha256:" + ("b" if tag.endswith("1003") else "a") * 64
        )
        return v2_registry.SifReference(
            image, digest, "sha256:" + "c" * 64, "sha256:" + "d" * 64
        )

    def run(command, check):
        calls.append(command)
        aliases[command[-1]] = command[2].partition("@")[2]

    monkeypatch.setattr(v2_registry, "resolve_sif", resolve)
    monkeypatch.setattr(v2_registry.subprocess, "run", run)
    assert v2_registry.finalize(
        "example.test/tools/demo", "1.0", "20260101", apply=False
    ) == {
        "1.0": "1.0_20261003",
        "latest": "1.0_20261003",
    }
    assert calls == []
    v2_registry.finalize("example.test/tools/demo", "1.0", "20260101", apply=True)
    assert calls == [
        ["oras", "tag", "example.test/tools/demo@sha256:" + "b" * 64, "1.0"],
        ["oras", "tag", "example.test/tools/demo@sha256:" + "b" * 64, "latest"],
    ]


def test_audit_reports_unavailable_releases_and_mirror_disagreement(
    tmp_path, monkeypatch
):
    (tmp_path / "demo").mkdir()
    (tmp_path / "demo/1.0.json").write_text(
        json.dumps({"apps": {"demo 1.0": {"version": "20261003"}}})
    )

    def resolve(image):
        if image.startswith("missing"):
            raise RegistryError("HTTP 401")
        return v2_registry.SifReference(
            image, "subject", "artifact", image.split("/")[0]
        )

    monkeypatch.setattr(v2_registry, "resolve_sif", resolve)
    rows, failed = v2_registry.audit_releases(
        tmp_path, ["one.test/org", "two.test/org", "missing.test/org"]
    )
    assert failed
    assert rows[0]["error"] == "Mirrors publish different SIF bytes"
    assert rows[0]["registries"]["missing.test/org"]["error"] == "HTTP 401"
    with pytest.raises(ValueError, match="No release"):
        v2_registry.audit_releases(tmp_path / "absent", [])
