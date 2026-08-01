from __future__ import annotations

import json

import pytest

from builder.image_fingerprint import (
    HttpResponse,
    ImageNotFound,
    RegistryClient,
    RegistryError,
    fingerprint_inspect_data,
    fingerprint_remote_config,
    main,
    parse_image_reference,
    remote_fingerprint,
    resolve_architecture,
    select_platform_manifest,
)


def _inspect_data(env: list[str], github_sha: str = "abc123") -> dict[str, object]:
    return {
        "Config": {
            "Cmd": ["/bin/bash"],
            "Env": env,
            "Labels": {
                "GITHUB_REPOSITORY": "neurodesk/neurocontainers",
                "GITHUB_SHA": github_sha,
                "recipe": "samri",
            },
        },
        "RootFS": {
            "Type": "layers",
            "Layers": ["sha256:layer"],
        },
    }


def _remote_config(
    env: list[str],
    github_sha: str = "abc123",
    diff_ids: list[str] | None = None,
) -> dict[str, object]:
    """The registry-served image config blob for the same image."""
    return {
        "architecture": "amd64",
        "os": "linux",
        "created": "2026-01-01T00:00:00Z",
        "history": [{"created_by": "RUN echo hi"}],
        "config": {
            "Cmd": ["/bin/bash"],
            "Env": env,
            "Labels": {
                "GITHUB_REPOSITORY": "neurodesk/neurocontainers",
                "GITHUB_SHA": github_sha,
                "recipe": "samri",
            },
        },
        "rootfs": {
            "type": "layers",
            "diff_ids": diff_ids or ["sha256:layer"],
        },
    }


def test_image_fingerprint_includes_runtime_config_env() -> None:
    original = _inspect_data(["DEPLOY_PATH=/opt/bru2:/opt/ants"])
    changed = _inspect_data(["DEPLOY_PATH=/opt/bru2:/opt/ants:/opt/miniconda/bin"])

    assert fingerprint_inspect_data(original) != fingerprint_inspect_data(changed)


def test_image_fingerprint_ignores_workflow_identity_labels() -> None:
    original = _inspect_data(["DEPLOY_PATH=/opt/bru2"], github_sha="abc123")
    rebuilt = _inspect_data(["DEPLOY_PATH=/opt/bru2"], github_sha="def456")

    assert fingerprint_inspect_data(original) == fingerprint_inspect_data(rebuilt)


def test_remote_and_local_fingerprints_agree_for_the_same_image() -> None:
    # The cached baseline is read from the registry while the freshly built
    # image is still inspected locally, so both views of one image must hash
    # identically or every build would look changed.
    env = ["DEPLOY_PATH=/opt/bru2"]

    assert fingerprint_remote_config(_remote_config(env)) == fingerprint_inspect_data(
        _inspect_data(env)
    )


def test_remote_fingerprint_ignores_workflow_identity_labels() -> None:
    original = _remote_config(["DEPLOY_PATH=/opt/bru2"], github_sha="abc123")
    rebuilt = _remote_config(["DEPLOY_PATH=/opt/bru2"], github_sha="def456")

    assert fingerprint_remote_config(original) == fingerprint_remote_config(rebuilt)


def test_remote_fingerprint_ignores_build_timestamp_and_history() -> None:
    original = _remote_config(["DEPLOY_PATH=/opt/bru2"])
    rebuilt = dict(original)
    rebuilt["created"] = "2026-06-06T12:00:00Z"
    rebuilt["history"] = [{"created_by": "RUN echo hi"}, {"created_by": "LABEL x=y"}]

    assert fingerprint_remote_config(original) == fingerprint_remote_config(rebuilt)


def test_remote_fingerprint_detects_runtime_config_change() -> None:
    original = _remote_config(["DEPLOY_PATH=/opt/bru2"])
    changed = _remote_config(["DEPLOY_PATH=/opt/bru2:/opt/ants"])

    assert fingerprint_remote_config(original) != fingerprint_remote_config(changed)


def test_remote_fingerprint_detects_filesystem_change() -> None:
    original = _remote_config(["DEPLOY_PATH=/opt/bru2"])
    changed = _remote_config(
        ["DEPLOY_PATH=/opt/bru2"], diff_ids=["sha256:layer", "sha256:other"]
    )

    assert fingerprint_remote_config(original) != fingerprint_remote_config(changed)


def test_docker_only_config_bookkeeping_does_not_change_the_fingerprint() -> None:
    # `docker inspect` renders fields the registry blob omits (and vice versa);
    # none of them affect what the container does at runtime.
    plain = _inspect_data(["DEPLOY_PATH=/opt/bru2"])
    decorated = _inspect_data(["DEPLOY_PATH=/opt/bru2"])
    decorated["Config"].update(  # type: ignore[union-attr]
        {
            "Hostname": "",
            "ArgsEscaped": True,
            "Image": "sha256:parent",
            "Volumes": None,
            "Entrypoint": [],
        }
    )

    assert fingerprint_inspect_data(plain) == fingerprint_inspect_data(decorated)


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("ghcr.io/neurodesk/samri_1.0:latest", ("ghcr.io", "neurodesk/samri_1.0", "latest")),
        ("ghcr.io/neurodesk/samri_1.0", ("ghcr.io", "neurodesk/samri_1.0", "latest")),
        (
            "ghcr.io/neurodesk/samri_1.0@sha256:abc",
            ("ghcr.io", "neurodesk/samri_1.0", "sha256:abc"),
        ),
        ("localhost:5000/samri:dev", ("localhost:5000", "samri", "dev")),
        ("ubuntu:24.04", ("docker.io", "library/ubuntu", "24.04")),
    ],
)
def test_parse_image_reference(reference: str, expected: tuple[str, str, str]) -> None:
    parsed = parse_image_reference(reference)

    assert (parsed.registry, parsed.repository, parsed.reference) == expected


def test_resolve_architecture_maps_workflow_names_to_oci_names() -> None:
    assert resolve_architecture("x86_64") == "amd64"
    assert resolve_architecture("aarch64") == "arm64"
    assert resolve_architecture(None) is None


def test_select_platform_manifest_skips_attestation_entries() -> None:
    manifests = [
        {"digest": "sha256:amd", "platform": {"os": "linux", "architecture": "amd64"}},
        {"digest": "sha256:arm", "platform": {"os": "linux", "architecture": "arm64"}},
        {"digest": "sha256:att", "platform": {"os": "unknown", "architecture": "unknown"}},
    ]

    assert select_platform_manifest(manifests, "arm64") == "sha256:arm"
    assert select_platform_manifest(manifests, None) == "sha256:amd"


def test_select_platform_manifest_reports_missing_architecture() -> None:
    manifests = [
        {"digest": "sha256:amd", "platform": {"os": "linux", "architecture": "amd64"}},
        {"digest": "sha256:arm", "platform": {"os": "linux", "architecture": "arm64"}},
    ]

    with pytest.raises(RegistryError):
        select_platform_manifest(manifests, "ppc64le")


class _FakeRegistry:
    """Registry stub that mimics GHCR's bearer-token handshake."""

    def __init__(self, blobs: dict[str, object], manifests: dict[str, object]) -> None:
        self.blobs = blobs
        self.manifests = manifests
        self.requests: list[str] = []
        self.token_requests = 0

    def __call__(self, method, url, headers, params=None):
        self.requests.append(url)
        if url.endswith("/token"):
            self.token_requests += 1
            return HttpResponse(200, {}, json.dumps({"token": "registry-token"}).encode())
        if headers.get("Authorization") != "Bearer registry-token":
            return HttpResponse(
                401,
                {
                    "WWW-Authenticate": 'Bearer realm="https://ghcr.io/token",'
                    'service="ghcr.io",scope="repository:neurodesk/samri_1.0:pull"'
                },
            )
        path = url.split("ghcr.io", 1)[1]
        for store in (self.manifests, self.blobs):
            if path in store:
                return HttpResponse(200, {}, json.dumps(store[path]).encode())
        return HttpResponse(404, {}, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')


def _fake_ghcr(config_blob: dict[str, object], multi_arch: bool = False) -> _FakeRegistry:
    repo = "/v2/neurodesk/samri_1.0"
    manifest = {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"digest": "sha256:config"},
        "layers": [{"digest": "sha256:layerblob", "size": 12345678}],
    }
    manifests: dict[str, object] = {f"{repo}/manifests/sha256:amd": manifest}
    if multi_arch:
        manifests[f"{repo}/manifests/latest"] = {
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {"digest": "sha256:att", "platform": {"os": "unknown", "architecture": "unknown"}},
                {"digest": "sha256:amd", "platform": {"os": "linux", "architecture": "amd64"}},
            ],
        }
    else:
        manifests[f"{repo}/manifests/latest"] = manifest
    return _FakeRegistry({f"{repo}/blobs/sha256:config": config_blob}, manifests)


def test_remote_fingerprint_reads_config_without_fetching_layers() -> None:
    env = ["DEPLOY_PATH=/opt/bru2"]
    registry = _fake_ghcr(_remote_config(env))

    fingerprint = remote_fingerprint(
        "ghcr.io/neurodesk/samri_1.0:latest", "x86_64", transport=registry
    )

    assert fingerprint == fingerprint_inspect_data(_inspect_data(env))
    assert registry.token_requests == 1
    assert not any("sha256:layerblob" in url for url in registry.requests)


def test_remote_fingerprint_selects_the_requested_architecture() -> None:
    registry = _fake_ghcr(_remote_config(["DEPLOY_PATH=/opt/bru2"]), multi_arch=True)

    remote_fingerprint("ghcr.io/neurodesk/samri_1.0:latest", "x86_64", transport=registry)

    assert any(url.endswith("/manifests/sha256:amd") for url in registry.requests)


def test_remote_fingerprint_raises_image_not_found_for_missing_latest() -> None:
    registry = _fake_ghcr(_remote_config(["DEPLOY_PATH=/opt/bru2"]))

    with pytest.raises(ImageNotFound):
        remote_fingerprint("ghcr.io/neurodesk/other:latest", "x86_64", transport=registry)


def test_registry_client_reports_server_errors() -> None:
    def transport(method, url, headers, params=None):
        return HttpResponse(500, {}, b"boom")

    client = RegistryClient("ghcr.io", transport=transport)

    with pytest.raises(RegistryError):
        client.get_image_config("neurodesk/samri_1.0", "latest")


def test_cli_allow_missing_prints_empty_baseline(monkeypatch, capsys) -> None:
    def missing(image_ref, architecture=None, transport=None):
        raise ImageNotFound(image_ref)

    monkeypatch.setattr("builder.image_fingerprint.remote_fingerprint", missing)

    exit_code = main(
        ["--remote", "--allow-missing", "--architecture", "x86_64", "ghcr.io/x/y:latest"]
    )

    assert exit_code == 0
    assert capsys.readouterr().out.strip() == ""


def test_cli_fails_on_registry_errors(monkeypatch) -> None:
    def broken(image_ref, architecture=None, transport=None):
        raise RegistryError("registry unavailable")

    monkeypatch.setattr("builder.image_fingerprint.remote_fingerprint", broken)

    assert main(["--remote", "--allow-missing", "ghcr.io/x/y:latest"]) == 1
