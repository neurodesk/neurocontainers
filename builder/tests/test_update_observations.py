from __future__ import annotations

import base64
import gzip
import hashlib
import subprocess
from dataclasses import FrozenInstanceError
from unittest.mock import Mock

import pytest
import requests

from builder import update_bundles, update_observations, update_records
from builder.update_sources import UpstreamRelease


def response(
    *,
    data: object | None = None,
    content: bytes = b"",
    headers: dict[str, str] | None = None,
    status: int = 200,
    url: str = "https://example.test/result",
) -> Mock:
    result = Mock()
    result.status_code = status
    result.headers = headers or {}
    result.content = content
    result.url = url
    result.json.return_value = data
    result.iter_content.return_value = [
        content[index : index + 3] for index in range(0, len(content), 3)
    ]
    if status >= 400:
        result.raise_for_status.side_effect = requests.HTTPError(str(status))
    return result


@pytest.fixture
def public_session(monkeypatch: pytest.MonkeyPatch) -> Mock:
    session = Mock()
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=session)
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(update_observations.requests, "Session", factory)
    return session


def test_observation_is_frozen() -> None:
    observed = update_observations.SourceObservation("1.2", "https://example.test")

    with pytest.raises(FrozenInstanceError):
        observed.value = "1.3"  # type: ignore[misc]


def test_existing_version_provider_delegates_and_preserves_raw_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    github_session = Mock()
    latest = Mock(
        return_value=UpstreamRelease(
            "1.10", "release/v1.10", "https://github.com/org/tool/tree/release%2Fv1.10"
        )
    )
    monkeypatch.setattr(update_observations, "latest_version", latest)
    config = {"method": "github_tags", "repo": "org/tool"}

    observed = update_observations.observe_source(config, github_session)

    assert observed == update_observations.SourceObservation(
        value="1.10",
        version="1.10",
        tag="release/v1.10",
        url="https://github.com/org/tool/tree/release%2Fv1.10",
    )
    latest.assert_called_once_with(config, github_session)


def test_existing_version_provider_rejects_empty_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(update_observations, "latest_version", Mock(return_value=None))

    with pytest.raises(ValueError, match="no stable version"):
        update_observations.observe_source(
            {"method": "pypi", "package": "demo"}, Mock()
        )


def test_github_commit_reads_version_file_at_observed_sha() -> None:
    session = Mock()
    sha = "a" * 40
    session.get.side_effect = [
        response(data={"sha": sha}),
        response(
            data={
                "encoding": "base64",
                "content": base64.b64encode(b"1.4.0\n").decode(),
            }
        ),
    ]

    observed = update_observations.observe_source(
        {
            "method": "github_commit",
            "repo": "org/tool",
            "ref": "main",
            "version_file": "version.txt",
        },
        session,
    )

    assert observed.value == sha
    assert observed.metadata == {"ref": "main", "version": "1.4.0"}
    assert session.get.call_args_list[1].kwargs == {
        "params": {"ref": sha},
        "timeout": 30,
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"encoding": "base64", "content": "MS40LjA=!!!!"},
        {"encoding": "base64", "content": base64.b64encode(b"\xff").decode()},
        {"encoding": "base64", "content": base64.b64encode(b"1" * 257).decode()},
        {"encoding": "none", "content": "1.4.0"},
    ],
)
def test_github_commit_rejects_non_plain_version_file(payload: dict) -> None:
    session = Mock()
    session.get.side_effect = [
        response(data={"sha": "a" * 40}),
        response(data=payload),
    ]

    with pytest.raises(ValueError, match="GitHub version file"):
        update_observations.observe_source(
            {
                "method": "github_commit",
                "repo": "org/tool",
                "version_file": "version.txt",
            },
            session,
        )


@pytest.mark.parametrize(
    "path", ["/version.txt", "../version.txt", "releases/../../version.txt"]
)
def test_github_commit_rejects_unsafe_version_file_path(path: str) -> None:
    with pytest.raises(ValueError, match="version_file"):
        update_observations.validate_source(
            {"method": "github_commit", "repo": "org/tool", "version_file": path}
        )


def test_zenodo_observation_uses_public_session_and_latest_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Mock()
    session.get.return_value = response(
        data={
            "id": 21929873,
            "files": [{"key": "model.pth"}],
            "metadata": {"version": "v1.4"},
        }
    )
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=session)
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(update_records.requests, "Session", factory)
    github_session = Mock()

    observed = update_observations.observe_source(
        {"method": "zenodo", "record": "19976940"}, github_session
    )

    assert observed == update_observations.SourceObservation(
        "21929873",
        "https://zenodo.org/records/21929873",
        metadata={"version": "1.4"},
    )
    session.get.assert_called_once_with(
        "https://zenodo.org/api/records/19976940/versions/latest", timeout=30
    )
    github_session.get.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {"id": 2, "files": [], "metadata": {"version": "1.0"}},
        {"id": 2, "files": [{}], "metadata": []},
        {"id": 2, "files": [{}], "metadata": {"version": "release one"}},
    ],
)
def test_zenodo_rejects_incomplete_record_payload(
    monkeypatch: pytest.MonkeyPatch, payload: dict
) -> None:
    session = Mock()
    session.get.return_value = response(data=payload)
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=session)
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(update_records.requests, "Session", factory)

    with pytest.raises(ValueError, match="Zenodo"):
        update_observations.observe_source(
            {"method": "zenodo", "record": "1"}, Mock()
        )


def test_bundle_provider_dispatches_before_generic_source_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {
        "method": "slicer_release",
        "app_id": "a" * 24,
        "extension": "SlicerDMRI",
    }
    expected = update_observations.SourceObservation(
        "https://download.example/slicer.tar.gz",
        "https://data.example/app",
        version="5.8.1",
    )
    validate = Mock()
    observe = Mock(return_value=expected)
    monkeypatch.setattr(update_observations, "validate_bundle", validate)
    monkeypatch.setattr(update_observations, "observe_bundle", observe)
    github_session = Mock()

    assert update_observations.observe_source(config, github_session) == expected

    validate.assert_called_once_with(config)
    observe.assert_called_once_with(config, github_session, None)


@pytest.mark.parametrize(
    "config",
    [
        {"method": "zenodo", "record": "0"},
        {"method": "zenodo", "record": "123", "extra": True},
        {"method": "github_commit", "repo": "org/tool", "extra": "x"},
        {"method": "github_commit", "repo": "https://github.com/org/tool"},
        {"method": "github_commit", "repo": "org/tool", "ref": "bad ref"},
        {
            "method": "git_commit",
            "url": "http://gitlab.example/org/tool.git",
            "ref": "main",
        },
        {
            "method": "git_commit",
            "url": "https://github.com/org/tool.git",
            "ref": "main",
        },
        {
            "method": "git_commit",
            "url": "https://gitlab.example/org/tool.git",
            "ref": "refs/*",
        },
        {
            "method": "oci_digest",
            "image": "registry.example/org/tool:latest",
            "tag": "latest",
        },
        {"method": "http_digest", "url": "https://user:secret@example.test/file"},
        {
            "method": "artifact_listing",
            "url": "https://example.test",
            "download_base": "https://example.test/files/",
        },
        {
            "method": "artifact_listing",
            "url": "https://example.test",
            "download_base": "http://example.test/files/",
            "version_regex": r"tool-(?P<version>\d+)",
        },
        {
            "method": "artifact_listing",
            "url": "https://example.test",
            "download_base": "https://example.test/files/",
            "version_regex": r"tool-(?P<version>\d+)",
            "listing_format": "custom",
        },
        {
            "method": "http_digest",
            "url": "https://example.test/tool.zip",
            "matlab_readme": "../readme.txt",
        },
        {
            "method": "apt",
            "package": "Bad_Package",
            "urls": ["https://deb.example/Packages.gz"],
        },
        {"method": "apt", "package": "demo", "urls": []},
        {
            "method": "apt",
            "package": "demo",
            "urls": ["http://deb.example/Packages.gz"],
        },
    ],
)
def test_validate_source_rejects_unsafe_or_incomplete_configs(config: dict) -> None:
    with pytest.raises(ValueError):
        update_observations.validate_source(config)


def test_github_commit_resolves_head_and_proves_forward_ancestry() -> None:
    current = "1" * 40
    latest = "2" * 40
    github_session = Mock()
    github_session.get.side_effect = [
        response(
            data={
                "sha": latest,
                "html_url": f"https://github.com/org/tool/commit/{latest}",
            }
        ),
        response(data={"status": "ahead", "ahead_by": 3}),
    ]

    observed = update_observations.observe_source(
        {"method": "github_commit", "repo": "org/tool", "ref": "HEAD"},
        github_session,
        current=current,
    )

    assert observed.value == latest
    assert observed.url == f"https://github.com/org/tool/commit/{latest}"
    assert observed.metadata == {"ref": "HEAD", "ancestry": "ahead", "ahead_by": 3}
    assert github_session.get.call_args_list[0].args[0].endswith("/commits/HEAD")
    assert (
        f"/compare/{current}...{latest}"
        in github_session.get.call_args_list[1].args[0]
    )


def test_github_commit_rejects_diverged_history() -> None:
    github_session = Mock()
    github_session.get.side_effect = [
        response(data={"sha": "2" * 40}),
        response(data={"status": "diverged", "ahead_by": 1}),
    ]

    with pytest.raises(ValueError, match="not a descendant"):
        update_observations.observe_source(
            {"method": "github_commit", "repo": "org/tool", "ref": "main"},
            github_session,
            current="1" * 40,
        )


def test_git_commit_uses_bounded_argument_vector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=[], returncode=0, stdout=f"{'a' * 40}\trefs/heads/main\n", stderr=""
    )
    run = Mock(return_value=completed)
    monkeypatch.setattr(update_observations.subprocess, "run", run)

    observed = update_observations.observe_source(
        {
            "method": "git_commit",
            "url": "https://gitlab.example/org/tool.git",
            "ref": "refs/heads/main",
        },
        Mock(),
    )

    assert observed.value == "a" * 40
    assert observed.url == "https://gitlab.example/org/tool.git#refs/heads/main"
    run.assert_called_once()
    assert run.call_args.args[0][:3] == ["git", "ls-remote", "--exit-code"]
    assert run.call_args.kwargs["timeout"] == 30
    assert "shell" not in run.call_args.kwargs


def test_http_digest_streams_on_public_session_without_github_credentials(
    public_session: Mock,
) -> None:
    payload = b"stream this payload in chunks"
    public_session.get.return_value = response(
        content=payload, url="https://cdn.example/tool.bin"
    )
    github_session = Mock()
    github_session.headers = {"Authorization": "Bearer github-secret"}

    observed = update_observations.observe_source(
        {"method": "http_digest", "url": "https://cdn.example/tool.bin"},
        github_session,
    )

    assert observed.value == hashlib.sha256(payload).hexdigest()
    assert observed.version is None
    assert observed.metadata == {"sha256": observed.value, "size": len(payload)}
    github_session.get.assert_not_called()
    public_session.get.assert_called_once_with(
        "https://cdn.example/tool.bin", stream=True, timeout=60
    )


def test_http_digest_extracts_bounded_matlab_runtime_metadata(
    public_session: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    read = Mock(
        return_value=(
            "a" * 64,
            123,
            "https://signed.example/tool.zip?token=secret",
            "2023b",
        )
    )
    monkeypatch.setattr(update_bundles, "read_matlab_artifact", read)
    url = "https://downloads.example/tool.zip"

    observed = update_observations.observe_source(
        {
            "method": "http_digest",
            "url": url,
            "matlab_readme": "application/readme.txt",
        },
        Mock(),
    )

    assert observed.url == url
    assert observed.metadata == {
        "sha256": "a" * 64,
        "size": 123,
        "runtime": "2023b",
    }
    read.assert_called_once_with(public_session, url, "application/readme.txt")


def test_oci_digest_uses_anonymous_bearer_session_and_keeps_index_digest(
    public_session: Mock,
) -> None:
    digest = "sha256:" + "b" * 64
    challenge = response(
        status=401,
        headers={
            "WWW-Authenticate": (
                'Bearer realm="https://auth.example/token",'
                'service="registry.example",scope="repository:org/tool:pull"'
            )
        },
    )
    public_session.get.side_effect = [
        challenge,
        response(data={"token": "anonymous-registry-token"}),
        response(
            content=b'{"mediaType":"application/vnd.oci.image.index.v1+json"}',
            headers={"Docker-Content-Digest": digest},
            url="https://registry.example/v2/org/tool/manifests/latest",
        ),
    ]
    github_session = Mock()
    github_session.headers = {"Authorization": "Bearer github-secret"}

    observed = update_observations.observe_source(
        {
            "method": "oci_digest",
            "image": "registry.example/org/tool",
            "tag": "latest",
        },
        github_session,
    )

    assert observed.value == digest
    assert observed.metadata == {"image": "registry.example/org/tool", "tag": "latest"}
    github_session.get.assert_not_called()
    assert public_session.get.call_args_list[1].kwargs["params"] == {
        "service": "registry.example",
        "scope": "repository:org/tool:pull",
    }
    assert (
        public_session.get.call_args_list[2].kwargs["headers"]["Authorization"]
        == "Bearer anonymous-registry-token"
    )


def test_artifact_listing_selects_stable_swift_object_and_hashes_download(
    public_session: Mock,
) -> None:
    artifact = b"compiled binary"
    public_session.get.side_effect = [
        response(
            data=[
                {"name": "tool-1.9-mcr2020b.tar.gz"},
                {"name": "tool-2.0rc1-mcr2024a.tar.gz"},
                {"name": "tool-1.10-mcr2024a.tar.gz"},
            ],
            headers={"Content-Type": "application/json"},
        ),
        response(
            content=artifact,
            url="https://objects.example/build/tool-1.10-mcr2024a.tar.gz",
        ),
    ]

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": "https://objects.example/build?format=json",
            "download_base": "https://objects.example/build/",
            "version_regex": (
                r"tool-(?P<version>\d+\.\d+(?:rc\d+)?)-"
                r"mcr(?P<runtime>\d+[ab])\.tar\.gz"
            ),
        },
        Mock(),
    )

    expected_url = "https://objects.example/build/tool-1.10-mcr2024a.tar.gz"
    assert observed.value == expected_url
    assert observed.version == "1.10"
    assert observed.tag == "tool-1.10-mcr2024a.tar.gz"
    assert observed.metadata == {
        "version": "1.10",
        "runtime": "2024a",
        "sha256": hashlib.sha256(artifact).hexdigest(),
        "size": len(artifact),
    }


def test_artifact_listing_reads_html_links(public_session: Mock) -> None:
    public_session.get.side_effect = [
        response(
            content=(
                b'<a href="downloads/tool-1.2.zip">old</a>'
                b'<a href="downloads/tool-1.3.zip">new</a>'
            ),
            headers={"Content-Type": "text/html"},
        ),
        response(content=b"new", url="https://vendor.example/downloads/tool-1.3.zip"),
    ]

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": "https://vendor.example/releases/",
            "download_base": "https://vendor.example/",
            "version_regex": r"downloads/tool-(?P<version>\d+\.\d+)\.zip",
        },
        Mock(),
    )

    assert observed.value == "https://vendor.example/downloads/tool-1.3.zip"
    assert observed.version == "1.3"


def test_artifact_listing_can_match_anchor_text_and_compare_year_letter_versions(
    public_session: Mock,
) -> None:
    public_session.get.side_effect = [
        response(
            content=(
                b'<a href="download/490/">Standalone R2024b</a>'
                b'<a href="download/491/">Standalone R2025a</a>'
            ),
            headers={"Content-Type": "text/html"},
        ),
        response(content=b"standalone", url="https://nitrc.example/download/491/"),
    ]

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": "https://nitrc.example/projects/tool/",
            "download_base": "https://nitrc.example/",
            "version_regex": r"Standalone R(?P<version>\d{4}[ab])",
            "version_scheme": "year_letter",
        },
        Mock(),
    )

    assert observed.value == "https://nitrc.example/download/491/"
    assert observed.version == "2025a"
    assert observed.tag == "Standalone R2025a"


def test_artifact_listing_preserves_underscore_version_while_sorting_numerically(
    public_session: Mock,
) -> None:
    public_session.get.side_effect = [
        response(
            data=[{"name": "mipav_11_9_0.zip"}, {"name": "mipav_11_10_0.zip"}],
            headers={"Content-Type": "application/json"},
        ),
        response(content=b"mipav", url="https://objects.example/mipav_11_10_0.zip"),
    ]

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": "https://objects.example/?format=json",
            "download_base": "https://objects.example/",
            "version_regex": r"mipav_(?P<version>\d+_\d+_\d+)\.zip",
        },
        Mock(),
    )

    assert observed.version == "11_10_0"
    assert observed.metadata["version"] == "11_10_0"


def test_artifact_listing_resolves_bounded_girder_item_download(
    public_session: Mock,
) -> None:
    artifact = b"slicersalt"
    public_session.get.side_effect = [
        response(
            data=[
                {
                    "name": "SlicerSALT-6.0.0-linux-amd64.tar.gz",
                    "_id": "68152a78f9c66e8c473d38c8",
                }
            ],
            headers={"Content-Type": "application/json"},
        ),
        response(
            content=artifact,
            url=(
                "https://data.kitware.com/api/v1/item/"
                "68152a78f9c66e8c473d38c8/download"
            ),
        ),
    ]

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": (
                "https://data.kitware.com/api/v1/item?"
                "folderId=5898b7ef8d777f07219fcb14&limit=0"
            ),
            "download_base": "https://data.kitware.com/api/v1/",
            "version_regex": (
                r"SlicerSALT-(?P<version>\d+\.\d+\.\d+)-"
                r"linux-amd64\.tar\.gz"
            ),
            "listing_format": "girder",
        },
        Mock(),
    )

    assert observed.value == (
        "https://data.kitware.com/api/v1/item/"
        "68152a78f9c66e8c473d38c8/download"
    )
    assert observed.version == "6.0.0"
    assert observed.metadata["sha256"] == hashlib.sha256(artifact).hexdigest()


def test_artifact_listing_rejects_invalid_girder_item_id(
    public_session: Mock,
) -> None:
    public_session.get.return_value = response(
        data=[{"name": "tool-1.0.zip", "_id": "../../credentials"}],
        headers={"Content-Type": "application/json"},
    )

    with pytest.raises(ValueError, match="24-hex"):
        update_observations.observe_source(
            {
                "method": "artifact_listing",
                "url": "https://data.example/api/v1/item?folderId=abc",
                "download_base": "https://data.example/api/v1/",
                "version_regex": r"tool-(?P<version>\d+\.\d+)\.zip",
                "listing_format": "girder",
            },
            Mock(),
        )


def test_artifact_listing_extracts_matlab_runtime_from_selected_file(
    public_session: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    public_session.get.return_value = response(
        data=[{"name": "tool-2.1.zip"}],
        headers={"Content-Type": "application/json"},
    )
    read = Mock(
        return_value=(
            "b" * 64,
            456,
            "https://objects.example/tool-2.1.zip",
            "2022a",
        )
    )
    monkeypatch.setattr(update_bundles, "read_matlab_artifact", read)

    observed = update_observations.observe_source(
        {
            "method": "artifact_listing",
            "url": "https://objects.example/?format=json",
            "download_base": "https://objects.example/",
            "version_regex": r"tool-(?P<version>\d+\.\d+)\.zip",
            "matlab_readme": "tool/readme.txt",
        },
        Mock(),
    )

    assert observed.version == "2.1"
    assert observed.metadata["runtime"] == "2022a"
    assert observed.metadata["sha256"] == "b" * 64
    read.assert_called_once_with(
        public_session,
        "https://objects.example/tool-2.1.zip",
        "tool/readme.txt",
    )


def test_apt_selects_latest_distribution_version_from_compressed_indexes(
    public_session: Mock,
) -> None:
    packages = b"""Package: demo
Version: 1:2.0-1ubuntu1

Package: other
Version: 99

Package: demo
Version: 1:2.0-1ubuntu2
"""
    public_session.get.return_value = response(content=gzip.compress(packages))

    observed = update_observations.observe_source(
        {
            "method": "apt",
            "package": "demo",
            "urls": [
                "https://archive.example/dists/noble/main/"
                "binary-amd64/Packages.gz"
            ],
        },
        Mock(),
    )

    assert observed.value == "1:2.0-1ubuntu2"
    assert observed.version is None
    assert observed.url.endswith("Packages.gz")
    assert observed.metadata == {"package": "demo"}


def test_apt_reports_missing_package(public_session: Mock) -> None:
    public_session.get.return_value = response(
        content=b"Package: other\nVersion: 1.0\n"
    )

    with pytest.raises(ValueError, match="package demo was not found"):
        update_observations.observe_source(
            {
                "method": "apt",
                "package": "demo",
                "urls": ["https://archive.example/Packages"],
            },
            Mock(),
        )
