"""Published bundles must be complete and ABI-consistent before planning."""

import hashlib
import io
import zipfile
from types import SimpleNamespace

import pytest

from builder import update_bundles as bundles


class Response:
    def __init__(self, data=b"", url="https://example.org/artifact"):
        self.data = data
        self.url = url

    def raise_for_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, size):
        yield self.data


class Session:
    def __init__(self, data):
        self.data = data
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return Response(self.data, url)


def slicer_package(version, revision, pre_release=False):
    return {"_id": "1" * 24, "meta": {"app_id": "0" * 24, "version": version, "pre_release": pre_release,
            "revision": revision, "os": "linux", "arch": "amd64"}}


def slicer_lists(monkeypatch, *, extension_revision="34045", extensions=1, query_revision="34045",
                 releases=("5.9.0", "5.10.0", "5.11.0rc1", "nightly"), packages=None):
    packages = packages if packages is not None else {"5.10.0": [slicer_package("5.10.0", "34045")]}
    extension = {"_id": "2" * 24, "meta": {"app_id": "0" * 24, "app_revision": extension_revision,
                 "baseName": "MONAILabel", "os": "linux", "arch": "amd64"}}
    calls = []

    def listing(session, url, **params):
        calls.append((url, params))
        if url.endswith("/release"):
            return [{"name": name} for name in releases]
        if url.endswith("/package"):
            return packages[params["release_id_or_name"]]
        assert params["app_revision"] == query_revision
        return [extension] * extensions

    monkeypatch.setattr(bundles, "_json_list", listing)
    return calls


def test_slicer_resolves_application_extension_and_abi_together(monkeypatch):
    slicer_lists(monkeypatch)
    downloads = []
    monkeypatch.setattr(bundles, "_download", lambda session, url, **kwargs: (downloads.append(url) or "a" * 64, 42))
    result = bundles._slicer({"app_id": "0" * 24, "extension": "MONAILabel"}, None)
    assert result.version == "5.10.0"
    assert result.metadata["revision"] == "34045"
    assert result.metadata["abi"] == "5.10"
    assert result.metadata["extension_item"] == "2" * 24
    assert result.metadata["extension_sha256"] == "a" * 64
    assert len(downloads) == 2


def test_slicer_falls_back_to_the_newest_promoted_release(monkeypatch):
    # Kitware publishes a stable release name before its Linux package exists and again
    # before promoting that package, so the newest promoted build has to win rather than
    # failing the whole update run.
    calls = slicer_lists(
        monkeypatch,
        query_revision="34627",
        extension_revision="34627",
        releases=("5.10.0", "5.12.3", "5.12.4", "5.12.5"),
        packages={"5.12.5": [],
                  "5.12.4": [slicer_package("5.12.4", "34645", pre_release=True)],
                  "5.12.3": [slicer_package("5.12.3", "34627")]},
    )
    monkeypatch.setattr(bundles, "_download", lambda session, url, **kwargs: ("a" * 64, 42))
    result = bundles._slicer({"app_id": "0" * 24, "extension": "MONAILabel"}, None)
    assert result.version == "5.12.3"
    assert result.metadata["revision"] == "34627"
    assert result.metadata["abi"] == "5.12"
    assert [params["release_id_or_name"] for url, params in calls
            if url.endswith("/package")] == ["5.12.5", "5.12.4", "5.12.3"]


def test_slicer_refuses_an_ambiguous_package_listing(monkeypatch):
    # Skipping empty listings must not also wave through a release publishing two Linux builds.
    slicer_lists(
        monkeypatch,
        releases=("5.12.4",),
        packages={"5.12.4": [slicer_package("5.12.4", "34645"), slicer_package("5.12.4", "34646")]},
    )
    monkeypatch.setattr(bundles, "_download", lambda *args, **kwargs: pytest.fail("ambiguous bundle downloaded"))
    with pytest.raises(ValueError, match="expected one Slicer Linux package"):
        bundles._slicer({"app_id": "0" * 24, "extension": "MONAILabel"}, None)


def test_slicer_refuses_a_listing_with_no_promoted_release(monkeypatch):
    slicer_lists(
        monkeypatch,
        releases=("5.12.4",),
        packages={"5.12.4": [slicer_package("5.12.4", "34645", pre_release=True)]},
    )
    monkeypatch.setattr(bundles, "_download", lambda *args, **kwargs: pytest.fail("pre-release bundle downloaded"))
    with pytest.raises(ValueError, match="promoted"):
        bundles._slicer({"app_id": "0" * 24, "extension": "MONAILabel"}, None)


@pytest.mark.parametrize("revision,count", [("99999", 1), ("34045", 0), ("34045", 2)])
def test_slicer_refuses_incomplete_or_mismatched_bundle_before_downloading(monkeypatch, revision, count):
    slicer_lists(monkeypatch, extension_revision=revision, extensions=count)
    monkeypatch.setattr(bundles, "_download", lambda *args, **kwargs: pytest.fail("incoherent bundle downloaded"))
    with pytest.raises(ValueError):
        bundles._slicer({"app_id": "0" * 24, "extension": "MONAILabel"}, None)


def test_download_verifies_upstream_digest_and_size():
    data = b"actual model bytes"
    session = Session(data)
    assert bundles._download(session, "https://example.org/model", expected_sha256=hashlib.sha256(data).hexdigest(), expected_size=len(data)) == (hashlib.sha256(data).hexdigest(), len(data))
    with pytest.raises(ValueError, match="SHA256"):
        bundles._download(session, "https://example.org/model", expected_sha256="0" * 64)
    with pytest.raises(ValueError, match="size"):
        bundles._download(session, "https://example.org/model", expected_size=1)


def test_freesurfer_resolves_annex_hashed_directory_and_verifies_key(monkeypatch):
    key = "SHA256E-s123--" + "a" * 64 + ".0.h5"
    pointer = f"../.git/annex/objects/zP/vP/{key}/{key}".encode()
    seen = []

    def download(session, url, **kwargs):
        seen.append((url, kwargs))
        return "a" * 64, 123

    monkeypatch.setattr(bundles, "_download", download)
    url, digest = bundles._model(Session(pointer), "https://raw.githubusercontent.com/freesurfer/freesurfer/v8.2.0/model.h5")
    hashed = hashlib.md5(key.encode()).hexdigest()
    assert f"/{hashed[:3]}/{hashed[3:6]}/{key}/{key}" in url
    assert digest == "a" * 64
    assert seen[0][1] == {"expected_sha256": "a" * 64, "expected_size": 123}


def test_freesurfer_rejects_malformed_annex_pointer():
    with pytest.raises(ValueError, match="pointer"):
        bundles._model(Session(b"../.git/annex/objects/aa/bb/not-a-key/not-a-key"), "https://example.org/model")


def test_freesurfer_uses_one_release_for_script_and_models(monkeypatch):
    monkeypatch.setattr(bundles, "latest_version", lambda *args: SimpleNamespace(version="8.2.0", tag="v8.2.0", url="https://github.com/freesurfer/freesurfer/tree/v8.2.0"))
    seen = []
    monkeypatch.setattr(bundles, "_download", lambda session, url: (seen.append(url) or "a" * 64, 1))
    monkeypatch.setattr(bundles, "_model", lambda session, url: (seen.append(url) or "https://example.org/model", "b" * 64))
    result = bundles._freesurfer({"script": "mri_synthseg/mri_synthseg", "models": {"model": "mri_synthseg/model.h5"}}, None, None)
    assert all("/v8.2.0/" in url for url in seen)
    assert result.metadata["model_sha256"] == "b" * 64
    assert result.metadata["version"] == "8.2.0"


@pytest.mark.parametrize("config", [
    {"method": "slicer_release", "app_id": "bad", "extension": "MONAILabel"},
    {"method": "slicer_release", "app_id": "0" * 24, "extension": "../bad"},
    {"method": "freesurfer_release", "script": "../script", "models": {"a": "model.h5"}},
    {"method": "freesurfer_release", "script": "script", "models": {}},
    {"method": "freesurfer_release", "script": "script", "models": {"../a": "model.h5"}},
])
def test_bundle_configuration_is_validated_before_network(config):
    with pytest.raises(ValueError):
        bundles.validate_bundle(config)


@pytest.mark.parametrize("text,expected", [
    ("Verify version 9.12 (R2022a) of MATLAB Runtime is installed", "2022a"),
    ("MATLAB Runtime(R2023b) is installed", "2023b"),
    ("MATLAB Runtime (R2023b), previously R2022a", None),
    ("This software needs a MATLAB runtime", None),
])
def test_compiler_readme_resolves_exact_runtime_or_rejects_ambiguity(text, expected):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("application/readme.txt", text)
    session = Session(stream.getvalue())
    if expected is None:
        with pytest.raises(ValueError, match="one runtime"):
            bundles.read_matlab_artifact(session, "https://example.org/app.zip", "application/readme.txt")
    else:
        result = bundles.read_matlab_artifact(session, "https://example.org/app.zip", "application/readme.txt")
        assert result[0] == hashlib.sha256(stream.getvalue()).hexdigest()
        assert result[3] == expected


def test_compiler_readme_member_must_be_unique_and_bounded():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("readme.txt", "x" * 65537)
    with pytest.raises(ValueError, match="bounded"):
        bundles.read_matlab_artifact(Session(stream.getvalue()), "https://example.org/app.zip", "readme.txt")
