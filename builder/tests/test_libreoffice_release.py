"""Stable LibreOffice builds must map to identical archived binaries."""

from unittest.mock import MagicMock

import pytest
import requests
import yaml

from builder import update_bundles as bundles
from builder.update_observations import observe_source
from builder.update_plan import plan_sources


@pytest.fixture
def published(monkeypatch):
    stable = bundles.LIBREOFFICE_DOWNLOAD + "stable/"
    source = bundles.LIBREOFFICE_DOWNLOAD + "src/26.8.0/"
    pages = {
        stable: '<a href="25.8.7/">old</a><a href="26.8.0/">stable</a>'
                '<a href="27.0.0rc1/">candidate</a>',
        source: '<a href="libreoffice-26.8.0.3.tar.xz">source</a>'
                '<a href="libreoffice-26.8.1.1.tar.xz">future candidate</a>'
                '<a href="libreoffice-dictionaries-26.8.0.3.tar.xz">other</a>',
    }
    for arch, file_arch, digest in [("x86_64", "x86-64", "a" * 64),
                                    ("aarch64", "aarch64", "b" * 64)]:
        for version, base in [("26.8.0", stable),
                              ("26.8.0.3", bundles.LIBREOFFICE_ARCHIVE)]:
            name = f"LibreOffice_{version}_Linux_{file_arch}_deb.tar.gz"
            pages[f"{base}{version}/deb/{arch}/{name}.sha256"] = f"{digest}  {name}\n"
    session = MagicMock()
    session.__enter__.return_value = session

    def get(url, **kwargs):
        response = requests.Response()
        response.url = url
        response.status_code = 200 if url in pages else 404
        response._content = pages.get(url, "missing").encode()
        return response

    session.get.side_effect = get
    monkeypatch.setattr(bundles.requests, "Session", lambda: session)
    return pages, session


def test_resolves_one_stable_build_and_both_architectures(published):
    _, session = published
    github = MagicMock()
    result = observe_source({"method": "libreoffice_release"}, github)
    assert result.value == result.version == "26.8.0.3"
    assert result.url == bundles.LIBREOFFICE_ARCHIVE + "26.8.0.3/"
    assert result.metadata == {"x86_64_sha256": "a" * 64, "aarch64_sha256": "b" * 64}
    github.get.assert_not_called()
    assert all("26.8.1.1" not in call.args[0] for call in session.get.call_args_list)


@pytest.mark.parametrize("source", ["", '<a href="libreoffice-26.8.0.4.tar.xz">other</a>'])
def test_rejects_missing_or_ambiguous_released_build(published, source):
    pages, session = published
    url = bundles.LIBREOFFICE_DOWNLOAD + "src/26.8.0/"
    pages[url] = pages[url] + source if source else ""
    with pytest.raises(ValueError, match="one released build"):
        bundles._libreoffice(session)


@pytest.mark.parametrize("failure", ["missing", "different", "malformed", "wrong_filename"])
def test_refuses_incomplete_or_mismatched_archived_bundle(published, failure):
    pages, session = published
    url = next(url for url in pages if "/old/" in url and "/aarch64/" in url)
    if failure == "missing":
        del pages[url]
        error = requests.HTTPError
    else:
        error = ValueError
        if failure == "different":
            pages[url] = pages[url].replace("b" * 64, "c" * 64)
        elif failure == "malformed":
            pages[url] = "not a checksum"
        else:
            pages[url] = pages[url].replace("Linux_aarch64", "Linux_x86-64")
    with pytest.raises(error):
        bundles._libreoffice(session)


def test_configuration_rejects_archive_or_release_overrides():
    with pytest.raises(ValueError, match="unsupported libreoffice_release fields"):
        bundles.validate_bundle({"method": "libreoffice_release", "version": "27.0.0.1"})


def test_plan_advances_build_digests_and_suite_together_and_then_is_idempotent(published, tmp_path):
    recipe = {
        "name": "office", "version": "26.2.4",
        "variables": {"upstream_version": "26.2.4.2", "x86_64_sha256": "c" * 64,
                      "aarch64_sha256": "d" * 64},
        "files": [
            {"name": arch, "url": "https://example.org/{{ context.upstream_version }}/" + arch,
             "sha256": "{{ context." + arch + "_sha256 }}"}
            for arch in ("x86_64", "aarch64")
        ],
        "auto_update": {"method": "sources", "sources": [{
            "id": "office", "method": "libreoffice_release",
            "target": {"variable": "upstream_version", "fulltest_variable": "upstream_version",
                       "variables": {"x86_64_sha256": "x86_64_sha256",
                                     "aarch64_sha256": "aarch64_sha256"}},
        }]},
    }
    path = tmp_path / "build.yaml"
    path.write_text(yaml.safe_dump(recipe))
    suite = path.with_name("fulltest.yaml")
    suite.write_text(yaml.safe_dump({"name": "office", "version": "26.2.4",
                                   "upstream_version": "26.2.4.2", "tests": []}))
    plan = plan_sources(path)
    assert plan is not None
    for patch in plan.patches:
        patch.path.write_text(patch.after)
    updated = yaml.safe_load(path.read_text())
    assert updated["version"] == "26.2.4.post1"
    assert updated["variables"] == {"upstream_version": "26.8.0.3",
                                    "x86_64_sha256": "a" * 64, "aarch64_sha256": "b" * 64}
    assert yaml.safe_load(suite.read_text())["upstream_version"] == "26.8.0.3"
    assert yaml.safe_load(suite.read_text())["version"] == "26.2.4.post1"
    assert plan_sources(path) is None
