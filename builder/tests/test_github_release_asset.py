"""A release archive's URL, version and bytes must come from the same tag."""

import hashlib
from pathlib import Path
from urllib.parse import urlparse

import pytest
import yaml

from builder import update_bundles
from builder.audit_updates import validate_update_policy
from builder.update_observations import observe_source, validate_source
from builder.update_plan import plan_sources


CONFIG = {
    "method": "github_release_asset",
    "repo": "example/tool",
    "asset": "tool-linux.zip",
}
TAGGED_URL = "https://github.com/example/tool/releases/download/v2.0.0/tool-linux.zip"


class Response:
    def __init__(self, *, document=None, data=b"", url="https://api.github.com/example"):
        self.document = document
        self.data = data
        self.url = url
        self.links = {}

    def raise_for_status(self):
        pass

    def json(self):
        return self.document

    def iter_content(self, size):
        yield self.data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class PublicSession:
    def __init__(self):
        self.headers = {}
        self.payload = b"release two archive"
        self.urls = []

    def mount(self, *args):
        pass

    def get(self, url, **kwargs):
        assert "Authorization" not in self.headers
        self.urls.append(url)
        assert url == TAGGED_URL
        return Response(data=self.payload, url="https://release-assets.githubusercontent.com/asset?sig=temporary")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class GitHubSession:
    def __init__(self, public):
        self.headers = {"Authorization": "Bearer example-test-token"}
        self.public = public
        self.urls = []
        self.releases = [
            {"tag_name": "v1.0.0"},
            {"tag_name": "v2.0.0"},
            {"tag_name": "v3.0.0rc1", "prerelease": True},
            {"tag_name": "v9.0.0", "draft": True},
        ]
        self.asset_overrides = {}
        self.asset_count = 1
        self.published_tag = "v2.0.0"

    def get(self, url, **kwargs):
        self.urls.append(url)
        if url.endswith("/releases?per_page=100"):
            return Response(document=self.releases)
        assert url == "https://api.github.com/repos/example/tool/releases/tags/v2.0.0"
        asset = {
            "name": CONFIG["asset"],
            "state": "uploaded",
            "size": len(self.public.payload),
            "digest": "sha256:" + hashlib.sha256(self.public.payload).hexdigest(),
            "browser_download_url": "https://github.com/example/tool/releases/latest/download/tool-linux.zip",
            **self.asset_overrides,
        }
        return Response(document={"tag_name": self.published_tag, "assets": [asset] * self.asset_count})


@pytest.fixture
def upstream(monkeypatch):
    public = PublicSession()
    github = GitHubSession(public)
    monkeypatch.setattr(update_bundles.requests, "Session", lambda: public)
    return github, public


def test_selects_one_stable_release_and_preserves_its_url_after_cdn_redirect(upstream):
    github, public = upstream
    validate_source(CONFIG)
    result = observe_source(CONFIG, github)
    assert result.value == TAGGED_URL
    assert result.tag == "v2.0.0"
    assert result.version == "2.0.0"
    assert result.url == "https://github.com/example/tool/releases/tag/v2.0.0"
    assert result.metadata == {"sha256": hashlib.sha256(public.payload).hexdigest(), "size": len(public.payload)}
    assert public.urls == [TAGGED_URL]
    assert all(urlparse(url).hostname == "api.github.com" for url in github.urls)
    assert not any("/latest" in url for url in github.urls + public.urls)


def test_older_assets_without_a_published_digest_are_hashed_from_downloaded_bytes(upstream):
    github, public = upstream
    github.asset_overrides = {"digest": None}
    result = observe_source(CONFIG, github)
    assert result.metadata["sha256"] == hashlib.sha256(public.payload).hexdigest()
    assert public.urls == [TAGGED_URL]


@pytest.mark.parametrize("count", [0, 2])
def test_missing_or_ambiguous_assets_are_rejected_before_download(upstream, count):
    github, public = upstream
    github.asset_count = count
    with pytest.raises(ValueError, match="expected one release asset"):
        observe_source(CONFIG, github)
    assert not public.urls


def test_release_without_suitable_stable_version_is_rejected(upstream):
    github, public = upstream
    github.releases = [{"tag_name": "v3.0.0rc1", "prerelease": True}]
    with pytest.raises(ValueError, match="no suitable release"):
        observe_source(CONFIG, github)
    assert not public.urls


def test_release_metadata_must_still_identify_selected_tag(upstream):
    github, public = upstream
    github.published_tag = "v1.0.0"
    with pytest.raises(ValueError, match="selected release changed"):
        observe_source(CONFIG, github)
    assert not public.urls


@pytest.mark.parametrize("overrides,message", [
    ({"size": -1}, "valid size"),
    ({"size": True}, "valid size"),
    ({"state": "new"}, "uploaded"),
    ({"digest": "sha256:not-a-digest"}, "invalid published SHA256"),
    ({"digest": "sha256:" + "0" * 64}, "disagrees with published digest"),
    ({"size": 1}, "size disagrees"),
])
def test_incomplete_or_changed_asset_metadata_cannot_produce_an_observation(upstream, overrides, message):
    github, _ = upstream
    github.asset_overrides = overrides
    with pytest.raises(ValueError, match=message):
        observe_source(CONFIG, github)


@pytest.mark.parametrize("change", [
    {"asset": "../tool.zip"}, {"asset": "https://example.org/tool.zip"},
    {"asset": "tool.zip\n"}, {"asset": ""}, {"asset": "TODO"},
    {"repo": "not-a-repository"}, {"unknown": "field"},
    {"include_prereleases": "false"},
])
def test_invalid_source_configuration_is_rejected(change):
    with pytest.raises(ValueError):
        validate_source({**CONFIG, **change})


def write_recipe(tmp_path: Path) -> Path:
    recipe = {
        "name": "demo", "version": "1.0.0",
        "variables": {"upstream_version": "1.0.0"},
        "auto_update": {"method": "sources", "sources": [{
            "id": "archive", **CONFIG,
            "target": {"file": "archive", "variables": {"upstream_version": "version"}},
        }]},
        "files": [{"name": "archive", "url": TAGGED_URL.replace("v2.0.0", "v1.0.0"), "sha256": "a" * 64}],
        "build": {"base-image": "ubuntu:22.04", "directives": [
            {"run": ["unzip {{ get_file('archive') }} -d /opt/tool"]},
        ]},
    }
    path = tmp_path / "build.yaml"
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    path.with_name("fulltest.yaml").write_text(yaml.safe_dump({
        "name": "demo", "version": "1.0.0", "upstream_version": "1.0.0",
        "tests": [{"name": "installed executable", "command": "test -x /opt/tool/tool"}],
    }, sort_keys=False))
    validate_update_policy(recipe, recipe_path=path)
    return path


def test_plan_couples_tagged_url_digest_and_version_and_reobserves_mutable_assets(tmp_path, upstream):
    github, public = upstream
    path = write_recipe(tmp_path)
    plan = plan_sources(path, github)
    plan.apply()
    changed = yaml.safe_load(path.read_text())
    assert changed["variables"]["upstream_version"] == "2.0.0"
    assert changed["files"][0]["url"] == TAGGED_URL
    assert changed["files"][0]["sha256"] == hashlib.sha256(public.payload).hexdigest()
    assert yaml.safe_load(path.with_name("fulltest.yaml").read_text())["upstream_version"] == "2.0.0"
    assert plan_sources(path, github) is None
    public.payload = b"replacement under exactly the same release tag and asset name"
    replaced = plan_sources(path, github)
    assert replaced is not None
    replaced.apply()
    refreshed = yaml.safe_load(path.read_text())
    assert refreshed["version"] == "1.0.0.post2"
    assert refreshed["variables"]["upstream_version"] == "2.0.0"
    assert refreshed["files"][0]["url"] == TAGGED_URL
    assert refreshed["files"][0]["sha256"] == hashlib.sha256(public.payload).hexdigest()
    assert plan_sources(path, github) is None
    assert public.urls == [TAGGED_URL] * 4


def test_observation_failure_keeps_recipe_and_runtime_suite_unchanged(tmp_path, upstream):
    github, _ = upstream
    path = write_recipe(tmp_path)
    suite = path.with_name("fulltest.yaml")
    before = path.read_text(), suite.read_text()
    github.asset_count = 0
    with pytest.raises(ValueError, match="expected one release asset"):
        plan_sources(path, github)
    assert (path.read_text(), suite.read_text()) == before
