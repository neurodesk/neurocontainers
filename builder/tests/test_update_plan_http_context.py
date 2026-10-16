"""Commit observations and their archive hashes use one staged source identity."""

import hashlib
from unittest.mock import Mock

import pytest
import requests
import yaml

from builder import update_observations
from builder.update_plan import plan_sources


OLD_COMMIT = "a" * 40
NEW_COMMIT = "b" * 40
OLD_ARCHIVE = b"archive of the installed commit"
NEW_ARCHIVE = b"archive of the newly observed commit"
ARCHIVE_URL = (
    "https://github.com/example/tool/archive/{{ context.source_commit }}.tar.gz"
)


def response(*, data=None, content=b"", url="https://api.github.com/result"):
    result = Mock()
    result.status_code = 200
    result.headers = {}
    result.url = url
    result.json.return_value = data
    result.iter_content.return_value = [content[:5], content[5:]]
    return result


@pytest.fixture
def network(monkeypatch, tmp_path):
    """Mock both network sessions, retaining the real HTTP digest implementation."""
    github = Mock()
    public = Mock()
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=public)
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(update_observations.requests, "Session", factory)
    monkeypatch.setenv("AUTO_UPDATE_HTTP_CACHE", str(tmp_path / "http-cache"))
    return github, public


def select_commit(github, commit):
    def get(url, **kwargs):
        if url.endswith("/commits/HEAD"):
            return response(data={"sha": commit})
        assert url.endswith(f"/compare/{OLD_COMMIT}...{commit}")
        return response(data={"status": "ahead", "ahead_by": 1})

    github.get.side_effect = get


def write_recipe(tmp_path, *, variables=None, digest_url=ARCHIVE_URL, forward=False):
    commit_source = {
        "id": "upstream",
        "method": "github_commit",
        "repo": "example/tool",
        "ref": "HEAD",
        "target": {"variable": "source_commit", "fulltest_variable": "source_commit"},
    }
    digest_source = {
        "id": "archive",
        "dependency": True,
        "method": "http_digest",
        "url": digest_url,
        "target": {"file": "source"},
    }
    recipe = {
        "name": "tool",
        "version": "1.0.0",
        "variables": {"source_commit": OLD_COMMIT, **(variables or {})},
        "auto_update": {
            "method": "sources",
            "container_version": False,
            "sources": (
                [digest_source, commit_source]
                if forward
                else [commit_source, digest_source]
            ),
        },
        "build": {"base-image": "ubuntu:24.04", "directives": []},
        "files": [
            {
                "name": "source",
                "url": ARCHIVE_URL,
                "sha256": hashlib.sha256(OLD_ARCHIVE).hexdigest(),
            }
        ],
    }
    path = tmp_path / "build.yaml"
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    path.with_name("fulltest.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "tool",
                "version": "1.0.0",
                "source_commit": OLD_COMMIT,
                "tests": [],
            },
            sort_keys=False,
        )
    )
    return path


def snapshot(path):
    return path.read_bytes(), path.with_name("fulltest.yaml").read_bytes()


@pytest.mark.parametrize("changed", [False, True])
def test_commit_and_its_archive_are_observed_together(tmp_path, network, changed):
    github, public = network
    path = write_recipe(tmp_path)
    before = snapshot(path)
    commit = NEW_COMMIT if changed else OLD_COMMIT
    payload = NEW_ARCHIVE if changed else OLD_ARCHIVE
    select_commit(github, commit)
    url = f"https://github.com/example/tool/archive/{commit}.tar.gz"
    public.get.return_value = response(content=payload, url=url)

    plan = plan_sources(path, github)

    assert snapshot(path) == before  # Observation and planning never write pins.
    public.get.assert_called_once_with(url, stream=True, timeout=60)
    assert (
        sum(
            call.args[0].endswith("/commits/HEAD") for call in github.get.call_args_list
        )
        == 1
    )
    if not changed:
        assert plan is None
        return
    assert not plan.held
    plan.apply()
    updated = yaml.safe_load(path.read_text())
    suite = yaml.safe_load(path.with_name("fulltest.yaml").read_text())
    assert updated["variables"]["source_commit"] == suite["source_commit"] == NEW_COMMIT
    assert updated["files"][0]["sha256"] == hashlib.sha256(NEW_ARCHIVE).hexdigest()
    assert updated["files"][0]["url"] == ARCHIVE_URL
    assert updated["auto_update"]["sources"][1]["url"] == ARCHIVE_URL
    assert updated["version"] == suite["version"] == "1.0.0"
    plan.apply()  # Applying or observing the same selected commit is idempotent.
    assert plan_sources(path, github) is None


def test_digest_download_failure_leaves_prior_commit_and_fulltest_untouched(
    tmp_path, network
):
    github, public = network
    path = write_recipe(tmp_path)
    before = snapshot(path)
    select_commit(github, NEW_COMMIT)
    public.get.side_effect = requests.ConnectionError("archive unavailable")

    with pytest.raises(requests.ConnectionError, match="archive unavailable"):
        plan_sources(path, github)

    assert snapshot(path) == before
    public.get.assert_called_once_with(
        f"https://github.com/example/tool/archive/{NEW_COMMIT}.tar.gz",
        stream=True,
        timeout=60,
    )


def test_unchanged_commit_still_checks_the_archive_digest(tmp_path, network):
    github, public = network
    path = write_recipe(tmp_path)
    before = snapshot(path)
    select_commit(github, OLD_COMMIT)
    url = f"https://github.com/example/tool/archive/{OLD_COMMIT}.tar.gz"
    public.get.return_value = response(content=NEW_ARCHIVE, url=url)

    plan = plan_sources(path, github)

    assert plan.held  # Preserve the configured dependency-only update behavior.
    assert snapshot(path) == before
    plan.apply()
    updated = yaml.safe_load(path.read_text())
    assert updated["variables"]["source_commit"] == OLD_COMMIT
    assert updated["files"][0]["sha256"] == hashlib.sha256(NEW_ARCHIVE).hexdigest()
    assert path.with_name("fulltest.yaml").read_bytes() == before[1]


@pytest.mark.parametrize(
    "variables,url,error",
    [
        (
            {},
            "https://example.org/{{ context.missing }}",
            "unknown context variable: context.missing",
        ),
        ({}, "https://example.org/{{ missing }}", "missing.*undefined"),
        (
            {"alias": "{{ context.missing }}"},
            "https://example.org/{{ context.alias }}",
            "unknown context variable",
        ),
        (
            {"alias": "{{ context.other }}", "other": "{{ context.alias }}"},
            "https://example.org/{{ context.alias }}",
            "cyclic context variable",
        ),
        (
            {"alias": ["not", "scalar"]},
            "https://example.org/{{ context.alias }}",
            "must be a scalar",
        ),
        (
            {"alias": "{{ '{{ context.missing }}' }}"},
            "https://example.org/{{ context.alias }}",
            "unresolved template",
        ),
        (
            {},
            "https://example.org/{{ context.source_commit",
            "cannot resolve http_digest URL",
        ),
        (
            {"host": "user:secret@example.org"},
            "https://{{ context.host }}/archive",
            "without credentials",
        ),
    ],
)
def test_invalid_context_never_downloads_or_writes(
    tmp_path, network, variables, url, error
):
    github, public = network
    path = write_recipe(tmp_path, variables=variables, digest_url=url)
    before = snapshot(path)
    select_commit(github, NEW_COMMIT)

    with pytest.raises(ValueError, match=error):
        plan_sources(path, github)

    public.get.assert_not_called()
    assert snapshot(path) == before


@pytest.mark.parametrize("alias", [False, True])
def test_forward_source_reference_does_not_hash_its_stale_pin(tmp_path, network, alias):
    github, public = network
    path = write_recipe(
        tmp_path,
        forward=True,
        variables={"alias": "{{ context.source_commit }}"} if alias else None,
        digest_url="https://example.org/{{ context.alias }}" if alias else ARCHIVE_URL,
    )
    before = snapshot(path)

    with pytest.raises(ValueError, match="source_commit has not been observed yet"):
        plan_sources(path, github)

    github.get.assert_not_called()
    public.get.assert_not_called()
    assert snapshot(path) == before


def test_context_alias_uses_the_observed_commit(tmp_path, network):
    github, public = network
    path = write_recipe(
        tmp_path,
        variables={"archive_path": "archive/{{ context.source_commit }}.tar.gz"},
        digest_url="https://github.com/example/tool/{{ context.archive_path }}",
    )
    select_commit(github, NEW_COMMIT)
    url = f"https://github.com/example/tool/archive/{NEW_COMMIT}.tar.gz"
    public.get.return_value = response(content=NEW_ARCHIVE, url=url)

    assert plan_sources(path, github) is not None

    public.get.assert_called_once_with(url, stream=True, timeout=60)


def test_literal_digest_url_is_requested_unchanged(tmp_path, network):
    github, public = network
    url = "https://example.org/snapshot.jar?literal={value}&version=1"
    path = write_recipe(tmp_path, digest_url=url)
    select_commit(github, OLD_COMMIT)
    public.get.return_value = response(content=OLD_ARCHIVE, url=url)

    assert plan_sources(path, github) is None

    public.get.assert_called_once_with(url, stream=True, timeout=60)


def test_digest_cannot_reference_its_own_target_pin(tmp_path, network):
    github, public = network
    path = write_recipe(
        tmp_path, digest_url="https://example.org/{{ context.archive_sha256 }}"
    )
    recipe = yaml.safe_load(path.read_text())
    recipe["variables"]["archive_sha256"] = hashlib.sha256(OLD_ARCHIVE).hexdigest()
    recipe["files"][0]["sha256"] = "{{ context.archive_sha256 }}"
    recipe["auto_update"]["sources"][1]["target"] = {"variable": "archive_sha256"}
    path.write_text(yaml.safe_dump(recipe, sort_keys=False))
    before = snapshot(path)
    select_commit(github, NEW_COMMIT)

    with pytest.raises(ValueError, match="archive_sha256 has not been observed yet"):
        plan_sources(path, github)

    public.get.assert_not_called()
    assert snapshot(path) == before
