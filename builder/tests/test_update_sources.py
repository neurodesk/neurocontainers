"""Upstream discovery must select installable stable versions across all pages."""

from unittest.mock import Mock

import pytest
import requests

from builder import update_sources


def response(data, next_url=None):
    result = Mock()
    result.status_code = 200
    result.headers = {}
    result.json.return_value = data
    result.links = {"next": {"url": next_url}} if next_url else {}
    return result


def test_gateway_retry_policy_is_bounded_and_does_not_repeat_pr_creation():
    with requests.Session() as session:
        update_sources.configure_read_retries(session)
        retry = session.get_adapter("https://api.github.com").max_retries
        assert retry.total == 3
        assert retry.is_retry("GET", 504)
        assert not retry.is_retry("POST", 504)
        assert not retry.is_retry("GET", 404)


def test_oci_anonymous_token_and_relative_pagination_preserve_credential_boundaries(
    public_session,
):
    github_session = Mock()
    url = "https://registry.example/v2/runtime/tags/list"
    challenge = response({})
    challenge.status_code = 401
    challenge.headers = {
        "WWW-Authenticate": 'Bearer realm="https://auth.example/token",service="registry.example",scope="repository:runtime:pull"'
    }
    public_session.get.side_effect = [
        challenge,
        response({"token": "anonymous-read-token"}),
        response(
            {"tags": ["r2025b", "r2026a-full"]}, "/v2/runtime/tags/list?last=r2025b"
        ),
        response({"tags": ["r2026a", "r2026b"]}),
    ]
    release = update_sources.latest_version(
        {
            "method": "oci",
            "url": url,
            "version_scheme": "year_letter",
            "version_regex": r"r(?P<version>20\d{2}[ab])",
        },
        github_session,
    )
    assert release.version == "2026b"
    github_session.get.assert_not_called()
    calls = public_session.get.call_args_list
    assert calls[0].kwargs["headers"] == {}
    assert "headers" not in calls[1].kwargs
    assert calls[1].kwargs["params"] == {
        "service": "registry.example",
        "scope": "repository:runtime:pull",
    }
    assert calls[2].kwargs["headers"] == {
        "Authorization": "Bearer anonymous-read-token"
    }
    assert (
        calls[3].args[0] == "https://registry.example/v2/runtime/tags/list?last=r2025b"
    )


def test_oci_rejects_pagination_to_another_registry(public_session):
    public_session.get.return_value = response(
        {"tags": ["v1.0"]}, "https://other.example/tags"
    )
    with pytest.raises(ValueError, match="pagination URL"):
        update_sources.latest_version(
            {"method": "oci", "url": "https://registry.example/v2/demo/tags/list"},
            Mock(),
        )
    assert public_session.get.call_count == 1


@pytest.mark.parametrize(
    "realm", ["http://auth.example/token", "https://user:password@auth.example/token"]
)
def test_oci_rejects_unsafe_token_endpoints(public_session, realm):
    challenge = response({})
    challenge.status_code = 401
    challenge.headers = {"WWW-Authenticate": f'Bearer realm="{realm}"'}
    public_session.get.return_value = challenge
    with pytest.raises(ValueError, match="HTTPS URL without credentials"):
        update_sources.latest_version(
            {"method": "oci", "url": "https://registry.example/v2/demo/tags/list"},
            Mock(),
        )
    assert public_session.get.call_count == 1


@pytest.mark.parametrize(
    "url",
    [
        "https://registry.example",
        "https://registry.example/v2/tags/list",
        "http://registry.example/v2/demo/tags/list",
    ],
)
def test_oci_requires_an_https_repository_tags_endpoint(url):
    with pytest.raises(ValueError):
        update_sources.validate_update_config({"method": "oci", "url": url})


@pytest.mark.parametrize(
    "config",
    [
        {"method": "github_release", "repo": "QSMxT/QSMxT"},
        {"method": "github_tags", "repo": "neurodesk/tool"},
        {"method": "pypi", "package": "a-tool.name"},
        {"method": "npm", "package": "@scope/package"},
        {"method": "dockerhub", "repo": "library/ubuntu"},
        {
            "method": "dockerhub",
            "repo": "vendor/tool",
            "version_scheme": "year_letter",
        },
        {
            "method": "webpage",
            "url": "https://vendor.org/downloads",
            "version_regex": r"tool-(?P<version>\d+\.\d+)\.tar\.gz",
        },
        {
            "method": "github_release",
            "repo": "org/tool",
            "version_regex": r"tool-v(?P<version>\d+\.\d+)",
        },
        {
            "method": "pypi",
            "package": "sampleproject",
            "mode": "notify",
            "reason": "The recipe pins the upstream source archive checksum.",
        },
        {
            "method": "manual",
            "reason": "The vendor requires an interactive licensed download.",
            "url": "https://vendor.org/downloads",
        },
    ],
)
def test_accepts_explicit_sources_and_documented_constraints(config):
    update_sources.validate_update_config(config)


@pytest.mark.parametrize(
    "config",
    [
        None,
        {},
        {"method": []},
        {"method": "unknown"},
        {"method": "github_release", "repo": "org/tool", "enabled": False},
        {"method": "github_release", "repo": "https://github.com/org/tool"},
        {"method": "github_release", "repo": "../tool"},
        {"method": "github_release", "repo": "org/tool.git"},
        {"method": "github_release", "repo": "TODO/tool"},
        {"method": "github_release", "repo": " org/tool"},
        {"method": "pypi", "package": "tool/other"},
        {"method": "npm", "package": "scope/package"},
        {"method": "npm", "package": "../package"},
        {"method": "webpage", "url": "https://vendor.org"},
        {"method": "webpage", "version_regex": r"(?P<version>\d+)"},
        {"method": "pypi", "package": "tool", "repo": "org/tool"},
        {"method": "pypi", "package": "tool", "mode": "disabled"},
        {"method": "pypi", "package": "tool", "version_scheme": "anything"},
        {"method": "pypi", "package": "tool", "mode": "notify"},
        {"method": "manual", "reason": "No updates"},
        {"method": "manual", "reason": "TODO: configure the upstream release source"},
        {
            "method": "manual",
            "reason": "The vendor requires an interactive licensed download.",
            "url": "http://vendor.org/downloads",
        },
        {"method": "pypi", "package": "tool", "version_regex": "["},
        {"method": "pypi", "package": "tool", "version_regex": r"v(\d+)"},
    ],
)
def test_rejects_invalid_or_incomplete_update_policy(config):
    with pytest.raises(ValueError):
        update_sources.validate_update_config(config)


def test_github_chooses_highest_stable_release_across_pages():
    session = Mock()
    page_two = "https://api.github.com/repos/org/tool/releases?per_page=100&page=2"
    session.get.side_effect = [
        response(
            [
                {"tag_name": "v1.9", "draft": False, "prerelease": False},
                {"tag_name": "v4.0", "draft": True},
                {"tag_name": "v5.0", "prerelease": True},
                {"tag_name": "v6.0rc1"},
                {"tag_name": "v7.0.dev2"},
                {"tag_name": "nightly"},
            ],
            page_two,
        ),
        response([{"tag_name": "v1.10"}]),
    ]

    release = update_sources.latest_version(
        {"method": "github_release", "repo": "org/tool"}, session
    )

    assert release == update_sources.UpstreamRelease(
        "1.10", "v1.10", "https://github.com/org/tool/releases/tag/v1.10"
    )
    assert session.get.call_args_list[1].args == (page_two,)


def test_empty_release_list_falls_back_to_highest_stable_tag():
    session = Mock()
    session.get.side_effect = [
        response([]),
        response([{"name": "v2.0rc1"}, {"name": "v1.10"}, {"name": "v1.9"}]),
    ]

    release = update_sources.latest_version(
        {"method": "github_release", "repo": "org/tool"}, session
    )

    assert release.version == "1.10"
    assert release.url == "https://github.com/org/tool/tree/v1.10"


def test_prerelease_only_repository_does_not_fall_back_to_tags():
    session = Mock()
    session.get.return_value = response([{"tag_name": "v2.0", "prerelease": True}])

    assert (
        update_sources.latest_version(
            {"method": "github_release", "repo": "org/tool"}, session
        )
        is None
    )
    session.get.assert_called_once()


def test_github_tags_method_filters_and_extracts_versions():
    session = Mock()
    session.get.return_value = response(
        [{"name": "tool/1.09"}, {"name": "other/3.0"}, {"name": "tool/1.10"}]
    )

    release = update_sources.latest_version(
        {
            "method": "github_tags",
            "repo": "org/tool",
            "version_regex": r"tool/(?P<version>\d+\.\d+)",
        },
        session,
    )

    assert release.version == "1.10"
    assert release.tag == "tool/1.10"
    assert release.url == "https://github.com/org/tool/tree/tool%2F1.10"
    assert "/tags?" in session.get.call_args.args[0]


@pytest.mark.parametrize("status", [403, 404, 429, 500])
def test_http_failures_are_reported_instead_of_falling_back(status):
    session = Mock()
    result = response({"message": "upstream error"})
    result.raise_for_status.side_effect = requests.HTTPError(str(status))
    session.get.return_value = result

    with pytest.raises(requests.HTTPError, match=str(status)):
        update_sources.latest_version(
            {"method": "github_release", "repo": "org/tool"}, session
        )
    session.get.assert_called_once()


@pytest.fixture
def public_session(monkeypatch):
    session = Mock()
    factory = Mock()
    factory.return_value.__enter__ = Mock(return_value=session)
    factory.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(update_sources.requests, "Session", factory)
    return session


def test_pypi_requires_non_yanked_files_and_never_uses_github_session(public_session):
    github_session = Mock()
    github_session.headers = {"Authorization": "Bearer secret"}
    public_session.get.return_value = response(
        {
            "releases": {
                "1.0": [{"yanked": False}],
                "1.1": [{"yanked": True}, {"yanked": False}],
                "2.0": [],
                "3.0": [{"yanked": True}],
                "4.0rc1": [{"yanked": False}],
                "5.0.dev2": [{"yanked": False}],
            }
        }
    )

    release = update_sources.latest_version(
        {"method": "pypi", "package": "sampleproject"}, github_session
    )

    assert release == update_sources.UpstreamRelease(
        "1.1", "1.1", "https://pypi.org/project/sampleproject/1.1/"
    )
    github_session.get.assert_not_called()
    public_session.get.assert_called_once_with(
        "https://pypi.org/pypi/sampleproject/json", timeout=30
    )


def test_dockerhub_paginates_and_filters_image_variants(public_session):
    github_session = Mock()
    page_two = "https://hub.docker.com/v2/namespaces/org/repositories/tool/tags?page=2"
    public_session.get.side_effect = [
        response(
            {
                "results": [{"name": "1.9-cpu"}, {"name": "9.0-gpu"}],
                "next": page_two,
            }
        ),
        response(
            {
                "results": [{"name": "1.10-cpu"}, {"name": "latest"}],
                "next": None,
            }
        ),
    ]

    release = update_sources.latest_version(
        {
            "method": "dockerhub",
            "repo": "org/tool",
            "version_regex": r"(?P<version>\d+\.\d+)-cpu",
        },
        github_session,
    )

    assert release.version == "1.10"
    assert release.tag == "1.10-cpu"
    assert public_session.get.call_args_list[1].args == (page_two,)
    github_session.get.assert_not_called()


def test_npm_uses_latest_dist_tag_and_public_session(public_session):
    github_session = Mock()
    public_session.get.return_value = response(
        {
            "dist-tags": {"latest": "1.2.0", "next": "2.0.0"},
            "versions": {"1.1.0": {}, "1.2.0": {}, "2.0.0": {}},
        }
    )

    release = update_sources.latest_version(
        {"method": "npm", "package": "@scope/tool"}, github_session
    )

    assert release == update_sources.UpstreamRelease(
        "1.2.0", "1.2.0", "https://www.npmjs.com/package/@scope/tool/v/1.2.0"
    )
    assert public_session.get.call_args.args == (
        "https://registry.npmjs.org/@scope%2Ftool",
    )
    github_session.get.assert_not_called()


def test_npm_falls_back_to_stable_versions_when_latest_is_prerelease(public_session):
    public_session.get.return_value = response(
        {
            "dist-tags": {"latest": "2.0.0-rc1"},
            "versions": {"1.9.0": {}, "1.10.0": {}, "2.0.0-rc1": {}},
        }
    )

    release = update_sources.latest_version(
        {"method": "npm", "package": "tool"}, Mock()
    )

    assert release.version == "1.10.0"


def test_webpage_selects_highest_stable_matching_download(public_session):
    github_session = Mock()
    public_session.get.return_value = response(None)
    public_session.get.return_value.text = (
        '<a href="other-9.0.tar.gz">Other tool</a>\n'
        '<a href="tool-1.9.tar.gz">Older version</a>\n'
        '<a href="tool-2.0rc1.tar.gz">Preview</a>\n'
        '<a href="tool-1.10.tar.gz">Latest version</a>\n'
    )

    release = update_sources.latest_version(
        {
            "method": "webpage",
            "url": "https://vendor.org/downloads",
            "version_regex": r"tool-(?P<version>[\w.]+)\.tar\.gz",
        },
        github_session,
    )

    assert release == update_sources.UpstreamRelease(
        "1.10", "1.10", "https://vendor.org/downloads"
    )
    github_session.get.assert_not_called()


@pytest.mark.parametrize("prefix", ["", "tool-v"])
@pytest.mark.parametrize("suffix", ["rc1", "-rc.1", "-beta1", ".dev2"])
def test_regex_cannot_hide_a_prerelease_suffix(prefix, suffix):
    session = Mock()
    session.get.return_value = response([{"name": f"{prefix}1.0{suffix}"}])

    assert (
        update_sources.latest_version(
            {
                "method": "github_tags",
                "repo": "org/tool",
                "version_regex": prefix + r"(?P<version>\d+\.\d+).*",
            },
            session,
        )
        is None
    )


def test_webpage_regex_cannot_drop_a_prerelease_suffix(public_session):
    public_session.get.return_value = response(None)
    public_session.get.return_value.text = (
        '<a href="tool-v1.0.zip">stable</a><a href="tool-v2.0rc1.zip">preview</a>'
    )
    release = update_sources.latest_version(
        {
            "method": "webpage",
            "url": "https://vendor.org/downloads",
            "version_regex": r"tool-v(?P<version>\d+\.\d+)",
        },
        Mock(),
    )
    assert release.version == "1.0"


@pytest.mark.parametrize(
    "variable", ["", "version", "original_version", "nested.tag", "x;exit"]
)
def test_raw_tag_variable_requires_a_separate_valid_variable_name(variable):
    with pytest.raises(ValueError):
        update_sources.validate_update_config(
            {"method": "github_tags", "repo": "org/demo", "tag_variable": variable}
        )


@pytest.mark.parametrize(
    "tags, expected",
    [
        (["22b", "23a", "23b", "24arc1", "25a.dev1"], "23b"),
        (["2024b", "2025a", "2025b", "2026rc1", "2027dev1"], "2025b"),
    ],
)
def test_explicit_vendor_year_letter_scheme_selects_stable_versions(
    tags, expected, public_session
):
    public_session.get.return_value = response(
        {"results": [{"name": tag} for tag in tags], "next": None}
    )

    release = update_sources.latest_version(
        {
            "method": "dockerhub",
            "repo": "vendor/tool",
            "version_scheme": "year_letter",
        },
        Mock(),
    )

    assert release.version == expected
    assert release.tag == expected


def test_numeric_scheme_does_not_assume_vendor_year_letter_releases():
    session = Mock()
    session.get.return_value = response([{"name": "v23a"}, {"name": "2025b"}])

    assert (
        update_sources.latest_version(
            {"method": "github_tags", "repo": "org/tool"}, session
        )
        is None
    )


@pytest.mark.parametrize("tag", ["23rc1", "23dev1", "23c", "2a", "20255b"])
def test_year_letter_scheme_rejects_other_versions(tag):
    session = Mock()
    session.get.return_value = response([{"name": tag}])

    assert (
        update_sources.latest_version(
            {
                "method": "github_tags",
                "repo": "org/tool",
                "version_scheme": "year_letter",
            },
            session,
        )
        is None
    )


@pytest.mark.parametrize(
    "next_url",
    ["https://untrusted.example/tags", "http://api.github.com/tags"],
)
def test_pagination_cannot_send_github_credentials_to_another_origin(next_url):
    session = Mock()
    session.get.return_value = response([], next_url)

    with pytest.raises(ValueError, match="pagination URL"):
        update_sources.latest_version(
            {"method": "github_tags", "repo": "org/tool"}, session
        )
    session.get.assert_called_once()


def test_repeated_pagination_is_reported():
    session = Mock()
    session.get.return_value = response(
        [], "https://api.github.com/repos/org/tool/tags?per_page=100"
    )

    with pytest.raises(ValueError, match="repeated pagination"):
        update_sources.latest_version(
            {"method": "github_tags", "repo": "org/tool"}, session
        )


def test_manual_policy_makes_no_network_requests(public_session):
    github_session = Mock()

    assert (
        update_sources.latest_version(
            {
                "method": "manual",
                "reason": "The vendor requires an interactive licensed download.",
            },
            github_session,
        )
        is None
    )
    public_session.get.assert_not_called()
    github_session.get.assert_not_called()


@pytest.mark.parametrize("tag", ["1.0/../../file", "1.0$(cmd)", "1.0\n", "1.0;cmd"])
def test_version_extraction_cannot_return_unsafe_recipe_values(tag):
    session = Mock()
    session.get.return_value = response([{"name": tag}])

    assert (
        update_sources.latest_version(
            {"method": "github_tags", "repo": "org/tool"}, session
        )
        is None
    )


def test_prerelease_only_package_requires_explicit_opt_in(public_session):
    public_session.get.return_value = response(
        {"releases": {"1.8a4": [{"yanked": False}], "1.9a1": [{"yanked": False}]}}
    )
    config = {"method": "pypi", "package": "dafne"}
    assert update_sources.latest_version(config, Mock()) is None
    config["include_prereleases"] = True
    assert update_sources.latest_version(config, Mock()).version == "1.9a1"


def test_prerelease_opt_in_still_excludes_drafts():
    session = Mock()
    session.get.return_value = response(
        [
            {"tag_name": "v2.0rc1", "prerelease": True},
            {"tag_name": "v3.0", "draft": True},
        ]
    )
    config = {
        "method": "github_release",
        "repo": "org/demo",
        "include_prereleases": True,
    }
    assert update_sources.latest_version(config, session).version == "2.0rc1"


def test_prerelease_opt_in_rejects_a_string_boolean():
    with pytest.raises(ValueError, match="boolean"):
        update_sources.validate_update_config(
            {"method": "pypi", "package": "demo", "include_prereleases": "false"}
        )


@pytest.mark.parametrize("assets", [{"": "demo.zip"}, {"main": ""}, {"TODO": "TODO"}])
def test_asset_patterns_must_be_configured(assets):
    with pytest.raises(ValueError, match="nonempty"):
        update_sources.validate_update_config(
            {"method": "github_release", "repo": "org/demo", "assets": assets}
        )
