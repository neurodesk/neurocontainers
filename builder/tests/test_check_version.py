"""Tests for the pure rewrite helpers in builder/check_version.py.

These cover the two ways an automated bump can corrupt a recipe: writing a
version that reloads as a different value, and moving a pinned revision that
belongs to some other project.
"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import yaml

MODULE_PATH = Path(__file__).resolve().parents[1] / "check_version.py"
spec = importlib.util.spec_from_file_location("check_version", MODULE_PATH)
check_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_version)


def test_rewrite_version_quotes_a_version_that_would_reload_as_a_float() -> None:
    """`version: 1.10` unquoted reloads as 1.1, silently mislabelling the image."""
    updated = check_version.rewrite_version("name: demo\nversion: 1.9\n", "1.10")

    assert str(yaml.safe_load(updated)["version"]) == "1.10"


def test_rewrite_version_preserves_existing_quoting() -> None:
    updated = check_version.rewrite_version('name: demo\nversion: "1.2.3"\n', "1.2.4")

    assert 'version: "1.2.4"' in updated
    assert yaml.safe_load(updated)["version"] == "1.2.4"


def test_rewrite_version_ignores_nested_version_keys() -> None:
    text = (
        "name: demo\n"
        "version: 1.2.3\n"
        "build:\n"
        "  directives:\n"
        "    - template:\n"
        "        name: ants\n"
        "        version: 2.4.3\n"
    )

    updated = check_version.rewrite_version(text, "1.2.4")
    reloaded = yaml.safe_load(updated)

    assert reloaded["version"] == "1.2.4"
    assert reloaded["build"]["directives"][0]["template"]["version"] == "2.4.3"


def test_rewrite_version_keeps_a_trailing_comment() -> None:
    updated = check_version.rewrite_version(
        "name: demo\nversion: 1.2.3  # keep in sync with the base image\n", "1.2.4"
    )

    assert "# keep in sync with the base image" in updated
    assert yaml.safe_load(updated)["version"] == "1.2.4"


def test_rewrite_fulltest_version_preserves_variable_indirection() -> None:
    text = (
        "name: demo\n"
        'tool_version: "1.2.3"\n'
        'version: "${tool_version}"\n'
        "tests:\n"
        "  - command: demo --version\n"
        '    expected_output_contains: "${version}"\n'
    )

    updated = check_version.rewrite_fulltest_version(text, "1.2.4")

    assert 'tool_version: "1.2.4"' in updated
    assert 'version: "${tool_version}"' in updated
    assert 'expected_output_contains: "${version}"' in updated


def test_prepare_fulltest_bump_updates_the_sibling_suite(tmp_path: Path) -> None:
    build_yaml = tmp_path / "build.yaml"
    fulltest_yaml = tmp_path / "fulltest.yaml"
    fulltest_yaml.write_text(
        "name: demo\nversion: 1.2.3\ntests: []\n",
        encoding="utf-8",
    )

    prepared = check_version.prepare_fulltest_bump(str(build_yaml), "1.2.4")

    path, updated, previous = prepared
    assert path == str(fulltest_yaml)
    assert previous == "1.2.3"
    assert yaml.safe_load(updated)["version"] == "1.2.4"


def test_prepare_fulltest_bump_is_optional_for_legacy_recipes(tmp_path: Path) -> None:
    build_yaml = tmp_path / "build.yaml"

    assert check_version.prepare_fulltest_bump(str(build_yaml), "1.2.4") is None


def test_submit_bump_commits_recipe_and_fulltest_together(
    tmp_path: Path, monkeypatch
) -> None:
    recipe_dir = tmp_path / "recipes" / "demo"
    recipe_dir.mkdir(parents=True)
    build_yaml = recipe_dir / "build.yaml"
    fulltest_yaml = recipe_dir / "fulltest.yaml"
    build_yaml.write_text(
        "name: demo\nversion: 1.2.3\nbuild: {}\n",
        encoding="utf-8",
    )
    fulltest_yaml.write_text(
        "name: demo\nversion: 1.2.3\ntests: []\n",
        encoding="utf-8",
    )
    git_calls = []
    monkeypatch.setattr(check_version.subprocess, "run", lambda *args, **kwargs: None)

    monkeypatch.setattr(check_version, "pull_request_state", lambda branch: None)
    monkeypatch.setattr(check_version, "remote_branch_exists", lambda branch: False)
    monkeypatch.setattr(check_version, "find_stale_update_issues", lambda path: [])
    monkeypatch.setattr(
        check_version,
        "open_pull_request",
        lambda *args, **kwargs: {"number": 1},
    )

    def fake_git(*args, **kwargs):
        git_calls.append(args)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(check_version, "git", fake_git)

    result = check_version.submit_bump(
        str(build_yaml),
        "demo",
        "1.2.3",
        "1.2.4",
        "example/demo",
        "v1.2.4",
        "main",
        False,
    )

    assert result == "opened"
    assert yaml.safe_load(build_yaml.read_text())["version"] == "1.2.4"
    assert yaml.safe_load(fulltest_yaml.read_text())["version"] == "1.2.4"
    assert (
        "add",
        "--",
        str(build_yaml),
        str(fulltest_yaml),
    ) in git_calls


RECIPE_WITH_TWO_PINS = """name: demo
version: 1.0.0
build:
  directives:
    - group:
        - variables:
            github_url: https://github.com/someoneelse/helper.git
            revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    - group:
        - variables:
            github_url: https://github.com/rordenlab/niimath.git
            revision: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
"""


def test_revision_rewrite_leaves_another_projects_pin_alone() -> None:
    """Proximity is not ownership: only the sha beside the upstream URL moves."""
    updated, changed = check_version.rewrite_revision(
        RECIPE_WITH_TWO_PINS, "rordenlab/niimath", "c" * 40
    )

    assert changed == [("b" * 40, "c" * 40)]
    assert "a" * 40 in updated
    assert "b" * 40 not in updated


def test_revision_rewrite_skips_a_recipe_that_pins_only_foreign_shas() -> None:
    assert (
        check_version.revisions_owned_by(RECIPE_WITH_TWO_PINS, "unrelated/project")
        == set()
    )

    _, changed = check_version.rewrite_revision(
        RECIPE_WITH_TWO_PINS, "unrelated/project", "c" * 40
    )
    assert changed == []


@pytest.mark.parametrize("repo", ["rordenlab/niimath", "RordenLab/NiiMath"])
def test_revisions_owned_by_matches_case_insensitively(repo: str) -> None:
    assert check_version.revisions_owned_by(RECIPE_WITH_TWO_PINS, repo) == {"b" * 40}


def write_update_recipe(root: Path, name: str, config: dict | None) -> None:
    path = root / "recipes" / name
    path.mkdir(parents=True)
    data = {
        "name": name,
        "version": "1.0.0",
        "build": {"directives": [{"run": "pip install demo=={{ context.version }}"}]},
    }
    if config is not None:
        data["auto_update"] = config
    (path / "build.yaml").write_text(yaml.safe_dump(data))
    (path / "fulltest.yaml").write_text("name: demo\nversion: 1.0.0\ntests: []\n")


def test_main_reports_missing_tracking_and_still_checks_other_recipes(
    tmp_path, monkeypatch
):
    import json

    write_update_recipe(tmp_path, "absent", None)
    write_update_recipe(tmp_path, "tracked", {"method": "pypi", "package": "demo"})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(check_version, "REPO", None)
    monkeypatch.setattr(
        check_version,
        "latest_version",
        lambda *args: SimpleNamespace(
            version="1.1.0", tag="1.1.0", url="https://pypi.org/project/demo/1.1.0/"
        ),
    )
    monkeypatch.setattr(
        check_version.sys,
        "argv",
        ["check_version", "--dry-run", "--json", "report.json"],
    )
    assert check_version.main() == 1
    rows = json.loads((tmp_path / "report.json").read_text())
    assert [row["status"] for row in rows] == ["error", "would-open"]
    assert "auto_update is required" in rows[0]["detail"]


def test_main_reports_lookup_failure_instead_of_false_success(tmp_path, monkeypatch):
    import json

    write_update_recipe(tmp_path, "broken", {"method": "pypi", "package": "demo"})
    monkeypatch.chdir(tmp_path)

    def unavailable(*args):
        raise check_version.requests.HTTPError("429 rate limited")

    monkeypatch.setattr(check_version, "latest_version", unavailable)
    monkeypatch.setattr(
        check_version.sys,
        "argv",
        ["check_version", "--dry-run", "--json", "report.json"],
    )
    assert check_version.main() == 1
    assert json.loads((tmp_path / "report.json").read_text())[0]["status"] == "error"


def test_pr_limit_does_not_hide_existing_or_closed_prs(tmp_path, monkeypatch):
    import json

    for name in ("a-new", "b-open", "c-closed", "d-new"):
        write_update_recipe(tmp_path, name, {"method": "pypi", "package": "demo"})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(check_version, "REPO", "owner/containers")
    monkeypatch.setattr(
        check_version,
        "latest_version",
        lambda *args: SimpleNamespace(
            version="2.0.0", tag="2.0.0", url="https://pypi.org/project/demo/2.0.0/"
        ),
    )
    monkeypatch.setattr(
        check_version,
        "pull_request_state",
        lambda branch: (
            "open" if "b-open" in branch else "closed" if "c-closed" in branch else None
        ),
    )
    monkeypatch.setattr(
        check_version.sys,
        "argv",
        ["check_version", "--dry-run", "--max-prs", "1", "--json", "report.json"],
    )
    assert check_version.main() == 0
    rows = json.loads((tmp_path / "report.json").read_text())
    assert [row["status"] for row in rows] == [
        "would-open",
        "pr-open",
        "pr-closed",
        "deferred",
    ]


def test_version_comparison_does_not_promote_prerelease_to_stable():
    assert check_version.newer("1.0.0-rc1", "1.0.0") is True


def test_package_registry_bump_does_not_rewrite_helper_commits(tmp_path):
    path = tmp_path / "build.yaml"
    path.write_text(RECIPE_WITH_TWO_PINS)
    updated, changes = check_version.prepare_bump(
        str(path), "1.0.0", "1.1.0", "", "1.1.0"
    )
    assert updated is not None
    assert "revision: " + "a" * 40 in updated
    assert "revision: " + "b" * 40 in updated
    assert len(changes) == 1


def test_real_update_refuses_untracked_work_before_checkout(tmp_path, monkeypatch):
    import subprocess

    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    draft = tmp_path / "draft.txt"
    draft.write_text("Uncommitted user work\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(check_version, "REPO", "org/recipes")
    monkeypatch.setattr(check_version, "TOKEN", "test-token")
    monkeypatch.delenv("AUTO_UPDATE_DRY_RUN", raising=False)
    monkeypatch.setattr(check_version.sys, "argv", ["check_version"])
    with pytest.raises(SystemExit) as error:
        check_version.main()
    assert error.value.code == 2
    assert draft.read_text() == "Uncommitted user work\n"


def test_empty_release_filter_is_an_error_for_monitored_packages(tmp_path, monkeypatch):
    import json

    write_update_recipe(
        tmp_path,
        "demo",
        {
            "method": "pypi",
            "package": "demo",
        },
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(check_version, "latest_version", lambda *args: None)
    monkeypatch.setattr(
        check_version.sys,
        "argv",
        ["check_version", "--dry-run", "--json", "report.json"],
    )
    assert check_version.main() == 1
    row = json.loads((tmp_path / "report.json").read_text())[0]
    assert row["status"] == "error"
    assert "no stable version found upstream" in row["detail"]


def test_recipe_bump_preserves_the_exact_upstream_tag_when_prefixes_change(tmp_path):
    path = tmp_path / "build.yaml"
    path.write_text(
        "name: demo\nversion: 1.9.3\n"
        "variables:\n  upstream_tag: '1.9.3'  # selected source\n  helper: 1.2.3\n"
        "auto_update:\n  method: github_release\n  repo: org/demo\n"
        "  tag_variable: upstream_tag\n"
    )
    updated, changes = check_version.prepare_bump(
        str(path), "1.9.3", "1.9.6", "org/demo", "v1.9.6"
    )
    recipe = yaml.safe_load(updated)
    assert recipe["version"] == "1.9.6"
    assert recipe["variables"] == {"upstream_tag": "v1.9.6", "helper": "1.2.3"}
    assert "# selected source" in updated
    assert len(changes) == 2


@pytest.mark.parametrize("tag", ["v2;exit", "$(exit)", "a b", "v2\nexit"])
def test_raw_upstream_tag_cannot_introduce_shell_syntax(tag):
    with pytest.raises(ValueError, match="safe for a source ref"):
        check_version.rewrite_upstream_tag(
            "variables:\n  upstream_tag: v1\n", "upstream_tag", tag
        )


def test_raw_tag_alias_cannot_rewrite_another_field():
    text = "version: &version 1.9.3\nvariables:\n  upstream_tag: *version\n"
    with pytest.raises(ValueError, match="without an alias"):
        check_version.rewrite_upstream_tag(text, "upstream_tag", "v1.9.6")


@pytest.mark.parametrize(
    "source",
    [
        "&shared 1.9.3\n  helper_tag: *shared",
        "!!str &shared 1.9.3\n  helper_tag: *shared",
        ">-\n    1.9.3",
        "|-\n    1.9.3",
    ],
)
def test_raw_tag_rewrite_rejects_anchors_and_block_scalars(source):
    text = f"variables:\n  upstream_tag: {source}\nauto_update: {{}}\n"
    with pytest.raises(ValueError, match="plain or quoted scalar"):
        check_version.rewrite_upstream_tag(text, "upstream_tag", "v1.9.6")


def raise_and_classify(exc):
    try:
        raise exc
    except Exception as caught:
        return check_version.unreachable_upstream(caught)


def test_unreachable_upstream_defers_the_outages_that_failed_scheduled_runs():
    # The exact failures that turned whole auto-update runs red while every recipe was fine.
    zenodo = requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='zenodo.org', port=443): Max retries exceeded"
    )
    tgvqsm = requests.exceptions.ConnectTimeout(
        "HTTPSConnectionPool(host='www.neuroimaging.at', port=443): Max retries exceeded"
    )
    assert raise_and_classify(zenodo)
    assert raise_and_classify(tgvqsm)
    assert raise_and_classify(requests.exceptions.ReadTimeout("read timed out"))
    assert raise_and_classify(requests.exceptions.RetryError("too many retries"))
    assert raise_and_classify(requests.exceptions.ChunkedEncodingError("truncated"))


@pytest.mark.parametrize("status,deferred", [(429, True), (503, True), (504, True), (404, False), (403, False)])
def test_unreachable_upstream_separates_server_outages_from_missing_resources(status, deferred):
    response = requests.Response()
    response.status_code = status
    assert check_version.unreachable_upstream(
        requests.exceptions.HTTPError(response=response)
    ) is deferred


def test_unreachable_upstream_still_fails_the_run_for_recipe_defects():
    # The Slicer metadata mismatch must stay an error; only the network gets a pass.
    assert not raise_and_classify(ValueError("Slicer package release or architecture metadata disagrees"))
    assert not raise_and_classify(KeyError("upstream_version"))
    assert not raise_and_classify(requests.exceptions.HTTPError("no response attached"))
