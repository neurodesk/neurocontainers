"""Tests for multi-domain CVMFS catalog sync and publication lag tracking."""

import json
import os
import subprocess
from pathlib import Path

import pytest

from cvmfs import fetch_catalog
from cvmfs.check_publication import PublicationUnavailable

ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = (
    ROOT / ".github" / "workflows" / "scripts" / "publish_cvmfs_metadata.sh"
)


def _git(cwd: Path, *args: str, env=None) -> str:
    merged_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=cwd,
        env=merged_env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_fetch_catalog_discovers_all_domains_and_deduplicates_dual_use(
    tmp_path: Path, monkeypatch
):
    payloads = {
        "https://catalog.example/domains.json": {
            "domains": {
                "neuroimaging": {"source_repositories": ["NeuroDesk/neurocontainers"]},
                "microscopy": {"source_repositories": ["NeuroDesk/neurocontainers"]},
                "datascience": {"source_repositories": ["NeuroDesk/neurocontainers"]},
            }
        },
        "https://catalog.example/neuroimaging/apps.json": {
            "elastix": {
                "apps": {"elastix 5.3.1": {"version": "20261121"}},
                "categories": ["image registration"],
            }
        },
        "https://catalog.example/microscopy/apps.json": {
            "elastix": {
                "apps": {"elastix 5.3.1": {"version": "20261121"}},
                "categories": ["image registration"],
            },
            "nextpyp": {
                "apps": {"nextpyp 0.7.3": {"version": "20261118"}},
                "categories": ["cryo EM"],
            },
        },
        "https://catalog.example/datascience/apps.json": {},
    }
    requested = []

    def fake_fetch(url: str, timeout: int = 30):
        requested.append(url)
        return payloads[url]

    monkeypatch.setattr(fetch_catalog, "_fetch_json", fake_fetch)

    catalog_out = tmp_path / "catalog.json"
    log_out = tmp_path / "log.txt"
    lines = fetch_catalog.write_catalog_and_log(
        "https://catalog.example", catalog_out, log_out
    )

    assert requested == [
        "https://catalog.example/domains.json",
        "https://catalog.example/datascience/apps.json",
        "https://catalog.example/microscopy/apps.json",
        "https://catalog.example/neuroimaging/apps.json",
    ]
    merged = json.loads(catalog_out.read_text(encoding="utf-8"))
    assert sorted(merged) == ["elastix", "nextpyp"]
    assert sorted(lines) == [
        "elastix_5.3.1_20261121 categories:image registration,",
        "nextpyp_0.7.3_20261118 categories:cryo EM,",
    ]
    assert log_out.read_text(encoding="utf-8").splitlines() == lines


def test_fetch_catalog_honors_explicit_domains_override(tmp_path: Path, monkeypatch):
    payloads = {
        "https://catalog.example/microscopy/apps.json": {
            "smartscope": {
                "apps": {"smartscope 0.10.0": {"version": "20261118"}},
                "categories": ["cryo EM"],
            }
        }
    }
    requested = []

    def fake_fetch(url: str, timeout: int = 30):
        requested.append(url)
        return payloads[url]

    monkeypatch.setattr(fetch_catalog, "_fetch_json", fake_fetch)

    catalog_out = tmp_path / "catalog.json"
    log_out = tmp_path / "log.txt"
    lines = fetch_catalog.write_catalog_and_log(
        "https://catalog.example/",
        catalog_out,
        log_out,
        domains_override="microscopy microscopy",
    )

    assert requested == ["https://catalog.example/microscopy/apps.json"]
    assert lines == ["smartscope_0.10.0_20261118 categories:cryo EM,"]


@pytest.mark.parametrize(
    ("domains_doc", "override", "match"),
    [
        ({"domains": {}}, "", "no configured domains"),
        ({"domains": {"../bad": {}}}, "", "Invalid catalog domain"),
        ({}, "../bad", "Invalid catalog domain"),
        ([], "", "expected a JSON object"),
    ],
)
def test_fetch_catalog_rejects_invalid_domains(
    monkeypatch, domains_doc, override, match
):
    monkeypatch.setattr(
        fetch_catalog, "_fetch_json", lambda url, timeout=30: domains_doc
    )
    with pytest.raises(ValueError, match=match):
        fetch_catalog.resolve_domains("https://catalog.example", override)


def test_fetch_catalog_rejects_conflicting_dual_use_tool(monkeypatch):
    payloads = {
        "https://catalog.example/domains.json": {"domains": {"a": {}, "b": {}}},
        "https://catalog.example/a/apps.json": {
            "tool": {"apps": {"tool 1.0": {"version": "20261118"}}, "categories": ["x"]}
        },
        "https://catalog.example/b/apps.json": {
            "tool": {"apps": {"tool 1.0": {"version": "20261119"}}, "categories": ["x"]}
        },
    }
    monkeypatch.setattr(
        fetch_catalog, "_fetch_json", lambda url, timeout=30: payloads[url]
    )
    with pytest.raises(ValueError, match="Conflicting app group"):
        fetch_catalog.fetch_merged_catalog("https://catalog.example")


@pytest.mark.parametrize(
    "bad_app", ["../escape 1.0", "foo/bar 1.0", "bad\tname 1.0", "\ttool 1.0"]
)
def test_fetch_catalog_rejects_unsafe_image_ids_before_writing_files(
    tmp_path: Path, monkeypatch, bad_app: str
):
    payloads = {
        "https://catalog.example/domains.json": {"domains": {"neuroimaging": {}}},
        "https://catalog.example/neuroimaging/apps.json": {
            "tool": {
                "apps": {bad_app: {"version": "20261118"}},
                "categories": ["testing"],
            }
        },
    }
    monkeypatch.setattr(
        fetch_catalog, "_fetch_json", lambda url, timeout=30: payloads[url]
    )
    catalog_out = tmp_path / "catalog.json"
    log_out = tmp_path / "log.txt"

    with pytest.raises(PublicationUnavailable):
        fetch_catalog.write_catalog_and_log(
            "https://catalog.example", catalog_out, log_out
        )
    assert not catalog_out.exists()
    assert not log_out.exists()


def _commit_catalog(catalog_repo: Path, domains_apps: dict, ts: int, message: str):
    (catalog_repo / "domains.json").write_text(
        json.dumps({"domains": {d: {} for d in sorted(domains_apps)}}),
        encoding="utf-8",
    )
    for domain, apps in domains_apps.items():
        domain_dir = catalog_repo / domain
        domain_dir.mkdir(exist_ok=True)
        (domain_dir / "apps.json").write_text(json.dumps(apps), encoding="utf-8")
    _git(catalog_repo, "add", ".")
    date_env = {"GIT_AUTHOR_DATE": f"{ts} +0000", "GIT_COMMITTER_DATE": f"{ts} +0000"}
    _git(catalog_repo, "commit", "-m", message, env=date_env)


def _setup_publish_fixture(tmp_path: Path):
    work = tmp_path / "work"
    work.mkdir()
    (work / "cvmfs").mkdir()
    (work / "cvmfs" / "check_publication.py").write_text(
        "import os, pathlib, sys\n"
        "missing_file = pathlib.Path(os.environ['STATE_DIR']) / 'missing-cvmfs'\n"
        "items = [l.strip() for l in missing_file.read_text().splitlines() if l.strip()] if missing_file.exists() else []\n"
        "for item in items:\n"
        "    print(f'[PENDING] Missing public container: {item}')\n"
        "sys.exit(3 if items else 0)\n",
        encoding="utf-8",
    )
    catalog = work / "apps-catalog"
    catalog.mkdir()
    _git(catalog, "init", "-b", "main")

    nectar = tmp_path / "nectar"
    aws = tmp_path / "aws"
    nectar.mkdir()
    aws.mkdir()
    state = tmp_path / "state"
    state.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "STATE_DIR": str(state),
        "CATALOG_ROOT": str(catalog),
        "APPS_JSON": str(catalog / "apps.json"),
        "CVMFS_METADATA_OBJECT_STORES": f"file://{nectar} file://{aws}",
        "CVMFS_METADATA_LAG_SECONDS": str(24 * 3600),
    }
    return work, catalog, nectar, aws, state, bin_dir, env


def _write_merged_apps(catalog: Path, domains_apps: dict):
    merged = {}
    for domain in sorted(domains_apps):
        merged.update(domains_apps[domain])
    (catalog / "apps.json").write_text(json.dumps(merged), encoding="utf-8")


def _upload(nectar: Path, aws: Path, image_id: str):
    for store in (nectar, aws):
        (store / f"{image_id}.simg").write_text("img", encoding="utf-8")


@pytest.mark.parametrize("missing_layer", ["object", "cvmfs"])
def test_publish_lag_does_not_reset_on_newer_catalog_commits(
    tmp_path: Path, missing_layer: str
):
    work, catalog, nectar, aws, state, bin_dir, env = _setup_publish_fixture(tmp_path)
    t0 = 1_700_000_000
    t1 = t0 + 12 * 3600

    first_state = {
        "microscopy": {
            "nextpyp": {
                "apps": {"nextpyp 0.7.3": {"version": "20261118"}},
                "categories": ["cryo EM"],
            }
        }
    }
    _commit_catalog(catalog, first_state, t0, "add nextpyp")

    second_state = {
        **first_state,
        "neuroimaging": {
            "tool": {
                "apps": {"tool 1.0": {"version": "20261119"}},
                "categories": ["testing"],
            }
        },
    }
    _commit_catalog(catalog, second_state, t1, "add tool")
    _write_merged_apps(catalog, second_state)

    _upload(nectar, aws, "tool_1.0_20261119")
    if missing_layer == "cvmfs":
        _upload(nectar, aws, "nextpyp_0.7.3_20261118")
        (state / "missing-cvmfs").write_text(
            "nextpyp_0.7.3_20261118\n", encoding="utf-8"
        )

    clock = bin_dir / "date"

    # At t0 + 24h - 1s: still within deadline.
    clock.write_text(f"#!/bin/sh\necho {t0 + 24 * 3600 - 1}\n", encoding="utf-8")
    clock.chmod(0o755)
    before = subprocess.run(
        ["bash", str(PUBLISH_SCRIPT)], cwd=work, env=env, capture_output=True, text=True
    )
    assert before.returncode == 0, before.stdout + before.stderr
    assert "oldest pending nextpyp_0.7.3_20261118" in before.stdout

    # At t0 + 24h: fails even though the latest commit (t1) is only 12h old.
    clock.write_text(f"#!/bin/sh\necho {t0 + 24 * 3600}\n", encoding="utf-8")
    clock.chmod(0o755)
    after = subprocess.run(
        ["bash", str(PUBLISH_SCRIPT)], cwd=work, env=env, capture_output=True, text=True
    )
    assert after.returncode == 1, after.stdout + after.stderr
    assert "oldest pending nextpyp_0.7.3_20261118" in after.stdout
    assert "Inventory publication lag exceeds" in after.stdout

    # Once nextpyp is published and only newer tool_1.0_20261119 is pending,
    # elapsed time is measured from t1 (12h < 24h) and defers cleanly.
    if missing_layer == "cvmfs":
        (state / "missing-cvmfs").write_text("tool_1.0_20261119\n", encoding="utf-8")
        newer_only = subprocess.run(
            ["bash", str(PUBLISH_SCRIPT)],
            cwd=work,
            env=env,
            capture_output=True,
            text=True,
        )
        assert newer_only.returncode == 0, newer_only.stdout + newer_only.stderr
        assert "oldest pending tool_1.0_20261119" in newer_only.stdout


def test_publish_object_missing_does_not_hide_overdue_cvmfs_missing(tmp_path: Path):
    work, catalog, nectar, aws, state, bin_dir, env = _setup_publish_fixture(tmp_path)
    t0 = 1_700_000_000
    t1 = t0 + 22 * 3600

    first_state = {
        "microscopy": {
            "nextpyp": {
                "apps": {"nextpyp 0.7.3": {"version": "20261118"}},
                "categories": ["cryo EM"],
            }
        }
    }
    _commit_catalog(catalog, first_state, t0, "add nextpyp")

    second_state = {
        **first_state,
        "neuroimaging": {
            "fresh": {
                "apps": {"fresh 1.0": {"version": "20261119"}},
                "categories": ["testing"],
            }
        },
    }
    _commit_catalog(catalog, second_state, t1, "add fresh")
    _write_merged_apps(catalog, second_state)

    # nextpyp is in object storage but missing from CVMFS (27h overdue);
    # fresh is not yet in object storage (only 5h old).
    _upload(nectar, aws, "nextpyp_0.7.3_20261118")
    (state / "missing-cvmfs").write_text("nextpyp_0.7.3_20261118\n", encoding="utf-8")

    clock = bin_dir / "date"
    clock.write_text(f"#!/bin/sh\necho {t0 + 27 * 3600}\n", encoding="utf-8")
    clock.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PUBLISH_SCRIPT)], cwd=work, env=env, capture_output=True, text=True
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "oldest pending nextpyp_0.7.3_20261118" in result.stdout
    assert "Inventory publication lag exceeds" in result.stdout


def test_publish_first_parent_history_ignores_side_branch_absence(tmp_path: Path):
    work, catalog, nectar, aws, state, bin_dir, env = _setup_publish_fixture(tmp_path)
    t_base = 1_700_000_000
    t_main = t_base + 3600
    t_side = t_main + 10 * 3600
    t_merge = t_main + 12 * 3600

    base_state = {
        "neuroimaging": {
            "old": {
                "apps": {"old 1.0": {"version": "20261110"}},
                "categories": ["testing"],
            }
        }
    }
    _commit_catalog(catalog, base_state, t_base, "base")

    # Side branch created off base does NOT contain mritools, and commits later than t_main.
    _git(catalog, "checkout", "-b", "side")
    side_state = {
        **base_state,
        "datascience": {},
    }
    _commit_catalog(catalog, side_state, t_side, "add datascience domain")

    # Main adds mritools at t_main, then merges side at t_merge.
    _git(catalog, "checkout", "main")
    main_state = {
        "neuroimaging": {
            **base_state["neuroimaging"],
            "mritools": {
                "apps": {"mritools 4.9.2": {"version": "20261122"}},
                "categories": ["testing"],
            },
        }
    }
    _commit_catalog(catalog, main_state, t_main, "add mritools")

    merge_env = {
        "GIT_AUTHOR_DATE": f"{t_merge} +0000",
        "GIT_COMMITTER_DATE": f"{t_merge} +0000",
    }
    _git(catalog, "merge", "--no-ff", "side", "-m", "merge side", env=merge_env)

    merged_state = {**main_state, "datascience": {}}
    _write_merged_apps(catalog, merged_state)
    _upload(nectar, aws, "old_1.0_20261110")
    _upload(nectar, aws, "mritools_4.9.2_20261122")
    (state / "missing-cvmfs").write_text("mritools_4.9.2_20261122\n", encoding="utf-8")

    # At t_main + 24h (only 12h after t_merge), mritools must still be overdue.
    clock = bin_dir / "date"
    clock.write_text(f"#!/bin/sh\necho {t_main + 24 * 3600}\n", encoding="utf-8")
    clock.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PUBLISH_SCRIPT)], cwd=work, env=env, capture_output=True, text=True
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "oldest pending mritools_4.9.2_20261122" in result.stdout
    assert "Inventory publication lag exceeds" in result.stdout
