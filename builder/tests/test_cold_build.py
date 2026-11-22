import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from workflows.cold_build import build_inputs_sha256, merge, plan


def create(root, name, base="ubuntu:24.04", fulltest=True, **extra):
    directory = root / "recipes" / name
    directory.mkdir(parents=True)
    data = {
        "name": name,
        "version": "1",
        "architectures": ["x86_64", "aarch64"],
        "build": {"base-image": base},
        **extra,
    }
    (directory / "build.yaml").write_text(yaml.safe_dump(data))
    if fulltest:
        (directory / "fulltest.yaml").write_text("tests: []")
    return directory / "build.yaml"


def update_recipe(path: Path, **updates) -> None:
    data = yaml.safe_load(path.read_text())
    data.update(updates)
    path.write_text(yaml.safe_dump(data))


def passed_state(targets: list[dict], when: date) -> dict:
    return merge(
        {},
        [
            {
                **row,
                "attempted_at": when.isoformat(),
                "succeeded_at": when.isoformat(),
                "succeeded_recipe_sha256": row["recipe_sha256"],
                "succeeded_build_inputs_sha256": row["build_inputs_sha256"],
                "status": "passed",
            }
            for row in targets
        ],
    )


def policy():
    return {
        "bases": {"ubuntu:24.04": {"expires": "2029-05-01"}},
        "cadence_days": 45,
        "coverage_days": 60,
        "retry_days": 7,
        "batch_size": 2,
    }


def test_expired_unknown_and_missing_tests_are_explicit(tmp_path):
    create(tmp_path, "supported")
    create(tmp_path, "unknown", "debian:bookworm")
    create(tmp_path, "missing", fulltest=False)
    report = plan(tmp_path, policy(), {}, date(2026, 10, 3))
    assert report["eligible"] == 2
    assert len(report["excluded"]) == 4
    assert {row["reason"] for row in report["excluded"]} == {
        "base lifecycle is not in the supported policy",
        "fulltest.yaml is missing",
    }
    assert plan(tmp_path, policy(), {}, date(2029, 5, 1))["eligible"] == 0


def test_failed_retries_do_not_starve_untested_targets(tmp_path):
    for name in ("a", "b", "c"):
        create(tmp_path, name)
    state, visited = {}, set()
    today = date(2026, 10, 3)
    for offset in range(60):
        now = today + timedelta(days=offset)
        report = plan(tmp_path, policy(), state, now)
        assert len(report["targets"]) <= 2
        visited.update(row["container"] for row in report["targets"])
        state = merge(
            state,
            [
                {**target, "attempted_at": now.isoformat(), "status": "failed"}
                for target in report["targets"]
            ],
        )
    assert len(visited) == 6
    assert len(report["overdue"]) == 6


def test_successful_coverage_and_change_rescheduling(tmp_path):
    path = create(tmp_path, "tool")
    today = date(2026, 10, 3)
    targets = plan(tmp_path, policy(), {}, today)["targets"]
    state = passed_state(targets, today)
    assert not plan(tmp_path, policy(), state, today + timedelta(days=44))["targets"]
    assert not plan(tmp_path, policy(), state, today + timedelta(days=59))["overdue"]
    assert (
        len(plan(tmp_path, policy(), state, today + timedelta(days=60))["overdue"]) == 2
    )
    update_recipe(path, version="2")
    assert len(plan(tmp_path, policy(), state, today)["targets"]) == 2


def test_failed_attempt_preserves_previous_success_for_coverage(tmp_path):
    create(tmp_path, "tool")
    today = date(2026, 10, 3)
    row = plan(tmp_path, policy(), {}, today)["targets"][0]
    state = merge({}, [{**row, "status": "passed", "succeeded_at": today.isoformat()}])
    state = merge(
        state, [{**row, "status": "failed", "attempted_at": today.isoformat()}]
    )
    assert state[row["container"]]["succeeded_at"] == today.isoformat()
    assert state[row["container"]]["status"] == "failed"


def test_sustained_batch_covers_all_targets_before_sixty_days(tmp_path):
    for number in range(10):
        create(tmp_path, f"tool{number}")
    state = {}
    today = date(2026, 10, 3)
    for offset in range(120):
        now = today + timedelta(days=offset)
        report = plan(tmp_path, policy(), state, now)
        state = merge(
            state,
            [
                {
                    **row,
                    "attempted_at": now.isoformat(),
                    "succeeded_at": now.isoformat(),
                    "succeeded_recipe_sha256": row["recipe_sha256"],
                    "succeeded_build_inputs_sha256": row["build_inputs_sha256"],
                    "status": "passed",
                }
                for row in report["targets"]
            ],
        )
        if offset >= 10:
            assert not report["overdue"]
    assert len(state) == 20


@pytest.mark.parametrize(
    "updates",
    [
        {"domains": ["microscopy"]},
        {"icon": "data:image/png;base64,aGVsbG8="},
        {"auto_update": {"method": "github_release", "repo": "example/tool"}},
        {"copyright": [{"license": "MIT", "url": "https://example.com/license"}]},
        {"draft": True},
        {"categories": ["structural imaging"]},
        {"readme": "Docs for {{ context.name }}/{{ context.version }}"},
        {"readme_url": "https://example.com/readme"},
        {"structured_readme": {"description": "Plain text for {{ arch }}"}},
    ],
)
def test_non_image_recipe_changes_keep_fresh_targets_current(tmp_path, updates):
    path = create(
        tmp_path,
        "tool",
        domains=["neuroimaging"],
        categories=["programming"],
        readme="Initial docs",
    )
    today = date(2026, 10, 3)
    initial = plan(tmp_path, policy(), {}, today)["targets"]
    state = passed_state(initial, today)

    update_recipe(path, **updates)

    report = plan(tmp_path, policy(), state, today)
    assert report["targets"] == []
    assert report["overdue"] == []
    assert (
        report["current_hashes"]["tool"]["recipe_sha256"] != initial[0]["recipe_sha256"]
    )
    assert (
        report["current_hashes"]["tool"]["build_inputs_sha256"]
        == initial[0]["build_inputs_sha256"]
    )


@pytest.mark.parametrize(
    "updates",
    [
        {"version": "2"},
        {"build": {"base-image": "ubuntu:24.04", "directives": [{"run": ["echo hi"]}]}},
        {"files": [{"name": "helper.sh", "contents": "echo hi"}]},
        {"variables": {"pinned": "1.2.3"}},
        {"readme": '{{ get_file("asset") }}'},
        {"categories": ["{{ context.category }}"]},
    ],
)
def test_build_input_changes_mark_fresh_targets_due_and_overdue(tmp_path, updates):
    path = create(tmp_path, "tool")
    today = date(2026, 10, 3)
    initial = plan(tmp_path, policy(), {}, today)["targets"]
    state = passed_state(initial, today)

    update_recipe(path, **updates)

    report = plan(tmp_path, policy(), state, today)
    assert len(report["targets"]) == 2
    assert len(report["overdue"]) == 2


def test_removing_non_passive_readme_template_marks_target_due(tmp_path):
    path = create(tmp_path, "tool", readme='{{ get_file("asset") }}')
    today = date(2026, 10, 3)
    state = passed_state(plan(tmp_path, policy(), {}, today)["targets"], today)

    update_recipe(path, readme="Plain documentation")

    report = plan(tmp_path, policy(), state, today)
    assert len(report["targets"]) == 2
    assert len(report["overdue"]) == 2


def test_legacy_state_stays_fresh_backfills_and_survives_catalog_edit(tmp_path):
    tool_path = create(tmp_path, "tool", domains=["neuroimaging"])
    create(tmp_path, "failed_tool")
    create(tmp_path, "stale_tool")
    today = date(2026, 10, 3)
    initial = plan(tmp_path, policy(), {}, today)
    hashes = initial["current_hashes"]

    legacy_state = {
        "tool": {
            "recipe": "tool",
            "container": "tool",
            "variant": "",
            "architecture": "x86_64",
            "version": "1",
            "base": "ubuntu:24.04",
            "recipe_sha256": hashes["tool"]["recipe_sha256"],
            "succeeded_recipe_sha256": hashes["tool"]["recipe_sha256"],
            "attempted_at": today.isoformat(),
            "succeeded_at": today.isoformat(),
            "status": "passed",
        },
        "tool_arm64": {
            "recipe": "tool",
            "container": "tool_arm64",
            "variant": "arm64",
            "architecture": "aarch64",
            "version": "1",
            "base": "ubuntu:24.04",
            "recipe_sha256": hashes["tool_arm64"]["recipe_sha256"],
            "succeeded_recipe_sha256": hashes["tool_arm64"]["recipe_sha256"],
            "attempted_at": today.isoformat(),
            "succeeded_at": today.isoformat(),
            "status": "passed",
        },
        "failed_tool": {
            "recipe": "failed_tool",
            "container": "failed_tool",
            "variant": "",
            "architecture": "x86_64",
            "version": "1",
            "base": "ubuntu:24.04",
            "recipe_sha256": hashes["failed_tool"]["recipe_sha256"],
            "attempted_at": today.isoformat(),
            "status": "failed",
        },
        "stale_tool": {
            "recipe": "stale_tool",
            "container": "stale_tool",
            "variant": "",
            "architecture": "x86_64",
            "version": "1",
            "base": "ubuntu:24.04",
            "recipe_sha256": "0" * 64,
            "succeeded_recipe_sha256": "0" * 64,
            "attempted_at": today.isoformat(),
            "succeeded_at": today.isoformat(),
            "status": "passed",
        },
    }

    # Unchanged raw build.yaml keeps legacy entries fresh (not due or overdue).
    rollout_report = plan(tmp_path, policy(), legacy_state, today)
    assert "tool" not in rollout_report["overdue"]
    assert "tool_arm64" not in rollout_report["overdue"]
    assert all(row["recipe"] != "tool" for row in rollout_report["targets"])
    assert "stale_tool" in rollout_report["overdue"]

    # Backfill runs during merge without mutating the caller's dict.
    backfilled = merge(
        legacy_state, [], current_hashes=rollout_report["current_hashes"]
    )
    assert "build_inputs_sha256" not in legacy_state["tool"]
    assert (
        backfilled["tool"]["build_inputs_sha256"]
        == hashes["tool"]["build_inputs_sha256"]
    )
    assert (
        backfilled["tool"]["succeeded_build_inputs_sha256"]
        == hashes["tool"]["build_inputs_sha256"]
    )
    assert (
        backfilled["failed_tool"]["build_inputs_sha256"]
        == hashes["failed_tool"]["build_inputs_sha256"]
    )
    assert "succeeded_build_inputs_sha256" not in backfilled["failed_tool"]
    assert "build_inputs_sha256" not in backfilled["stale_tool"]
    assert "succeeded_build_inputs_sha256" not in backfilled["stale_tool"]

    # Subsequent domains-only edit does not reschedule the backfilled targets.
    update_recipe(tool_path, domains=["microscopy"])
    after_catalog_edit = plan(tmp_path, policy(), backfilled, today)
    assert "tool" not in after_catalog_edit["overdue"]
    assert "tool_arm64" not in after_catalog_edit["overdue"]
    assert all(row["recipe"] != "tool" for row in after_catalog_edit["targets"])


def test_cli_plan_and_merge_backfill_legacy_state_automatically(tmp_path):
    tool_path = create(tmp_path, "tool", domains=["neuroimaging"])
    today = date(2026, 10, 3)
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(policy()))
    state_path = tmp_path / "state.json"
    plan_path = tmp_path / "plan.json"
    observations_dir = tmp_path / "observations"
    observations_dir.mkdir()

    raw_hash = plan(tmp_path, policy(), {}, today)["current_hashes"]["tool"][
        "recipe_sha256"
    ]
    legacy_state = {
        container: {
            "recipe": "tool",
            "container": container,
            "variant": "" if container == "tool" else "arm64",
            "architecture": "x86_64" if container == "tool" else "aarch64",
            "version": "1",
            "base": "ubuntu:24.04",
            "recipe_sha256": raw_hash,
            "succeeded_recipe_sha256": raw_hash,
            "attempted_at": today.isoformat(),
            "succeeded_at": today.isoformat(),
            "status": "passed",
        }
        for container in ("tool", "tool_arm64")
    }
    state_path.write_text(json.dumps(legacy_state))

    subprocess.run(
        [
            sys.executable,
            "-m",
            "workflows.cold_build",
            "plan",
            "--root",
            str(tmp_path),
            "--policy",
            str(policy_path),
            "--state",
            str(state_path),
            "--today",
            today.isoformat(),
            "--output",
            str(plan_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "workflows.cold_build",
            "merge",
            "--state",
            str(state_path),
            "--observations",
            str(observations_dir),
            "--plan",
            str(plan_path),
            "--output",
            str(state_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    merged = json.loads(state_path.read_text())
    expected_inputs_hash = build_inputs_sha256(yaml.safe_load(tool_path.read_text()))
    assert merged["tool"]["build_inputs_sha256"] == expected_inputs_hash
    assert merged["tool"]["succeeded_build_inputs_sha256"] == expected_inputs_hash

    update_recipe(tool_path, domains=["microscopy"])
    after_edit = plan(tmp_path, policy(), merged, today)
    assert after_edit["targets"] == []
    assert after_edit["overdue"] == []
