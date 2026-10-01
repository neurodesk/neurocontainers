from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import pytest

from workflows.summarize_deploy_results import _summarise_builtin, summarise_results_file


SCRIPT = Path("workflows/test_deploy.sh").resolve()


@pytest.mark.parametrize("value", [None, "", ":", ":::"])
def test_deploy_script_rejects_empty_deployment(tmp_path: Path, value: str | None) -> None:
    env = os.environ.copy()
    for name in ("DEPLOY_BINS", "DEPLOY_PATH"):
        if value is None:
            env.pop(name, None)
        else:
            env[name] = value

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        cwd=tmp_path,
        env=env,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert result.returncode == 1
    assert any(
        entry["name"] == "deploy.commands" and entry["status"] == "failed"
        for entry in payload["tests"]
    )


@pytest.mark.parametrize("mode", [None, 0o644, 0o755])
def test_deploy_script_requires_commands_in_deploy_path(
    tmp_path: Path, mode: int | None
) -> None:
    deploy_dir = tmp_path / "bin"
    deploy_dir.mkdir(mode=0o755)
    if mode is not None:
        tool = deploy_dir / "tool"
        tool.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
        tool.chmod(mode)
    has_command = mode == 0o755
    env = os.environ.copy()
    env.update({"DEPLOY_BINS": "", "DEPLOY_PATH": "bin"})

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        cwd=tmp_path,
        env=env,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert result.returncode == (0 if has_command else 1)
    assert any(
        entry["name"] == "deploy.commands"
        and entry["status"] == ("passed" if has_command else "failed")
        for entry in payload["tests"]
    )
    if has_command:
        assert any(
            entry["name"] == "deploy.commands"
            and entry["message"] == "Deployment commands found: 1."
            for entry in payload["tests"]
        )


def _run_deploy_script(tmp_path: Path, mode: int) -> tuple[int, dict]:
    tool = tmp_path / "owner-only-tool"
    tool.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
    tool.chmod(mode)

    env = os.environ.copy()
    env.update(
        {
            "DEPLOY_BINS": "owner-only-tool",
            "DEPLOY_PATH": "",
            "PATH": f".:{env.get('PATH', '')}",
        }
    )
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        cwd=tmp_path,
        env=env,
        text=True,
    )
    return result.returncode, json.loads(result.stdout)


def test_deploy_script_rejects_owner_only_executable(tmp_path: Path) -> None:
    returncode, payload = _run_deploy_script(tmp_path, stat.S_IRWXU)

    assert returncode == 1
    failed = {
        entry["name"]: entry["message"]
        for entry in payload["tests"]
        if entry["status"] == "failed"
    }
    assert any(name.startswith("file.access.arbitrary_user:") for name in failed)
    assert any("arbitrary runtime users" in message for message in failed.values())


def test_deploy_script_accepts_world_accessible_executable(tmp_path: Path) -> None:
    returncode, payload = _run_deploy_script(
        tmp_path,
        stat.S_IRUSR
        | stat.S_IWUSR
        | stat.S_IXUSR
        | stat.S_IRGRP
        | stat.S_IXGRP
        | stat.S_IROTH
        | stat.S_IXOTH,
    )

    assert returncode == 0
    assert payload["failed"] == 0


def test_deploy_script_rejects_path_as_wrapper_command(tmp_path: Path) -> None:
    tool = tmp_path / "tool"
    tool.write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
    tool.chmod(
        stat.S_IRUSR
        | stat.S_IWUSR
        | stat.S_IXUSR
        | stat.S_IRGRP
        | stat.S_IXGRP
        | stat.S_IROTH
        | stat.S_IXOTH
    )
    env = os.environ.copy()
    env.update({"DEPLOY_BINS": str(tool), "DEPLOY_PATH": ""})

    result = subprocess.run(
        ["bash", str(SCRIPT)],
        capture_output=True,
        cwd=tmp_path,
        env=env,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert result.returncode == 1
    assert {
        "name": f"deploy_bin.name:{tool}",
        "status": "failed",
        "message": (
            f"DEPLOY_BINS entry {tool} must be a command name without '/'. "
            "Add its directory to PATH or DEPLOY_PATH."
        ),
    } in payload["tests"]


def test_deploy_script_checks_directory_access_without_find(tmp_path: Path) -> None:
    deploy_root = Path(tempfile.mkdtemp(prefix="neurocontainers-deploy-", dir="/tmp"))
    try:
        deploy_root.chmod(
            stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH
        )
        deploy_dir = deploy_root / "tool"
        deploy_dir.mkdir()
        deploy_dir.chmod(
            stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH
        )

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        for command in ("mktemp", "rm", "stat", "dirname", "tr", "sort"):
            command_path = shutil.which(command)
            assert command_path is not None
            (bin_dir / command).symlink_to(command_path)

        env = os.environ.copy()
        env.update(
            {
                "DEPLOY_BINS": "",
                "DEPLOY_PATH": str(deploy_dir),
                "PATH": str(bin_dir),
            }
        )
        result = subprocess.run(
            ["/bin/bash", str(SCRIPT)],
            capture_output=True,
            cwd=tmp_path,
            env=env,
            text=True,
        )
    finally:
        shutil.rmtree(deploy_root)

    payload = json.loads(result.stdout)

    assert result.returncode == 1
    assert {
        "name": f"directory.access.arbitrary_user:{deploy_dir}",
        "status": "passed",
        "message": f"Directory {deploy_dir} is traversable by arbitrary runtime users.",
    } in payload["tests"]


def test_deploy_summary_preserves_runtime_user_access_failures() -> None:
    payload = {
        "total": 2,
        "passed": 1,
        "failed": 1,
        "skipped": 0,
        "tests": [
            {
                "name": "deploy_bin:tool",
                "status": "passed",
                "message": "Binary tool found at /usr/local/bin/tool.",
            },
            {
                "name": "file.access.arbitrary_user:/usr/local/bin/tool",
                "status": "failed",
                "message": "File /opt/tool/bin/tool is not readable by arbitrary runtime users.",
            },
        ],
    }

    summary, changed = _summarise_builtin(payload)

    assert changed is True
    assert summary["failed"] == 1
    assert summary["tests"][0]["name"] == "tool"
    assert summary["tests"][0]["status"] == "failed"
    assert "arbitrary runtime users" in summary["tests"][0]["message"]


def test_deploy_summary_ignores_json_scalar_stdout(tmp_path: Path) -> None:
    results_path = tmp_path / "test-results-ants.json"
    results = {
        "test_results": [
            {
                "name": "ImageMath mean intensity",
                "status": "passed",
                "stdout": "66.1212\n",
                "stderr": "",
            }
        ]
    }
    results_path.write_text(json.dumps(results), encoding="utf-8")

    assert summarise_results_file(results_path) is False
    assert json.loads(results_path.read_text(encoding="utf-8")) == results


def test_deploy_summary_ignores_scalar_test_entries() -> None:
    payload = {
        "total": 2,
        "passed": 1,
        "failed": 1,
        "skipped": 0,
        "tests": [
            66.1212,
            {
                "name": "deploy_bin:tool",
                "status": "passed",
                "message": "Binary tool found at /usr/local/bin/tool.",
            },
        ],
    }

    summary, changed = _summarise_builtin(payload)

    assert changed is True
    assert summary["failed"] == 0
    assert summary["tests"] == [
        {
            "name": "tool",
            "status": "passed",
            "message": "Found at /usr/local/bin/tool",
        }
    ]
