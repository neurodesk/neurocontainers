from __future__ import annotations

import argparse
import subprocess
from types import SimpleNamespace

import pytest

from builder import cli


def test_cmd_login_returns_docker_exit_code_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = argparse.Namespace(dry_run=False, offline_mode=False)
    commands: list[list[str]] = []

    monkeypatch.setattr(cli, "cmd_build", lambda args: 0)
    monkeypatch.setattr(
        cli,
        "compile_from_args",
        lambda args: (
            object(),
            SimpleNamespace(
                tag="tool:1.0",
                architecture="x86_64",
                recipe_dir=cli.Path("/repo/recipes/tool"),
            ),
        ),
    )
    monkeypatch.setattr(
        subprocess,
        "call",
        lambda command: commands.append(command) or 130,
    )
    monkeypatch.setattr(
        subprocess,
        "check_call",
        lambda command: (_ for _ in ()).throw(
            AssertionError("cmd_login should not raise on docker run exit")
        ),
    )

    assert cli.cmd_login(args) == 130
    assert commands == [
        [
            "docker",
            "run",
            "--platform",
            "linux/amd64",
            "--rm",
            "-v",
            "/repo/recipes/tool:/buildhostdirectory",
            "-it",
            "tool:1.0",
        ]
    ]


def test_cmd_stage_can_download_declared_url_files(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    compiled = SimpleNamespace(
        name="tool",
        version="1.0",
        architecture="x86_64",
        staging_plan=SimpleNamespace(files={"archive": object()}),
    )
    config = SimpleNamespace(repo_root=object(), output_root=object())
    build_dir = SimpleNamespace()
    dockerfile_path = SimpleNamespace()

    def fake_write_build_files(*args, **kwargs):
        calls.append(kwargs)
        return build_dir, dockerfile_path

    monkeypatch.setattr(cli, "compile_from_args", lambda args: (config, compiled))
    monkeypatch.setattr(cli, "write_build_files", fake_write_build_files)

    args = argparse.Namespace(output_root=None, recreate=True, download=True)

    assert cli.cmd_stage(args) == 0
    assert calls == [{"recreate": True, "stage": True, "download": True}]


def test_write_build_files_rejects_empty_readme(tmp_path: cli.Path) -> None:
    compiled = SimpleNamespace(name="tool", readme=" \n")

    with pytest.raises(ValueError, match="compiled README.*cannot be empty"):
        cli.write_build_files(tmp_path, compiled, tmp_path / "build")
