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
        lambda args: (object(), SimpleNamespace(tag="tool:1.0")),
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
    assert commands == [["docker", "run", "--rm", "-it", "tool:1.0"]]
