from __future__ import annotations

import subprocess

from builder import run_tests


def test_container_runtime_command_prefers_apptainer(monkeypatch) -> None:
    def fake_which(command: str) -> str | None:
        return f"/usr/bin/{command}" if command in {"apptainer", "singularity"} else None

    monkeypatch.setattr(run_tests.shutil, "which", fake_which)

    assert run_tests.container_runtime_command() == "apptainer"


def test_container_runtime_command_falls_back_to_singularity(monkeypatch) -> None:
    def fake_which(command: str) -> str | None:
        return "/usr/bin/singularity" if command == "singularity" else None

    monkeypatch.setattr(run_tests.shutil, "which", fake_which)

    assert run_tests.container_runtime_command() == "singularity"


def test_container_runtime_command_keeps_existing_error_path(monkeypatch) -> None:
    monkeypatch.setattr(run_tests.shutil, "which", lambda command: None)

    assert run_tests.container_runtime_command() == "apptainer"


def test_substitute_variables_expands_nested_values() -> None:
    variables = {
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-${tool_version}",
    }

    assert (
        run_tests.substitute_variables("${tool_dir}/bin/tool", variables)
        == "/opt/tool-3.2.8/bin/tool"
    )


def test_top_level_variables_support_container_patterns() -> None:
    config = {
        "name": "tool",
        "version": "${tool_version}",
        "container": "tool_${tool_version}_*.simg",
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-${tool_version}",
        "tests": [],
    }

    variables = run_tests.collect_top_level_variables(config)

    assert variables == {
        "name": "tool",
        "version": "3.2.8",
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-3.2.8",
    }
    assert (
        run_tests.substitute_variables(config["container"], variables)
        == "tool_3.2.8_*.simg"
    )


def test_container_variables_expand_literal_recipe_version() -> None:
    config = {
        "name": "deeplabcut",
        "version": "2.3.11",
        "container": "${name}_${version}_REFERENCE.simg",
    }

    variables = run_tests.collect_top_level_variables(config)

    assert (
        run_tests.substitute_variables(config["container"], variables)
        == "deeplabcut_2.3.11_REFERENCE.simg"
    )


def test_script_runner_expands_independent_version_variables(tmp_path) -> None:
    runner = tmp_path / "runner-2.3"
    runner.write_text('#!/bin/sh\nprintf "runner-2.3\\n"\nexec bash "$1"\n')
    runner.chmod(0o755)
    result = run_tests.run_single_test(
        {"name": "versioned runner", "script": "printf 'payload-${upstream_version}\\n'",
         "expected_output_contains": "runner-2.3\npayload-2.3"},
        None,
        {"upstream_version": "2.3", "runner_dir": str(tmp_path)},
        tmp_path,
        script_runner='${runner_dir}/runner-${upstream_version}',
    )
    assert result.passed, result.stderr or result.message


def test_container_setup_and_tests_share_the_output_directory(tmp_path, monkeypatch) -> None:
    work = tmp_path / "suite"
    work.mkdir()
    fallback = tmp_path / "runtime-default"
    fallback.mkdir()
    real_run = subprocess.run

    def runtime_run(command, **kwargs):
        # Model a runtime whose default cwd differs from the host subprocess cwd.
        cwd = command[command.index("--pwd") + 1] if "--pwd" in command else fallback
        payload = command[command.index("image.sif") + 1:]
        return real_run(payload, **{**kwargs, "cwd": cwd})

    monkeypatch.setattr(run_tests.subprocess, "run", runtime_run)
    error = run_tests._run_setup_in_container(
        "mkdir -p output\nprintf fixture > output/input", "image.sif", work, {}
    )
    assert error is None, error
    assert (work / "output/input").read_text() == "fixture"
    result = run_tests.run_single_test(
        {"name": "relative output", "command": "cp output/input output/result",
         "validate": [{"output_exists": "output/result"}]},
        "image.sif", {}, work,
    )
    assert result.passed, result.message
    assert (work / "output/result").read_text() == "fixture"
