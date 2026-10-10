from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from builder import run_tests
from builder.runtime_execution import TestSpec as RuntimeTestSpec, evaluate_process_assertions


@pytest.mark.parametrize(
    ("assertions", "exit_code", "expected_failure"),
    [
        ({}, 0, None),
        ({}, 7, "Expected exit code 0, got 7"),
        ({"expected_exit_code": 7}, 7, None),
        ({"expected_exit_code": 7}, 0, "Expected exit code 7, got 0"),
        ({"expected_exit_code": 7, "expected_exit_code_not": 0}, 3, None),
        ({"expected_exit_code": 7, "expected_exit_code_not": 0}, 0, "Exit code should not be 0"),
        ({"expected_exit_code_not": None}, 7, "Expected exit code 0, got 7"),
        ({"ignore_exit_code": True, "expected_exit_code_not": 7}, 7, None),
        ({"ignore_exit_code": True, "expected_output_contains": "missing"}, 7, "Expected output not found: 'missing...'"),
        ({"expected_output_contains": "missing"}, 7, "Expected exit code 0, got 7"),
    ],
)
def test_process_assertion_precedence(assertions, exit_code, expected_failure) -> None:
    spec = RuntimeTestSpec.from_mapping({"command": "true", **assertions})

    assert evaluate_process_assertions(spec, exit_code, "", "", {}) == expected_failure


def test_output_assertions_expand_variables_and_include_both_streams() -> None:
    spec = RuntimeTestSpec.from_mapping({
        "command": "true",
        "expected_output_contains": ["${value}", 42, "", "outerr"],
        "description": "accepted metadata",
        "depends_on": ["earlier"],
        "custom_metadata": {"owner": "suite"},
    })

    assert evaluate_process_assertions(spec, 0, "value 42 out", "err", {"value": "value"}) is None


def test_command_takes_precedence_over_script_and_preserves_result(tmp_path) -> None:
    result = run_tests.run_single_test(
        {
            "name": "nonzero",
            "command": "printf stdout; printf stderr >&2; exit 7",
            "script": "exit 99",
            "expected_exit_code": 7,
        },
        None, {}, tmp_path,
    )

    assert result.passed
    assert result.name == "nonzero"
    assert result.message == "OK"
    assert result.stdout == "stdout"
    assert result.stderr == "stderr"
    assert result.exit_code == 7
    assert result.start_time
    assert result.duration >= 0
    assert not list(tmp_path.glob(".test*"))


def test_missing_command_keeps_result_defaults(tmp_path) -> None:
    result = run_tests.run_single_test({"name": "missing"}, None, {}, tmp_path)

    assert not result.passed
    assert result.message == "No command or script specified"
    assert result.duration == 0
    assert result.exit_code == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("failed_creation", [1, 2])
def test_script_preparation_failure_cleans_up_all_files(tmp_path, monkeypatch, failed_creation) -> None:
    original_chmod = Path.chmod
    creations = 0

    def fail_chmod(path, mode):
        nonlocal creations
        creations += 1
        if creations == failed_creation:
            raise PermissionError("script permission denied")
        return original_chmod(path, mode)

    monkeypatch.setattr(Path, "chmod", fail_chmod)
    result = run_tests.run_single_test({"script": "true"}, None, {}, tmp_path)

    assert not result.passed
    assert result.message == "Failed to create test script: script permission denied"
    assert result.exit_code == 0
    assert result.stderr == ""
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("invalid_fields", [{"validate": None}, {"expected_output_contains": 42}])
def test_named_parse_failure_preserves_identity_and_traceback(tmp_path, invalid_fields) -> None:
    result = run_tests.run_single_test(
        {"name": "retry this test", "command": "true", **invalid_fields},
        None, {}, tmp_path,
    )

    assert not result.passed
    assert result.name == "retry this test"
    assert result.message.startswith("Error: ")
    assert "Traceback (most recent call last)" in result.stderr
    assert "TypeError:" in result.stderr
    assert result.exit_code == 0
    assert not list(tmp_path.iterdir())


def test_unexpected_execution_failure_is_contained_with_traceback(tmp_path, monkeypatch) -> None:
    def fail_execution(*args, **kwargs):
        raise RuntimeError("runtime failed unexpectedly")

    monkeypatch.setattr(run_tests.subprocess, "run", fail_execution)
    result = run_tests.run_single_test({"name": "broken", "script": "true"}, None, {}, tmp_path)

    assert not result.passed
    assert result.name == "broken"
    assert result.message == "Error: runtime failed unexpectedly"
    assert "Traceback (most recent call last)" in result.stderr
    assert "RuntimeError: runtime failed unexpectedly" in result.stderr
    assert result.exit_code == 0
    assert not list(tmp_path.iterdir())


def test_unexpected_assertion_failure_preserves_process_output(tmp_path, monkeypatch) -> None:
    def fail_check(path):
        raise RuntimeError("validation bug")

    monkeypatch.setattr(run_tests, "check_file_exists", fail_check)
    result = run_tests.run_single_test({
        "command": "printf stdout; printf stderr >&2; exit 7",
        "expected_exit_code": 7,
        "validate": [{"output_exists": "output"}],
    }, None, {}, tmp_path)

    assert not result.passed
    assert result.message == "Error: validation bug"
    assert result.stdout == "stdout"
    assert result.stderr.startswith("stderr\n\nTraceback")
    assert "RuntimeError: validation bug" in result.stderr
    assert result.exit_code == 7
    assert not list(tmp_path.iterdir())


def test_timeout_preserves_real_process_output_and_cleans_scripts(tmp_path) -> None:
    result = run_tests.run_single_test({
        "script": "printf partial-out; printf partial-err >&2; exec sleep 5",
        "timeout": 0.2,
    }, None, {}, tmp_path)

    assert not result.passed
    assert result.message == "Timeout after 0.2s"
    assert result.stdout == "partial-out"
    assert result.stderr == "partial-err"
    assert result.exit_code == 0
    assert result.duration >= 0.2
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("output", [b"partial\xff", "partial", None])
def test_timeout_decodes_output_and_uses_suite_default(tmp_path, monkeypatch, output) -> None:
    def timeout_execution(command, **kwargs):
        assert kwargs["timeout"] == 31
        raise subprocess.TimeoutExpired(command, 31, output=output, stderr=output)

    monkeypatch.setattr(run_tests.subprocess, "run", timeout_execution)
    result = run_tests.run_single_test({"command": "true"}, None, {}, tmp_path, default_timeout=31)

    expected = "partial\ufffd" if isinstance(output, bytes) else output or ""
    assert not result.passed
    assert result.message == "Timeout after 31s"
    assert result.stdout == result.stderr == expected
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("env_override", "expected"),
    [({}, "global"), ({"env_setup": "export VALUE=local"}, "local"), ({"env_setup": None}, "unset"), ({"env_setup": ""}, "unset")],
)
def test_test_environment_overrides_suite_environment(tmp_path, env_override, expected) -> None:
    result = run_tests.run_single_test(
        {"command": "printf '%s' \"${VALUE:-unset}\"", **env_override},
        None, {}, tmp_path,
        global_env_setup="export VALUE=global",
    )

    assert result.passed, result.message
    assert result.stdout == expected


@pytest.mark.parametrize("script_runner", [None, "bash"])
def test_script_payload_runs_in_work_directory_with_spaces(tmp_path, script_runner) -> None:
    work = tmp_path / "work directory"
    work.mkdir()
    result = run_tests.run_single_test(
        {"script": "printf '${value}' > output", "validate": [{"output_exists": "output"}]},
        None, {"value": "payload"}, work, script_runner=script_runner,
    )

    assert result.passed, result.stderr or result.message
    assert (work / "output").read_text() == "payload"
    assert not list(work.glob(".test*"))


def test_script_runner_receives_extension_and_prepared_environment(tmp_path) -> None:
    runner = tmp_path / "runner"
    runner.write_text('#!/bin/sh\ncase "$1" in *.m) ;; *) exit 3 ;; esac\nexec bash "$1"\n')
    runner.chmod(0o755)
    result = run_tests.run_single_test(
        {"script": "printf '%s' \"$VALUE\""}, None, {"value": "configured"}, tmp_path,
        global_env_setup="export VALUE=${value}", script_runner=str(runner), script_ext=".m",
    )

    assert result.passed, result.stderr or result.message
    assert result.stdout == "configured"
    assert not list(tmp_path.glob(".test*"))


@pytest.mark.parametrize(
    ("command", "expected_message"),
    [("exit 7", "Expected exit code 0, got 7"), ("true", "Expected output not found: 'missing...'")],
)
def test_process_failures_precede_file_assertions(tmp_path, monkeypatch, command, expected_message) -> None:
    def unexpected_check(path):
        pytest.fail("file assertions must follow process assertions")

    monkeypatch.setattr(run_tests, "check_file_exists", unexpected_check)
    result = run_tests.run_single_test({
        "command": command,
        "expected_output_contains": "missing",
        "validate": [{"output_exists": "missing"}],
    }, None, {}, tmp_path)

    assert not result.passed
    assert result.message == expected_message


def test_file_assertions_expand_variables_and_fail_in_order(tmp_path) -> None:
    result = run_tests.run_single_test({
        "command": "true",
        "validate": ["ignored metadata", {"unknown_assertion": True}, {"output_exists": "${first}"}, {"output_exists": "second"}],
    }, None, {"first": "first"}, tmp_path)

    assert not result.passed
    assert result.message == f"Output file not found: {tmp_path / 'first'}"


def test_dimension_assertion_checks_actual_images(tmp_path) -> None:
    nib = pytest.importorskip("nibabel")
    np = pytest.importorskip("numpy")

    nib.save(nib.Nifti1Image(np.zeros((2, 3, 4)), np.eye(4)), tmp_path / "first.nii")
    nib.save(nib.Nifti1Image(np.zeros((2, 3, 5)), np.eye(4)), tmp_path / "second.nii")
    result = run_tests.run_single_test({
        "command": "true",
        "validate": [{"same_dimensions": ["${first}", "second.nii"]}],
    }, None, {"first": "first.nii"}, tmp_path)

    assert not result.passed
    assert result.message == "Dimension mismatch: (2, 3, 4) vs (2, 3, 5)"


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


def test_setup_preserves_container_software_and_binds_host_data(tmp_path, monkeypatch) -> None:
    work = tmp_path / "work"
    data = tmp_path / "data"
    work.mkdir()
    data.mkdir()
    commands = []

    def runtime_run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(run_tests.subprocess, "run", runtime_run)
    variables = {"tool_dir": "/opt/tool", "input": str(data / "image.nii")}
    error = run_tests._run_setup_in_container("true", "image.sif", work, variables)
    assert error is None
    result = run_tests.run_single_test(
        {"name": "software available", "command": "true"},
        "image.sif", variables, work,
    )
    assert result.passed
    for command in commands:
        assert "--writable-tmpfs" not in command
        binds = [command[index + 1] for index, arg in enumerate(command) if arg == "-B"]
        assert "/opt:/opt" not in binds
        assert f"{work}:{work}" in binds
        assert f"{data}:{data}" in binds

    health = run_tests._run_container_health_check(Path("image.sif"), work, variables)
    assert health.passed, health.message
    assert "--writable-tmpfs" not in commands[-1]


@pytest.mark.skipif(
    not os.environ.get("NEUROCONTAINERS_TEST_SIF"),
    reason="requires an Apptainer image in NEUROCONTAINERS_TEST_SIF",
)
def test_real_container_root_is_readonly_and_workspace_is_writable(tmp_path, monkeypatch):
    image = str(Path(os.environ["NEUROCONTAINERS_TEST_SIF"]).resolve())
    real_run = subprocess.run
    checked = []

    def check_root(command, **kwargs):
        payload = command.index(image) + 1
        probe = '''
options=$(awk '$2 == "/" {print $4}' /proc/mounts)
case ",$options," in
  *,ro,*) ;;
  *) echo "Expected read-only image root, got $options" >&2; exit 1 ;;
esac
exec "$@"
'''
        result = real_run(
            [*command[:payload], "bash", "-c", probe, "root-probe", *command[payload:]],
            **kwargs,
        )
        checked.append(result)
        return result

    monkeypatch.setattr(run_tests.subprocess, "run", check_root)
    health = run_tests._run_container_health_check(Path(image), tmp_path, {})
    assert health.passed, health.stderr or health.message
    error = run_tests._run_setup_in_container(
        "printf fixture > input", image, tmp_path, {}
    )
    assert error is None, error
    result = run_tests.run_single_test(
        {"name": "bound fixture", "command": "cat input > output"},
        image, {}, tmp_path,
    )
    assert result.passed, result.stderr or result.message
    assert (tmp_path / "output").read_text() == "fixture"
    assert len(checked) == 3
