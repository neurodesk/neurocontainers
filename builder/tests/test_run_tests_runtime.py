from __future__ import annotations

import os
import queue
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml

from builder import run_tests
from builder.runtime_execution import (
    TestSpec as RuntimeTestSpec,
    evaluate_process_assertions,
)


def _write_host_suite(
    path: Path, *, host_script: str = "", cleanup_script: str = "", **variables
) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "name": path.stem,
                "version": "1.0.0",
                "setup": {"host_script": host_script},
                "cleanup": {"script": cleanup_script},
                "tests": [],
                **variables,
            }
        )
    )
    return path


@pytest.fixture
def host_health(monkeypatch) -> list[dict[str, bytes]]:
    calls = []

    def health(container_path, work_dir, variables):
        calls.append(
            {p.name: p.read_bytes() for p in work_dir.iterdir() if p.is_file()}
        )
        return run_tests.TestResult(name="health sentinel", passed=True, duration=0)

    monkeypatch.setattr(run_tests, "_run_container_health_check", health)
    return calls


@pytest.mark.parametrize("phase", ["setup", "cleanup"])
def test_oversized_host_program_runs_completely(tmp_path, host_health, phase) -> None:
    work_dir = tmp_path / "work with 'quotes' $dollars;"
    work_dir.mkdir()
    data = "".join(f"record {i:06d} | café '$HOME' ; \\literal\n" for i in range(6000))
    script = (
        "cat > data <<'PAYLOAD'\n"
        + data
        + "PAYLOAD\nprintf END > end\n"
        + ("exit 7" if phase == "cleanup" else "")
    )
    yaml_path = _write_host_suite(
        tmp_path / "oversized.yaml",
        host_script=script if phase == "setup" else "",
        cleanup_script=script if phase == "cleanup" else "",
    )

    result = run_tests.run_test_suite(
        yaml_path, tmp_path, work_dir, container_override="/bin/true"
    )

    assert (work_dir / "end").is_file(), f"host {phase} did not reach END"
    expected = {"data": data.encode(), "end": b"END"}
    assert {p.name: p.read_bytes() for p in work_dir.iterdir()} == expected
    assert host_health == [expected if phase == "setup" else {}]
    assert result.total == result.failed == 0
    assert result.results == []


@pytest.mark.parametrize("phase", ["setup", "cleanup"])
def test_host_program_preserves_substitution_and_exact_bytes(
    tmp_path, host_health, phase
) -> None:
    work_dir = tmp_path / "work with 'quotes' $dollars;"
    work_dir.mkdir()
    script = (
        'cat "$0" > program\nprintf %s "$0" > script-path\n'
        "cat > data <<'PAYLOAD'\n"
        "${alias}|$label|$HOME|${untouched}|\\literal\n"
        "PAYLOAD\nprintf '%s' '" + os.fsdecode(b"\xfe") + "' >> data"
    )
    yaml_path = _write_host_suite(
        tmp_path / "bytes.yaml",
        host_script=script if phase == "setup" else "",
        cleanup_script=script if phase == "cleanup" else "",
        alias="${label}",
        label="café",
    )

    result = run_tests.run_test_suite(
        yaml_path, tmp_path, work_dir, container_override="/bin/true"
    )

    assert result.failed == 0
    assert (work_dir / "data").read_bytes() == (
        "café|café|$HOME|${untouched}|\\literal\n".encode() + b"\xfe"
    )
    expected_script = script.replace("${alias}", "café").replace("$label", "café")
    assert (work_dir / "program").read_bytes() == os.fsencode(expected_script)
    script_path = Path((work_dir / "script-path").read_text())
    assert script_path.is_absolute()
    assert not script_path.is_relative_to(work_dir.resolve())
    assert not script_path.parent.exists()
    assert len(host_health) == 1


def test_host_program_inherits_real_stdin_and_environment(tmp_path) -> None:
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    yaml_path = _write_host_suite(
        tmp_path / "stdin.yaml",
        host_script=(
            "IFS= read -r received\n"
            'printf "%s|%s|%s\\n" "$received" "$HOST_TRANSPORT_VALUE" "$#" '
            "> inherited\n"
            "cat >> inherited"
        ),
        env={"HOST_TRANSPORT_VALUE": "suite-only"},
        env_setup="export HOST_TRANSPORT_VALUE=setup-only",
    )
    driver = """
import sys
from pathlib import Path
from builder import run_tests
run_tests._run_container_health_check = lambda *args: run_tests.TestResult(
    name="health sentinel", passed=True, duration=0
)
result = run_tests.run_test_suite(
    Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[2]),
    container_override="/bin/true",
)
assert result.failed == 0, result
"""
    result = subprocess.run(
        [sys.executable, "-c", driver, str(yaml_path), str(work_dir)],
        input=b"input \\ literal\nchild-input\n",
        env={**os.environ, "HOST_TRANSPORT_VALUE": "inherited"},
        capture_output=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert (work_dir / "inherited").read_bytes() == (
        b"input \\ literal|inherited|0\nchild-input\n"
    )


@pytest.mark.parametrize("stderr", ["setup-error", ""])
def test_host_setup_nonzero_returns_before_health_and_cleanup(
    tmp_path, host_health, stderr
) -> None:
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    yaml_path = _write_host_suite(
        tmp_path / "nonzero.yaml",
        host_script=f"printf setup-out; printf '%s' '{stderr}' >&2; exit 7",
        cleanup_script="touch cleanup-ran",
    )
    callbacks = []
    results = queue.Queue()

    result = run_tests.run_test_suite(
        yaml_path,
        tmp_path,
        work_dir,
        container_override="/bin/true",
        on_test_complete=lambda *args: callbacks.append(args),
        result_queue=results,
    )

    assert result.total == 0
    assert result.failed == 1
    (failure,) = result.results
    assert failure.name == "Setup (host)"
    assert not failure.passed
    if stderr:
        assert failure.message == "Host setup failed: setup-error"
    else:
        assert failure.message.startswith("Host setup failed: Command '")
        assert "returned non-zero exit status 7" in failure.message
    assert failure.stdout == failure.stderr == failure.start_time == ""
    assert failure.exit_code == failure.duration == 0
    assert not host_health
    assert not callbacks
    assert results.empty()
    assert not list(work_dir.iterdir())


@pytest.mark.parametrize(
    ("body", "status", "stdout", "stderr"),
    [
        ("false\nprintf 'continued\\377'", 0, b"continued\xff", b""),
        ("set -e\nfalse\nprintf unreachable", 1, b"", b""),
        (
            'set -eu\nvalue=selected\nprintf %s "$value"\nfalse',
            1,
            b"selected",
            b"",
        ),
        (
            "trap 'printf trapped >&2' EXIT\nprintf out\nexit 7",
            7,
            b"out",
            b"trapped",
        ),
        (
            "exec /bin/sh -c 'printf replaced; printf \"\\376\" >&2; exit 9'",
            9,
            b"replaced",
            b"\xfe",
        ),
    ],
    ids=["default-options", "errexit", "script-options", "exit-trap", "exec"],
)
def test_host_shell_policy_and_file_disposal(
    tmp_path, body, status, stdout, stderr
) -> None:
    script = '#!/not/a/real/interpreter\nprintf %s "$0" > script-path\n' + body

    result = run_tests._run_host_script(script, tmp_path)

    assert result.returncode == status
    assert result.stdout == stdout
    assert result.stderr == stderr
    script_path = Path((tmp_path / "script-path").read_text())
    assert not script_path.parent.exists()


@pytest.mark.parametrize("failure", ["partial-write", "missing-cwd", "unexpected"])
def test_host_temporary_files_are_removed_on_errors(
    tmp_path, monkeypatch, failure
) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    monkeypatch.setattr(run_tests.tempfile, "gettempdir", lambda: str(scratch))
    launches = []
    if failure == "partial-write":
        write_bytes = Path.write_bytes

        def fail_write(path, data):
            write_bytes(path, data[:5])
            raise OSError("partial write")

        monkeypatch.setattr(Path, "write_bytes", fail_write)
        error, message = OSError, "partial write"
    elif failure == "missing-cwd":
        work_dir = work_dir / "missing"
        real_run = subprocess.run

        def missing_cwd(command, **kwargs):
            launches.append(command)
            return real_run(command, **kwargs)

        monkeypatch.setattr(run_tests.subprocess, "run", missing_cwd)
        error, message = FileNotFoundError, "missing"
    else:

        def fail_run(command, **kwargs):
            launches.append(command)
            assert Path(command[1]).read_bytes() == b"printf unused"
            raise RuntimeError("unexpected launch error")

        monkeypatch.setattr(run_tests.subprocess, "run", fail_run)
        error, message = RuntimeError, "unexpected launch error"

    with pytest.raises(error, match=message):
        run_tests._run_host_script("printf unused", work_dir)
    assert not list(scratch.iterdir())
    assert len(launches) == (0 if failure == "partial-write" else 1)


@pytest.mark.skipif(os.geteuid() == 0, reason="requires enforced directory permissions")
@pytest.mark.parametrize("temp_root", ["overlapping-alias", "unwritable"])
def test_host_scratch_does_not_require_writable_work_parent(
    tmp_path, monkeypatch, temp_root
) -> None:
    parent = tmp_path / "parent"
    work_dir = parent / "work"
    work_dir.mkdir(parents=True)
    standard = tmp_path / "standard-temp"
    if temp_root == "overlapping-alias":
        (work_dir / "scratch").mkdir()
        standard.symlink_to(work_dir / "scratch", target_is_directory=True)
    else:
        standard.mkdir(mode=0o555)
    monkeypatch.setattr(run_tests.tempfile, "gettempdir", lambda: str(standard))
    parent.chmod(0o555)
    try:
        with pytest.raises(PermissionError):
            (parent / "sibling").mkdir()
        result = run_tests._run_host_script('printf %s "$0" > script-path', work_dir)
        assert result.returncode == 0, result.stderr
        script_path = Path((work_dir / "script-path").read_text())
        assert not script_path.is_relative_to(work_dir.resolve())
        assert not script_path.parent.exists()
        assert not list(standard.iterdir())
    finally:
        parent.chmod(0o755)
        if temp_root == "unwritable":
            standard.chmod(0o755)


@pytest.mark.parametrize("phase", ["setup", "cleanup"])
def test_host_allocation_failure_preserves_phase_error_policy(
    tmp_path, monkeypatch, host_health, phase
) -> None:
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    yaml_path = _write_host_suite(
        tmp_path / "allocation.yaml",
        host_script="true" if phase == "setup" else "",
        cleanup_script="true" if phase == "cleanup" else "",
    )
    roots = []

    def fail_allocation(*, prefix, dir):
        roots.append(dir)
        raise PermissionError("scratch unavailable")

    monkeypatch.setattr(run_tests.tempfile, "gettempdir", lambda: "/tmp")
    monkeypatch.setattr(run_tests.tempfile, "TemporaryDirectory", fail_allocation)
    if phase == "setup":
        with pytest.raises(PermissionError, match="scratch unavailable"):
            run_tests.run_test_suite(
                yaml_path, tmp_path, work_dir, container_override="/bin/true"
            )
        assert not host_health
    else:
        result = run_tests.run_test_suite(
            yaml_path, tmp_path, work_dir, container_override="/bin/true"
        )
        assert result.failed == 0
        assert host_health == [{}]
    assert roots == list(
        dict.fromkeys([Path("/tmp").resolve(), Path("/var/tmp").resolve()])
    )


def test_concurrent_host_suites_use_distinct_live_scripts(
    tmp_path, host_health
) -> None:
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    script = (
        'set -e\nprintf %s "$0" > ${token}.path\n'
        + shlex.quote(sys.executable)
        + " - <<'WAIT'\n"
        "import time\nfrom pathlib import Path\n"
        "Path('${token}.ready').touch()\n"
        "deadline = time.monotonic() + 10\n"
        "while not Path('release').exists():\n"
        "    if time.monotonic() >= deadline:\n"
        "        raise SystemExit('barrier timed out')\n"
        "    time.sleep(0.01)\n"
        "WAIT\nprintf '${token}-END' > ${token}.out"
    )
    suites = [
        _write_host_suite(tmp_path / f"{token}.yaml", host_script=script, token=token)
        for token in ("first", "second")
    ]
    paths = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                run_tests.run_test_suite,
                suite,
                tmp_path,
                work_dir,
                container_override="/bin/true",
            )
            for suite in suites
        ]
        try:
            deadline = time.monotonic() + 5
            while len(list(work_dir.glob("*.ready"))) != 2:
                assert time.monotonic() < deadline, "scripts must reach the barrier"
                time.sleep(0.01)
            for token in ("first", "second"):
                path = Path((work_dir / f"{token}.path").read_text())
                assert path.is_absolute()
                assert not path.is_relative_to(work_dir.resolve())
                assert path.parent.stat().st_mode & 0o777 == 0o700
                assert not path.stat().st_mode & 0o111
                assert path.read_bytes() == os.fsencode(
                    script.replace("${token}", token)
                )
                paths.append(path)
            assert paths[0].parent != paths[1].parent
        finally:
            (work_dir / "release").touch()
        for future in futures:
            assert future.result(timeout=15).failed == 0
    assert len(host_health) == 2
    assert (work_dir / "first.out").read_bytes() == b"first-END"
    assert (work_dir / "second.out").read_bytes() == b"second-END"
    assert all(not path.parent.exists() for path in paths)


@pytest.mark.parametrize(
    ("assertions", "exit_code", "expected_failure"),
    [
        ({}, 0, None),
        ({}, 7, "Expected exit code 0, got 7"),
        ({"expected_exit_code": 7}, 7, None),
        ({"expected_exit_code": 7}, 0, "Expected exit code 7, got 0"),
        ({"expected_exit_code": 7, "expected_exit_code_not": 0}, 3, None),
        (
            {"expected_exit_code": 7, "expected_exit_code_not": 0},
            0,
            "Exit code should not be 0",
        ),
        ({"expected_exit_code_not": None}, 7, "Expected exit code 0, got 7"),
        ({"ignore_exit_code": True, "expected_exit_code_not": 7}, 7, None),
        (
            {"ignore_exit_code": True, "expected_output_contains": "missing"},
            7,
            "Expected output not found: 'missing...'",
        ),
        ({"expected_output_contains": "missing"}, 7, "Expected exit code 0, got 7"),
    ],
)
def test_process_assertion_precedence(assertions, exit_code, expected_failure) -> None:
    spec = RuntimeTestSpec.from_mapping({"command": "true", **assertions})

    assert evaluate_process_assertions(spec, exit_code, "", "", {}) == expected_failure


def test_output_assertions_expand_variables_and_include_both_streams() -> None:
    spec = RuntimeTestSpec.from_mapping(
        {
            "command": "true",
            "expected_output_contains": ["${value}", 42, "", "outerr"],
            "description": "accepted metadata",
            "depends_on": ["earlier"],
            "custom_metadata": {"owner": "suite"},
        }
    )

    assert (
        evaluate_process_assertions(spec, 0, "value 42 out", "err", {"value": "value"})
        is None
    )


def test_command_takes_precedence_over_script_and_preserves_result(tmp_path) -> None:
    result = run_tests.run_single_test(
        {
            "name": "nonzero",
            "command": "printf stdout; printf stderr >&2; exit 7",
            "script": "exit 99",
            "expected_exit_code": 7,
        },
        None,
        {},
        tmp_path,
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
def test_script_preparation_failure_cleans_up_all_files(
    tmp_path, monkeypatch, failed_creation
) -> None:
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


@pytest.mark.parametrize(
    "invalid_fields", [{"validate": None}, {"expected_output_contains": 42}]
)
def test_named_parse_failure_preserves_identity_and_traceback(
    tmp_path, invalid_fields
) -> None:
    result = run_tests.run_single_test(
        {"name": "retry this test", "command": "true", **invalid_fields},
        None,
        {},
        tmp_path,
    )

    assert not result.passed
    assert result.name == "retry this test"
    assert result.message.startswith("Error: ")
    assert "Traceback (most recent call last)" in result.stderr
    assert "TypeError:" in result.stderr
    assert result.exit_code == 0
    assert not list(tmp_path.iterdir())


def test_unexpected_execution_failure_is_contained_with_traceback(
    tmp_path, monkeypatch
) -> None:
    def fail_execution(*args, **kwargs):
        raise RuntimeError("runtime failed unexpectedly")

    monkeypatch.setattr(run_tests, "_execute_test_script", fail_execution)
    result = run_tests.run_single_test(
        {"name": "broken", "script": "true"}, None, {}, tmp_path
    )

    assert not result.passed
    assert result.name == "broken"
    assert result.message == "Error: runtime failed unexpectedly"
    assert "Traceback (most recent call last)" in result.stderr
    assert "RuntimeError: runtime failed unexpectedly" in result.stderr
    assert result.exit_code == 0
    assert not list(tmp_path.iterdir())


def test_unexpected_assertion_failure_preserves_process_output(
    tmp_path, monkeypatch
) -> None:
    def fail_check(path):
        raise RuntimeError("validation bug")

    monkeypatch.setattr(run_tests, "check_file_exists", fail_check)
    result = run_tests.run_single_test(
        {
            "command": "printf stdout; printf stderr >&2; exit 7",
            "expected_exit_code": 7,
            "validate": [{"output_exists": "output"}],
        },
        None,
        {},
        tmp_path,
    )

    assert not result.passed
    assert result.message == "Error: validation bug"
    assert result.stdout == "stdout"
    assert result.stderr.startswith("stderr\n\nTraceback")
    assert "RuntimeError: validation bug" in result.stderr
    assert result.exit_code == 7
    assert not list(tmp_path.iterdir())


def test_timeout_preserves_real_process_output_and_cleans_scripts(tmp_path) -> None:
    result = run_tests.run_single_test(
        {
            "script": "printf partial-out; printf partial-err >&2; exec sleep 5",
            "timeout": 0.2,
        },
        None,
        {},
        tmp_path,
    )

    assert not result.passed
    assert result.message == "Timeout after 0.2s"
    assert result.stdout == "partial-out"
    assert result.stderr == "partial-err"
    assert result.exit_code == 0
    assert result.duration >= 0.2
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("output", [b"partial\xff", "partial", None])
def test_timeout_decodes_output_and_uses_suite_default(
    tmp_path, monkeypatch, output
) -> None:
    def timeout_execution(script, container, variables, work, timeout):
        assert timeout == 31
        raise subprocess.TimeoutExpired(script, timeout, output=output, stderr=output)

    monkeypatch.setattr(run_tests, "_execute_test_script", timeout_execution)
    result = run_tests.run_single_test(
        {"command": "true"}, None, {}, tmp_path, default_timeout=31
    )

    expected = "partial\ufffd" if isinstance(output, bytes) else output or ""
    assert not result.passed
    assert result.message == "Timeout after 31s"
    assert result.stdout == result.stderr == expected
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("env_override", "expected"),
    [
        ({}, "global"),
        ({"env_setup": "export VALUE=local"}, "local"),
        ({"env_setup": None}, "unset"),
        ({"env_setup": ""}, "unset"),
    ],
)
def test_test_environment_overrides_suite_environment(
    tmp_path, env_override, expected
) -> None:
    result = run_tests.run_single_test(
        {"command": "printf '%s' \"${VALUE:-unset}\"", **env_override},
        None,
        {},
        tmp_path,
        global_env_setup="export VALUE=global",
    )

    assert result.passed, result.message
    assert result.stdout == expected


@pytest.mark.parametrize("script_runner", [None, "bash"])
def test_script_payload_runs_in_work_directory_with_spaces(
    tmp_path, script_runner
) -> None:
    work = tmp_path / "work directory"
    work.mkdir()
    result = run_tests.run_single_test(
        {
            "script": "printf '${value}' > output",
            "validate": [{"output_exists": "output"}],
        },
        None,
        {"value": "payload"},
        work,
        script_runner=script_runner,
    )

    assert result.passed, result.stderr or result.message
    assert (work / "output").read_text() == "payload"
    assert not list(work.glob(".test*"))


def test_script_runner_receives_extension_and_prepared_environment(tmp_path) -> None:
    runner = tmp_path / "runner"
    runner.write_text(
        '#!/bin/sh\ncase "$1" in *.m) ;; *) exit 3 ;; esac\nexec bash "$1"\n'
    )
    runner.chmod(0o755)
    result = run_tests.run_single_test(
        {"script": "printf '%s' \"$VALUE\""},
        None,
        {"value": "configured"},
        tmp_path,
        global_env_setup="export VALUE=${value}",
        script_runner=str(runner),
        script_ext=".m",
    )

    assert result.passed, result.stderr or result.message
    assert result.stdout == "configured"
    assert not list(tmp_path.glob(".test*"))


@pytest.mark.parametrize(
    ("command", "expected_message"),
    [
        ("exit 7", "Expected exit code 0, got 7"),
        ("true", "Expected output not found: 'missing...'"),
    ],
)
def test_process_failures_precede_file_assertions(
    tmp_path, monkeypatch, command, expected_message
) -> None:
    def unexpected_check(path):
        pytest.fail("file assertions must follow process assertions")

    monkeypatch.setattr(run_tests, "check_file_exists", unexpected_check)
    result = run_tests.run_single_test(
        {
            "command": command,
            "expected_output_contains": "missing",
            "validate": [{"output_exists": "missing"}],
        },
        None,
        {},
        tmp_path,
    )

    assert not result.passed
    assert result.message == expected_message


def test_file_assertions_expand_variables_and_fail_in_order(tmp_path) -> None:
    result = run_tests.run_single_test(
        {
            "command": "true",
            "validate": [
                {"output_exists": "${first}"},
                {"output_exists": "second"},
            ],
        },
        None,
        {"first": "first"},
        tmp_path,
    )

    assert not result.passed
    assert result.message == f"Output file not found: {tmp_path / 'first'}"


def test_dimension_assertion_checks_actual_images(tmp_path) -> None:
    nib = pytest.importorskip("nibabel")
    np = pytest.importorskip("numpy")

    nib.save(nib.Nifti1Image(np.zeros((2, 3, 4)), np.eye(4)), tmp_path / "first.nii")
    nib.save(nib.Nifti1Image(np.zeros((2, 3, 5)), np.eye(4)), tmp_path / "second.nii")
    result = run_tests.run_single_test(
        {
            "command": "true",
            "validate": [{"same_dimensions": ["${first}", "second.nii"]}],
        },
        None,
        {"first": "first.nii"},
        tmp_path,
    )

    assert not result.passed
    assert result.message == "Dimension mismatch: (2, 3, 4) vs (2, 3, 5)"


def test_container_runtime_command_prefers_apptainer(monkeypatch) -> None:
    def fake_which(command: str) -> str | None:
        return (
            f"/usr/bin/{command}" if command in {"apptainer", "singularity"} else None
        )

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
        {
            "name": "versioned runner",
            "script": "printf 'payload-${upstream_version}\\n'",
            "expected_output_contains": "runner-2.3\npayload-2.3",
        },
        None,
        {"upstream_version": "2.3", "runner_dir": str(tmp_path)},
        tmp_path,
        script_runner="${runner_dir}/runner-${upstream_version}",
    )
    assert result.passed, result.stderr or result.message


def test_container_setup_and_tests_share_the_output_directory(
    tmp_path, monkeypatch
) -> None:
    work = tmp_path / "suite"
    work.mkdir()
    fallback = tmp_path / "runtime-default"
    fallback.mkdir()
    real_popen = subprocess.Popen

    def runtime_run(command, **kwargs):
        # Model a runtime whose default cwd differs from the host subprocess cwd.
        cwd = command[command.index("--pwd") + 1] if "--pwd" in command else fallback
        payload = command[command.index("image.sif") + 1 :]
        return real_popen(payload, **{**kwargs, "cwd": cwd})

    monkeypatch.setattr(run_tests.subprocess, "Popen", runtime_run)
    error = run_tests._run_setup_in_container(
        "mkdir -p output\nprintf fixture > output/input", "image.sif", work, {}
    )
    assert error is None, error
    assert (work / "output/input").read_text() == "fixture"
    result = run_tests.run_single_test(
        {
            "name": "relative output",
            "command": "cp output/input output/result",
            "validate": [{"output_exists": "output/result"}],
        },
        "image.sif",
        {},
        work,
    )
    assert result.passed, result.message
    assert (work / "output/result").read_text() == "fixture"


def test_setup_preserves_container_software_and_binds_host_data(
    tmp_path, monkeypatch
) -> None:
    work = tmp_path / "work"
    data = tmp_path / "data"
    work.mkdir()
    data.mkdir()
    commands = []
    real_popen = subprocess.Popen

    def runtime_run(command, **kwargs):
        commands.append(command)
        return real_popen(["true"], **kwargs)

    monkeypatch.setattr(run_tests.subprocess, "Popen", runtime_run)
    variables = {"tool_dir": "/opt/tool", "input": str(data / "image.nii")}
    error = run_tests._run_setup_in_container("true", "image.sif", work, variables)
    assert error is None
    result = run_tests.run_single_test(
        {"name": "software available", "command": "true"},
        "image.sif",
        variables,
        work,
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
def test_real_container_root_is_readonly_and_workspace_is_writable(
    tmp_path, monkeypatch
):
    image = str(Path(os.environ["NEUROCONTAINERS_TEST_SIF"]).resolve())
    real_popen = subprocess.Popen
    checked = []

    def check_root(command, **kwargs):
        payload = command.index(image) + 1
        probe = """
options=$(awk '$2 == "/" {print $4}' /proc/mounts)
case ",$options," in
  *,ro,*) ;;
  *) echo "Expected read-only image root, got $options" >&2; exit 1 ;;
esac
exec "$@"
"""
        result = real_popen(
            [*command[:payload], "bash", "-c", probe, "root-probe", *command[payload:]],
            **kwargs,
        )
        checked.append(result)
        return result

    monkeypatch.setattr(run_tests.subprocess, "Popen", check_root)
    health = run_tests._run_container_health_check(Path(image), tmp_path, {})
    assert health.passed, health.stderr or health.message
    error = run_tests._run_setup_in_container(
        "printf fixture > input", image, tmp_path, {}
    )
    assert error is None, error
    result = run_tests.run_single_test(
        {"name": "bound fixture", "command": "cat input > output"},
        image,
        {},
        tmp_path,
    )
    assert result.passed, result.stderr or result.message
    assert (tmp_path / "output").read_text() == "fixture"
    assert len(checked) == 3
