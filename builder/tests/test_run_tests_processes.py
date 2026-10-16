from __future__ import annotations

import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from builder import run_tests

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="process liveness checks require Linux /proc"
)


def is_running(pid: int) -> bool:
    try:
        status = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return False
    return status.rsplit(")", 1)[1].split()[0] != "Z"


@pytest.fixture
def process_tree(tmp_path):
    gate = tmp_path / "gate"
    os.mkfifo(gate)
    descriptor = os.open(gate, os.O_RDWR | os.O_NONBLOCK)
    (tmp_path / "grandchild.sh").write_text(
        "trap '' TERM\n"
        'printf "%s" "$BASHPID" > grandchild.pid\n'
        "printf partial-out; printf partial-err >&2\n"
        "read -r line < gate\n"
        "printf leaked > after-timeout.txt\n"
        "read -r line < gate\n"
    )
    (tmp_path / "child.sh").write_text(
        "trap '' TERM\n"
        'printf "%s" "$BASHPID" > child.pid\n'
        "bash grandchild.sh &\nwait\n"
    )
    try:
        yield descriptor
    finally:
        for path in tmp_path.glob("*.pid"):
            pid = int(path.read_text())
            if is_running(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        os.close(descriptor)


@pytest.mark.parametrize(
    "container",
    [
        None,
        pytest.param(
            os.environ.get("NEUROCONTAINERS_TEST_SIF"),
            marks=pytest.mark.skipif(
                not os.environ.get("NEUROCONTAINERS_TEST_SIF"),
                reason="requires an Apptainer image in NEUROCONTAINERS_TEST_SIF",
            ),
        ),
    ],
    ids=["local", "container"],
)
@pytest.mark.parametrize(
    ("parent_action", "redirect"),
    [
        ("wait", ""),
        ("wait", ">/dev/null 2>&1"),
        ("exit 0", ""),
        ('kill -TERM "$$"', ""),
    ],
    ids=["waiting-parent", "closed-output", "exited-parent", "signaled-parent"],
)
def test_timeout_stops_child_and_grandchild_before_next_test(
    tmp_path, process_tree, parent_action, redirect, container
) -> None:
    result = run_tests.run_single_test(
        {
            "command": (
                'printf "%s" "$BASHPID" > launcher.pid\n'
                f"bash child.sh {redirect} &\n"
                "while [ ! -s grandchild.pid ]; do sleep 0.01; done\n"
                f"{parent_action}\n"
            ),
            "timeout": 1,
        },
        container,
        {},
        tmp_path,
    )

    assert not result.passed
    assert result.message == "Timeout after 1s"
    assert result.exit_code == 0
    assert 1 <= result.duration < 5
    assert result.stdout == ("" if redirect else "partial-out")
    assert result.stderr == ("" if redirect else "partial-err")
    pids = {
        name: int((tmp_path / f"{name}.pid").read_text())
        for name in ("launcher", "child", "grandchild")
    }
    os.write(process_tree, b"continue\n")
    following = run_tests.run_single_test(
        {"command": "sleep 0.1; printf next-test"}, container, {}, tmp_path
    )
    assert following.passed
    assert following.stdout == "next-test"
    observed = {
        "delayed_write": (tmp_path / "after-timeout.txt").exists(),
        "running": [name for name, pid in pids.items() if is_running(pid)],
    }
    assert observed == {"delayed_write": False, "running": []}
    assert not Path(f"/proc/{pids['launcher']}").exists()
    assert not list(tmp_path.glob(".test*"))


@pytest.mark.parametrize("timeout", [None, 5])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_process_output_and_exit_status(tmp_path, timeout, exit_code) -> None:
    result = run_tests.run_single_test(
        {
            "command": f"printf stdout; printf stderr >&2; exit {exit_code}",
            "expected_exit_code": exit_code,
            "timeout": timeout,
        },
        None,
        {},
        tmp_path,
    )

    assert result.passed, result.message
    assert result.stdout == "stdout"
    assert result.stderr == "stderr"
    assert result.exit_code == exit_code
    assert not list(tmp_path.iterdir())


def test_timeout_does_not_stop_an_independent_concurrent_test(tmp_path) -> None:
    gate = tmp_path / "healthy-gate"
    os.mkfifo(gate)
    descriptor = os.open(gate, os.O_RDWR | os.O_NONBLOCK)
    parent_group = os.getpgrp()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            healthy = executor.submit(
                run_tests.run_single_test,
                {
                    "command": "printf ready > ready; read -r line < healthy-gate; printf healthy",
                    "timeout": 10,
                },
                None,
                {},
                tmp_path,
            )
            try:
                deadline = time.monotonic() + 5
                while not (tmp_path / "ready").exists():
                    assert time.monotonic() < deadline, (
                        "concurrent command did not start"
                    )
                    time.sleep(0.01)
                failed = run_tests.run_single_test(
                    {"command": "exec sleep 10", "timeout": 0.1},
                    None,
                    {},
                    tmp_path,
                )
                assert failed.message == "Timeout after 0.1s"
                assert not healthy.done()
            finally:
                os.write(descriptor, b"continue\n")
            result = healthy.result(timeout=5)
    finally:
        os.close(descriptor)

    assert result.passed, result.message
    assert result.stdout == "healthy"
    assert os.getpgrp() == parent_group
    assert not list(tmp_path.glob(".test*"))
