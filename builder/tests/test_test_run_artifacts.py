from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import select
import subprocess
import sys

import pytest

from workflows import test_run_artifacts as artifacts


CTX = multiprocessing.get_context("fork")


def _completed(root: Path, destination: Path) -> Path:
    with artifacts.managed_run(root, results_path=destination) as run:
        (run.path / "log").write_text("diagnostic")
    return run.path


def _publish(destination: Path, run: Path | None = None) -> None:
    destination.write_text(
        json.dumps({"fulltest_artifacts": {"log": str(run / "log")} if run else {}})
    )


def _receive(connection):
    assert connection.poll(10), "child did not reach barrier"
    return connection.recv()


def _join(process):
    process.join(10)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("child did not finish")
    assert process.exitcode == 0


def _active(root, destination, connection):
    with artifacts.managed_run(root, results_path=destination) as run:
        connection.send(str(run.path))
        connection.recv()
        _publish(destination, run.path)


def test_process_ownership_and_latest_publication(tmp_path):
    destination = tmp_path / "results.json"
    _publish(destination)
    parent, child = CTX.Pipe()
    process = CTX.Process(target=_active, args=(tmp_path, destination, child))
    process.start()
    try:
        path = Path(_receive(parent))
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].reason
            == "active run or another cleaner"
        )
        assert path.is_dir()
        parent.send("publish")
        _join(process)
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].reason
            == "referenced by latest results"
        )
        _publish(destination)
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].action == "deleted"
        )
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
        child.close()


def test_age_boundary_dry_run_and_repeat(tmp_path, monkeypatch):
    destination = tmp_path / "results.json"
    _publish(destination)
    monkeypatch.setattr(artifacts.time, "time", lambda: 1000000)
    path = _completed(tmp_path, destination)
    before = {item.relative_to(path): item.read_bytes() for item in path.iterdir()}
    monkeypatch.setattr(artifacts.time, "time", lambda: 1000000 + 7 * 86400 - 1)
    assert artifacts.cleanup_runs(tmp_path)[0].reason == "within retention period"
    monkeypatch.setattr(artifacts.time, "time", lambda: 1000000 + 7 * 86400)
    assert artifacts.cleanup_runs(tmp_path, dry_run=True)[0].action == "would-delete"
    assert {
        item.relative_to(path): item.read_bytes() for item in path.iterdir()
    } == before
    assert artifacts.cleanup_runs(tmp_path)[0].action == "deleted"
    assert artifacts.cleanup_runs(tmp_path) == []


def test_each_external_destination_protects_its_latest(tmp_path):
    root = tmp_path / "runs"
    one, two = tmp_path / "one.json", tmp_path / "two.json"
    old = _completed(root, one)
    first = _completed(root, one)
    second = _completed(root, two)
    _publish(one, first)
    _publish(two, second)
    actions = {
        entry.path: entry.action
        for entry in artifacts.cleanup_runs(root, older_than_days=0)
    }
    assert actions == {old: "deleted", first: "retained", second: "retained"}


@pytest.mark.parametrize(
    "latest",
    [
        None,
        "{",
        "[]",
        '{"fulltest_artifacts": null}',
        '{"fulltest_artifacts": []}',
        '{"fulltest_artifacts": {"log": 1}}',
        '{"fulltest_artifacts": {"log": "relative/log"}}',
        '{"fulltest_artifacts": {"log": ""}}',
        '{"fulltest_artifacts": {"log": "\\u0000"}}',
    ],
)
def test_unverifiable_latest_is_retained(tmp_path, latest):
    destination = tmp_path / "results.json"
    path = _completed(tmp_path, destination)
    if latest is not None:
        destination.write_text(latest)
    (entry,) = artifacts.cleanup_runs(tmp_path, older_than_days=0)
    assert entry.action == "retained"
    assert "latest results unavailable or invalid" in entry.reason
    assert (path / "log").read_text() == "diagnostic"


@pytest.mark.parametrize("latest", [{}, {"fulltest_artifacts": {}}])
def test_valid_results_without_artifacts_supersede_run(tmp_path, latest):
    destination = tmp_path / "results.json"
    path = _completed(tmp_path, destination)
    destination.write_text(json.dumps(latest))
    assert artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].action == "deleted"
    assert not path.exists()


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 2},
        {"run_id": "fulltest-run-other"},
        {"results_path": "relative.json"},
        {"results_path": None},
        {"extra": 1},
        {"completed_at": None},
        {"completed_at": True},
        {"completed_at": -1},
        {"completed_at": float("nan")},
        {"completed_at": float("inf")},
        {"completed_at": 10**100},
    ],
)
def test_invalid_metadata_is_retained(tmp_path, change):
    destination = tmp_path / "results.json"
    _publish(destination)
    path = _completed(tmp_path, destination)
    metadata = path / artifacts._METADATA
    record = json.loads(metadata.read_text())
    record.update(change)
    metadata.write_text(json.dumps(record))
    (entry,) = artifacts.cleanup_runs(tmp_path, older_than_days=0)
    assert entry.action == "retained"
    assert "invalid metadata" in entry.reason
    assert path.exists()


def test_legacy_unfinished_and_symlinks_are_safe(tmp_path):
    root = tmp_path / "runs"
    destination = tmp_path / "results.json"
    _publish(destination)
    target = tmp_path / "outside"
    target.mkdir()
    (target / "important").write_text("keep")
    completed = _completed(root, destination)
    (completed / "nested-link").symlink_to(target, target_is_directory=True)
    legacy = root / "fulltest-run-legacy"
    legacy.mkdir()
    (legacy / "log").write_text("old")
    (root / "fulltest-run-link").symlink_to(target, target_is_directory=True)
    with pytest.raises(RuntimeError):
        with artifacts.managed_run(root, results_path=destination) as run:
            unfinished = run.path
            raise RuntimeError("publication failed")
    actions = {
        entry.path: entry.action
        for entry in artifacts.cleanup_runs(root, older_than_days=0)
    }
    assert actions[completed] == "deleted"
    assert (
        actions[legacy]
        == actions[unfinished]
        == actions[root / "fulltest-run-link"]
        == "retained"
    )
    assert (target / "important").read_text() == "keep"
    assert (legacy / "log").read_text() == "old"


@pytest.mark.parametrize("loop", [False, True])
def test_symlink_metadata_and_latest_are_retained(tmp_path, loop):
    destination = tmp_path / "results.json"
    _publish(destination)
    one = _completed(tmp_path, destination)
    metadata = one / artifacts._METADATA
    real_metadata = tmp_path / "record"
    metadata.rename(real_metadata)
    metadata.symlink_to(real_metadata)
    two = _completed(tmp_path, destination)
    actual_results = tmp_path / "actual.json"
    destination.rename(actual_results)
    destination.symlink_to(destination.name if loop else actual_results)
    assert {
        entry.path: entry.action
        for entry in artifacts.cleanup_runs(tmp_path, older_than_days=0)
    } == {
        one: "retained",
        two: "retained",
    }


def _paused_cleaner(root, connection):
    original = artifacts.shutil.rmtree

    def remove(path):
        connection.send(str(path))
        connection.recv()
        original(path)

    artifacts.shutil.rmtree = remove
    connection.send(artifacts.cleanup_runs(root, older_than_days=0))


def test_two_cleaners_do_not_delete_under_each_other(tmp_path):
    destination = tmp_path / "results.json"
    _publish(destination)
    path = _completed(tmp_path, destination)
    parent, child = CTX.Pipe()
    process = CTX.Process(target=_paused_cleaner, args=(tmp_path, child))
    process.start()
    try:
        assert Path(_receive(parent)) == path
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].reason
            == "active run or another cleaner"
        )
        parent.send("delete")
        assert _receive(parent)[0].action == "deleted"
        _join(process)
        assert artifacts.cleanup_runs(tmp_path, older_than_days=0) == []
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
        child.close()


def _parent_with_child(root, destination, connection, ready, proceed):
    with artifacts.managed_run(root, results_path=destination) as run:
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import os,sys; from pathlib import Path; "
                "os.write(int(sys.argv[1]), b'R'); os.read(int(sys.argv[2]), 1); "
                "Path(sys.argv[3]).write_text('child survived'); os.write(int(sys.argv[1]), b'D')",
                str(ready),
                str(proceed),
                str(run.path / "child-log"),
            ],
            pass_fds=(ready, proceed),
        )
        connection.send((str(run.path), child.pid))
        connection.recv()


def test_killed_parent_with_live_child_retains_unfinished(tmp_path):
    destination = tmp_path / "results.json"
    _publish(destination)
    ready_read, ready_write = os.pipe()
    proceed_read, proceed_write = os.pipe()
    parent, child = CTX.Pipe()
    process = CTX.Process(
        target=_parent_with_child,
        args=(
            tmp_path,
            destination,
            child,
            ready_write,
            proceed_read,
        ),
    )
    process.start()
    child_pid = None
    try:
        path_string, child_pid = _receive(parent)
        path = Path(path_string)
        assert select.select([ready_read], [], [], 10)[0]
        assert os.read(ready_read, 1) == b"R"
        process.kill()
        process.join(10)
        os.kill(child_pid, 0)
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].reason
            == "unfinished run"
        )
        os.write(proceed_write, b"G")
        assert select.select([ready_read], [], [], 10)[0]
        assert os.read(ready_read, 1) == b"D"
        assert (path / "child-log").read_text() == "child survived"
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        if child_pid:
            try:
                os.kill(child_pid, 9)
            except ProcessLookupError:
                pass
        for descriptor in (ready_read, ready_write, proceed_read, proceed_write):
            os.close(descriptor)
        parent.close()
        child.close()


@pytest.mark.parametrize("age", [-1, float("nan"), float("inf"), -float("inf")])
def test_invalid_age_rejected_without_changes(tmp_path, age):
    with pytest.raises(ValueError, match="finite and nonnegative"):
        artifacts.cleanup_runs(tmp_path, older_than_days=age)
    assert list(tmp_path.iterdir()) == []


def test_locked_inode_must_still_match_directory_name(tmp_path, monkeypatch):
    destination = tmp_path / "results.json"
    _publish(destination)
    path = _completed(tmp_path, destination)
    moved = tmp_path / "moved"
    flock = artifacts.fcntl.flock

    def replace_after_lock(descriptor, operation):
        flock(descriptor, operation)
        path.rename(moved)
        path.mkdir()
        (path / "important").write_text("replacement")

    monkeypatch.setattr(artifacts.fcntl, "flock", replace_after_lock)
    (entry,) = artifacts.cleanup_runs(tmp_path, older_than_days=0)
    assert entry.reason == "directory identity changed"
    assert (path / "important").read_text() == "replacement"
    assert (moved / "log").read_text() == "diagnostic"


def _before_creation_lock(root, destination, connection):
    original = artifacts.os.open
    waiting = True

    def open_directory(path, flags, *args, **kwargs):
        nonlocal waiting
        if waiting and Path(path).name.startswith("fulltest-run-"):
            waiting = False
            connection.send(str(path))
            connection.recv()
        return original(path, flags, *args, **kwargs)

    artifacts.os.open = open_directory
    with artifacts.managed_run(root, results_path=destination) as run:
        _publish(destination, run.path)


def test_creation_before_lock_is_not_collectible(tmp_path):
    destination = tmp_path / "results.json"
    _publish(destination)
    parent, child = CTX.Pipe()
    process = CTX.Process(
        target=_before_creation_lock, args=(tmp_path, destination, child)
    )
    process.start()
    try:
        path = Path(_receive(parent))
        (entry,) = artifacts.cleanup_runs(tmp_path, older_than_days=0)
        assert entry.action == "retained"
        assert "unmanaged or invalid metadata" in entry.reason
        assert path.is_dir()
        parent.send("lock")
        _join(process)
        assert (
            artifacts.cleanup_runs(tmp_path, older_than_days=0)[0].reason
            == "referenced by latest results"
        )
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
        child.close()


def _real_main_publisher(root, destination, config, connection, *, pause=False):
    from workflows import release_test_runner as runner
    from workflows import summarize_deploy_results as summarizer

    def execute(args, *, run_dir):
        (run_dir / "log").write_text("diagnostic")
        connection.send(("run", str(run_dir)))
        return runner.RunOutcome(
            {
                "total_tests": 1,
                "passed": 1,
                "failed": 0,
                "fulltest_artifacts": {"log": str(run_dir / "log")},
                "test_results": [
                    {
                        "stdout": json.dumps(
                            {
                                "tests": [
                                    {
                                        "name": "deploy_bin:demo",
                                        "status": "passed",
                                        "message": "",
                                    }
                                ]
                            }
                        )
                    }
                ],
            }
        )

    runner.run_fulltest_release = execute
    original_flock = artifacts.fcntl.flock

    def observe_wait(descriptor, operation):
        if operation == artifacts.fcntl.LOCK_EX:
            try:
                original_flock(descriptor, operation | artifacts.fcntl.LOCK_NB)
            except BlockingIOError:
                connection.send(("blocked", None))
            else:
                return
        original_flock(descriptor, operation)

    artifacts.fcntl.flock = observe_wait
    original_builtin = summarizer._summarise_builtin

    def enrich(payload):
        connection.send(
            (
                "enrich",
                json.loads(destination.read_text()).get("fulltest_artifacts", {}),
            )
        )
        if pause:
            connection.recv()
        return original_builtin(payload)

    summarizer._summarise_builtin = enrich
    status = runner.main(
        [
            "--recipe",
            "demo",
            "--version",
            "1",
            "--release-file",
            "unused",
            "--test-config",
            str(config),
            "--output-dir",
            str(root),
            "--results-path",
            str(destination),
        ]
    )
    connection.send(("done", status))


@pytest.mark.parametrize("cross_root", [False, True])
@pytest.mark.parametrize("skipped", [False, True])
def test_real_publication_serializes_enrichment_and_cleanup(
    tmp_path, cross_root, skipped
):
    root = tmp_path / "runs"
    other = tmp_path / "other" if cross_root else root
    destination = tmp_path / "results.json"
    config = tmp_path / "fulltest.yaml"
    config.write_text("name: demo")
    old = _completed(root, destination)
    _publish(destination, old)
    first, first_child = CTX.Pipe()
    second, second_child = CTX.Pipe()
    a = CTX.Process(
        target=_real_main_publisher,
        args=(root, destination, config, first_child),
        kwargs={"pause": True},
    )
    b = CTX.Process(
        target=_real_main_publisher,
        args=(
            other,
            destination,
            tmp_path / "missing" if skipped else config,
            second_child,
        ),
    )
    a.start()
    try:
        event, run_a = _receive(first)
        assert event == "run"
        assert _receive(first) == ("enrich", {"log": str(Path(run_a) / "log")})
        b.start()
        if not skipped:
            event, run_b = _receive(second)
            assert event == "run"
        assert _receive(second) == ("blocked", None)
        entries = {
            entry.path: entry
            for entry in artifacts.cleanup_runs(root, older_than_days=0)
        }
        assert entries[old].reason == "results publication or cleanup in progress"
        assert old.exists()
        first.send("finish enrichment")
        assert _receive(first)[0] == "done"
        _join(a)
        references = {}
        if not skipped:
            event, references = _receive(second)
            assert event == "enrich"
            assert references == {"log": str(Path(run_b) / "log")}
        assert _receive(second)[0] == "done"
        _join(b)
        artifacts.cleanup_runs(root, older_than_days=0)
        if cross_root and not skipped:
            artifacts.cleanup_runs(other, older_than_days=0)
        latest = json.loads(destination.read_text()).get("fulltest_artifacts", {})
        assert latest == references
        assert all(Path(value).is_file() for value in latest.values())
        assert not old.exists()
        assert not Path(run_a).exists()
    finally:
        for process in (a, b):
            if process.pid and process.is_alive():
                process.kill()
                process.join()
        for connection in (first, first_child, second, second_child):
            connection.close()


def test_publication_waits_for_cleanup_through_delete(tmp_path):
    root = tmp_path / "runs"
    destination = tmp_path / "results.json"
    _publish(destination)
    old = _completed(root, destination)
    config = tmp_path / "fulltest.yaml"
    config.write_text("name: demo")
    cleaner, cleaner_child = CTX.Pipe()
    publisher, publisher_child = CTX.Pipe()
    a = CTX.Process(target=_paused_cleaner, args=(root, cleaner_child))
    b = CTX.Process(
        target=_real_main_publisher, args=(root, destination, config, publisher_child)
    )
    a.start()
    try:
        assert Path(_receive(cleaner)) == old
        b.start()
        assert _receive(publisher)[0] == "run"
        assert _receive(publisher) == ("blocked", None)
        cleaner.send("delete")
        assert _receive(cleaner)[0].action == "deleted"
        _join(a)
        assert _receive(publisher)[0] == "enrich"
        assert _receive(publisher)[0] == "done"
        _join(b)
        latest = json.loads(destination.read_text())["fulltest_artifacts"]
        assert all(Path(value).is_file() for value in latest.values())
    finally:
        for process in (a, b):
            if process.pid and process.is_alive():
                process.kill()
                process.join()
        for connection in (cleaner, cleaner_child, publisher, publisher_child):
            connection.close()


def test_result_parent_collision_retained_without_deadlock(tmp_path):
    path = _completed(tmp_path, tmp_path / "placeholder.json")
    destination = path / "results.json"
    record_path = path / artifacts._METADATA
    record = json.loads(record_path.read_text())
    record["results_path"] = str(destination)
    record_path.write_text(json.dumps(record))
    _publish(destination)
    (entry,) = artifacts.cleanup_runs(tmp_path, older_than_days=0)
    assert entry.reason == "results publication or cleanup in progress"
    assert path.exists()


def test_symlink_result_alias_uses_target_parent_gate(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    destination = target / "results.json"
    _publish(destination)
    alias = tmp_path / "alias.json"
    alias.symlink_to(destination)
    with artifacts.publication_guard(destination):
        with pytest.raises(BlockingIOError):
            with artifacts.publication_guard(alias, blocking=False):
                pytest.fail("alias escaped destination gate")
