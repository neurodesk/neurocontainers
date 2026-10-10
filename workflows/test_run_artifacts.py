"""Explicit cleanup of completed, superseded release-test run directories."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import json
import math
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import time
from typing import Iterator, Literal


_METADATA = ".fulltest-run.json"


@dataclass(frozen=True)
class ManagedRun:
    path: Path


@dataclass(frozen=True)
class CleanupEntry:
    path: Path
    action: Literal["retained", "deleted", "would-delete", "error"]
    reason: str


def _same_directory(path: Path, descriptor: int) -> bool:
    current = path.lstat()
    locked = os.fstat(descriptor)
    return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == (
        locked.st_dev,
        locked.st_ino,
    )


@contextmanager
def managed_run(output_dir: Path, *, results_path: Path) -> Iterator[ManagedRun]:
    """Own a run through execution and publication."""
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    while True:
        path = Path(tempfile.mkdtemp(prefix="fulltest-run-", dir=output_dir))
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except FileNotFoundError:
            continue
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            if _same_directory(path, descriptor):
                break
        except FileNotFoundError:
            pass
        except BaseException:
            os.close(descriptor)
            raise
        os.close(descriptor)

    record = {
        "version": 1,
        "run_id": path.name,
        "results_path": str(results_path.absolute()),
    }
    try:
        (path / _METADATA).write_text(json.dumps(record), encoding="utf-8")
        yield ManagedRun(path)
        record["completed_at"] = time.time()
        temporary = path / (_METADATA + ".tmp")
        temporary.write_text(json.dumps(record), encoding="utf-8")
        temporary.replace(path / _METADATA)
    finally:
        os.close(descriptor)


@contextmanager
def publication_guard(results_path: Path, *, blocking: bool = True) -> Iterator[None]:
    """Serialize publication and cleanup on the results parent directory inode."""
    try:
        parent = results_path.resolve().parent
    except RuntimeError as exc:
        raise ValueError(str(exc)) from exc
    if blocking:
        parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        operation = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(descriptor, operation)
        if not _same_directory(parent, descriptor):
            raise OSError("results directory identity changed")
        yield
    finally:
        os.close(descriptor)


def _read_json(path: Path) -> object:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, encoding="utf-8") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("not a regular file")
        return json.load(handle)


def _read_record(path: Path, now: float) -> tuple[Path, float | None]:
    record = _read_json(path / _METADATA)
    if not isinstance(record, dict) or set(record) not in (
        {"version", "run_id", "results_path"},
        {"version", "run_id", "results_path", "completed_at"},
    ):
        raise ValueError("invalid metadata fields")
    if (
        type(record["version"]) is not int
        or record["version"] != 1
        or record["run_id"] != path.name
    ):
        raise ValueError("invalid metadata identity or version")
    destination = record["results_path"]
    if (
        not isinstance(destination, str)
        or not destination
        or "\0" in destination
        or not Path(destination).is_absolute()
    ):
        raise ValueError("invalid results destination")
    completed = record.get("completed_at")
    if "completed_at" in record and (
        type(completed) not in (int, float)
        or not math.isfinite(completed)
        or not 0 <= completed <= now
    ):
        raise ValueError("invalid completion timestamp")
    return Path(destination), completed


def _references_run(destination: Path, path: Path) -> bool:
    results = _read_json(destination)
    if not isinstance(results, dict):
        raise ValueError("latest results must be an object")
    artifacts = results.get("fulltest_artifacts", {})
    if not isinstance(artifacts, dict):
        raise ValueError("invalid artifact references")
    references = []
    for value in artifacts.values():
        if (
            not isinstance(value, str)
            or not value
            or "\0" in value
            or not Path(value).is_absolute()
        ):
            raise ValueError("invalid artifact path")
        references.append(Path(os.path.normpath(value)))
    return any(
        reference == path or path in reference.parents for reference in references
    )


def _age(value: float) -> float:
    if not math.isfinite(value) or value < 0:
        raise ValueError("older-than-days must be finite and nonnegative")
    return value


def cleanup_runs(
    output_dir: Path,
    *,
    older_than_days: float = 7,
    dry_run: bool = False,
) -> list[CleanupEntry]:
    """Retain uncertain records and remove only completed, superseded runs."""
    age = _age(older_than_days)
    now = time.time()
    cutoff = now - age * 86400
    entries = []
    for path in sorted(output_dir.resolve().iterdir()):
        if not path.name.startswith("fulltest-run-"):
            continue
        descriptor = None
        try:
            if not stat.S_ISDIR(path.lstat().st_mode):
                entries.append(CleanupEntry(path, "retained", "not a real directory"))
                continue
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                entries.append(
                    CleanupEntry(path, "retained", "active run or another cleaner")
                )
                continue
            if not _same_directory(path, descriptor):
                entries.append(
                    CleanupEntry(path, "retained", "directory identity changed")
                )
                continue
            try:
                destination, completed = _read_record(path, now)
            except (OSError, ValueError, OverflowError) as exc:
                entries.append(
                    CleanupEntry(
                        path, "retained", f"unmanaged or invalid metadata: {exc}"
                    )
                )
                continue
            if completed is None:
                entries.append(CleanupEntry(path, "retained", "unfinished run"))
                continue
            if completed > cutoff:
                entries.append(
                    CleanupEntry(path, "retained", "within retention period")
                )
                continue
            try:
                with publication_guard(destination, blocking=False):
                    latest = _references_run(destination, path)
                    if latest:
                        entries.append(
                            CleanupEntry(
                                path, "retained", "referenced by latest results"
                            )
                        )
                    elif dry_run:
                        entries.append(
                            CleanupEntry(
                                path, "would-delete", "completed and superseded"
                            )
                        )
                    else:
                        try:
                            shutil.rmtree(path)
                        except OSError as exc:
                            entries.append(CleanupEntry(path, "error", str(exc)))
                        else:
                            entries.append(
                                CleanupEntry(
                                    path, "deleted", "completed and superseded"
                                )
                            )
            except BlockingIOError:
                entries.append(
                    CleanupEntry(
                        path, "retained", "results publication or cleanup in progress"
                    )
                )
            except (OSError, ValueError) as exc:
                entries.append(
                    CleanupEntry(
                        path,
                        "retained",
                        f"latest results unavailable or invalid: {exc}",
                    )
                )
        except FileNotFoundError:
            entries.append(CleanupEntry(path, "retained", "already removed"))
        except OSError as exc:
            entries.append(CleanupEntry(path, "error", str(exc)))
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return entries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("builder"),
        help="Directory containing retained test runs (default: builder)",
    )
    parser.add_argument(
        "--older-than-days",
        type=float,
        default=7,
        help="Minimum days since completion (default: 7; zero selects all ages)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show eligible runs without deleting them",
    )
    args = parser.parse_args(argv)
    try:
        _age(args.older_than_days)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        entries = cleanup_runs(
            args.output_dir,
            older_than_days=args.older_than_days,
            dry_run=args.dry_run,
        )
    except OSError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    for entry in entries:
        print(f"{entry.action}: {entry.path}: {entry.reason}")
    return int(any(entry.action == "error" for entry in entries))


if __name__ == "__main__":
    raise SystemExit(main())
