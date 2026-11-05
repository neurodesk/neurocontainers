#!/usr/bin/env python3
"""Stage SIF signature sidecars for publication inside a CVMFS transaction.

Mirroring does not verify signatures or authenticate an unpacked image.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cvmfs.check_publication import PublicationUnavailable, inventory_ids  # noqa: E402


SUFFIXES = (".sigstore.json", ".promotion.json", ".promotion.sigstore.json")
MAX_SIDECAR_BYTES = 4 * 1024 * 1024


def validate(content: bytes, location: str) -> bytes:
    if len(content) > MAX_SIDECAR_BYTES:
        raise ValueError(f"Sidecar exceeds size limit: {location}")
    if not isinstance(json.loads(content), dict):
        raise ValueError(f"Sidecar is not a JSON object: {location}")
    return content


def download(url: str) -> bytes | None:
    try:
        with urlopen(
            Request(url, headers={"User-Agent": "neurodesk-cvmfs-sync"}), timeout=30
        ) as response:
            content = response.read(MAX_SIDECAR_BYTES + 1)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise
    return validate(content, url)


def download_set(name: str, source: str | None, bucket: str) -> dict[str, bytes | None]:
    filenames = [name + ".simg" + suffix for suffix in SUFFIXES]
    if source:
        return {
            filename: download(source.rstrip("/") + "/" + quote(filename))
            for filename in filenames
        }
    # Public S3 returns 403 for absent keys without ListBucket permission. Only
    # a successful authenticated listing establishes that a release is unsigned.
    listed = subprocess.run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            name + ".simg.",
            "--output",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    keys = {entry["Key"] for entry in json.loads(listed.stdout).get("Contents", [])}
    files = dict.fromkeys(filenames)
    with tempfile.TemporaryDirectory(prefix="cvmfs-sidecars-") as temporary:
        for filename in filenames:
            if filename not in keys:
                continue
            path = Path(temporary) / filename
            subprocess.run(
                [
                    "aws",
                    "s3api",
                    "get-object",
                    "--bucket",
                    bucket,
                    "--key",
                    filename,
                    str(path),
                ],
                check=True,
                capture_output=True,
                timeout=120,
            )
            with path.open("rb") as stream:
                files[filename] = validate(stream.read(MAX_SIDECAR_BYTES + 1), filename)
    return files


def destination(root: Path, name: str) -> Path | None:
    containers = root / "containers"
    if containers.is_symlink() or not containers.is_dir():
        raise ValueError(f"Invalid containers directory: {containers}")
    container = containers / name
    if container.is_symlink():
        raise ValueError(f"Container is a symbolic link: {container}")
    if not (container / "commands.txt").is_file():
        return None
    target = container / "signatures"
    if target.is_symlink() or (target.exists() and not target.is_dir()):
        raise ValueError(f"Invalid sidecar directory: {target}")
    return target


def prepare(
    root: Path,
    inventory: Path,
    source: str | None,
    staging: Path,
    bucket: str = "neurocontainers",
) -> dict:
    staging.mkdir(parents=True, exist_ok=True)
    if any(staging.iterdir()):
        raise ValueError("Staging directory must be empty")
    report = {"changed": {}, "unsigned": [], "not_installed": []}
    for name in sorted(inventory_ids(inventory.read_text())):
        target = destination(root, name)
        if target is None:
            report["not_installed"].append(name)
            continue
        files = download_set(name, source, bucket)
        present = sum(value is not None for value in files.values())
        if present == 0:
            if any((target / filename).exists() for filename in files):
                raise ValueError(
                    f"Previously mirrored sidecars are unavailable: {name}"
                )
            report["unsigned"].append(name)
            continue
        if present != len(SUFFIXES):
            raise ValueError(f"Incomplete signature sidecar set: {name}")
        unchanged = True
        for filename, content in files.items():
            current = target / filename
            if current.is_symlink():
                raise ValueError(f"Sidecar is a symbolic link: {current}")
            if not current.is_file() or current.read_bytes() != content:
                unchanged = False
        if unchanged:
            continue
        (staging / name).mkdir()
        report["changed"][name] = {}
        for filename, content in files.items():
            (staging / name / filename).write_bytes(content)
            report["changed"][name][filename] = hashlib.sha256(content).hexdigest()
    # Only a completely staged inventory may be applied. A failed download never
    # leaves this completion marker, even if earlier releases staged successfully.
    (staging / "plan.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def apply(root: Path, staging: Path) -> None:
    report = json.loads((staging / "plan.json").read_text())
    writes = []
    for name, files in report["changed"].items():
        inventory_ids(f"{name} categories:")
        if set(files) != {name + ".simg" + suffix for suffix in SUFFIXES}:
            raise ValueError(f"Incomplete staged sidecar set: {name}")
        target = destination(root, name)
        if target is None:
            raise ValueError(f"Container is no longer installed: {name}")
        for filename, digest in files.items():
            content = (staging / name / filename).read_bytes()
            if hashlib.sha256(content).hexdigest() != digest:
                raise ValueError(f"Staged sidecar changed: {filename}")
            writes.append((target / filename, content))
    # Validate every staged set before any writes. CVMFS publish/abort makes the
    # complete inventory visible atomically, including filesystem-error rollback.
    for path, content in writes:
        try:
            path.parent.mkdir()
        except FileExistsError:
            pass
        else:
            path.parent.chmod(0o755)
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            try:
                temporary.write(content)
                temporary.flush()
                temporary_path.chmod(0o644)
                os.replace(temporary_path, path)
            finally:
                temporary_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "apply"))
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--log", type=Path)
    parser.add_argument("--source", help="HTTP mirror that returns 404 for absent keys")
    parser.add_argument("--bucket", default="neurocontainers")
    args = parser.parse_args()
    if args.mode == "prepare" and args.log is None:
        parser.error("prepare requires --log")
    try:
        if args.mode == "apply":
            apply(args.repo_root, args.staging)
            return 0
        report = prepare(
            args.repo_root, args.log, args.source, args.staging, args.bucket
        )
        print(json.dumps(report, sort_keys=True))
        return int(bool(report["changed"]))
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
        PublicationUnavailable,
    ) as error:
        print(f"[ERROR] Signature sidecar mirroring failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
