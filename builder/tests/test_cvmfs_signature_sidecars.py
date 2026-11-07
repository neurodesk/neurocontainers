"""Mirror complete sidecar sets without publishing failed or partial downloads."""

from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading
from urllib.error import HTTPError

import pytest

from cvmfs import mirror_signature_sidecars as mirror


NAME = "tool_1.0_20261105"


@pytest.fixture
def installation(tmp_path):
    root = tmp_path / "cvmfs"
    container = root / "containers" / NAME
    container.mkdir(parents=True)
    (container / "commands.txt").write_text("tool\n")
    inventory = tmp_path / "log.txt"
    inventory.write_text(f"{NAME} categories:programming\n")
    return root, inventory


@contextmanager
def served(directory):
    class Handler(SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if (directory / "deny").exists():
                self.send_error(403)
            elif (directory / "fail").exists():
                self.send_error(500)
            else:
                super().do_GET()

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(Handler, directory=directory)
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.fixture
def storage(tmp_path):
    path = tmp_path / "storage"
    path.mkdir()
    for suffix in mirror.SUFFIXES:
        (path / (NAME + ".simg" + suffix)).write_text(
            json.dumps({"fixture": suffix}) + "\n"
        )
    return path


def contents(path):
    return {p.name: p.read_bytes() for p in path.iterdir()}


@pytest.mark.parametrize("umask", [0o022, 0o077])
def test_complete_set_is_staged_without_writes_then_mirrored_idempotently(
    installation, storage, tmp_path, umask
):
    root, inventory = installation
    target = root / "containers" / NAME / "signatures"
    staging = tmp_path / "staging"
    with served(storage) as source:
        report = mirror.prepare(root, inventory, source, staging)
        assert set(report["changed"]) == {NAME}
        assert not target.exists()
        old_umask = os.umask(umask)
        try:
            mirror.apply(root, staging)
        finally:
            os.umask(old_umask)
        assert target.stat().st_mode & 0o777 == 0o755
        assert contents(target) == contents(storage)
        assert all(path.stat().st_mode & 0o777 == 0o644 for path in target.iterdir())
        assert not mirror.prepare(root, inventory, source, tmp_path / "again")[
            "changed"
        ]
        (storage / (NAME + ".simg.sigstore.json")).write_text('{"new_bundle":true}\n')
        mirror.prepare(root, inventory, source, tmp_path / "updated")
        mirror.apply(root, tmp_path / "updated")
        assert contents(target) == contents(storage)


@pytest.mark.parametrize("fault", ["partial", "invalid", "deny", "fail", "disappear"])
def test_download_failure_preserves_existing_set_and_cannot_be_applied(
    installation, storage, tmp_path, fault
):
    root, inventory = installation
    with served(storage) as source:
        initial = tmp_path / "initial"
        mirror.prepare(root, inventory, source, initial)
        mirror.apply(root, initial)
        target = root / "containers" / NAME / "signatures"
        before = contents(target)
        if fault == "partial":
            (storage / (NAME + ".simg.promotion.json")).unlink()
        elif fault == "invalid":
            (storage / (NAME + ".simg.promotion.json")).write_text("not json")
        elif fault == "disappear":
            for path in storage.iterdir():
                path.unlink()
        else:
            (storage / fault).touch()
        staging = tmp_path / "failed"
        with pytest.raises((ValueError, HTTPError)):
            mirror.prepare(root, inventory, source, staging)
        assert not (staging / "plan.json").exists()
        with pytest.raises(FileNotFoundError):
            mirror.apply(root, staging)
        assert contents(target) == before


def test_absent_sidecars_are_explicitly_unsigned_and_uninstalled_images_are_skipped(
    installation, storage, tmp_path
):
    root, inventory = installation
    for path in storage.iterdir():
        path.unlink()
    inventory.write_text(
        inventory.read_text() + "other_1_20261105 categories:statistics\n"
    )
    with served(storage) as source:
        report = mirror.prepare(root, inventory, source, tmp_path / "staging")
    assert report == {
        "changed": {},
        "unsigned": [NAME],
        "not_installed": ["other_1_20261105"],
    }
    assert not (root / "containers" / NAME / "signatures").exists()


def test_bad_second_release_never_commits_successfully_staged_first_release(
    installation, storage, tmp_path
):
    root, inventory = installation
    later = "zzz_1_20261105"
    (root / "containers" / later).mkdir()
    (root / "containers" / later / "commands.txt").touch()
    inventory.write_text(inventory.read_text() + f"{later} categories:statistics\n")
    (storage / (later + ".simg.sigstore.json")).write_text("{}")
    staging = tmp_path / "staging"
    with served(storage) as source, pytest.raises(ValueError, match="Incomplete"):
        mirror.prepare(root, inventory, source, staging)
    assert (staging / NAME).exists()
    assert not (staging / "plan.json").exists()
    assert not list(root.glob("containers/*/signatures"))


def test_staged_tampering_is_rejected_before_any_writes(
    installation, storage, tmp_path
):
    root, inventory = installation
    staging = tmp_path / "staging"
    with served(storage) as source:
        mirror.prepare(root, inventory, source, staging)
    (staging / NAME / (NAME + ".simg.promotion.json")).write_text("{}")
    with pytest.raises(ValueError, match="changed"):
        mirror.apply(root, staging)
    assert not list(root.glob("containers/*/signatures"))


@pytest.mark.parametrize("entry", ["container", "signatures", "sidecar"])
def test_symlinks_cannot_redirect_writes_outside_the_installation(
    installation, storage, tmp_path, entry
):
    root, inventory = installation
    container = root / "containers" / NAME
    outside = tmp_path / "outside"
    outside.mkdir()
    if entry == "container":
        (container / "commands.txt").unlink()
        container.rmdir()
        container.symlink_to(outside, target_is_directory=True)
    elif entry == "signatures":
        (container / "signatures").symlink_to(outside, target_is_directory=True)
    else:
        (container / "signatures").mkdir()
        (container / "signatures" / (NAME + ".simg.sigstore.json")).symlink_to(
            outside / "missing"
        )
    with served(storage) as source, pytest.raises(ValueError, match="link|directory"):
        mirror.prepare(root, inventory, source, tmp_path / "staging")
    assert not list(outside.iterdir())


def test_authenticated_s3_listing_distinguishes_unsigned_from_permission_errors(
    installation, storage, tmp_path, monkeypatch
):
    binary = tmp_path / "bin"
    binary.mkdir()
    aws = binary / "aws"
    aws.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
storage = pathlib.Path(os.environ["FAKE_S3"])
if (storage / "deny").exists():
    sys.exit(254)
if args[1] == "list-objects-v2":
    prefix = args[args.index("--prefix") + 1]
    print(json.dumps({"Contents": [{"Key": p.name} for p in storage.iterdir() if p.name.startswith(prefix)]}))
elif args[1] == "get-object":
    name = args[args.index("--key") + 1]
    pathlib.Path(args[-1]).write_bytes((storage / name).read_bytes())
else:
    sys.exit(2)
""")
    aws.chmod(0o755)
    monkeypatch.setenv("PATH", str(binary) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_S3", str(storage))
    root, inventory = installation
    staging = tmp_path / "staging"
    assert NAME in mirror.prepare(root, inventory, None, staging)["changed"]
    mirror.apply(root, staging)
    assert contents(root / "containers" / NAME / "signatures") == contents(storage)
    (storage / "deny").touch()
    with pytest.raises(subprocess.CalledProcessError):
        mirror.prepare(root, inventory, None, tmp_path / "denied")
    assert not (tmp_path / "denied" / "plan.json").exists()
    # An empty successful listing, rather than an access error, is unsigned.
    for path in storage.iterdir():
        path.unlink()
    for path in (root / "containers" / NAME / "signatures").iterdir():
        path.unlink()
    report = mirror.prepare(root, inventory, None, tmp_path / "unsigned")
    assert report["unsigned"] == [NAME]


@pytest.mark.parametrize(
    "outcome", ["publish", "unchanged", "unsigned", "download_failure", "apply_failure"]
)
def test_transaction_wrapper_only_publishes_complete_sets(
    installation, storage, tmp_path, outcome
):
    root, inventory = installation
    script_dir = Path(mirror.__file__).parent
    events = tmp_path / "events"
    if outcome == "download_failure":
        (storage / "fail").touch()
    elif outcome == "unsigned":
        for path in storage.iterdir():
            path.unlink()
    program = """
source "$1/mirror_signature_sidecars.sh"
open_cvmfs_transaction() {
    echo open >> "$EVENTS"
    if [[ "$OUTCOME" == apply_failure ]]; then
        touch "$REPO_ROOT/containers/$NAME/signatures"
    fi
}
publish_cvmfs_transaction() { echo publish >> "$EVENTS"; }
abort_cvmfs_transaction() { echo abort >> "$EVENTS"; }
mirror_signature_sidecars "$1" "$2" "$3"
"""
    with served(storage) as source:
        if outcome == "unchanged":
            mirror.prepare(root, inventory, source, tmp_path / "initial")
            mirror.apply(root, tmp_path / "initial")
        result = subprocess.run(
            ["bash", "-c", program, "test", str(script_dir), str(root), str(inventory)],
            env={
                **os.environ,
                "CVMFS_SIGNATURE_SOURCE": source,
                "EVENTS": str(events),
                "OUTCOME": outcome,
                "REPO_ROOT": str(root),
                "NAME": NAME,
            },
            capture_output=True,
            text=True,
        )
    observed = events.read_text().splitlines() if events.exists() else []
    assert (
        observed
        == {
            "publish": ["open", "publish"],
            "unchanged": [],
            "unsigned": [],
            "download_failure": [],
            "apply_failure": ["open", "abort"],
        }[outcome]
    ), result.stderr
    assert result.returncode == (
        0 if outcome in {"publish", "unchanged", "unsigned"} else 2
    )
    if outcome == "publish":
        assert contents(root / "containers" / NAME / "signatures") == contents(storage)
