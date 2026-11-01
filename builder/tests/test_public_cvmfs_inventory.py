"""Read cleanup protection through the CVMFS client process boundary."""

import os
from pathlib import Path

import pytest

from cvmfs import check_publication as publication


@pytest.fixture
def client(tmp_path, monkeypatch):
    binary = tmp_path / "bin"
    binary.mkdir()
    script = binary / "cvmfs2"
    script.write_text(
        """#!/usr/bin/env python3
import os, pathlib, signal, sys, time
root = pathlib.Path(sys.argv[-1])
pathlib.Path(os.environ['CLIENT_ROOT']).write_text(str(root))
config = pathlib.Path(sys.argv[sys.argv.index('-o')+1].split('=',1)[1]).read_text()
pathlib.Path(os.environ['CLIENT_CONFIG']).write_text(config)
if os.environ.get('CLIENT_FAIL'):
    sys.exit(16)
if not os.environ.get('CLIENT_HANG'):
    containers = root / 'containers'
    containers.mkdir()
    (containers / 'modules').mkdir()
    (containers / 'retired_1.0_20200101').mkdir()
    if not os.environ.get('CLIENT_EMPTY'):
        for name in ('tool_1.0_20261005', 'tool_1.0_20261030'):
            (containers / name).mkdir()
            (containers / name / (name + '.simg')).mkdir()
        (containers / 'tool_1.0_20261005' / 'commands.txt').write_text('tool')
    if os.environ.get('CLIENT_LINK'):
        (containers / 'linked_1.0_20261005').symlink_to(containers / 'modules')
    if os.environ.get('CLIENT_IMAGE_LINK'):
        image = containers / 'tool_1.0_20261005' / 'tool_1.0_20261005.simg'
        image.rmdir()
        image.symlink_to(containers / 'modules')
    attrs = {
        'revision': '123', 'root_hash': 'a'*40,
        'fqrn': 'neurodesk.ardc.edu.au',
        'host': 'http://stratum0.neurodesk.cloud.edu.au/cvmfs/neurodesk.ardc.edu.au',
    }
    for key, value in attrs.items():
        os.setxattr(root, 'user.'+key, value.encode())
    (root / 'mounted').touch()
if os.environ.get('CLIENT_CANCEL'):
    os.kill(os.getppid(), signal.SIGTERM)
while True:
    time.sleep(1)
"""
    )
    script.chmod(0o755)
    unmount = binary / "fusermount"
    unmount.write_text(
        "#!/usr/bin/env python3\nimport pathlib,sys\n"
        '(pathlib.Path(sys.argv[-1])/"mounted").unlink()\n'
    )
    unmount.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binary}:{os.environ['PATH']}")
    monkeypatch.setenv("CLIENT_ROOT", str(tmp_path / "root"))
    monkeypatch.setenv("CLIENT_CONFIG", str(tmp_path / "config"))
    monkeypatch.setattr(
        publication.os.path, "ismount", lambda root: (Path(root) / "mounted").exists()
    )


def test_inventory_protects_payloads_and_excludes_retired_wrappers(client, tmp_path):
    assert (
        publication.inspect_published_containers()
        == publication.PublishedContainers(
            "123", "a" * 40, frozenset({"tool_1.0_20261005", "tool_1.0_20261030"})
        )
    )
    assert not Path((tmp_path / "root").read_text()).exists()
    config = (tmp_path / "config").read_text()
    assert "CVMFS_IGNORE_SIGNATURE=no\n" in config
    assert "CVMFS_AUTO_UPDATE=no\n" in config
    assert "CVMFS_ROOT_HASH" not in config


@pytest.mark.parametrize(
    "mode",
    [
        "CLIENT_FAIL",
        "CLIENT_HANG",
        "CLIENT_EMPTY",
        "CLIENT_LINK",
        "CLIENT_IMAGE_LINK",
        "CLIENT_CANCEL",
    ],
)
def test_unavailable_or_invalid_snapshot_refuses_cleanup_protection(
    client, tmp_path, monkeypatch, mode
):
    monkeypatch.setenv(mode, "1")
    monkeypatch.setattr(publication, "PROBE_SECONDS", 1)
    with pytest.raises(publication.PublicationUnavailable):
        publication.inspect_published_containers()
    assert not Path((tmp_path / "root").read_text()).exists()


def test_changed_snapshot_refuses_cleanup_protection(client, monkeypatch):
    real = publication._identity
    calls = 0

    def identity(root):
        nonlocal calls
        calls += 1
        revision, root_hash = real(root)
        return str(int(revision) + calls), root_hash

    monkeypatch.setattr(publication, "_identity", identity)
    with pytest.raises(publication.PublicationUnavailable, match="changed"):
        publication.inspect_published_containers()


def test_readiness_still_checks_completion_markers(client, tmp_path):
    apps = tmp_path / "apps.json"
    apps.write_text('{"tool":{"apps":{"tool 1.0":{"version":"20261030"}}}}')
    assert publication.inspect_publication_from_apps(apps).missing == (
        "tool_1.0_20261030",
    )
