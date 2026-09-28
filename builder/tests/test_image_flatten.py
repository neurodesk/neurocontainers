import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import uuid
import tarfile
from pathlib import Path

import pytest

from builder import image_flatten
from builder.image_contexts import ImageContext, validate_layout


def layered_image(root):
    blobs = root / "blobs" / "sha256"
    blobs.mkdir(parents=True)

    def blob(payload, media_type):
        digest = hashlib.sha256(payload).hexdigest()
        (blobs / digest).write_bytes(payload)
        return {
            "digest": "sha256:" + digest,
            "size": len(payload),
            "mediaType": media_type,
        }

    def layer(entries):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            for name, value in entries:
                info = tarfile.TarInfo(name)
                info.uid = 1234
                info.gid = 4321
                info.mode = 0o751
                info.mtime = 1234567890
                if isinstance(value, tuple):
                    info.type, info.linkname = value
                    archive.addfile(info)
                else:
                    info.size = len(value)
                    archive.addfile(info, io.BytesIO(value))
        payload = stream.getvalue()
        return (
            blob(
                gzip.compress(payload, mtime=0),
                "application/vnd.oci.image.layer.v1.tar+gzip",
            ),
            "sha256:" + hashlib.sha256(payload).hexdigest(),
        )

    bottom, bottom_diff = layer(
        [
            ("data/removed", b"delete me"),
            ("data/changed", b"before"),
            ("data/original", b"original inode"),
            ("data/link", (tarfile.LNKTYPE, "data/original")),
            ("data/symlink", (tarfile.SYMTYPE, "changed")),
            ("opaque/old", b"obsolete"),
            ("volume/asset", b"volume data"),
        ]
    )
    top, top_diff = layer(
        [
            ("data/.wh.removed", b""),
            ("data/changed", b"after"),
            ("data/original", b"new inode"),
            ("opaque/new", b"visible"),
            ("opaque/.wh..wh..opq", b""),
        ]
    )
    config = {
        "architecture": "amd64",
        "os": "linux",
        "created": "2025-01-01T00:00:00Z",
        "author": "fixture",
        "custom": {"unknown": "retained"},
        "config": {
            "Env": ["PATH=/opt/tool/bin:/usr/bin", "VALUE=spaces and $dollars"],
            "Cmd": ["--help"],
            "Entrypoint": ["/opt/tool/bin/tool"],
            "User": "1234:4321",
            "WorkingDir": "/data",
            "Labels": {"tool": "fixture"},
            "Volumes": {"/volume": {}},
            "ExposedPorts": {"8080/tcp": {}},
            "StopSignal": "SIGQUIT",
        },
        "rootfs": {"type": "layers", "diff_ids": [bottom_diff, top_diff]},
        "history": [{"created_by": "first"}, {"created_by": "second"}],
    }
    cfg = blob(json.dumps(config).encode(), "application/vnd.oci.image.config.v1+json")
    manifest = blob(
        json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": cfg,
                "layers": [bottom, top],
                "annotations": {"source": "fixture"},
            }
        ).encode(),
        "application/vnd.oci.image.manifest.v1+json",
    )
    (root / "index.json").write_text(
        json.dumps({"schemaVersion": 2, "manifests": [manifest]})
    )
    (root / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
    digest = validate_layout(root, "x86_64")
    return ImageContext("neurocontainers-flatten-source", digest, root, digest), config


def read_image(root):
    blobs = root / "blobs" / "sha256"
    index = json.loads((root / "index.json").read_text())
    manifest = json.loads(
        (blobs / index["manifests"][0]["digest"].split(":")[1]).read_text()
    )
    config = json.loads(
        (blobs / manifest["config"]["digest"].split(":")[1]).read_text()
    )
    layer = blobs / manifest["layers"][0]["digest"].split(":")[1]
    return manifest, config, layer


def test_flatten_preserves_config_and_validates_layer_hashes(tmp_path, monkeypatch):
    source, original = layered_image(tmp_path / "source")
    source_manifest = read_image(source.layout_dir)[0]
    layer = (
        source.layout_dir
        / "blobs"
        / "sha256"
        / source_manifest["layers"][0]["digest"].split(":")[1]
    )
    export = tmp_path / "export.tar"
    export.write_bytes(gzip.decompress(layer.read_bytes()))
    monkeypatch.setattr(
        image_flatten,
        "_export_command",
        lambda *args: [
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(open(sys.argv[1], 'rb').read())",
            str(export),
        ],
    )
    destination = tmp_path / "flattened"
    image_flatten.flatten_image(source, "x86_64", destination)
    validate_layout(destination, "x86_64")
    manifest, config, layer = read_image(destination)
    assert len(manifest["layers"]) == 1
    assert manifest["annotations"] == {"source": "fixture"}
    assert {k: v for k, v in config.items() if k not in {"rootfs", "history"}} == {
        k: v for k, v in original.items() if k not in {"rootfs", "history"}
    }
    assert config["rootfs"]["diff_ids"] == [
        "sha256:" + hashlib.sha256(export.read_bytes()).hexdigest()
    ]
    assert gzip.decompress(layer.read_bytes()) == export.read_bytes()
    assert sum(not entry.get("empty_layer", False) for entry in config["history"]) == 1
    assert read_image(source.layout_dir)[1] == original


def test_failed_export_cannot_produce_a_valid_layout(tmp_path, monkeypatch):
    source, _ = layered_image(tmp_path / "source")
    monkeypatch.setattr(
        image_flatten,
        "_export_command",
        lambda *args: [sys.executable, "-c", "print('partial'); exit(7)"],
    )
    with pytest.raises(subprocess.CalledProcessError):
        image_flatten.flatten_image(source, "x86_64", tmp_path / "flattened")
    assert not (tmp_path / "flattened" / "index.json").exists()


def test_buildctl_export_uses_the_validated_oci_context(tmp_path, monkeypatch):
    source, _ = layered_image(tmp_path / "source")
    monkeypatch.setattr(
        image_flatten.shutil,
        "which",
        lambda name: "/bin/buildctl" if name == "buildctl" else None,
    )
    command = image_flatten._export_command(source, "aarch64", tmp_path)
    assert "platform=linux/arm64" in command
    assert source.buildctl_args("flatten-source")[1] in command
    assert source.buildctl_args("flatten-source")[3] in command
    assert command[-1] == "type=tar"


@pytest.mark.skipif(
    os.environ.get("NEUROCONTAINERS_TEST_BUILDKIT") != "1",
    reason="requires a running BuildKit builder",
)
def test_buildkit_flattens_real_layers_without_losing_runtime_files(tmp_path):
    source, original = layered_image(tmp_path / "source")
    destination = tmp_path / "flattened"
    image_flatten.flatten_image(source, "x86_64", destination)
    validate_layout(destination, "x86_64")
    manifest, config, layer = read_image(destination)
    assert config["config"] == original["config"]
    assert len(manifest["layers"]) == 1
    with tarfile.open(layer, "r:gz") as archive:
        members = {
            entry.name.removeprefix("./").rstrip("/"): entry
            for entry in archive.getmembers()
        }
        assert "data/removed" not in members
        assert "opaque/old" not in members
        assert not any(".wh." in name for name in members)
        for name, content in {
            "data/changed": b"after",
            "data/original": b"new inode",
            "data/link": b"original inode",
            "opaque/new": b"visible",
            "volume/asset": b"volume data",
        }.items():
            assert archive.extractfile(members[name]).read() == content
        assert members["data/symlink"].issym()
        assert members["data/symlink"].linkname == "changed"
        assert members["data/changed"].mode == 0o751
        assert members["data/changed"].uid == 1234
        assert members["data/changed"].gid == 4321
        assert members["data/changed"].mtime == 1234567890

    if image_flatten.shutil.which("docker"):
        downstream = tmp_path / "consumer"
        downstream.mkdir()
        (downstream / "Dockerfile").write_text("FROM flattened-base\n")
        context = ImageContext(
            "flattened-base",
            source.source_digest,
            destination,
            validate_layout(destination, "x86_64"),
        )
        tag = "neurocontainers-flatten-test:" + uuid.uuid4().hex
        try:
            subprocess.run(
                [
                    "docker",
                    "buildx",
                    "build",
                    "--load",
                    "--tag",
                    tag,
                    *context.buildx_args(),
                    str(downstream),
                ],
                check=True,
            )
            actual = json.loads(
                subprocess.check_output(["docker", "image", "inspect", tag])
            )[0]
            for key, value in original["config"].items():
                assert actual["Config"][key] == value
            assert len(actual["RootFS"]["Layers"]) == 1
        finally:
            subprocess.run(
                ["docker", "image", "rm", tag],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


@pytest.mark.skipif(
    os.environ.get("NEUROCONTAINERS_TEST_BUILDKIT") != "1",
    reason="requires Docker and public registry access",
)
def test_registry_conversion_and_flattening_runs_the_result(tmp_path):
    from builder.image_contexts import stage_image

    reference = "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e"
    build = tmp_path / "build"
    build.mkdir()
    context = stage_image(reference, "x86_64", tmp_path / "cache", build, flatten=True)
    assert validate_layout(context.layout_dir, "x86_64") == context.manifest_digest
    (build / "Dockerfile").write_text(f"FROM {reference}\n")
    (build / ".dockerignore").write_text("oci-base/\n")
    tag = "neurocontainers-converter-test:" + uuid.uuid4().hex
    try:
        subprocess.run(
            [
                "docker",
                "buildx",
                "build",
                "--load",
                "--tag",
                tag,
                *context.buildx_args(),
                str(build),
            ],
            check=True,
        )
        result = subprocess.check_output(
            [
                "docker",
                "run",
                "--rm",
                tag,
                "sh",
                "-c",
                "printf converted-and-flattened",
            ],
            text=True,
        )
        assert result == "converted-and-flattened"
    finally:
        subprocess.run(
            ["docker", "image", "rm", tag],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
