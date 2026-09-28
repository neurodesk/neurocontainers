"""Flatten OCI filesystems through BuildKit while retaining their image configuration."""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .image_contexts import ImageContext

FLATTEN_VERSION = 1


def _export_command(source: ImageContext, architecture: str, work: Path) -> list[str]:
    platform = {"x86_64": "linux/amd64", "aarch64": "linux/arm64"}[architecture]
    if docker := shutil.which("docker"):
        try:
            available = (
                subprocess.run(
                    [docker, "info", "--format", "{{.ServerVersion}}"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                ).returncode
                == 0
            )
        except (OSError, subprocess.TimeoutExpired):
            available = False
        if available:
            return [
                docker,
                "buildx",
                "build",
                "--platform",
                platform,
                *source.buildx_args(),
                "--output",
                "type=tar,dest=-",
                str(work),
            ]
    if buildctl := shutil.which("buildctl"):
        return [
            buildctl,
            "build",
            "--frontend=dockerfile.v0",
            "--local",
            f"context={work}",
            "--local",
            f"dockerfile={work}",
            "--opt",
            f"platform={platform}",
            *source.buildctl_args("flatten-source"),
            "--output",
            "type=tar",
        ]
    raise RuntimeError(
        "Flattening a base image requires Docker Buildx or a configured buildctl daemon"
    )


def _json_blob(layout: Path, value: dict, media_type: str) -> dict:
    payload = json.dumps(value, separators=(",", ":")).encode()
    digest = hashlib.sha256(payload).hexdigest()
    (layout / "blobs" / "sha256" / digest).write_bytes(payload)
    return {"mediaType": media_type, "digest": "sha256:" + digest, "size": len(payload)}


def flatten_image(source: ImageContext, architecture: str, destination: Path) -> None:
    """Write a one-layer OCI layout; never extract an image onto the host filesystem."""
    blob_dir = source.layout_dir / "blobs" / "sha256"
    index = json.loads((source.layout_dir / "index.json").read_text())
    manifest = json.loads(
        (blob_dir / source.manifest_digest.removeprefix("sha256:")).read_text()
    )
    config = json.loads(
        (blob_dir / manifest["config"]["digest"].removeprefix("sha256:")).read_text()
    )
    destination.mkdir()
    (destination / "blobs" / "sha256").mkdir(parents=True)
    work = destination / "export"
    work.mkdir()
    (work / "Dockerfile").write_text(
        f"FROM scratch\nCOPY --from={source.from_ref} / /\n"
    )
    command = _export_command(source, architecture, work)
    layer_path = destination / "blobs" / "sha256" / "layer.tmp"
    diff_hash = hashlib.sha256()
    with subprocess.Popen(command, stdout=subprocess.PIPE) as process:
        try:
            assert process.stdout is not None
            with layer_path.open("wb") as output:
                with gzip.GzipFile(
                    fileobj=output, mode="wb", filename="", mtime=0, compresslevel=6
                ) as compressed:
                    while chunk := process.stdout.read(1024 * 1024):
                        diff_hash.update(chunk)
                        compressed.write(chunk)
            if process.wait() != 0:
                raise subprocess.CalledProcessError(process.returncode, command)
        except BaseException:
            process.kill()
            raise
    shutil.rmtree(work)
    layer_hash = hashlib.sha256()
    with layer_path.open("rb") as layer:
        for chunk in iter(lambda: layer.read(1024 * 1024), b""):
            layer_hash.update(chunk)
    layer_descriptor = {
        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
        "digest": "sha256:" + layer_hash.hexdigest(),
        "size": layer_path.stat().st_size,
    }
    layer_path.rename(layer_path.with_name(layer_hash.hexdigest()))
    config["rootfs"] = {
        "type": "layers",
        "diff_ids": ["sha256:" + diff_hash.hexdigest()],
    }
    history = config.setdefault("history", [])
    for entry in history:
        entry["empty_layer"] = True
    history.append({"created_by": "NeuroContainers flatten-base-image"})
    manifest["config"] = _json_blob(
        destination, config, "application/vnd.oci.image.config.v1+json"
    )
    manifest["layers"] = [layer_descriptor]
    descriptor = _json_blob(
        destination, manifest, "application/vnd.oci.image.manifest.v1+json"
    )
    index["manifests"][0].update(descriptor)
    (destination / "index.json").write_text(json.dumps(index) + "\n")
    (destination / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
