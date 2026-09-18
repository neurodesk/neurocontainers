"""Stage legacy registry images as OCI contexts understood by both build backends."""
from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .cache import link_or_copy
from .image_fingerprint import parse_image_reference
from .update_observations import observe_source

SKOPEO_IMAGE = "quay.io/skopeo/stable@sha256:e5d9c4af8ec327785c7ca938d1e4f8452c6a05014850e58e2ff9456899ebd97c"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
METADATA_FILE = "build-contexts.json"
LAYOUT_NAME = "oci-base"
CONVERSION_VERSION = 1


@dataclass(frozen=True)
class Converter:
    executable: str
    container: bool
    identity: str


def select_converter() -> Converter:
    if docker := shutil.which("docker"):
        try:
            available = subprocess.run(
                [docker, "info", "--format", "{{.ServerVersion}}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            ).returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            available = False
        if available:
            return Converter(docker, True, SKOPEO_IMAGE)
    if skopeo := shutil.which("skopeo"):
        identity = "skopeo-sha256:" + hashlib.sha256(Path(skopeo).read_bytes()).hexdigest()
        return Converter(skopeo, False, identity)
    raise RuntimeError("Install skopeo or provide access to a Docker daemon to convert this legacy base image")


@dataclass(frozen=True)
class ImageContext:
    from_ref: str
    source_digest: str
    layout_dir: Path
    manifest_digest: str

    def buildx_args(self) -> list[str]:
        return ["--build-context", f"{self.from_ref}=oci-layout://{self.layout_dir.resolve()}@{self.manifest_digest}"]

    def buildctl_args(self, name: str) -> list[str]:
        return ["--oci-layout", f"{name}={self.layout_dir.resolve()}",
                "--opt", f"context:{self.from_ref}=oci-layout://{name}@{self.manifest_digest}"]


def _validate_reference(reference: str) -> None:
    if not isinstance(reference, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]*", reference):
        raise ValueError("invalid base image reference")


def resolve_source(reference: str) -> tuple[str, str]:
    _validate_reference(reference)
    parsed = parse_image_reference(reference)
    repository = parsed.repository.rsplit(":", 1)[0]
    image = f"{parsed.registry}/{repository}"
    if DIGEST.fullmatch(parsed.reference):
        digest = parsed.reference
    else:
        observed = observe_source({"method": "oci_digest", "image": image, "tag": parsed.reference}, None)
        digest = observed.value
    if not DIGEST.fullmatch(digest):
        raise ValueError("base image did not resolve to a SHA-256 digest")
    return f"{image}@{digest}", digest


def _blob(layout: Path, descriptor: dict) -> Path:
    digest = descriptor.get("digest", "")
    if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
        raise ValueError("OCI descriptor requires a SHA-256 digest")
    path = layout / "blobs" / "sha256" / digest.removeprefix("sha256:")
    if not path.is_file() or path.stat().st_size != descriptor.get("size"):
        raise ValueError(f"OCI blob missing or truncated: {digest}")
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    if "sha256:" + checksum.hexdigest() != digest:
        raise ValueError(f"OCI blob digest mismatch: {digest}")
    return path


def validate_layout(layout: Path, architecture: str) -> str:
    if json.loads((layout / "oci-layout").read_text()) != {"imageLayoutVersion": "1.0.0"}:
        raise ValueError("unsupported OCI layout version")
    index = json.loads((layout / "index.json").read_text())
    manifests = index.get("manifests", [])
    if index.get("schemaVersion") != 2 or len(manifests) != 1:
        raise ValueError("converted OCI layout must contain exactly one image")
    descriptor = manifests[0]
    manifest_bytes = _blob(layout, descriptor).read_bytes()
    if "sha256:" + hashlib.sha256(manifest_bytes).hexdigest() != descriptor["digest"]:
        raise ValueError("OCI manifest digest mismatch")
    manifest = json.loads(manifest_bytes)
    if manifest.get("mediaType") != "application/vnd.oci.image.manifest.v1+json":
        raise ValueError("converted base is not an OCI image manifest")
    config_bytes = _blob(layout, manifest["config"]).read_bytes()
    if "sha256:" + hashlib.sha256(config_bytes).hexdigest() != manifest["config"]["digest"]:
        raise ValueError("OCI config digest mismatch")
    config = json.loads(config_bytes)
    expected = {"x86_64": "amd64", "aarch64": "arm64"}[architecture]
    if config.get("architecture") != expected or config.get("os") != "linux":
        raise ValueError(f"OCI base does not match linux/{expected}")
    for layer in manifest["layers"]:
        _blob(layout, layer)
    return descriptor["digest"]


def convert_image(source: str, architecture: str, layout: Path, *, converter: Converter) -> None:
    arch = {"x86_64": "amd64", "aarch64": "arm64"}[architecture]
    if not converter.container:
        command = [converter.executable]
        destination = str(layout)
    else:
        command = [converter.executable, "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}",
                   "--volume", f"{layout.parent.resolve()}:/output", SKOPEO_IMAGE]
        destination = f"/output/{layout.name}"
    command += ["--override-os", "linux", "--override-arch", arch, "copy", "--retry-times", "3",
                "--src-no-creds", "--remove-signatures", "--format", "oci", f"docker://{source}",
                f"oci:{destination}:base"]
    subprocess.run(command, check=True)


def stage_image(reference: str, architecture: str, cache_root: Path, build_dir: Path) -> ImageContext:
    source, digest = resolve_source(reference)
    converter = select_converter()
    identity = json.dumps([source, architecture, converter.identity, CONVERSION_VERSION]).encode()
    cache_root.mkdir(parents=True, exist_ok=True)
    cached = cache_root / hashlib.sha256(identity).hexdigest()
    if not cached.exists():
        temporary = Path(tempfile.mkdtemp(prefix=".converting-", dir=cache_root))
        try:
            layout = temporary / "layout"
            convert_image(source, architecture, layout, converter=converter)
            validate_layout(layout, architecture)
            try:
                layout.rename(cached)
            except OSError as error:
                if error.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                validate_layout(cached, architecture)
        finally:
            shutil.rmtree(temporary)
    manifest_digest = validate_layout(cached, architecture)
    staged = build_dir / LAYOUT_NAME
    if staged.exists():
        shutil.rmtree(staged)
    shutil.copytree(cached, staged, copy_function=lambda a, b: link_or_copy(Path(a), Path(b)))
    return ImageContext(reference, digest, staged, manifest_digest)


def write_contexts(build_dir: Path, architecture: str, contexts: tuple[ImageContext, ...], *, required: bool = False) -> None:
    data = {"version": 1, "architecture": architecture, "required": required, "images": [
        {"from_ref": item.from_ref, "source_digest": item.source_digest,
         "layout": item.layout_dir.relative_to(build_dir).as_posix(), "manifest_digest": item.manifest_digest}
        for item in contexts
    ]}
    (build_dir / METADATA_FILE).write_text(json.dumps(data, indent=2) + "\n")
    if contexts:
        ignore = build_dir / ".dockerignore"
        existing = ignore.read_text() if ignore.exists() else ""
        if LAYOUT_NAME + "/" not in existing.splitlines():
            ignore.write_text(existing.rstrip() + "\n" + LAYOUT_NAME + "/\n")


def read_contexts(build_dir: Path) -> tuple[ImageContext, ...]:
    path = build_dir / METADATA_FILE
    if not path.exists():
        return ()
    data = json.loads(path.read_text())
    if data.get("version") != 1 or data.get("architecture") not in {"x86_64", "aarch64"}:
        raise ValueError("unsupported staged image context metadata")
    if data.get("required") and not data["images"]:
        raise ValueError("This base image requires conversion; run builder stage --download first")
    contexts = []
    for item in data["images"]:
        _validate_reference(item["from_ref"])
        if not DIGEST.fullmatch(item["source_digest"]) or not DIGEST.fullmatch(item["manifest_digest"]):
            raise ValueError("invalid staged image digest")
        relative = Path(item["layout"])
        layout = (build_dir / relative).resolve()
        if relative.is_absolute() or ".." in relative.parts or not layout.is_relative_to(build_dir.resolve()):
            raise ValueError("OCI layout must remain inside its build directory")
        if validate_layout(layout, data["architecture"]) != item["manifest_digest"]:
            raise ValueError("staged OCI manifest does not match its recorded digest")
        contexts.append(ImageContext(item["from_ref"], item["source_digest"], layout, item["manifest_digest"]))
    return tuple(contexts)
