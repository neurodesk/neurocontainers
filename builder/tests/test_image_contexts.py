import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest

from builder import image_contexts as images
from builder.adapters import BuildInputs, BuildKitAdapter, DockerAdapter


def fixture_layout(root, arch="amd64"):
    def blob(data):
        payload = data if isinstance(data, bytes) else json.dumps(data).encode()
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        path = root / "blobs" / "sha256" / digest.removeprefix("sha256:")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return {"digest": digest, "size": len(payload)}
    config = blob({"architecture": arch, "os": "linux", "config": {"Env": ["TOOL=legacy"]}})
    layer = blob(b"test layer")
    manifest = blob({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                     "config": config, "layers": [layer]})
    (root / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [manifest]}))
    (root / "oci-layout").write_text(json.dumps({"imageLayoutVersion": "1.0.0"}))
    return manifest["digest"]


@pytest.fixture
def source(monkeypatch):
    monkeypatch.setattr(images, "select_converter", lambda: images.Converter("docker", True, images.SKOPEO_IMAGE))
    source = "docker.io/example/legacy@sha256:" + "a" * 64
    monkeypatch.setattr(images, "resolve_source", lambda ref: (source, "sha256:" + "a" * 64))
    return source


def test_staging_reuses_complete_conversion_and_preserves_backend_contexts(tmp_path, monkeypatch, source):
    calls = []
    def convert(ref, arch, target, **kwargs):
        calls.append((ref, arch))
        fixture_layout(target)
    monkeypatch.setattr(images, "convert_image", convert)
    for name in ["one", "two"]:
        build = tmp_path / name
        build.mkdir()
        context = images.stage_image("example/legacy:v1", "x86_64", tmp_path / "cache", build)
        images.write_contexts(build, "x86_64", (context,), required=True)
        loaded = images.read_contexts(build)
        assert loaded == (context,)
        inputs = BuildInputs("demo", "1", "demo:1", "x86_64", build, build / "Dockerfile", image_contexts=loaded)
        docker = DockerAdapter().command(inputs)
        buildkit = BuildKitAdapter().command(inputs, build / "image.tar")
        assert context.buildx_args()[1] in docker
        assert context.buildctl_args("base-image-0")[1] in buildkit
        assert context.buildctl_args("base-image-0")[3] in buildkit
        assert images.LAYOUT_NAME + "/" in (build / ".dockerignore").read_text()
    assert calls == [(source, "x86_64")]


def test_failed_conversion_is_not_reused(tmp_path, monkeypatch, source):
    def failed(ref, arch, target, **kwargs):
        target.mkdir()
        (target / "partial").write_text("incomplete")
        raise RuntimeError("download interrupted")
    monkeypatch.setattr(images, "convert_image", failed)
    with pytest.raises(RuntimeError, match="interrupted"):
        images.stage_image("example/legacy:v1", "x86_64", tmp_path / "cache", tmp_path / "build")
    assert list((tmp_path / "cache").iterdir()) == []
    monkeypatch.setattr(images, "convert_image", lambda ref, arch, target, **kwargs: fixture_layout(target))
    images.stage_image("example/legacy:v1", "x86_64", tmp_path / "cache", tmp_path / "build")


def test_concurrent_conversions_publish_one_complete_cache(tmp_path, monkeypatch, source):
    barrier = Barrier(2)
    def convert(ref, arch, target, **kwargs):
        fixture_layout(target)
        barrier.wait(timeout=10)
    monkeypatch.setattr(images, "convert_image", convert)
    def stage(index):
        return images.stage_image("example/legacy:v1", "x86_64", tmp_path / "cache", tmp_path / str(index))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(stage, [1, 2]))
    assert results[0].manifest_digest == results[1].manifest_digest
    assert len(list((tmp_path / "cache").iterdir())) == 1


def test_layout_rejects_wrong_architecture_and_missing_blob(tmp_path):
    fixture_layout(tmp_path)
    with pytest.raises(ValueError, match="linux/arm64"):
        images.validate_layout(tmp_path, "aarch64")
    layer = tmp_path / "blobs" / "sha256" / hashlib.sha256(b"test layer").hexdigest()
    layer.write_bytes(b"bad! layer")
    with pytest.raises(ValueError, match="digest mismatch"):
        images.validate_layout(tmp_path, "x86_64")
    layer.unlink()
    with pytest.raises(ValueError, match="missing or truncated"):
        images.validate_layout(tmp_path, "x86_64")


def test_staged_metadata_cannot_select_external_layout(tmp_path):
    build = tmp_path / "build"
    layout = build / images.LAYOUT_NAME
    digest = fixture_layout(layout)
    context = images.ImageContext("example/legacy:v1", "sha256:" + "a" * 64, layout, digest)
    images.write_contexts(build, "x86_64", (context,), required=True)
    path = build / images.METADATA_FILE
    data = json.loads(path.read_text())
    data["images"][0]["layout"] = "../outside"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="inside its build"):
        images.read_contexts(build)


def test_metadata_only_staging_cannot_silently_skip_conversion(tmp_path):
    images.write_contexts(tmp_path, "x86_64", (), required=True)
    with pytest.raises(ValueError, match="stage --download"):
        images.read_contexts(tmp_path)


def test_native_converter_change_invalidates_cache_identity(tmp_path, monkeypatch):
    executable = tmp_path / "skopeo"
    executable.write_bytes(b"first converter")
    monkeypatch.setattr(images.shutil, "which", lambda name: str(executable) if name == "skopeo" else None)
    first = images.select_converter()
    executable.write_bytes(b"second converter")
    second = images.select_converter()
    assert first.identity != second.identity
    assert not first.container
    monkeypatch.setattr(images.shutil, "which", lambda name: "/usr/bin/docker" if name == "docker" else str(executable))
    monkeypatch.setattr(images.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1))
    assert images.select_converter() == second
    monkeypatch.setattr(images.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    assert images.select_converter().identity == images.SKOPEO_IMAGE
