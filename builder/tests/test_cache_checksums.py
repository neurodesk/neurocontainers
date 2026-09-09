from __future__ import annotations

import hashlib
import http.client
import io
import shutil
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
import yaml

from builder.cache import DownloadError, HttpCache, sha256_text
from builder.recipe import compile_recipe
from builder.staging import materialize_plan
from builder.validation import FileInfo


def digest(contents: bytes) -> str:
    return hashlib.sha256(contents).hexdigest()


@pytest.mark.parametrize("retry", [0, 1])
@pytest.mark.parametrize("chunked", [False, True])
def test_truncated_http_body_is_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, retry: int, chunked: bool
) -> None:
    class Socket:
        def __init__(self, body: bytes):
            self.body = body

        def makefile(self, mode: str):
            header = b"Transfer-Encoding: chunked" if chunked else b"Content-Length: 8"
            return io.BytesIO(b"HTTP/1.1 200 OK\r\n" + header + b"\r\n\r\n" + self.body)

    bodies = iter(
        [b"8\r\nshort", b"8\r\ncomplete\r\n0\r\n\r\n"]
        if chunked else [b"short", b"complete"]
    )

    def urlopen(request, timeout):
        response = http.client.HTTPResponse(Socket(next(bodies)))
        response.begin()
        return response

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr("builder.cache.time.sleep", lambda delay: None)
    cache = HttpCache(tmp_path / "httpcache")
    if retry:
        path = cache.get("https://example.com/model", retry=retry)
        assert path.read_bytes() == b"complete"
        assert list(cache.root.iterdir()) == [path]
    else:
        with pytest.raises(DownloadError, match="IncompleteRead" if chunked else "Content-Length"):
            cache.get("https://example.com/model", retry=retry)
        assert list(cache.root.iterdir()) == []


def test_checksum_cache_keeps_distinct_contents_at_the_same_url(tmp_path: Path) -> None:
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"first release")
    cache = HttpCache(tmp_path / "httpcache")
    first = cache.get(source.as_uri(), sha256=digest(b"first release"))

    source.write_bytes(b"second release")
    second = cache.get(source.as_uri(), sha256=digest(b"second release"))

    assert first != second
    assert first.read_bytes() == b"first release"
    assert second.read_bytes() == b"second release"
    source.unlink()
    assert cache.get(source.as_uri(), sha256=digest(b"first release")) == first
    assert cache.path_for(source.as_uri()) not in (first, second)


@pytest.mark.parametrize("download", [False, True])
def test_checksum_cache_rejects_corrupt_cached_bytes(
    tmp_path: Path, download: bool
) -> None:
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"release")
    cache = HttpCache(tmp_path / "httpcache")
    sha256 = digest(b"release")
    cached = cache.get(source.as_uri(), sha256=sha256)
    cached.write_bytes(b"corrupted")

    with pytest.raises(DownloadError, match="SHA-256 mismatch for cached"):
        cache.get(source.as_uri(), sha256=sha256, download=download)


@pytest.mark.parametrize("contents", [b"incorrect release", b""])
def test_checksum_mismatch_never_publishes_download(
    tmp_path: Path, contents: bytes
) -> None:
    source = tmp_path / "artifact.bin"
    source.write_bytes(contents)
    cache = HttpCache(tmp_path / "httpcache")

    with pytest.raises(DownloadError, match="SHA-256 mismatch") as error:
        cache.get(source.as_uri(), sha256=digest(b"expected"), file_name="model")

    assert "model" in str(error.value)
    assert source.as_uri() in str(error.value)
    assert list(cache.root.iterdir()) == []


def test_checksum_can_verify_an_empty_file(tmp_path: Path) -> None:
    source = tmp_path / "empty.bin"
    source.write_bytes(b"")
    cache = HttpCache(tmp_path / "httpcache")
    cached = cache.get(source.as_uri(), sha256=digest(b""))
    source.unlink()

    assert cache.get(source.as_uri(), sha256=digest(b"")) == cached
    assert cached.read_bytes() == b""


def test_no_checksum_preserves_existing_cache_key_and_behavior(tmp_path: Path) -> None:
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"first release")
    cache = HttpCache(tmp_path / "httpcache")
    cached = cache.get(source.as_uri())
    source.write_bytes(b"second release")

    assert cached.name == sha256_text(source.as_uri())
    assert cache.get(source.as_uri()).read_bytes() == b"first release"


def test_concurrent_downloads_use_independent_temporary_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "artifact.bin"
    source.write_bytes(b"release" * 10000)
    cache = HttpCache(tmp_path / "httpcache")
    ready = Barrier(2)
    temporary_files: list[str] = []
    copyfileobj = shutil.copyfileobj

    def overlapping_copy(response, handle) -> None:
        temporary_files.append(handle.name)
        ready.wait(timeout=5)
        copyfileobj(response, handle)

    monkeypatch.setattr("builder.cache.shutil.copyfileobj", overlapping_copy)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(cache.get, source.as_uri(), sha256=digest(source.read_bytes()))
            for _ in range(2)
        ]
        paths = [future.result(timeout=10) for future in futures]

    assert len(set(temporary_files)) == 2
    assert paths[0] == paths[1]
    assert paths[0].read_bytes() == source.read_bytes()
    assert list(cache.root.iterdir()) == [paths[0]]


@pytest.mark.parametrize("sha256", ["", "abc", "g" * 64, "0" * 63, 123])
def test_invalid_checksum_fails_before_download(tmp_path: Path, sha256) -> None:
    cache = HttpCache(tmp_path / "httpcache")
    with pytest.raises(ValueError, match="64 hexadecimal"):
        cache.get((tmp_path / "absent.bin").as_uri(), sha256=sha256)
    assert list(cache.root.iterdir()) == []


def test_checksum_case_does_not_split_cache_identity(tmp_path: Path) -> None:
    cache = HttpCache(tmp_path / "httpcache")
    sha256 = digest(b"release")
    assert cache.path_for("https://example.com/model", sha256=sha256) == cache.path_for(
        "https://example.com/model", sha256=sha256.upper()
    )


@pytest.mark.parametrize("source", [{"filename": "model"}, {"contents": "model"}])
def test_schema_rejects_checksum_on_sources_it_cannot_verify(source: dict) -> None:
    with pytest.raises(ValueError, match="sha256 requires a URL source"):
        FileInfo(name="model", sha256=digest(b"model"), **source)


def write_recipe(recipe_dir: Path, files: list[dict], variables: dict) -> None:
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "checksum-test",
                "version": "1.0.0",
                "architectures": ["x86_64"],
                "categories": ["programming"],
                "readme": "Checksum staging fixture.",
                "variables": variables,
                "files": files,
                "build": {
                    "kind": "neurodocker",
                    "base-image": "ubuntu:24.04",
                    "pkg-manager": "apt",
                    "directives": [{"run": ['cat {{ get_file("old") }} {{ get_file("new") }}']}],
                },
            }
        )
    )


def test_recipe_renders_checksums_and_stages_both_versions_of_one_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.com/artifact.bin"
    source = tmp_path / "upstream.bin"
    source.write_bytes(b"old release")
    urlopen = urllib.request.urlopen

    def local_download(request, timeout):
        assert request.full_url == url
        return urlopen(source.as_uri(), timeout=timeout)

    monkeypatch.setattr(urllib.request, "urlopen", local_download)
    cache = HttpCache(tmp_path / "httpcache")
    cache.get(url, sha256=digest(b"old release"))
    source.write_bytes(b"new release")
    recipe_dir = tmp_path / "recipe"
    write_recipe(
        recipe_dir,
        files=[
            {"name": "old", "url": url, "sha256": "{{ context.old_sha }}"},
            {"name": "new", "url": url, "sha256": "{{ context.new_sha }}"},
        ],
        variables={"old_sha": digest(b"old release"), "new_sha": digest(b"new release")},
    )

    compiled = compile_recipe(recipe_dir, architecture="x86_64")
    assert compiled.staging_plan.files["old"].sha256 == digest(b"old release")
    assert compiled.staging_plan.files["new"].sha256 == digest(b"new release")
    staged = materialize_plan(
        compiled.staging_plan, recipe_dir, tmp_path / "build",
        http_cache_dir=cache.root, download=True,
    )

    mounts = list(compiled.staging_plan.cache_mounts.items())
    assert len(mounts) == 1
    mount_id, names = mounts[0]
    assert set(names) == {"old", "new"}
    assert (staged / mount_id / names["old"]).read_bytes() == b"old release"
    assert (staged / mount_id / names["new"]).read_bytes() == b"new release"


def test_recipe_rejects_invalid_rendered_checksum_before_staging(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "recipe"
    write_recipe(
        recipe_dir,
        files=[{"name": "old", "url": "https://example.com/model", "sha256": "{{ context.bad }}"}],
        variables={"bad": "not-a-checksum"},
    )
    with pytest.raises(ValueError, match="64 hexadecimal"):
        compile_recipe(recipe_dir, architecture="x86_64")
