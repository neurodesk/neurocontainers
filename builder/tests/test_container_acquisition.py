from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
from typing import Any

import pytest

from workflows import container_tester as ct


def retained_path(stack: ExitStack, image: Any) -> Path:
    assert image is not None
    if isinstance(image, str):
        return Path(image)
    stack.enter_context(image)
    return image.path


@pytest.mark.parametrize("outer_fails", [False, True])
def test_overlapping_release_refreshes_keep_their_own_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outer_fails: bool
) -> None:
    downloader = ct.ReleaseContainerDownloader(str(tmp_path))
    downloader.base_urls = [("test", "https://example.invalid")]
    destinations = []
    nested = []
    with ExitStack() as resources:
        def transfer(url: str, filename: str, **kwargs) -> tuple[str, None]:
            destinations.append(Path(filename))
            with open(filename, "wb") as output:
                if len(destinations) == 1:
                    output.write(b"outer")
                    output.flush()
                    image = downloader.download_from_release(
                        "tool", "1", "20261003", use_cache=False
                    )
                    nested.append(retained_path(resources, image))
                    if outer_fails:
                        raise OSError("outer transfer failed")
                else:
                    output.write(b"inner")
            return filename, None

        monkeypatch.setattr(ct.urllib.request, "urlretrieve", transfer)
        outer = downloader.download_from_release(
            "tool", "1", "20261003", use_cache=False
        )
        if outer_fails:
            assert outer is None
        else:
            outer_path = retained_path(resources, outer)
            assert outer_path.read_bytes() == b"outer"
        assert nested[0].read_bytes() == b"inner"
        assert len(set(destinations)) == 2
        assert (tmp_path / "tool_1_20261003.simg").read_bytes() == (
            b"inner" if outer_fails else b"outer"
        )
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "tool_1_20261003.simg"
    ]


def test_reader_survives_refresh_and_explicit_cache_eviction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ct.ReleaseContainerDownloader(str(tmp_path))
    versions = iter([b"first image", b"replacement image"])

    def transfer(url: str, filename: str, **kwargs) -> tuple[str, None]:
        Path(filename).write_bytes(next(versions))
        return filename, None

    monkeypatch.setattr(ct.urllib.request, "urlretrieve", transfer)
    cache = tmp_path / "tool_1_20261003.simg"
    with ExitStack() as resources:
        first = retained_path(resources, downloader.download_from_release(
            "tool", "1", "20261003", use_cache=False
        ))
        assert first.stat().st_ino == cache.stat().st_ino
        assert first.stat().st_dev == cache.stat().st_dev
        second = retained_path(resources, downloader.download_from_release(
            "tool", "1", "20261003", use_cache=False
        ))
        assert first.read_bytes() == b"first image"
        downloader.cleanup_downloaded_container(str(cache))
        assert first.read_bytes() == b"first image"
        assert second.read_bytes() == b"replacement image"
    assert not first.exists()
    assert not second.exists()
    assert list(tmp_path.iterdir()) == []


def test_failed_pull_preserves_stale_image_and_existing_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    converter = ct.DockerToSimgConverter(str(tmp_path))
    cache = tmp_path / "tool.simg"
    cache.write_bytes(b"last valid image")
    reader = tmp_path / "reader.simg"
    os.link(cache, reader)
    os.utime(cache, (1, 1))
    binary = tmp_path / "docker-save-to-simg"
    binary.write_bytes(b"converter")
    monkeypatch.setattr(ct.shutil, "which", lambda name: "/bin/" + name)
    monkeypatch.setattr(converter, "_ensure_binary", lambda *args, **kwargs: str(binary))

    def fail_pull(command: list[str], **kwargs) -> None:
        assert command[:2] == ["docker", "pull"]
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(ct.subprocess, "run", fail_pull)
    with pytest.raises(subprocess.CalledProcessError):
        converter.convert("tool:1", "tool.simg")
    assert cache.exists(), "failed refresh removed the last valid cache image"
    assert cache.read_bytes() == b"last valid image"
    assert reader.read_bytes() == b"last valid image"
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "docker-save-to-simg", "reader.simg", "tool.simg"
    ]


def test_failed_converter_build_preserves_cached_binary_and_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "converter.go"
    source.write_text("package main\n")
    converter = ct.DockerToSimgConverter(str(tmp_path), str(source))
    binary = Path(converter.binary_path)
    binary.write_bytes(b"last working converter")
    reader = tmp_path / "active-converter"
    os.link(binary, reader)
    os.utime(binary, (1, 1))
    monkeypatch.setattr(ct.shutil, "which", lambda name: "/bin/" + name)

    def fail_build(command: list[str], **kwargs) -> None:
        assert command[:2] == ["go", "build"]
        Path(command[command.index("-o") + 1]).write_bytes(b"partial binary")
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(ct.subprocess, "run", fail_build)
    with pytest.raises(subprocess.CalledProcessError):
        converter.convert("tool:1", "tool.simg")
    assert binary.read_bytes() == b"last working converter"
    assert reader.read_bytes() == b"last working converter"
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "active-converter", "converter.go", "docker-save-to-simg"
    ]
