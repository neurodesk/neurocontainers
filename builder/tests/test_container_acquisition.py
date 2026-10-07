import os
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest

from workflows import container_tester as ct
from workflows.test_runner import ContainerTestRunner, TestRequest as RunRequest


def retained_path(stack: ExitStack, image: ct.AcquiredImage | None) -> Path:
    assert image is not None
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


@pytest.mark.parametrize("failure", ["converter", "launch", "save", "empty", "publish"])
def test_conversion_failure_preserves_cache_and_reaps_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    tools = tmp_path / "tools"
    tools.mkdir()
    docker = tools / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import sys, time\n"
        "if sys.argv[1] == 'save':\n"
        + ("    time.sleep(2)\n" if failure in {"converter", "launch"}
           else "    sys.stdout.write('archive')\n")
        + ("    sys.exit(3)\n" if failure == "save" else "")
    )
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    source = tmp_path / "converter.go"
    source.write_text("package main\n")
    os.utime(source, (1, 1))
    converter = ct.DockerToSimgConverter(str(cache_dir), str(source))
    binary = Path(converter.binary_path)
    binary.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, sys\n"
        + ("" if failure == "converter" else "sys.stdin.read()\n")
        + "pathlib.Path(sys.argv[2]).write_bytes("
        + ("b''" if failure == "empty" else "b'partial or complete output'")
        + ")\n"
        + ("sys.exit(2)\n" if failure == "converter" else "")
    )
    binary.chmod(0o644 if failure == "launch" else 0o755)
    cache = cache_dir / "tool.simg"
    cache.write_bytes(b"previous image")
    os.utime(cache, (1, 1))
    reader = tmp_path / "reader.simg"
    os.link(cache, reader)
    children = []
    real_popen = subprocess.Popen

    def spawn(*args, **kwargs) -> subprocess.Popen:
        process = real_popen(*args, **kwargs)
        children.append(process)
        return process

    monkeypatch.setattr(ct.subprocess, "Popen", spawn)
    if failure == "publish":
        def fail_replace(*args) -> None:
            raise OSError("publication failed")
        monkeypatch.setattr(ct.os, "replace", fail_replace)
    with pytest.raises((RuntimeError, OSError)):
        converter.convert("tool:1", "tool.simg")
    assert cache.read_bytes() == b"previous image"
    assert reader.read_bytes() == b"previous image"
    assert sorted(path.name for path in cache_dir.iterdir()) == [
        "docker-save-to-simg", "tool.simg"
    ]
    assert all(process.poll() is not None for process in children)
    producer = next(process for process in children if process.args[:2] == ["docker", "save"])
    if failure in {"converter", "launch"}:
        assert producer.returncode < 0


def test_cache_hit_retains_selected_inode_during_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ct.ReleaseContainerDownloader(str(tmp_path))
    cache = tmp_path / "tool_1_20261003.simg"
    cache.write_bytes(b"selected")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"new generation")
    link = os.link

    def link_then_replace(source: Path, destination: Path) -> None:
        link(source, destination)
        os.replace(replacement, cache)

    monkeypatch.setattr(ct.os, "link", link_then_replace)
    image = downloader.download_from_release("tool", "1", "20261003")
    assert image is not None
    with image:
        assert image.path.read_bytes() == b"selected"
        assert cache.read_bytes() == b"new generation"
    assert not image.path.exists()


@pytest.mark.parametrize("configuration", ["empty", "raises"])
def test_runner_closes_acquisition_on_early_configuration_return(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configuration: str
) -> None:
    recipe = tmp_path / "recipes" / "tool"
    recipe.mkdir(parents=True)
    config = recipe / "fulltest.yaml"
    config.write_text("tests: []\n")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache = cache_dir / "tool_1_20261003.simg"
    cache.write_bytes(b"image")
    runner = ContainerTestRunner(repo_root=tmp_path)
    runner.tester.release_downloader = ct.ReleaseContainerDownloader(str(cache_dir))
    monkeypatch.setattr(runner, "_resolve_release", lambda *args: (None, "1", None))
    monkeypatch.setattr(runner, "_resolve_test_config", lambda *args: (config, None))
    paths = []

    def acquire(*args, **kwargs) -> str:
        image = ct.AcquiredImage(cache)
        os.link(cache, image.path)
        runner.tester._acquired_images.append(image)
        paths.append(image.path)
        return str(image.path)

    def extract(*args) -> dict:
        assert paths[0].read_bytes() == b"image"
        if configuration == "raises":
            raise ValueError("invalid configuration")
        return {"tests": []}

    runner.tester.test_extractor = SimpleNamespace(extract_from_file=extract)
    monkeypatch.setattr(
        runner.tester, "select_runtime",
        lambda *args: SimpleNamespace(name="apptainer"),
    )
    monkeypatch.setattr(runner.tester, "find_container", acquire)
    outcome = runner.run(RunRequest(recipe="tool", version="1"))
    assert outcome.status == ("skipped" if configuration == "empty" else "failed")
    assert not paths[0].exists()
    assert cache.read_bytes() == b"image"
    assert list(cache_dir.iterdir()) == [cache]


def test_repeated_cache_cleanup_keeps_all_active_readers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tester = ct.ContainerTester()
    tester.release_downloader = ct.ReleaseContainerDownloader(str(tmp_path))
    cache = tmp_path / "tool_1_20261003.simg"
    cache.write_bytes(b"image")
    release = tmp_path / "release.json"
    release.write_text('{"apps": {"tool": {"version": "20261003"}}}')
    with tester:
        first = Path(tester.find_container("tool", "1", "release", str(release)))
        second = Path(tester.find_container("tool", "1", "release", str(release)))
        assert first != second
        assert tester.cleanup_downloaded_containers()
        assert tester.cleanup_downloaded_containers()
        assert not cache.exists()
        assert first.read_bytes() == second.read_bytes() == b"image"
    assert not first.exists()
    assert not second.exists()


def test_overlapping_converter_builds_retain_their_own_binary_and_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tools = tmp_path / "tools"
    tools.mkdir()
    docker = tools / "docker"
    docker.write_text("#!/bin/sh\nexit 0\n")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])
    source = tmp_path / "converter.go"
    source.write_text("package main\n")
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    converter = ct.DockerToSimgConverter(str(cache_dir), str(source))
    builds = []
    nested = []
    real_run = subprocess.run
    real_which = ct.shutil.which
    monkeypatch.setattr(
        ct.shutil, "which", lambda name: "/fake/go" if name == "go" else real_which(name)
    )
    with ExitStack() as resources:
        def run(command: list[str], **kwargs) -> subprocess.CompletedProcess:
            if command[:2] != ["go", "build"]:
                return real_run(command, **kwargs)
            binary = Path(command[command.index("-o") + 1])
            builds.append(binary)
            generation = "outer" if len(builds) == 1 else "inner"
            binary.write_text(f'#!/bin/sh\nprintf {generation} > "$2"\n')
            binary.chmod(0o755)
            if generation == "outer":
                nested.append(resources.enter_context(
                    converter.convert("tool:1", "tool.simg")
                ))
                os.utime(nested[0].cache_path, (1, 1))
            return subprocess.CompletedProcess(command, 0)

        monkeypatch.setattr(ct.subprocess, "run", run)
        outer = resources.enter_context(converter.convert("tool:1", "tool.simg"))
        assert len(set(builds)) == 2
        assert outer.path.read_bytes() == b"outer"
        assert nested[0].path.read_bytes() == b"inner"
        assert "outer" in builds[0].read_text()
        assert "inner" in builds[1].read_text()
        assert outer.cache_path.read_bytes() == b"outer"
    assert sorted(path.name for path in cache_dir.iterdir()) == [
        "docker-save-to-simg", "tool.simg"
    ]


def test_orphaned_acquisition_is_swept_while_live_acquisition_survives(
    tmp_path: Path,
) -> None:
    holder = (
        "import sys\n"
        "from pathlib import Path\n"
        "from workflows import container_tester as ct\n"
        "image = ct.AcquiredImage(Path(sys.argv[1]))\n"
        "image.path.write_bytes(b'held')\n"
        "print(image.path, flush=True)\n"
        "sys.stdin.read()\n"
    )
    repo_root = Path(__file__).resolve().parents[2]

    def hold(name: str) -> tuple[subprocess.Popen, Path]:
        process = subprocess.Popen(
            [sys.executable, "-c", holder, str(tmp_path / name)],
            cwd=repo_root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        return process, Path(process.stdout.readline().strip())

    killed, orphan = hold("killed.simg")
    live, retained = hold("live.simg")
    try:
        killed.kill()
        killed.wait()
        assert orphan.exists()

        downloader = ct.ReleaseContainerDownloader(str(tmp_path))
        downloader.cleanup_all_cache()

        assert not orphan.parent.exists()
        assert retained.read_bytes() == b"held"

        with ct.AcquiredImage(tmp_path / "next.simg"):
            assert retained.read_bytes() == b"held"
    finally:
        live.communicate("")
    assert not retained.parent.exists()
    assert list(tmp_path.iterdir()) == []
