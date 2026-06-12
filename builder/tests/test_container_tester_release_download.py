from __future__ import annotations

from pathlib import Path
from urllib.error import HTTPError

import pytest

from workflows import container_tester
from workflows.container_tester import ContainerTester, ReleaseContainerDownloader


def test_release_downloader_prefers_s3_over_nectar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        Path(filename).write_text("simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release("globus", "3.2.8", "20260514")

    assert path == str(tmp_path / "globus_3.2.8_20260514.simg")
    assert Path(path).read_text(encoding="utf-8") == "simg"
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/globus_3.2.8_20260514.simg",
    ]


def test_release_downloader_falls_back_to_nectar_when_s3_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        if "neurocontainers.s3.us-east-2.amazonaws.com" in url:
            raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
        Path(filename).write_text("simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release("globus", "3.2.8", "20260514")

    assert path == str(tmp_path / "globus_3.2.8_20260514.simg")
    assert Path(path).read_text(encoding="utf-8") == "simg"
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/globus_3.2.8_20260514.simg",
        "https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk/globus_3.2.8_20260514.simg",
    ]


def test_release_downloader_can_refresh_existing_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    cache_path = tmp_path / "globus_3.2.8_20260514.simg"
    cache_path.write_text("stale", encoding="utf-8")
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        Path(filename).write_text("fresh", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release(
        "globus",
        "3.2.8",
        "20260514",
        use_cache=False,
    )

    assert path == str(cache_path)
    assert cache_path.read_text(encoding="utf-8") == "fresh"
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/globus_3.2.8_20260514.simg",
    ]


def test_release_downloader_prefers_image_basename_from_release_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        if "neurodesktop_20260428_arm64_20260519.simg" not in url:
            raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
        Path(filename).write_text("arm64 simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release(
        "neurodesktop",
        "20260428-arm64",
        "20260519",
        image_basename="neurodesktop_20260428_arm64",
    )

    assert path == str(tmp_path / "neurodesktop_20260428_arm64_20260519.simg")
    assert Path(path).read_text(encoding="utf-8") == "arm64 simg"
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/neurodesktop_20260428_arm64_20260519.simg",
    ]


def test_release_downloader_falls_back_to_computed_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        if "custom_globus_20260514.simg" in url:
            raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
        Path(filename).write_text("fallback simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release(
        "globus",
        "3.2.8",
        "20260514",
        image_basename="custom_globus",
    )

    assert path == str(tmp_path / "globus_3.2.8_20260514.simg")
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/custom_globus_20260514.simg",
        "https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk/custom_globus_20260514.simg",
        "https://neurocontainers.s3.us-east-2.amazonaws.com/globus_3.2.8_20260514.simg",
    ]


def test_release_downloader_extracts_sanitized_image_basename(
    tmp_path: Path,
) -> None:
    release_file = tmp_path / "20260428-arm64.json"
    release_file.write_text(
        """
        {
          "apps": {
            "neurodesktop 20260428 arm64": {
              "version": "20260519",
              "image": "https://example.invalid/nested/neurodesktop_20260428_arm64.simg?download=1"
            }
          }
        }
        """,
        encoding="utf-8",
    )

    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path / "cache"))

    assert (
        downloader.extract_image_basename_from_release(str(release_file))
        == "neurodesktop_20260428_arm64"
    )


def test_release_downloader_accepts_image_basename_with_build_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        calls.append(url)
        Path(filename).write_text("simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release(
        "neurodesktop",
        "20260428-arm64",
        "20260519",
        image_basename="neurodesktop_20260428_arm64_20260519.simg",
    )

    assert path == str(tmp_path / "neurodesktop_20260428_arm64_20260519.simg")
    assert calls == [
        "https://neurocontainers.s3.us-east-2.amazonaws.com/neurodesktop_20260428_arm64_20260519.simg",
    ]


def test_release_downloader_reports_download_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))

    def fake_urlretrieve(url: str, filename: str, **kwargs) -> tuple[str, None]:
        reporthook = kwargs["reporthook"]
        reporthook(0, 1024, 10 * 1024 * 1024)
        reporthook(5120, 1024, 10 * 1024 * 1024)
        reporthook(10240, 1024, 10 * 1024 * 1024)
        Path(filename).write_text("simg", encoding="utf-8")
        return filename, None

    monkeypatch.setattr(
        container_tester.urllib.request, "urlretrieve", fake_urlretrieve
    )

    path = downloader.download_from_release("globus", "3.2.8", "20260514")

    assert path == str(tmp_path / "globus_3.2.8_20260514.simg")
    output = capsys.readouterr().out
    assert "Download size for globus_3.2.8_20260514.simg: 10.0 MB" in output
    assert "Download progress for globus_3.2.8_20260514.simg: 50%" in output
    assert "Download progress for globus_3.2.8_20260514.simg: 100%" in output


def test_auto_location_passes_release_image_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_file = tmp_path / "release.json"
    release_file.write_text(
        """
        {
          "apps": {
            "neurodesktop": {
              "version": "20260519",
              "image": "neurodesktop_20260428_arm64"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    tester = ContainerTester()
    tester.release_downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    tester.selected_runtime = type("Runtime", (), {"name": "apptainer"})()
    calls: list[dict[str, object]] = []

    def fake_download(*args, **kwargs) -> None:
        calls.append({"args": args, "kwargs": kwargs})
        return None

    monkeypatch.setattr(tester.cvmfs, "is_available", lambda: False)
    monkeypatch.setattr(tester.release_downloader, "download_from_release", fake_download)

    assert (
        tester.find_container(
            "neurodesktop",
            "20260428-arm64",
            location="release",
            release_file=str(release_file),
        )
        is None
    )
    assert calls == [
        {
            "args": ("neurodesktop", "20260428-arm64", "20260519"),
            "kwargs": {"image_basename": "neurodesktop_20260428_arm64"},
        }
    ]


def test_auto_location_does_not_return_docker_tag_for_apptainer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tester = ContainerTester()
    tester.release_downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    tester.selected_runtime = type("Runtime", (), {"name": "apptainer"})()

    monkeypatch.setattr(tester.cvmfs, "is_available", lambda: False)
    monkeypatch.setattr(
        tester.release_downloader, "download_from_release", lambda *args, **kwargs: None
    )

    assert tester.find_container("globus", "3.2.8", location="auto") is None


def test_auto_location_can_return_docker_tag_for_docker_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tester = ContainerTester()
    tester.release_downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    tester.selected_runtime = type("Runtime", (), {"name": "docker"})()

    monkeypatch.setattr(tester.cvmfs, "is_available", lambda: False)
    monkeypatch.setattr(
        tester.release_downloader, "download_from_release", lambda *args, **kwargs: None
    )

    assert tester.find_container("globus", "3.2.8", location="auto") == "globus:3.2.8"
