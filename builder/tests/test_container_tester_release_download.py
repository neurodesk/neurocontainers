from __future__ import annotations

from pathlib import Path
from urllib.error import HTTPError

import pytest

from workflows import container_tester
from workflows.container_tester import ContainerTester, ReleaseContainerDownloader


def test_release_downloader_falls_back_to_s3_when_nectar_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    calls: list[str] = []

    def fake_urlretrieve(url: str, filename: str) -> tuple[str, None]:
        calls.append(url)
        if "object-store.rc.nectar.org.au" in url:
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
        "https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/neurodesk/globus_3.2.8_20260514.simg",
        "https://neurocontainers.s3.us-east-2.amazonaws.com/globus_3.2.8_20260514.simg",
    ]


def test_auto_location_does_not_return_docker_tag_for_apptainer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tester = ContainerTester()
    tester.release_downloader = ReleaseContainerDownloader(cache_dir=str(tmp_path))
    tester.selected_runtime = type("Runtime", (), {"name": "apptainer"})()

    monkeypatch.setattr(tester.cvmfs, "is_available", lambda: False)
    monkeypatch.setattr(
        tester.release_downloader, "download_from_release", lambda *args: None
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
        tester.release_downloader, "download_from_release", lambda *args: None
    )

    assert tester.find_container("globus", "3.2.8", location="auto") == "globus:3.2.8"
