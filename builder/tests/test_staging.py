from __future__ import annotations

import io
from pathlib import Path
import urllib.error
import urllib.request

import pytest

from builder.cache import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    DownloadError,
    HttpCache,
    get_guest_filename,
)
from builder.config import default_config, resolve_recipe
from builder.recipe import compile_recipe
from builder.staging import (
    DeclaredFile,
    StagingPlan,
    declared_file_from_mapping,
    materialize_plan,
)


class FakeResponse(io.BytesIO):
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def test_url_guest_filename_uses_url_basename() -> None:
    assert (
        get_guest_filename("downloaded_file", "https://example.com/releases/tool.tar.gz")
        == "tool.tar.gz"
    )


def test_http_cache_download_sets_user_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[urllib.request.Request] = []
    timeouts: list[int] = []

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> FakeResponse:
        requests.append(request)
        timeouts.append(timeout)
        return FakeResponse(b"downloaded")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    path = HttpCache(tmp_path / "httpcache").get("https://example.com/tool.tar.gz")

    assert path.read_bytes() == b"downloaded"
    headers = {key.lower(): value for key, value in requests[0].header_items()}
    assert headers["user-agent"] == DEFAULT_USER_AGENT
    assert timeouts == [DEFAULT_TIMEOUT_SECONDS]


def test_http_cache_retries_retryable_errors_with_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    sleeps: list[float] = []

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> FakeResponse:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                hdrs=None,
                fp=None,
            )
        return FakeResponse(b"ok")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr("builder.cache.time.sleep", lambda delay: sleeps.append(delay))

    path = HttpCache(tmp_path / "httpcache").get("https://example.com/tool.tar.gz", retry=2)

    assert path.read_bytes() == b"ok"
    assert calls == 3
    assert sleeps == [1.0, 2.0]


def test_materialize_plan_reports_declared_file_context_on_download_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = "https://example.com/blocked.sh"
    calls = 0

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> FakeResponse:
        nonlocal calls
        calls += 1
        raise urllib.error.HTTPError(
            request.full_url,
            403,
            "Forbidden",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr("builder.cache.time.sleep", lambda delay: None)

    plan = StagingPlan()
    plan.add_file(declared_file_from_mapping("tinytex_install", {"url": url, "retry": 1}))

    with pytest.raises(DownloadError) as exc_info:
        materialize_plan(
            plan,
            tmp_path,
            tmp_path / "build",
            http_cache_dir=tmp_path / "httpcache",
            download=True,
        )

    message = str(exc_info.value)
    assert "tinytex_install" in message
    assert url in message
    assert "HTTP 403: Forbidden" in message
    assert "after 2 attempt(s)" in message
    assert calls == 2


def test_stage_dcm2niix_creates_placeholder_without_download(tmp_path: Path) -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    cache_dir = materialize_plan(
        compiled.staging_plan,
        compiled.recipe_dir,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    assert (cache_dir / "dcm2niix_lnx.zip").exists()
    mounted = list(cache_dir.glob("h*/dcm2niix_lnx.zip"))
    assert mounted, "declared files used via get_file() must be staged under their cache mount id"


def test_stage_literal_file(tmp_path: Path) -> None:
    plan = StagingPlan()
    plan.add_file(DeclaredFile(name="script.sh", contents="#!/bin/sh\necho ok", executable=True))
    cache_dir = materialize_plan(
        plan,
        tmp_path,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    target = cache_dir / "script.sh"
    assert target.read_text() == "#!/bin/sh\necho ok"
    assert target.stat().st_mode & 0o111


def test_fixture_stages_filename_and_contents(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "file_variants"
    compiled = compile_recipe(fixture, architecture="x86_64")
    cache_dir = materialize_plan(
        compiled.staging_plan,
        fixture,
        tmp_path / "build",
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    assert (cache_dir / "inline.txt").read_text() == "hello inline"
    assert (cache_dir / "copied.txt").read_text() == "hello copied\n"


def test_declared_copy_sources_are_staged_into_build_context(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "copy_declared_files"
    compiled = compile_recipe(fixture, architecture="x86_64")
    build_dir = tmp_path / "build"
    materialize_plan(
        compiled.staging_plan,
        fixture,
        build_dir,
        http_cache_dir=tmp_path / "httpcache",
        download=False,
    )
    assert (build_dir / "inline.txt").read_text() == "hello inline"
    assert (build_dir / "copied.txt").read_text() == "hello copied\n"
