from __future__ import annotations

import hashlib
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_USER_AGENT = "NeuroContainers-builder (+https://github.com/neurodesk/neurocontainers)"
DEFAULT_RETRIES = 2
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 30.0
RETRYABLE_HTTP_CODES = {403, 408, 425, 429, 500, 502, 503, 504}


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def get_guest_filename(name: str, url: str | None = None) -> str:
    if url:
        parsed = urllib.parse.urlparse(url)
        basename = os.path.basename(urllib.parse.unquote(parsed.path))
        if basename not in {"", ".", ".."}:
            return basename
    return name


def link_or_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        try:
            if source.samefile(destination):
                return
        except OSError:
            pass
        destination.unlink()
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


class DownloadError(RuntimeError):
    pass


def _is_retryable_download_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRYABLE_HTTP_CODES
    if isinstance(exc, (urllib.error.URLError, TimeoutError, OSError)):
        return True
    return False


def _format_download_error(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        reason = getattr(exc, "reason", None) or getattr(exc, "msg", "")
        return f"HTTP {exc.code}: {reason}"
    if isinstance(exc, urllib.error.URLError):
        return f"URL error: {exc.reason}"
    return f"{type(exc).__name__}: {exc}"


def _download_label(file_name: str | None) -> str:
    if file_name:
        return f"declared file {file_name!r}"
    return "declared URL"


class HttpCache:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, url: str) -> Path:
        return self.root / sha256_text(url)

    def get(
        self,
        url: str,
        *,
        download: bool = True,
        file_name: str | None = None,
        retry: int | None = None,
    ) -> Path:
        path = self.path_for(url)
        if path.exists() and path.stat().st_size > 0:
            return path
        if not download:
            return path
        tmp = path.with_suffix(path.suffix + ".tmp")
        retries = DEFAULT_RETRIES if retry is None else max(0, retry)
        attempts = retries + 1
        last_error: BaseException | None = None

        for attempt in range(1, attempts + 1):
            request = urllib.request.Request(
                url,
                headers={"User-Agent": DEFAULT_USER_AGENT},
            )
            try:
                with (
                    urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response,
                    tmp.open("wb") as handle,
                ):
                    shutil.copyfileobj(response, handle)
                tmp.replace(path)
                return path
            except (
                urllib.error.HTTPError,
                urllib.error.URLError,
                TimeoutError,
                OSError,
            ) as exc:
                last_error = exc
                if tmp.exists():
                    tmp.unlink()
                if attempt >= attempts or not _is_retryable_download_error(exc):
                    break
                delay = min(
                    DEFAULT_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                    MAX_BACKOFF_SECONDS,
                )
                time.sleep(delay)

        assert last_error is not None
        raise DownloadError(
            f"failed to download {_download_label(file_name)} from {url} "
            f"after {attempt} attempt(s): {_format_download_error(last_error)}"
        ) from last_error
