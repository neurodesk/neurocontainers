from __future__ import annotations

import hashlib
import http.client
import os
import re
import shutil
import tempfile
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


def normalize_sha256(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError("sha256 must contain exactly 64 hexadecimal characters")
    return value.lower()


def download_cache_key(url: str, sha256: str | None = None) -> str:
    if sha256 is None:
        return sha256_text(url)
    return sha256_text(f"{url}\0sha256:{normalize_sha256(sha256)}")


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


def _verify_checksum(path: Path, expected: str, label: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise DownloadError(
            f"SHA-256 mismatch for {label}: expected {expected}, got {actual}"
        )


def _is_retryable_download_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRYABLE_HTTP_CODES
    if isinstance(exc, (urllib.error.URLError, TimeoutError, OSError, http.client.IncompleteRead)):
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

    def path_for(self, url: str, *, sha256: str | None = None) -> Path:
        return self.root / download_cache_key(url, sha256)

    def get(
        self,
        url: str,
        *,
        download: bool = True,
        file_name: str | None = None,
        retry: int | None = None,
        sha256: str | None = None,
    ) -> Path:
        expected = normalize_sha256(sha256) if sha256 is not None else None
        path = self.path_for(url, sha256=expected)
        label = f"{_download_label(file_name)} from {url}"
        if path.exists():
            if expected is not None:
                _verify_checksum(path, expected, f"cached {label}")
                return path
            if path.stat().st_size > 0:
                return path
        if not download:
            return path
        retries = DEFAULT_RETRIES if retry is None else max(0, retry)
        attempts = retries + 1
        last_error: BaseException | None = None

        for attempt in range(1, attempts + 1):
            tmp: Path | None = None
            request = urllib.request.Request(
                url,
                headers={"User-Agent": DEFAULT_USER_AGENT},
            )
            try:
                with (
                    urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response,
                    tempfile.NamedTemporaryFile(
                        dir=self.root, prefix=f".{path.name}.", suffix=".tmp", delete=False
                    ) as handle,
                ):
                    tmp = Path(handle.name)
                    shutil.copyfileobj(response, handle)
                    content_length = response.headers.get("Content-Length")
                    if content_length is not None and handle.tell() != int(content_length):
                        raise OSError(
                            f"Content-Length mismatch: expected {content_length} bytes, "
                            f"received {handle.tell()}"
                        )
                if expected is not None:
                    _verify_checksum(tmp, expected, label)
                tmp.chmod(0o644)
                tmp.replace(path)
                return path
            except (
                urllib.error.HTTPError,
                urllib.error.URLError,
                TimeoutError,
                OSError,
                http.client.IncompleteRead,
            ) as exc:
                last_error = exc
                if attempt >= attempts or not _is_retryable_download_error(exc):
                    break
                delay = min(
                    DEFAULT_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                    MAX_BACKOFF_SECONDS,
                )
                time.sleep(delay)
            finally:
                if tmp is not None:
                    tmp.unlink(missing_ok=True)

        assert last_error is not None
        raise DownloadError(
            f"failed to download {_download_label(file_name)} from {url} "
            f"after {attempt} attempt(s): {_format_download_error(last_error)}"
        ) from last_error
