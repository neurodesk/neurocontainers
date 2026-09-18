"""Persist HTTP validators for update-source checksum observations."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import requests


SHA256 = re.compile(r"[0-9a-f]{64}")


def _cache_root() -> Path:
    configured = os.getenv("AUTO_UPDATE_HTTP_CACHE")
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "neurocontainers-update-http"


def _cache_path(url: str) -> Path:
    key = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return _cache_root() / f"{key}.json"


def _safe_https_url(url: object) -> str:
    if not isinstance(url, str):
        raise ValueError("download response URL must be an HTTPS URL")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("download response URL must be an HTTPS URL")
    return url


def _header(headers: object, name: str) -> str | None:
    if not hasattr(headers, "items"):
        return None
    for key, value in headers.items():
        if str(key).lower() == name.lower() and isinstance(value, str):
            return value
    return None


def _validator(value: object, limit: int) -> str | None:
    if (
        isinstance(value, str)
        and value
        and len(value) <= limit
        and "\n" not in value
        and "\r" not in value
    ):
        return value
    return None


def _load(url: str) -> dict[str, object] | None:
    try:
        value = json.loads(_cache_path(url).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("url") != url:
        return None
    digest = value.get("sha256")
    size = value.get("size")
    if (
        not isinstance(digest, str)
        or not SHA256.fullmatch(digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        return None
    etag = _validator(value.get("etag"), 1024)
    last_modified = _validator(value.get("last_modified"), 128)
    if value.get("etag") is not None and etag is None:
        return None
    if value.get("last_modified") is not None and last_modified is None:
        return None
    return {
        "url": url,
        "sha256": digest,
        "size": size,
        "etag": etag,
        "last_modified": last_modified,
    }


def _store(url: str, digest: str, size: int, headers: object) -> None:
    root = _cache_root()
    value = {
        "url": url,
        "sha256": digest,
        "size": size,
        "etag": _validator(_header(headers, "ETag"), 1024),
        "last_modified": _validator(
            _header(headers, "Last-Modified"), 128
        ),
    }
    temporary = root / f".{_cache_path(url).name}.{uuid.uuid4().hex}.tmp"
    try:
        root.mkdir(parents=True, exist_ok=True)
        temporary.write_text(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, _cache_path(url))
    except OSError:
        try:
            temporary.unlink()
        except OSError:
            pass


def stream_sha256(
    session: requests.Session, url: str
) -> tuple[str, int, str]:
    """Hash a response or reuse its checksum only after an HTTP 304."""
    canonical_url = _safe_https_url(url)
    cached = _load(canonical_url)
    headers: dict[str, str] = {}
    if cached is not None:
        if etag := cached["etag"]:
            headers["If-None-Match"] = str(etag)
        if modified := cached["last_modified"]:
            headers["If-Modified-Since"] = str(modified)
    request_options: dict[str, object] = {"stream": True, "timeout": 60}
    if headers:
        request_options["headers"] = headers
    response = session.get(canonical_url, **request_options)
    status = getattr(response, "status_code", 200)
    if status == 304:
        if cached is None or not headers:
            raise ValueError("server returned 304 without a validated cache entry")
        return str(cached["sha256"]), int(cached["size"]), canonical_url
    response.raise_for_status()
    _safe_https_url(getattr(response, "url", canonical_url) or canonical_url)
    digest = hashlib.sha256()
    size = 0
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            digest.update(chunk)
            size += len(chunk)
    checksum = digest.hexdigest()
    _store(canonical_url, checksum, size, response.headers)
    return checksum, size, canonical_url
