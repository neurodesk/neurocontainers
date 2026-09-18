from __future__ import annotations

import hashlib
import json
from unittest.mock import Mock

import pytest
import requests

from builder import update_http_cache


def response(
    content: bytes,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    url: str = "https://cdn.example/signed?token=secret",
) -> Mock:
    result = Mock()
    result.status_code = status
    result.headers = headers or {}
    result.url = url
    result.iter_content.return_value = [content]
    if status >= 400 and status != 304:
        result.raise_for_status.side_effect = requests.HTTPError(str(status))
    return result


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTO_UPDATE_HTTP_CACHE", str(tmp_path / "http-cache"))


def test_conditional_304_reuses_successful_stream_without_storing_redirect() -> None:
    canonical = "https://downloads.example/tool.zip"
    payload = b"large artifact"
    session = Mock()
    first = response(
        payload,
        headers={
            "ETag": '"release-1"',
            "Last-Modified": "Mon, 07 Sep 2026 03:00:00 GMT",
        },
    )
    second = response(b"", status=304, url=canonical)
    session.get.side_effect = [first, second]

    observed = update_http_cache.stream_sha256(session, canonical)
    repeated = update_http_cache.stream_sha256(session, canonical)

    expected = (hashlib.sha256(payload).hexdigest(), len(payload), canonical)
    assert observed == expected
    assert repeated == expected
    assert second.iter_content.call_count == 0
    assert session.get.call_args_list[1].kwargs["headers"] == {
        "If-None-Match": '"release-1"',
        "If-Modified-Since": "Mon, 07 Sep 2026 03:00:00 GMT",
    }
    cached = json.loads(update_http_cache._cache_path(canonical).read_text())
    assert cached["url"] == canonical
    assert "token=secret" not in json.dumps(cached)


def test_conditional_200_rehashes_and_replaces_cached_metadata() -> None:
    url = "https://downloads.example/tool.zip"
    session = Mock()
    session.get.side_effect = [
        response(b"old", headers={"ETag": '"old"'}, url=url),
        response(b"new", headers={"ETag": '"new"'}, url=url),
    ]

    update_http_cache.stream_sha256(session, url)
    observed = update_http_cache.stream_sha256(session, url)

    assert observed[:2] == (hashlib.sha256(b"new").hexdigest(), 3)
    assert session.get.call_args_list[1].kwargs["headers"] == {
        "If-None-Match": '"old"'
    }
    cached = json.loads(update_http_cache._cache_path(url).read_text())
    assert cached["etag"] == '"new"'
    assert cached["sha256"] == observed[0]


def test_response_without_validators_is_rehashed_on_every_run() -> None:
    url = "https://downloads.example/tool.zip"
    session = Mock()
    first = response(b"same", url=url)
    second = response(b"same", url=url)
    session.get.side_effect = [first, second]

    update_http_cache.stream_sha256(session, url)
    update_http_cache.stream_sha256(session, url)

    assert session.get.call_args_list[1].kwargs == {
        "stream": True,
        "timeout": 60,
    }
    assert session.get.call_count == 2
    first.iter_content.assert_called_once_with(chunk_size=1024 * 1024)
    second.iter_content.assert_called_once_with(chunk_size=1024 * 1024)


def test_malformed_cache_is_ignored_and_atomically_replaced() -> None:
    url = "https://downloads.example/tool.zip"
    path = update_http_cache._cache_path(url)
    path.parent.mkdir(parents=True)
    path.write_text('{"url": "https://attacker.example", "sha256": 7}')
    session = Mock()
    session.get.return_value = response(
        b"trusted", headers={"ETag": '"trusted"'}, url=url
    )

    observed = update_http_cache.stream_sha256(session, url)

    assert observed[0] == hashlib.sha256(b"trusted").hexdigest()
    assert "headers" not in session.get.call_args.kwargs
    assert not [item for item in path.parent.iterdir() if item.name.endswith(".tmp")]
    assert json.loads(path.read_text())["etag"] == '"trusted"'


def test_failed_stream_does_not_publish_cache_entry() -> None:
    url = "https://downloads.example/tool.zip"
    failed = response(b"", headers={"ETag": '"partial"'}, url=url)
    failed.iter_content.side_effect = OSError("connection reset")
    session = Mock()
    session.get.return_value = failed

    with pytest.raises(OSError, match="connection reset"):
        update_http_cache.stream_sha256(session, url)

    assert not update_http_cache._cache_path(url).exists()
