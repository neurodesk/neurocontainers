from __future__ import annotations

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

from builder.cache import DownloadError, HttpCache


@pytest.mark.parametrize(
    "behavior",
    [
        "resume",
        "ignored",
        "changed",
        "invalid_range",
        "invalid_total",
        "invalid_length",
        "missing_validator",
        "weak_validator",
        "sha256",
        "sha256_changed",
    ],
)
def test_interrupted_http_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, behavior: str
) -> None:
    requests: list[tuple[str | None, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requested = self.headers.get("Range")
            requests.append((requested, self.headers.get("If-Range")))
            body = (
                b"changed!"
                if requested and behavior == "sha256_changed"
                else b"complete"
            )
            start = 5 if requested else 0
            if behavior == "ignored" and requested:
                start = 0
            self.send_response(206 if start else 200)
            self.send_header(
                "Content-Length",
                str(
                    4
                    if requested and behavior == "invalid_length"
                    else len(body) - start
                ),
            )
            self.send_header("Accept-Ranges", "bytes")
            if behavior == "weak_validator":
                self.send_header("ETag", 'W/"fixed"')
            elif behavior not in {"missing_validator", "sha256", "sha256_changed"}:
                self.send_header(
                    "ETag",
                    '"changed"' if requested and behavior == "changed" else '"fixed"',
                )
            if start:
                actual_start = 4 if behavior == "invalid_range" else start
                total = 9 if behavior == "invalid_total" else 8
                self.send_header("Content-Range", f"bytes {actual_start}-7/{total}")
            self.end_headers()
            self.wfile.write(body[:5] if not requested else body[start:])
            self.close_connection = True

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr("builder.cache.time.sleep", lambda delay: None)
    cache = HttpCache(tmp_path / "cache")
    try:
        url = f"http://127.0.0.1:{server.server_port}/weights"
        sha256 = (
            hashlib.sha256(b"complete").hexdigest()
            if behavior.startswith("sha256")
            else None
        )
        if behavior not in {"resume", "ignored", "sha256"}:
            error = (
                "SHA-256 mismatch"
                if behavior == "sha256_changed"
                else "Content-Length mismatch"
                if behavior in {"missing_validator", "weak_validator"}
                else "Invalid response to resumed download"
            )
            with pytest.raises(DownloadError, match=error):
                cache.get(url, retry=1, sha256=sha256)
            assert list(cache.root.iterdir()) == []
        else:
            path = cache.get(url, retry=1, sha256=sha256)
            assert path.read_bytes() == b"complete"
            assert list(cache.root.iterdir()) == [path]
        if behavior in {"missing_validator", "weak_validator"}:
            assert requests == [(None, None), (None, None)]
        else:
            assert requests == [
                (None, None),
                ("bytes=5-", None if sha256 else '"fixed"'),
            ]
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
