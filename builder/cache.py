from __future__ import annotations

import hashlib
import os
import shutil
import urllib.parse
import urllib.request
from pathlib import Path


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


class HttpCache:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, url: str) -> Path:
        return self.root / sha256_text(url)

    def get(self, url: str, *, download: bool = True) -> Path:
        path = self.path_for(url)
        if path.exists() and path.stat().st_size > 0:
            return path
        if not download:
            return path
        tmp = path.with_suffix(path.suffix + ".tmp")
        with urllib.request.urlopen(url) as response, tmp.open("wb") as handle:
            shutil.copyfileobj(response, handle)
        tmp.replace(path)
        return path
