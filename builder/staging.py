from __future__ import annotations

import shutil
from dataclasses import dataclass, field, replace
from pathlib import Path

from .cache import (
    HttpCache,
    download_cache_key,
    get_guest_filename,
    link_or_copy,
    normalize_sha256,
    sha256_text,
)


@dataclass(frozen=True)
class DeclaredFile:
    name: str
    filename: str | None = None
    url: str | None = None
    contents: str | None = None
    executable: bool = False
    guest_filename: str | None = None
    retry: int | None = None
    sha256: str | None = None


@dataclass
class CopySource:
    source: str
    declared_name: str | None = None


@dataclass
class StagingPlan:
    files: dict[str, DeclaredFile] = field(default_factory=dict)
    copy_sources: list[CopySource] = field(default_factory=list)
    recipe_dir: Path = field(default_factory=Path)
    _cache_mounts: dict[str, dict[str, str]] = field(default_factory=dict, init=False, repr=False)
    _unavailable_files: set[str] = field(default_factory=set, init=False, repr=False)
    _mount_sources: dict[str, dict[str, str]] = field(default_factory=dict, init=False, repr=False)

    def add_file(
        self,
        file: DeclaredFile,
        *,
        relative_to: Path | None = None,
        renderable: bool = True,
    ) -> None:
        if file.name in self.files:
            raise ValueError(f"duplicate declared file: {file.name}")
        if file.filename is not None and relative_to is not None and relative_to != self.recipe_dir:
            filename = Path(file.filename)
            if not filename.is_absolute():
                file = replace(file, filename=str(relative_to / filename))
        self.files[file.name] = file
        if not renderable:
            self._unavailable_files.add(file.name)

    def _source_identity(self, file: DeclaredFile) -> str:
        if file.url is not None:
            return str(Path.home() / ".cache" / "neurocontainers" / download_cache_key(file.url, file.sha256))
        if file.filename is not None:
            source = Path(file.filename)
            if not source.is_absolute():
                source = self.recipe_dir / source
            return str(source.resolve())
        return file.name

    @property
    def cache_mounts(self) -> dict[str, dict[str, str]]:
        if not self._mount_sources:
            return self._cache_mounts
        sources = {
            self._source_identity(file): name
            for name, file in self.files.items()
            if name not in self._unavailable_files
        }
        for cache_id, guests in self._mount_sources.items():
            self._cache_mounts[cache_id] = {
                sources[source]: guest for guest, source in guests.items()
                if source in sources
            }
        return self._cache_mounts

    def file_path(self, name: str, cache_id: str | None = None) -> str:
        if name not in self.files or name in self._unavailable_files:
            raise KeyError(name)
        file = self.files[name]
        guest = file.guest_filename or name
        if cache_id is None:
            return f"/.neurocontainer-cache/{guest}"
        names = self._mount_sources.setdefault(cache_id, {})
        source = self._source_identity(file)
        target = Path.home() / ".cache" / "neurocontainers" / "build-context" / cache_id / guest
        source_path = Path(source)
        conflicts_existing = False
        if target.exists():
            if file.contents is not None and file.url is None and file.filename is None:
                try:
                    conflicts_existing = target.read_text() != file.contents
                except OSError:
                    conflicts_existing = True
            else:
                try:
                    conflicts_existing = not source_path.samefile(target)
                except OSError:
                    conflicts_existing = True
                if conflicts_existing and source_path.exists():
                    try:
                        conflicts_existing = source_path.read_bytes() != target.read_bytes()
                    except OSError:
                        conflicts_existing = True
        if (guest in names and names[guest] != source) or conflicts_existing:
            stem, dot, suffix = guest.rpartition(".")
            digest = sha256_text(source)[:12]
            guest = f"{stem}_{digest}.{suffix}" if dot else f"{guest}_{digest}"
        names[guest] = source
        return f"/.neurocontainer-cache/{cache_id}/{guest}"

    def add_copy_source(self, source: str) -> str:
        if source in self.files and source not in self._unavailable_files:
            file = self.files[source]
            resolved = file.guest_filename or source
            self.copy_sources.append(CopySource(source=resolved, declared_name=source))
            return resolved
        self.copy_sources.append(CopySource(source=source))
        return source


def disambiguated_cache_name(cache_dir: Path, preferred: str, source: Path) -> str:
    candidate = preferred
    target = cache_dir / candidate
    if not target.exists():
        return candidate
    try:
        if source.samefile(target):
            return candidate
    except OSError:
        pass
    if target.read_bytes() == source.read_bytes():
        return candidate
    stem = Path(preferred).stem
    suffix = Path(preferred).suffix
    return f"{stem}_{sha256_text(str(source))[:12]}{suffix}"


def _stage_file(source: Path, target: Path, *, executable: bool) -> None:
    source_mode = source.stat().st_mode & 0o777
    mode = 0o755 if executable or source_mode & 0o111 else 0o644
    if source_mode == mode:
        link_or_copy(source, target)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.unlink(missing_ok=True)
        shutil.copyfile(source, target)
        target.chmod(mode)


def materialize_plan(
    plan: StagingPlan,
    recipe_dir: Path,
    build_dir: Path,
    *,
    http_cache_dir: Path,
    download: bool = False,
) -> Path:
    cache_dir = build_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    http_cache = HttpCache(http_cache_dir)
    materialized: dict[str, Path] = {}

    for file in plan.files.values():
        preferred = file.guest_filename or file.name
        if file.contents is not None:
            target = cache_dir / preferred
            target.parent.mkdir(parents=True, exist_ok=True)
            target.unlink(missing_ok=True)
            target.write_text(file.contents)
            target.chmod(0o755 if file.executable else 0o644)
            materialized[file.name] = target
            continue

        if file.filename is not None:
            source = Path(file.filename)
            if not source.is_absolute():
                source = recipe_dir / source
            if not source.exists():
                raise FileNotFoundError(f"declared file not found: {source}")
            name = disambiguated_cache_name(cache_dir, preferred, source)
            target = cache_dir / name
            _stage_file(source, target, executable=file.executable)
            materialized[file.name] = target
            continue

        if file.url is not None:
            source = http_cache.get(
                file.url,
                download=download,
                file_name=file.name,
                retry=file.retry,
                sha256=file.sha256,
            )
            if not source.exists():
                target = cache_dir / preferred
                target.parent.mkdir(parents=True, exist_ok=True)
                target.unlink(missing_ok=True)
                target.touch()
                target.chmod(0o755 if file.executable else 0o644)
            else:
                name = disambiguated_cache_name(cache_dir, preferred, source)
                target = cache_dir / name
                _stage_file(source, target, executable=file.executable)
            materialized[file.name] = target
            continue

        raise ValueError(f"declared file {file.name!r} has no source")

    for cache_id, files in plan.cache_mounts.items():
        cache_mount_dir = cache_dir / cache_id
        cache_mount_dir.mkdir(parents=True, exist_ok=True)
        for file_name, guest_name in files.items():
            source = materialized.get(file_name)
            if source is None:
                continue
            target = cache_mount_dir / guest_name
            link_or_copy(source, target)

    for source in plan.copy_sources:
        target = build_dir / source.source
        if source.declared_name is not None:
            source_path = materialized.get(source.declared_name)
            if source_path is None:
                raise FileNotFoundError(f"declared copy source not materialized: {source.declared_name}")
            link_or_copy(source_path, target)
            continue

        source_path = recipe_dir / source.source
        if source_path.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.copytree(source_path, target)
        elif source_path.is_file():
            link_or_copy(source_path, target)

    return cache_dir


def declared_file_from_mapping(name: str, mapping: dict[str, object]) -> DeclaredFile:
    url = mapping.get("url")
    filename = mapping.get("filename")
    contents = mapping.get("contents")
    executable = bool(mapping.get("executable", False))
    retry = mapping.get("retry")
    sha256 = mapping.get("sha256")
    if sha256 is not None:
        if url is None or filename is not None or contents is not None:
            raise ValueError(f"declared file {name!r}: sha256 requires a URL source")
        sha256 = normalize_sha256(sha256)
    url_str = str(url) if url is not None else None
    guest_filename = get_guest_filename(name, url_str)
    return DeclaredFile(
        name=name,
        filename=str(filename) if filename is not None else None,
        url=url_str,
        contents=str(contents) if contents is not None else None,
        executable=executable,
        guest_filename=guest_filename,
        retry=int(retry) if retry is not None else None,
        sha256=sha256,
    )
