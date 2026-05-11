from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shutil

from .cache import HttpCache, get_guest_filename, link_or_copy, sha256_text


@dataclass(frozen=True)
class DeclaredFile:
    name: str
    filename: str | None = None
    url: str | None = None
    contents: str | None = None
    executable: bool = False
    guest_filename: str | None = None


@dataclass
class StagingPlan:
    files: dict[str, DeclaredFile] = field(default_factory=dict)
    copy_sources: list[str] = field(default_factory=list)

    def add_file(self, file: DeclaredFile) -> None:
        if file.name in self.files:
            raise ValueError(f"duplicate declared file: {file.name}")
        self.files[file.name] = file


@dataclass(frozen=True)
class StageResult:
    build_dir: Path
    dockerfile_path: Path
    cache_dir: Path


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

    for file in plan.files.values():
        preferred = file.guest_filename or file.name
        if file.contents is not None:
            target = cache_dir / preferred
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(file.contents)
            if file.executable:
                target.chmod(0o755)
            continue

        if file.filename is not None:
            source = Path(file.filename)
            if not source.is_absolute():
                source = recipe_dir / source
            if not source.exists():
                raise FileNotFoundError(f"declared file not found: {source}")
            name = disambiguated_cache_name(cache_dir, preferred, source)
            target = cache_dir / name
            link_or_copy(source, target)
            if file.executable:
                target.chmod(0o755)
            continue

        if file.url is not None:
            source = http_cache.get(file.url, download=download)
            if not source.exists():
                target = cache_dir / preferred
                target.parent.mkdir(parents=True, exist_ok=True)
                target.touch()
            else:
                name = disambiguated_cache_name(cache_dir, preferred, source)
                target = cache_dir / name
                link_or_copy(source, target)
            if file.executable:
                target.chmod(0o755)
            continue

        raise ValueError(f"declared file {file.name!r} has no source")

    for source in plan.copy_sources:
        source_path = recipe_dir / source
        target = build_dir / source
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
    url_str = str(url) if url is not None else None
    guest_filename = get_guest_filename(name, url_str)
    return DeclaredFile(
        name=name,
        filename=str(filename) if filename is not None else None,
        url=url_str,
        contents=str(contents) if contents is not None else None,
        executable=executable,
        guest_filename=guest_filename,
    )
