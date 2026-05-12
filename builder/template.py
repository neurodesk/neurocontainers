from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import jinja2


class TemplateError(ValueError):
    pass


@dataclass
class TemplateMethods:
    get_file: Callable[[str], str]
    has_local: Callable[[str], bool]
    get_local: Callable[[str], str]


@dataclass
class RenderContext:
    name: str
    version: str
    arch: str
    original_version: str | None = None
    parallel_jobs: int = 1
    values: dict[str, Any] = field(default_factory=dict)
    options: Any = None
    local_keys: set[str] = field(default_factory=set)
    file_paths: dict[str, str] = field(default_factory=dict)
    file_sources: dict[str, str] = field(default_factory=dict)
    file_contents: dict[str, str] = field(default_factory=dict)
    cache_filenames: dict[str, dict[str, str]] = field(default_factory=dict)
    requested_files: list[str] = field(default_factory=list)
    requested_locals: list[str] = field(default_factory=list)
    current_cache_id: str | None = None

    def __getattr__(self, key: str) -> Any:
        if key == "original_version":
            return self.original_version or self.version
        if key in self.values:
            return self.values[key]
        raise AttributeError(key)

    def has_local(self, key: str) -> bool:
        return key in self.local_keys

    def get_local(self, key: str) -> str:
        if key not in self.local_keys:
            raise TemplateError(f"local context not available: {key}")
        self.requested_locals.append(key)
        return f"/.neurocontainer-local/{key}"

    def get_file(self, name: str) -> str:
        if name not in self.file_paths:
            raise TemplateError(f"declared file not available: {name}")
        self.requested_files.append(name)
        guest = self.file_paths[name]
        if self.current_cache_id is not None:
            names = self.cache_filenames.setdefault(self.current_cache_id, {})
            source = self.file_sources.get(name, name)
            target = Path.home() / ".cache" / "neurocontainers" / "build-context" / self.current_cache_id / guest
            source_path = Path(source)
            conflicts_existing = False
            if target.exists():
                if name in self.file_contents:
                    try:
                        conflicts_existing = target.read_text() != self.file_contents[name]
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
                digest = __import__("hashlib").sha256(source.encode("utf-8")).hexdigest()[:12]
                guest = f"{stem}_{digest}.{suffix}" if dot else f"{guest}_{digest}"
            names[guest] = source
            return f"/.neurocontainer-cache/{self.current_cache_id}/{guest}"
        return f"/.neurocontainer-cache/{guest}"


class TemplateRenderer:
    def __init__(self) -> None:
        self.env = jinja2.Environment(undefined=jinja2.StrictUndefined)

    def make_namespace(self, context: RenderContext) -> dict[str, Any]:
        namespace = {
            "context": context,
            "local": context,
            "arch": context.arch,
            "parallel_jobs": context.parallel_jobs,
            "get_file": context.get_file,
            "has_local": context.has_local,
            "get_local": context.get_local,
        }
        namespace.update(context.values)
        return namespace

    def render_string(self, value: str, context: RenderContext) -> str:
        try:
            return self.env.from_string(value).render(**self.make_namespace(context))
        except jinja2.TemplateError as exc:
            raise TemplateError(str(exc)) from exc

    def render_condition(self, condition: str, context: RenderContext) -> bool:
        rendered = self.render_string("{{ " + condition + " }}", context).strip()
        return rendered == "True"

    def render_value(self, value: Any, context: RenderContext) -> Any:
        if isinstance(value, str):
            return self.render_string(value, context)
        if isinstance(value, list):
            return [self.render_value(item, context) for item in value]
        if isinstance(value, dict):
            if "try" in value and isinstance(value["try"], list):
                for option in value["try"]:
                    if not isinstance(option, dict):
                        continue
                    condition = option.get("condition")
                    if isinstance(condition, str) and self.render_condition(condition, context):
                        return self.render_value(option.get("value"), context)
                raise TemplateError("no try condition matched")
            return {
                str(self.render_value(key, context)): self.render_value(item, context)
                for key, item in value.items()
            }
        return value
