from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import jinja2

from .staging import StagingPlan


class TemplateError(ValueError):
    pass


@dataclass
class RenderScope:
    cache_id: str
    requested_file: bool = False
    local_keys: list[str] = field(default_factory=list)

    @property
    def mounts(self) -> tuple[str, ...]:
        mounts = []
        if self.requested_file:
            mounts.append(
                "--mount=type=bind,"
                f"from=neurocontainer-cache,source=/{self.cache_id},"
                f"target=/.neurocontainer-cache/{self.cache_id},readonly"
            )
        mounts.extend(
            f"--mount=type=bind,from={key},source=/,target=/.neurocontainer-local/{key},readonly"
            for key in self.local_keys
        )
        return tuple(dict.fromkeys(mounts))


@dataclass
class RenderContext:
    name: str
    version: str
    arch: str
    variant: str = ""
    original_version: str | None = None
    parallel_jobs: int = 1
    values: dict[str, Any] = field(default_factory=dict)
    options: Any = None
    local_keys: set[str] = field(default_factory=set)
    staging_plan: StagingPlan = field(default_factory=StagingPlan)
    _scope: RenderScope | None = field(default=None, init=False, repr=False)

    @contextmanager
    def mount_scope(self, cache_id: str) -> Iterator[RenderScope]:
        previous_scope = self._scope
        scope = RenderScope(cache_id)
        self._scope = scope
        try:
            yield scope
        finally:
            self._scope = previous_scope

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
        if self._scope is not None:
            self._scope.local_keys.append(key)
        return f"/.neurocontainer-local/{key}"

    def get_file(self, name: str) -> str:
        cache_id = self._scope.cache_id if self._scope is not None else None
        try:
            path = self.staging_plan.file_path(name, cache_id)
        except KeyError as exc:
            raise TemplateError(f"declared file not available: {name}") from exc
        if self._scope is not None:
            self._scope.requested_file = True
        return path


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
