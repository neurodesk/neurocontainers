from __future__ import annotations

from dataclasses import dataclass, field
from typing import Union


@dataclass(frozen=True)
class From:
    image: str


@dataclass(frozen=True)
class Env:
    values: dict[str, str]


@dataclass(frozen=True)
class Run:
    command: str


@dataclass(frozen=True)
class Install:
    packages: tuple[str, ...]
    opts: str | None = None


@dataclass(frozen=True)
class RunWithMounts:
    mounts: tuple[str, ...]
    command: str


@dataclass(frozen=True)
class Copy:
    sources: tuple[str, ...]
    destination: str


@dataclass(frozen=True)
class Workdir:
    path: str


@dataclass(frozen=True)
class User:
    user: str


@dataclass(frozen=True)
class Entrypoint:
    command: str


@dataclass(frozen=True)
class LiteralFile:
    name: str
    contents: str
    executable: bool = False


Directive = Union[From, Env, Install, Run, RunWithMounts, Copy, Workdir, User, Entrypoint, LiteralFile]


@dataclass
class Definition:
    directives: list[Directive] = field(default_factory=list)
    pkg_manager: str = "apt"
    fix_locale_def: bool = False

    def add(self, directive: Directive) -> None:
        self.directives.append(directive)
