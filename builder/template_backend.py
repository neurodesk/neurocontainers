from __future__ import annotations

import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jinja2
import yaml

from .dockerfile import _install_command
from .template import rewrite_legacy_self


_TEMPLATE_DIR = Path(__file__).with_name("templates")
_JINJA = jinja2.Environment()
_JINJA.filters["shellwords"] = shlex.split


def _raise_template_error(message: str) -> None:
    raise ValueError(message)


_JINJA.globals["raise"] = _raise_template_error


def _apt_install_debs(urls: list[str], opts: str | None = None) -> str:
    opts = "-q" if opts is None else opts
    parts: list[str] = []
    for url in sorted(urls):
        parts.append(
            '_reproenv_tmppath="$(mktemp -t tmp.XXXXXXXXXX.deb)"\n'
            f'curl -fsSL --retry 5 -o "${{_reproenv_tmppath}}" {url}\n'
            f'apt-get install --yes {opts} "${{_reproenv_tmppath}}"\n'
            'rm "${_reproenv_tmppath}"'
        )
    parts.append(
        "apt-get update -qq\n"
        "apt-get install --yes --quiet --fix-missing\n"
        "rm -rf /var/lib/apt/lists/*"
    )
    return "\n".join(parts)


@dataclass
class TemplateMethod:
    data: dict[str, Any]
    values: dict[str, str]
    pkg_manager: str
    directory: Path = _TEMPLATE_DIR

    def __getattr__(self, key: str) -> Any:
        if key in self.values:
            return self.values[key]
        if key == "urls":
            return self.data.get("urls", {})
        if key == "env":
            env: dict[str, Any] = {}
            for directive in self.data.get("directives", []):
                if isinstance(directive, dict) and isinstance(
                    directive.get("environment"), dict
                ):
                    env.update(directive["environment"])
            return env
        raise AttributeError(key)

    def dependencies(self, pkg_manager: str) -> list[str]:
        deps = self.data.get("dependencies", {})
        if not isinstance(deps, dict):
            return []
        values = deps.get(pkg_manager, [])
        if not isinstance(values, list):
            return []
        return [_render_string(str(item), self) for item in values]

    def install(self, pkgs: list[str], opts: str | None = None) -> str:
        return _install_command(
            self.pkg_manager, tuple(str(item) for item in pkgs), opts
        )

    def install_dependencies(self, opts: str | None = None) -> str:
        command = ""
        packages = self.dependencies(self.pkg_manager)
        if packages:
            command += _install_command(self.pkg_manager, tuple(packages), opts)
        if self.pkg_manager == "apt":
            debs = self.dependencies("debs")
            if debs:
                command += "\n" + _apt_install_debs(debs)
        return command


def _render_string(source: str, method: TemplateMethod) -> str:
    renders = 0
    while "{{" in source and "}}" in source:
        source = rewrite_legacy_self(source, _JINJA)
        source = _JINJA.from_string(source).render(template=method)
        renders += 1
        if renders > 20:
            raise ValueError("template rendering exceeded 20 nested passes")
    return source


def _load_template(name: str) -> dict[str, Any]:
    if not name or not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(f"invalid builtin template name: {name!r}")
    path = _TEMPLATE_DIR / f"{name}.yaml"
    if not path.is_file():
        raise NotImplementedError(
            f"local template backend does not yet implement template {name!r}"
        )
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"template file must contain a mapping: {path}")
    return data


def _method_values(
    method_data: dict[str, Any], params: dict[str, Any]
) -> dict[str, str]:
    args = method_data.get("arguments", {})
    optional = args.get("optional", {}) if isinstance(args, dict) else {}
    values: dict[str, str] = {}
    if isinstance(optional, dict):
        for key, value in optional.items():
            values[str(key)] = str(value)
    for key, value in params.items():
        if key != "method":
            values[str(key)] = str(value)
    return values


def expand_builtin_template(
    name: str, params: dict[str, Any], pkg_manager: str
) -> tuple[list[dict[str, Any]], TemplateMethod]:
    template = _load_template(name)
    method_name = str(
        params.get("method") or ("binaries" if "binaries" in template else "source")
    )
    method_data = template.get(method_name)
    if not isinstance(method_data, dict):
        raise NotImplementedError(
            f"local template backend does not yet implement template {name!r} method {method_name!r}"
        )
    builder = method_data.get("builder", "neurodocker")
    if builder != "neurodocker":
        raise ValueError(
            f"template {name!r} method {method_name!r} uses unsupported builder {builder!r}"
        )

    method = TemplateMethod(
        method_data, _method_values(method_data, params), pkg_manager, _TEMPLATE_DIR
    )
    method.pkg_manager = pkg_manager

    directives = method_data.get("directives")
    if not isinstance(directives, list):
        raise ValueError(
            f"template {name!r} method {method_name!r} directives must be a list"
        )

    return directives, method
