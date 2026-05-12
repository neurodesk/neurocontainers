from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import jinja2
import yaml

from .dockerfile import _install_command
from .ir import Env, Install, Run


_TEMPLATE_DIR = Path(__file__).with_name("neurodocker_templates")
_JINJA = jinja2.Environment()


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

    def __getattr__(self, key: str) -> Any:
        if key in self.values:
            return self.values[key]
        if key == "urls":
            return self.data.get("urls", {})
        if key == "env":
            env: dict[str, Any] = {}
            for directive in self.data.get("directives", []):
                if isinstance(directive, dict) and isinstance(directive.get("environment"), dict):
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
        return [str(item) for item in values]

    def install(self, pkgs: list[str], opts: str | None = None) -> str:
        return _install_command(self.pkg_manager, tuple(str(item) for item in pkgs), opts)

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
        source = source.replace("self.", "template.")
        source = _JINJA.from_string(source).render(template=method)
        renders += 1
        if renders > 20:
            raise ValueError("template rendering exceeded 20 nested passes")
    return source


def _load_template(name: str) -> dict[str, Any]:
    path = _TEMPLATE_DIR / f"{name}.yaml"
    if not path.is_file():
        raise NotImplementedError(f"build3 local template backend does not yet implement template {name!r}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"template file must contain a mapping: {path}")
    return data


def _method_values(method_data: dict[str, Any], params: dict[str, Any]) -> dict[str, str]:
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


def apply_builtin_template(name: str, params: dict[str, Any], pkg_manager: str, add: Callable[[Any], None]) -> None:
    template = _load_template(name)
    method_name = str(params.get("method") or ("binaries" if "binaries" in template else "source"))
    method_data = template.get(method_name)
    if not isinstance(method_data, dict):
        raise NotImplementedError(
            f"build3 local template backend does not yet implement template {name!r} method {method_name!r}"
        )

    method = TemplateMethod(method_data, _method_values(method_data, params), pkg_manager)
    method.pkg_manager = pkg_manager

    directives = method_data.get("directives")
    if directives is None:
        directives = []
        env = method_data.get("env", {})
        if isinstance(env, dict) and env:
            directives.append({"environment": env})
        instructions = method_data.get("instructions", "")
        if instructions:
            directives.append({"run": instructions})
    if not isinstance(directives, list):
        raise ValueError(f"template {name!r} method {method_name!r} directives must be a list")

    for directive in directives:
        if not isinstance(directive, dict):
            raise ValueError(f"template directive must be a mapping: {directive!r}")
        if "environment" in directive:
            env = directive["environment"]
            if not isinstance(env, dict):
                raise ValueError("template environment directive must be a mapping")
            if env:
                add(Env({str(_render_string(str(key), method)): _render_string(str(value), method) for key, value in env.items()}))
        elif "install" in directive:
            packages = directive["install"]
            if not isinstance(packages, list):
                raise ValueError("template install directive must be a list")
            add(Install(tuple(_render_string(str(package), method) for package in packages)))
        elif "run" in directive:
            run = directive["run"]
            if isinstance(run, list):
                command = " \\\n && ".join(_render_string(str(item), method) for item in run if item is not None)
            else:
                command = _render_string(str(run), method)
            if command.strip():
                add(Run(command))
        else:
            raise ValueError(f"unsupported template directive: {directive}")
