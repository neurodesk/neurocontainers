from __future__ import annotations

import json
import shlex
from pathlib import PurePosixPath
from typing import Any

from .ir import Copy, Definition, Directive, Entrypoint, Env, From, Install, LiteralFile, Run, RunWithMounts, User, Workdir


def _indent_run_instruction(string: str, indent: int = 4) -> str:
    out: list[str] = []
    lines = string.splitlines()
    for index, line in enumerate(lines):
        line = line.rstrip()
        if not line:
            continue
        is_last_line = index == len(lines) - 1
        already_cont = line.startswith(("&&", "&", "||", "|", "fi"))
        is_comment = line.startswith("#")
        previous_cont = lines[index - 1].endswith("\\") or lines[index - 1].startswith("if")
        if index:
            if not already_cont and not previous_cont and not is_comment:
                line = "&& " + line
            if not already_cont and previous_cont:
                line = " " * (indent + 3) + line
            else:
                line = " " * indent + line
        if not is_last_line and not line.endswith("\\") and not is_comment:
            line += " \\"
        out.append(line)
    return "\n".join(out)


def _json_save_command(spec: dict[str, Any]) -> str:
    text = json.dumps(spec, indent=2)
    text = text.replace("\\", "\\\\")
    text = text.replace("(", "\\(").replace(")", "\\)")
    text = " \\\n".join(text.splitlines())
    text = text.replace("%", "%%")
    text = text.replace("'", "'\"'\"'")
    return f"printf '{text}' > /.reproenv.json"


def _install_command(pkg_manager: str, packages: tuple[str, ...], opts: str | None = None) -> str:
    if pkg_manager == "apt":
        sorted_packages = sorted(packages)
        opts = "-q --no-install-recommends" if opts is None else opts
        return "\n".join(
            (
                "apt-get update -qq",
                f"apt-get install -y {opts} \\",
                "    " + " \\\n    ".join(sorted_packages),
                "rm -rf /var/lib/apt/lists/*",
            )
        )
    if pkg_manager in {"yum", "rpm"}:
        sorted_packages = sorted(packages)
        opts = "-q" if opts is None else opts
        return "\n".join(
            (
                f"yum install -y {opts} \\",
                "    " + " \\\n    ".join(sorted_packages),
                "yum clean all",
                "rm -rf /var/cache/yum/*",
            )
        )
    raise ValueError(f"unsupported package manager: {pkg_manager}")


def _quote_copy(value: str) -> str:
    return json.dumps(value)


def _render_env(values: dict[str, str]) -> list[str]:
    if not values:
        return []
    keys = list(values)
    if len(keys) == 1:
        key = keys[0]
        return [f'ENV {key}="{values[key]}"']
    lines = []
    for index, key in enumerate(keys):
        suffix = " \\" if index < len(keys) - 1 else ""
        prefix = "ENV " if index == 0 else "    "
        lines.append(f'{prefix}{key}="{values[key]}"{suffix}')
    return lines


def _render_literal_file(item: LiteralFile) -> str:
    target = shlex.quote(item.name)
    parent = str(PurePosixPath(item.name).parent)
    commands: list[str] = []
    if parent not in {"", "."}:
        commands.append(f"mkdir -p {shlex.quote(parent)}")
    commands.append(f"cat > {target} <<'EOF'\n{item.contents.rstrip()}\nEOF")
    if item.executable:
        commands.append(f"chmod +x {target}")
    return " &&\n ".join(commands)


def render_directive(directive: Directive, pkg_manager: str = "apt") -> list[str]:
    if isinstance(directive, From):
        if not directive.image:
            raise ValueError("FROM image cannot be empty")
        return [f"FROM {directive.image}"]
    if isinstance(directive, Env):
        return _render_env(directive.values)
    if isinstance(directive, Install):
        command = _indent_run_instruction(_install_command(pkg_manager, directive.packages, directive.opts))
        return [_indent_run_instruction(f"RUN {command}")]
    if isinstance(directive, Run):
        return [_indent_run_instruction(f"RUN {directive.command}")]
    if isinstance(directive, RunWithMounts):
        prefix = " ".join(directive.mounts)
        return [_indent_run_instruction(f"RUN {prefix}{directive.command}")]
    if isinstance(directive, Copy):
        if not directive.sources:
            raise ValueError("COPY requires at least one source")
        parts = [*directive.sources, directive.destination]
        return ['COPY ["{}"]'.format('", \\\n      "'.join(parts))]
    if isinstance(directive, Workdir):
        return [f"WORKDIR {directive.path}"]
    if isinstance(directive, User):
        return [f"USER {directive.user}"]
    if isinstance(directive, Entrypoint):
        return ['ENTRYPOINT ["{}"]'.format(directive.command)]
    if isinstance(directive, LiteralFile):
        return [_indent_run_instruction(f"RUN {_render_literal_file(directive)}")]
    raise TypeError(f"unsupported directive: {directive!r}")


def _instruction_records(directive: Directive, pkg_manager: str = "apt") -> list[dict[str, Any]]:
    if isinstance(directive, From):
        return [{"name": "from_", "kwds": {"base_image": directive.image}}]
    if isinstance(directive, Env):
        return [{"name": "env", "kwds": dict(directive.values)}]
    if isinstance(directive, Install):
        install = {
            "name": "install",
            "kwds": {"pkgs": list(directive.packages), "opts": directive.opts},
        }
        run = {
            "name": "run",
            "kwds": {"command": _indent_run_instruction(_install_command(pkg_manager, directive.packages, directive.opts))},
        }
        return [install, run]
    if isinstance(directive, Run):
        return [{"name": "run", "kwds": {"command": directive.command}}]
    if isinstance(directive, RunWithMounts):
        command = " ".join(directive.mounts) + directive.command
        return [{"name": "run", "kwds": {"command": command}}]
    if isinstance(directive, Copy):
        source = [*directive.sources, directive.destination]
        return [{"name": "copy", "kwds": {"source": source, "destination": directive.destination}}]
    if isinstance(directive, Workdir):
        return [{"name": "workdir", "kwds": {"path": directive.path}}]
    if isinstance(directive, User):
        return [{"name": "user", "kwds": {"user": directive.user}}]
    if isinstance(directive, Entrypoint):
        return [{"name": "entrypoint", "kwds": {"args": [directive.command]}}]
    if isinstance(directive, LiteralFile):
        return [{"name": "run", "kwds": {"command": _render_literal_file(directive)}}]
    raise TypeError(f"unsupported directive: {directive!r}")


def render_dockerfile(definition: Definition) -> str:
    lines = ["# Generated by Neurodocker and Reproenv.", ""]
    created_users = {"root"}
    current_user = "root"
    for directive in definition.directives:
        if isinstance(directive, User):
            rendered = []
            if directive.user not in created_users:
                rendered.append(
                    f'RUN test "$(getent passwd {directive.user})" \\\n'
                    f"    || useradd --no-user-group --create-home --shell /bin/bash {directive.user}"
                )
                created_users.add(directive.user)
            rendered.append(f"USER {directive.user}")
            current_user = directive.user
        else:
            rendered = render_directive(directive, definition.pkg_manager)
        if rendered:
            lines.extend(rendered)
    records: list[dict[str, Any]] = []
    for directive in definition.directives:
        records.extend(_instruction_records(directive, definition.pkg_manager))
    spec = {
        "pkg_manager": definition.pkg_manager,
        "existing_users": ["root"],
        "instructions": records,
    }
    lines.extend(
        [
            "",
            "# Save specification to JSON.",
        ]
    )
    if current_user != "root":
        lines.append("USER root")
    lines.append(f"RUN {_json_save_command(spec)}")
    if current_user != "root":
        lines.append(f"USER {current_user}")
    lines.append("# End saving to specification to JSON.")
    output = "\n".join(lines).strip()
    if definition.fix_locale_def:
        rendered_lines = output.split("\n")
        for index, line in enumerate(rendered_lines):
            if "localedef" in line:
                rendered_lines[index] = ""
                break
        output = "\n".join(rendered_lines)
    return output
