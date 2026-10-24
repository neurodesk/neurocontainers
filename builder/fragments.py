"""Path and shape contracts shared by macro validation and execution."""

from pathlib import Path, PurePosixPath
from typing import Any

import yaml


def macro_path(name: str) -> str:
    if not isinstance(name, str):
        raise ValueError("include must be a path under macros/")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ValueError(f"include escapes macros/: {name!r}")
    normalized = path.as_posix()
    return normalized if normalized.startswith("macros/") else "macros/" + normalized


def resolve_macro(name: str, roots: tuple[Path, ...]) -> Path:
    relative = macro_path(name)
    for root in roots:
        base = root / "macros" if root.name != "macros" else root
        path = base / relative.removeprefix("macros/")
        if not path.resolve().is_relative_to(base.resolve()):
            raise ValueError(f"include escapes macros/: {name!r}")
        if path.is_file():
            return path.resolve()
    raise FileNotFoundError(f"include not found: {relative}")


def load_fragment(path: Path) -> list[dict[str, Any]]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict) or not isinstance(value.get("directives"), list):
        raise ValueError(f"fragment must contain a directives list: {path}")
    from .validation import (
        parse_directive_from_dict,
        validate_directive_template_syntax,
    )

    for index, directive in enumerate(value["directives"]):
        if not isinstance(directive, dict):
            raise ValueError(f"fragment directive {index} must be a mapping: {path}")
        parse_directive_from_dict(directive)
        validate_directive_template_syntax(directive, f"{path}.directives[{index}]")
    return value["directives"]


def validate_macro_tree(
    directives: list[dict[str, Any]],
    roots: tuple[Path, ...],
    stack: tuple[Path, ...] = (),
    directory: Path | None = None,
) -> None:
    """Validate literal includes and owned assets without rendering data."""
    for directive in directives:
        file = directive.get("file")
        if (
            directory is not None
            and isinstance(file, dict)
            and isinstance(file.get("filename"), str)
        ):
            filename = file["filename"]
            if not any(token in filename for token in ("{{", "{%", "{#")):
                asset = directory / filename
                if not asset.resolve().is_relative_to(directory.resolve()):
                    raise ValueError(
                        f"fragment asset escapes its directory: {filename}"
                    )
                if not asset.is_file():
                    raise FileNotFoundError(f"fragment asset not found: {asset}")
        if "group" in directive:
            validate_macro_tree(directive["group"], roots, stack, directory)
        name = directive.get("include")
        if not isinstance(name, str) or any(
            token in name for token in ("{{", "{%", "{#")
        ):
            continue
        path = resolve_macro(name, roots)
        if path in stack:
            raise ValueError("include cycle: " + " -> ".join(map(str, (*stack, path))))
        children = load_fragment(path)
        validate_macro_tree(children, roots, (*stack, path), path.parent)
