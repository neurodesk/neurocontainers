from __future__ import annotations

from typing import Any


ARCHITECTURES = {"x86_64", "aarch64"}
BUILD_KINDS = {"neurodocker"}
PKG_MANAGERS = {"apt", "yum", "rpm"}


def require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value


def require_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return value


def validate_recipe_dict(recipe: dict[str, Any]) -> None:
    for field in ("name", "version", "build"):
        if field not in recipe or recipe[field] in (None, ""):
            raise ValueError(f"recipe is missing required field: {field}")

    architectures = recipe.get("architectures")
    if not isinstance(architectures, list) or not architectures:
        raise ValueError("architectures must be a non-empty list")
    for arch in architectures:
        if str(arch) not in ARCHITECTURES:
            raise ValueError(f"unsupported architecture: {arch}")

    build = require_mapping(recipe["build"], "build")
    kind = str(build.get("kind", ""))
    if kind not in BUILD_KINDS:
        raise ValueError(f"unsupported build kind: {kind}")
    if not build.get("base-image"):
        raise ValueError("build.base-image is required")
    pkg_manager = str(build.get("pkg-manager", ""))
    if pkg_manager not in PKG_MANAGERS:
        raise ValueError(f"unsupported package manager: {pkg_manager}")
    require_list(build.get("directives", []), "build.directives")

    for file in recipe.get("files", []):
        mapping = require_mapping(file, "files[]")
        if not mapping.get("name"):
            raise ValueError("file.name is required")
        source_count = sum(1 for key in ("filename", "url", "contents") if key in mapping)
        if source_count != 1:
            raise ValueError(f"file {mapping.get('name')!r} must have exactly one source")
