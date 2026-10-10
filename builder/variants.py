from __future__ import annotations

import warnings
from typing import Any


ARCHITECTURE_ALIASES = {
    "x86_64": "x86_64",
    "AMD64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
    "ARM64": "aarch64",
}


def normalize_declared_architecture(value: str) -> str:
    try:
        return ARCHITECTURE_ALIASES[value]
    except KeyError as exc:
        raise ValueError(f"unsupported architecture: {value}") from exc


def make_variant_spec(
    recipe_name: str,
    recipe_variant: str,
    architecture: str,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one concrete container identity from its parts."""
    suffix = "_arm64" if architecture == "aarch64" else ""
    selector = f"{recipe_variant}{suffix}" if recipe_variant else suffix.lstrip("_")
    return {
        "variant": selector,
        "recipe_variant": recipe_variant,
        "name": f"{recipe_name}_{selector}" if selector else recipe_name,
        "architecture": architecture,
        "options": dict(options or {}),
    }


def forced_variant_spec(
    recipe: dict[str, Any],
    variant: str,
    architecture: str,
) -> dict[str, Any]:
    """Build an identity for an architecture the recipe does not declare.

    Only reachable via --ignore-architectures, which exists so a recipe can be
    built (usually under emulation) on a machine it does not advertise support
    for. Naming still follows the normal rules so the escape hatch cannot
    produce an identity that collides with a declared one.
    """
    recipe_variant = variant
    if recipe_variant.endswith("_arm64"):
        recipe_variant = recipe_variant[: -len("_arm64")]
    elif recipe_variant == "arm64":
        recipe_variant = ""
    config = (recipe.get("variants") or {}).get(recipe_variant) or {}
    if recipe_variant and not config:
        raise ValueError(
            f"recipe {recipe['name']} does not declare variant '{recipe_variant}'"
        )
    return make_variant_spec(
        str(recipe["name"]),
        recipe_variant,
        architecture,
        config.get("options"),
    )


def concrete_variant_specs(recipe: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand architectures and alternatives into concrete container identities."""
    architectures = [
        normalize_declared_architecture(str(item))
        for item in recipe.get("architectures", [])
    ]
    if not architectures:
        raise ValueError(
            f"recipe {recipe.get('name', '<unknown>')} has no architectures"
        )
    default_architecture = "x86_64" if "x86_64" in architectures else architectures[0]

    specs: list[dict[str, Any]] = []
    ordered_architectures = [
        default_architecture,
        *[arch for arch in architectures if arch != default_architecture],
    ]
    if recipe.get("build_default", True):
        for architecture in ordered_architectures:
            specs.append(make_variant_spec(str(recipe["name"]), "", architecture))

    for variant_name, config in (recipe.get("variants") or {}).items():
        configured_architectures = config.get("architectures") or (
            [config["architecture"]]
            if config.get("architecture")
            else [default_architecture]
        )
        for configured_architecture in configured_architectures:
            architecture = normalize_declared_architecture(str(configured_architecture))
            specs.append(
                make_variant_spec(
                    str(recipe["name"]),
                    str(variant_name),
                    architecture,
                    config.get("options"),
                )
            )

    if not specs:
        raise ValueError(f"recipe {recipe['name']} has no enabled builds")
    selectors = [str(spec["variant"]) for spec in specs]
    if len(selectors) != len(set(selectors)):
        raise ValueError(
            f"recipe {recipe['name']} declares duplicate concrete variants"
        )
    return specs


def variant_specs(recipe: dict[str, Any]) -> list[dict[str, str]]:
    """Return the public workflow representation of concrete containers."""
    return [
        {key: str(spec[key]) for key in ("variant", "name", "architecture")}
        for spec in concrete_variant_specs(recipe)
    ]


def select_concrete_variant(
    recipe: dict[str, Any],
    *,
    host_architecture: str,
    host_platform: str,
    architecture: str | None = None,
    variant: str | None = None,
    ignore_architecture: bool = False,
) -> dict[str, Any]:
    """Select a container using explicit selectors and host information."""
    specs = concrete_variant_specs(recipe)
    requested_arch = (
        normalize_declared_architecture(architecture or host_architecture)
        if architecture is not None
        else None
    )
    requested_variant = variant or ""
    selection_arch = requested_arch
    if not requested_variant or requested_variant in (recipe.get("variants") or {}):
        selection_arch = requested_arch or normalize_declared_architecture(
            host_architecture
        )
    candidates = [spec for spec in specs if spec["variant"] == requested_variant]
    if not requested_variant:
        candidates = [
            spec
            for spec in specs
            if not spec["recipe_variant"] and spec["architecture"] == selection_arch
        ]
    elif requested_variant in (recipe.get("variants") or {}):
        candidates = [
            spec
            for spec in specs
            if spec["recipe_variant"] == requested_variant
            and spec["architecture"] == selection_arch
        ]
    if requested_arch is not None:
        candidates = [
            spec for spec in candidates if spec["architecture"] == requested_arch
        ]
    if (
        not candidates
        and requested_arch is None
        and not ignore_architecture
        and selection_arch == "aarch64"
        and host_platform == "Darwin"
    ):
        variants = recipe.get("variants") or {}
        candidates = [
            spec
            for spec in specs
            if spec["architecture"] == "x86_64"
            and (
                (not requested_variant and not spec["recipe_variant"])
                or (
                    requested_variant in variants
                    and spec["recipe_variant"] == requested_variant
                )
            )
        ]
        if candidates:
            selection_arch = "x86_64"
            warnings.warn(
                f"{recipe['name']} does not support the host architecture aarch64 "
                "on macOS; automatically selecting x86_64",
                UserWarning,
                stacklevel=2,
            )
    if not candidates and ignore_architecture:
        forced_arch = requested_arch
        if forced_arch is None:
            forced_arch = (
                "aarch64"
                if requested_variant == "arm64" or requested_variant.endswith("_arm64")
                else normalize_declared_architecture(host_architecture)
            )
        candidates = [forced_variant_spec(recipe, requested_variant, forced_arch)]
    if not candidates:
        available = ", ".join(str(spec["variant"]) or "default" for spec in specs)
        raise ValueError(
            f"unknown variant/architecture '{requested_variant or 'default'}'/{selection_arch or 'default'} "
            f"for {recipe['name']}; available: {available}"
        )
    selected_variant = candidates[0]
    arch = str(selected_variant["architecture"])
    allowed = [str(item) for item in recipe.get("architectures", [])]
    if arch not in allowed and not ignore_architecture:
        raise ValueError(f"architecture {arch} not supported by {recipe['name']}")

    return selected_variant
