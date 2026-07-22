from __future__ import annotations

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


def concrete_variant_specs(recipe: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand architectures and alternatives into concrete container identities."""
    architectures = [
        normalize_declared_architecture(str(item))
        for item in recipe.get("architectures", [])
    ]
    if not architectures:
        raise ValueError(f"recipe {recipe.get('name', '<unknown>')} has no architectures")
    default_architecture = "x86_64" if "x86_64" in architectures else architectures[0]

    specs: list[dict[str, Any]] = []
    ordered_architectures = [
        default_architecture,
        *[arch for arch in architectures if arch != default_architecture],
    ]
    for architecture in ordered_architectures:
        architecture_variant = "arm64" if architecture == "aarch64" else ""
        specs.append(
            {
                "variant": architecture_variant,
                "recipe_variant": "",
                "name": (
                    f"{recipe['name']}_{architecture_variant}"
                    if architecture_variant
                    else str(recipe["name"])
                ),
                "architecture": architecture,
                "options": {},
            }
        )

    for variant_name, config in (recipe.get("variants") or {}).items():
        configured_architectures = config.get("architectures") or (
            [config["architecture"]]
            if config.get("architecture")
            else [default_architecture]
        )
        for configured_architecture in configured_architectures:
            architecture = normalize_declared_architecture(str(configured_architecture))
            suffix = (
                f"{variant_name}_arm64"
                if architecture == "aarch64"
                else str(variant_name)
            )
            specs.append(
                {
                    "variant": suffix,
                    "recipe_variant": str(variant_name),
                    "name": f"{recipe['name']}_{suffix}",
                    "architecture": architecture,
                    "options": dict(config.get("options") or {}),
                }
            )

    selectors = [str(spec["variant"]) for spec in specs]
    if len(selectors) != len(set(selectors)):
        raise ValueError(f"recipe {recipe['name']} declares duplicate concrete variants")
    return specs


def variant_specs(recipe: dict[str, Any]) -> list[dict[str, str]]:
    """Return the public workflow representation of concrete containers."""
    return [
        {key: str(spec[key]) for key in ("variant", "name", "architecture")}
        for spec in concrete_variant_specs(recipe)
    ]
