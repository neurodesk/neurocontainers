"""Select concrete recipe builds for read-only container verification."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from builder.variants import concrete_variant_specs, normalize_declared_architecture


def build_targets(root: Path, requested: str, architecture: str, debug: bool) -> list[dict[str, str]]:
    available = {path.parent.name: path for path in (root / "recipes").glob("*/build.yaml")}
    names = list(dict.fromkeys(name.strip() for name in requested.split(",") if name.strip()))
    if not names:
        names = ["niimath"] if debug else sorted(available)
    missing = sorted(set(names) - available.keys())
    if missing:
        raise ValueError(f"Unknown recipes: {', '.join(missing)}")
    selected_arch = None if architecture == "all" else normalize_declared_architecture(architecture)
    targets = []
    for name in names:
        recipe = yaml.safe_load(available[name].read_text())
        for spec in concrete_variant_specs(recipe):
            if selected_arch is not None and spec["architecture"] != selected_arch:
                continue
            targets.append({
                "recipe": name,
                "container": spec["name"],
                "variant": spec["variant"],
                "architecture": spec["architecture"],
                "version": str(recipe["version"]),
            })
    if not targets:
        raise ValueError("No selected recipe supports the requested architecture")
    if len(targets) > 256:
        raise ValueError("More than 256 builds selected; dispatch each architecture separately")
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recipes", default="")
    parser.add_argument("--architecture", default="x86_64")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    print(json.dumps(build_targets(args.root, args.recipes, args.architecture, args.debug)))


if __name__ == "__main__":
    main()
