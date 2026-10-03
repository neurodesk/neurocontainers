"""Generate recipe labels before a build and copy image labels to SIF annotations."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING

from .image_fingerprint import RegistryClient, parse_image_reference, resolve_credentials, select_platform_manifest
from .ir import From
if TYPE_CHECKING:
    from .recipe import CompiledRecipe


PREFIX = "org.opencontainers.image."


def recipe_labels(compiled: CompiledRecipe, build_date: str, revision: str) -> dict[str, str]:
    created = datetime.strptime(build_date, "%Y%m%d").strftime("%Y-%m-%dT00:00:00Z")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Image revision must be a full Git commit SHA")
    structured = compiled.metadata.get("structured_readme") or {}
    description = str(structured.get("description") or "").strip()
    if not description:
        description = next((line.strip() for line in compiled.readme.splitlines()
                            if line.strip() and not line.lstrip().startswith(("#", "---", "```"))), compiled.name)
    licenses = list(dict.fromkeys(str(item["license"]).strip()
                                 for item in compiled.metadata.get("copyright") or [] if item.get("license")))
    license_expression = " AND ".join(f"({item})" if " " in item else item for item in licenses)
    base = next(directive.image for directive in compiled.definition.directives if isinstance(directive, From))
    values = {
        "title": compiled.name,
        "ref.name": compiled.name,
        "version": f"{compiled.version}_{build_date}",
        "created": created,
        "revision": revision,
        "authors": "mail.neurodesk@gmail.com",
        "vendor": "Neurodesk",
        "source": "https://github.com/neurodesk/neurocontainers",
        "url": "https://neurodesk.org",
        "documentation": f"https://github.com/neurodesk/neurocontainers/tree/{revision}/recipes/{compiled.base_name}",
        "description": " ".join(description.split()),
        "licenses": license_expression or "NOASSERTION",
        "base.name": base,
    }
    return {PREFIX + key: value for key, value in values.items()}


def image_labels(image: str, architecture: str) -> dict[str, str]:
    ref = parse_image_reference(image)
    client = RegistryClient(ref.registry, credentials=resolve_credentials(ref.registry))
    manifest = client.get_manifest(ref.repository, ref.reference)
    if "manifests" in manifest:
        manifest = client.get_manifest(ref.repository, select_platform_manifest(manifest["manifests"], architecture))
    config = client.get_config_blob(ref.repository, manifest["config"]["digest"])
    labels = {key: value for key, value in (config.get("config", {}).get("Labels") or {}).items() if key.startswith(PREFIX)}
    if not all(labels.get(PREFIX + key) for key in ("title", "version", "created", "revision")):
        raise ValueError("Published image is missing recipe identity labels; rebuild it before SIF publication")
    return labels


def emit(labels: dict[str, str], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(labels, indent=2))
    else:
        flag = "--label" if output_format == "labels" else "--annotation"
        for key, value in sorted(labels.items()):
            if "\0" in value:
                raise ValueError("OCI metadata must not contain NUL characters")
            sys.stdout.buffer.write(f"{flag}\0{key}={value}\0".encode())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    recipe = subparsers.add_parser("recipe")
    recipe.add_argument("path", type=Path)
    recipe.add_argument("--architecture", required=True)
    recipe.add_argument("--variant", default="")
    recipe.add_argument("--build-date", required=True)
    recipe.add_argument("--revision", required=True)
    image = subparsers.add_parser("image")
    image.add_argument("reference")
    image.add_argument("--architecture", default="amd64")
    for command in (recipe, image):
        command.add_argument("--format", choices=("json", "labels", "annotations"), default="json")
    args = parser.parse_args()
    if args.command == "recipe":
        from .recipe import compile_recipe

        compiled = compile_recipe(args.path, architecture=args.architecture, variant=args.variant)
        labels = recipe_labels(compiled, args.build_date, args.revision)
    else:
        labels = image_labels(args.reference, args.architecture)
    emit(labels, args.format)


if __name__ == "__main__":
    main()
