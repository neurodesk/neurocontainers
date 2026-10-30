"""Check recipe domain declarations against the catalog repository registry."""

from __future__ import annotations

import argparse
import json
import re
import urllib.request
from pathlib import Path

import yaml


DEFAULT_REGISTRY = (
    "https://raw.githubusercontent.com/scigetorg/apps.json/main/domains.json"
)
DOMAIN_ID = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")


def load_registry(location: str) -> dict:
    if location.startswith("https://"):
        with urllib.request.urlopen(location, timeout=30) as response:
            document = json.load(response)
    else:
        document = json.loads(Path(location).read_text(encoding="utf-8"))
    domains = document.get("domains")
    if not isinstance(domains, dict) or not domains:
        raise ValueError("domains.json needs a non-empty domains object")
    return domains


def check_labels(
    labels: object, location: str, repository: str, registry: dict
) -> None:
    if not isinstance(labels, list) or not labels:
        raise ValueError(f"{location}: domains must be a non-empty list")
    if any(
        not isinstance(label, str) or not DOMAIN_ID.fullmatch(label) for label in labels
    ):
        raise ValueError(f"{location}: invalid domain ID")
    if len(set(labels)) != len(labels):
        raise ValueError(f"{location}: duplicate domain ID")
    for label in labels:
        config = registry.get(label)
        if config is None:
            raise ValueError(f"{location}: unknown domain {label}")
        sources = config.get("source_repositories")
        if not isinstance(sources, list) or repository.casefold() not in {
            source.casefold() for source in sources if isinstance(source, str)
        }:
            raise ValueError(
                f"{location}: {repository} is not in {label}.source_repositories"
            )


def validate(root: Path, repository: str, registry: dict) -> int:
    count = 0
    for path in sorted((root / "recipes").glob("*/build.yaml")):
        recipe = yaml.safe_load(path.read_text(encoding="utf-8"))
        check_labels(recipe.get("domains"), str(path), repository, registry)
        count += 1
    manifest = root / "workflows/retired_recipes.yaml"
    if manifest.is_file():
        entries = yaml.safe_load(manifest.read_text(encoding="utf-8")).get(
            "retired", []
        )
        for entry in entries:
            check_labels(
                entry.get("domains"),
                f"{manifest}:{entry.get('recipe')}",
                repository,
                registry,
            )
            count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--repository", required=True)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    args = parser.parse_args()
    count = validate(args.repo_root, args.repository, load_registry(args.registry))
    print(f"Validated domains for {count} recipes and retirements")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
