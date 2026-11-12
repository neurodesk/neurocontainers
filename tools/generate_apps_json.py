#!/usr/bin/env python3
"""
Tool to generate apps.json from individual release files.

This tool reads all release files from the releases/ directory and creates
a consolidated apps.json file that matches the original format.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Any

import yaml

SCRIPT_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_REPO_ROOT))

from builder.recipe import CompiledRecipe, compile_recipe
from builder.release_plan import literal_categories


VISIBILITY_FIELDS = ("show_in_menu", "show_in_applist")


def collect_release_files(releases_dir: str) -> Dict[str, list]:
    """
    Collect all release files organized by container.

    Returns: Dict mapping container_name -> list of (version, file_path)
    """
    containers = {}

    if not os.path.exists(releases_dir):
        print(f"Warning: Releases directory {releases_dir} does not exist")
        return containers

    for container_dir in os.listdir(releases_dir):
        container_path = os.path.join(releases_dir, container_dir)

        if not os.path.isdir(container_path):
            continue

        containers[container_dir] = []

        for file_name in os.listdir(container_path):
            if file_name.endswith(".json"):
                version = file_name[:-5]  # Remove .json extension
                file_path = os.path.join(container_path, file_name)
                containers[container_dir].append((version, file_path))

        # Sort by version for consistent ordering
        containers[container_dir].sort(key=lambda x: x[0])

    return containers


def load_release_file(file_path: str) -> Dict[str, Any]:
    """Load a single release file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {"apps": {}, "categories": []}


def is_legacy_arm64_release(release_data: Dict[str, Any]) -> bool:
    """Return whether this uses the old version-suffixed arm64 contract."""
    if release_data.get("variant"):
        return False
    architecture = release_data.get("architecture")
    if architecture in {"aarch64", "arm64"}:
        return True

    apps = release_data.get("apps", {}) or {}
    return any(
        app_data.get("architecture") in {"aarch64", "arm64"}
        for app_data in apps.values()
        if isinstance(app_data, dict)
    )


def merge_container_releases(
    container_name: str, release_files: list
) -> Dict[str, Any]:
    """
    Merge all release files for a container into a single entry.

    Args:
        container_name: Name of the container
        release_files: List of (version, file_path) tuples

    Returns:
        Container data in apps.json format
    """
    merged_apps = {}
    merged_categories = set()
    merged_visibility: Dict[str, Any] = {}

    for version, file_path in release_files:
        print(f"  Processing {container_name} {version}")

        release_data = load_release_file(file_path)
        if is_legacy_arm64_release(release_data):
            print(
                f"    Skipping {container_name} {version}: legacy arm64 releases are not included in apps.json"
            )
            continue

        # Merge apps
        apps = release_data.get("apps", {})
        for app_name, app_data in apps.items():
            merged_apps[app_name] = app_data

        # Merge categories
        categories = release_data.get("categories", [])
        merged_categories.update(categories)

        for field in VISIBILITY_FIELDS:
            if field not in release_data:
                continue
            value = release_data[field]
            if value is None:
                continue
            if value is False or field not in merged_visibility:
                merged_visibility[field] = value

    container_data = {
        "apps": merged_apps,
        "categories": sorted(list(merged_categories)),
    }
    return {**merged_visibility, **container_data}


def source_recipe(
    container_name: str, release_files: list, recipes_dir: Path
) -> tuple[Path, dict, dict] | None:
    """Return the recipe directory, parsed build.yaml and newest release data.

    Named variants record their source recipe in release metadata. Never infer
    it by splitting container names, which may themselves contain underscores.
    """
    releases = [
        data
        for _, path in release_files
        if not is_legacy_arm64_release(data := load_release_file(path))
    ]
    sources = {data.get("recipe", container_name) for data in releases}
    if len(sources) != 1:
        return None
    source = sources.pop()
    if (
        not isinstance(source, str)
        or source in {".", ".."}
        or Path(source).name != source
    ):
        return None
    path = recipes_dir / source / "build.yaml"
    if not path.is_file():
        return None
    try:
        recipe = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        print(f"Warning: ignoring recipe metadata for {container_name}: {error}")
        return None
    if not isinstance(recipe, dict):
        return None
    return path.parent, recipe, releases[-1]


def compile_for_release(
    container_name: str, recipe_dir: Path, release: dict
) -> CompiledRecipe | None:
    """Render the recipe for the variant and architecture the release names."""
    try:
        return compile_recipe(
            recipe_dir,
            architecture=release.get("architecture"),
            variant=release.get("variant"),
            ignore_architecture=True,
            resolve_readme_url=False,
        )
    except Exception as error:
        print(f"Warning: no citation or description for {container_name}: {error}")
        return None


def recipe_text_fields(compiled: CompiledRecipe, recipe: dict) -> dict[str, str]:
    """Return the non-empty citation and description for a catalog entry.

    Each prefers structured_readme and falls back to the readme: README.md
    when the recipe has one, else the rendered build.yaml readme.
    """
    readme_path = compiled.recipe_dir / "README.md"
    readme = (
        readme_path.read_text(encoding="utf-8")
        if readme_path.is_file()
        else compiled.readme
    )
    structured = recipe.get("structured_readme")
    citation = structured.get("citation") if isinstance(structured, dict) else None
    description = compiled.metadata["structured_readme"].get("description")
    fields = {
        "citation": citation
        if isinstance(citation, str) and citation.strip()
        else parse_readme_citation(readme),
        "description": description
        if isinstance(description, str) and description.strip()
        else parse_readme_description(readme),
    }
    cleaned = {
        field: LINE_SEPARATORS.sub(" ", value).strip()
        for field, value in fields.items()
        if value
    }
    return {field: value for field, value in cleaned.items() if value}


LINE_SEPARATORS = re.compile(r"[ \t]*[\u2028\u2029]+[ \t]*")
CITATION_HEADER = re.compile(
    r"^(?:#{1,6}\s*)?\**Citations?(?:\s*\([^)]*\))?\**\s*:\**\s*(.*)$"
    r"|^#{1,6}\s*Citations?\s*$",
    re.IGNORECASE,
)
SETEXT_CITATION_HEADER = re.compile(r"^Citations?$", re.IGNORECASE)
SETEXT_UNDERLINE = re.compile(r"^(-+|=+)$")
CITATION_STOP = re.compile(
    r"^[*_]*(#|---|License|Licensing|To run|Homepage|Documentation|"
    r"More documentation|Example|Usage)",
    re.IGNORECASE,
)
FENCE = re.compile(r"^(\\*`){3}")
README_TITLE = re.compile(r"^##\s+.+##\s*$")
DESCRIPTION_STOP = re.compile(
    r"^(##\s|```|Example|---+$|More documentation|To setup|To run|License|"
    r"Licensing|Usage)",
    re.IGNORECASE,
)


def find_citation_header(lines: list[str]) -> tuple[int, str] | None:
    """Return the index of the first citation body line and any inline text."""
    for i, line in enumerate(lines):
        stripped = line.strip()
        match = CITATION_HEADER.match(stripped)
        if match:
            return i + 1, match.group(1) or ""
        next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
        if SETEXT_CITATION_HEADER.match(stripped) and SETEXT_UNDERLINE.match(next_line):
            return i + 2, ""
    return None


def parse_readme_citation(readme_text: str) -> str | None:
    """Extract the Citation section from a neurocontainers readme.

    The citation is the text after the header, usually wrapped in a code
    fence, up to the next section (License:, ---, a heading, ...).
    """
    lines = readme_text.splitlines()
    header = find_citation_header(lines)
    if header is None:
        return None
    start, inline = header

    collected = [inline]
    in_fence = False
    for line in lines[start:]:
        stripped = line.strip()
        if FENCE.match(stripped):
            in_fence = not in_fence
            continue
        if not in_fence and CITATION_STOP.match(stripped):
            break
        collected.append(stripped)

    text = re.sub(r"\n{3,}", "\n\n", "\n".join(collected)).strip()
    return text or None


def parse_readme_description(readme_text: str) -> str | None:
    """Extract the paragraph after the ``## name/version ##`` readme title."""
    lines = readme_text.splitlines()
    start = next(
        (i + 1 for i, line in enumerate(lines) if README_TITLE.match(line.strip())),
        None,
    )
    if start is None:
        return None

    description_lines: list[str] = []
    for line in lines[start:]:
        stripped = line.strip()
        if DESCRIPTION_STOP.match(stripped) or CITATION_HEADER.match(stripped):
            break
        description_lines.append(stripped)

    text = re.sub(r"\s+", " ", "\n".join(description_lines)).strip()
    return text or None


def generate_apps_json(
    releases_dir: str, output_file: str, recipes_dir: str | None = None
) -> None:
    """
    Generate apps.json from all release files.

    Args:
        releases_dir: Directory containing release files
        output_file: Path to write the generated apps.json
        recipes_dir: Current recipes; defaults to a sibling of releases_dir
    """
    print(f"Collecting release files from: {releases_dir}")

    # Collect all release files
    containers = collect_release_files(releases_dir)
    recipe_root = (
        Path(recipes_dir)
        if recipes_dir is not None
        else Path(releases_dir).parent / "recipes"
    )

    if not containers:
        print("No release files found!")
        return

    print(f"Found {len(containers)} containers")

    # Generate consolidated apps.json
    apps_json = {}
    app_owners: Dict[str, str] = {}

    for container_name in sorted(containers.keys()):
        print(f"Processing container: {container_name}")
        release_files = containers[container_name]

        if not release_files:
            print(f"  Warning: No release files for {container_name}")
            continue

        print(f"  Found {len(release_files)} releases")

        # Merge all releases for this container
        container_data = merge_container_releases(container_name, release_files)
        recipe_source = source_recipe(container_name, release_files, recipe_root)
        if recipe_source is not None:
            recipe_dir, recipe, release = recipe_source
            categories = literal_categories(recipe) if "categories" in recipe else None
            if categories is not None:
                container_data["categories"] = sorted(set(categories))
            compiled = compile_for_release(container_name, recipe_dir, release)
            if compiled is not None:
                container_data.update(recipe_text_fields(compiled, recipe))
        for app_name in container_data["apps"]:
            existing_owner = app_owners.get(app_name)
            if existing_owner is not None:
                owners = ", ".join(sorted((existing_owner, container_name)))
                raise ValueError(
                    f"Duplicate app identity '{app_name}' found in release "
                    f"containers: {owners}"
                )
            app_owners[app_name] = container_name
        apps_json[container_name] = container_data

    # Write the generated apps.json
    print(f"Writing apps.json to: {output_file}")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(apps_json, f, indent=4)

    # Print summary
    total_apps = sum(
        len(container_data["apps"]) for container_data in apps_json.values()
    )
    print("Generated apps.json successfully!")
    print(f"  Containers: {len(apps_json)}")
    print(f"  Total apps: {total_apps}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate apps.json from release files"
    )
    parser.add_argument(
        "--releases-dir", default="releases", help="Directory containing release files"
    )
    parser.add_argument(
        "--recipes-dir",
        help=(
            "Current recipes for catalog categories, citations and descriptions "
            "(defaults to sibling of releases)"
        ),
    )
    parser.add_argument(
        "--output", default="apps.json", help="Output path for generated apps.json"
    )

    args = parser.parse_args()

    # Resolve paths
    releases_dir = os.path.abspath(args.releases_dir)
    output_file = os.path.abspath(args.output)

    generate_apps_json(releases_dir, output_file, args.recipes_dir)

    return 0


if __name__ == "__main__":
    exit(main())
