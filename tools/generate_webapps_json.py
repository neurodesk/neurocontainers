#!/usr/bin/env python3
"""
Tool to generate webapps.json from container recipe build.yaml files.

This tool scans all recipe directories for build.yaml files that contain
a webapp configuration under deploy.webapp and creates a consolidated
webapps.json file for use by neurodesktop.
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    exit(1)


def load_recipe(recipe_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load a recipe's build.yaml file.

    Args:
        recipe_path: Path to the recipe directory

    Returns:
        Parsed YAML content or None if file doesn't exist/is invalid
    """
    build_yaml = recipe_path / "build.yaml"

    if not build_yaml.exists():
        return None

    try:
        with open(build_yaml, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"  Warning: Error loading {build_yaml}: {e}")
        return None


def extract_webapp_config(recipe: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract webapp configuration from a recipe.

    Args:
        recipe: Parsed recipe YAML content

    Returns:
        Webapp configuration dict or None if not present
    """
    deploy = recipe.get("deploy", {})
    if not deploy:
        return None

    webapp = deploy.get("webapp")
    if not webapp:
        return None

    recipe_name = recipe.get("name")
    recipe_version = recipe.get("version")

    # Build webapp config with defaults from recipe
    webapp_config = webapp.copy()

    # Default module to recipe name if not specified
    if "module" not in webapp_config:
        webapp_config["module"] = recipe_name

    # Add version from recipe (used for module loading: ml module/version)
    webapp_config["version"] = recipe_version

    return webapp_config


def collect_webapp_configs(recipes_dir: Path) -> Dict[str, Dict[str, Any]]:
    """
    Collect all webapp configurations from recipes.

    Args:
        recipes_dir: Path to the recipes directory

    Returns:
        Dict mapping webapp name -> webapp configuration
    """
    webapps = {}

    if not recipes_dir.exists():
        print(f"Warning: Recipes directory {recipes_dir} does not exist")
        return webapps

    for recipe_dir in sorted(recipes_dir.iterdir()):
        if not recipe_dir.is_dir():
            continue

        recipe = load_recipe(recipe_dir)
        if not recipe:
            continue

        webapp_config = extract_webapp_config(recipe)
        if webapp_config:
            module = webapp_config.get("module")
            if module:
                print(f"  Found webapp: {module} (from {recipe_dir.name})")
                webapps[module] = webapp_config
            else:
                print(f"  Warning: Webapp in {recipe_dir.name} missing 'module' field")

    return webapps


def generate_webapps_json(recipes_dir: str, output_file: str):
    """
    Generate webapps.json from all recipe build.yaml files.

    Args:
        recipes_dir: Directory containing recipe subdirectories
        output_file: Path to write the generated webapps.json
    """
    recipes_path = Path(recipes_dir)
    output_path = Path(output_file)

    print(f"Scanning recipes in: {recipes_path}")

    # Collect all webapp configurations
    webapps = collect_webapp_configs(recipes_path)

    if not webapps:
        print("No webapp configurations found!")
        # Still create an empty file for consistency
        webapps_json = {"version": "1.0", "webapps": {}}
    else:
        webapps_json = {
            "version": "1.0",
            "webapps": webapps
        }

    # Write the generated webapps.json
    print(f"\nWriting webapps.json to: {output_path}")

    # Create parent directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(webapps_json, f, indent=2)

    # Print summary
    print(f"\nGenerated webapps.json successfully!")
    print(f"  Webapps found: {len(webapps)}")
    for name, config in webapps.items():
        print(f"    - {name}: {config.get('title', 'No title')} (port {config.get('ports', {}).get('main', '?')})")


def main():
    parser = argparse.ArgumentParser(
        description="Generate webapps.json from container recipe build.yaml files"
    )
    parser.add_argument(
        "--recipes-dir",
        default="recipes",
        help="Directory containing recipe subdirectories (default: recipes)"
    )
    parser.add_argument(
        "--output",
        default="webapps.json",
        help="Output path for generated webapps.json (default: webapps.json)"
    )

    args = parser.parse_args()

    # Resolve paths relative to script location if not absolute
    script_dir = Path(__file__).parent.parent

    recipes_dir = Path(args.recipes_dir)
    if not recipes_dir.is_absolute():
        recipes_dir = script_dir / recipes_dir

    output_file = Path(args.output)
    if not output_file.is_absolute():
        output_file = Path.cwd() / output_file

    generate_webapps_json(str(recipes_dir), str(output_file))

    return 0


if __name__ == "__main__":
    exit(main())
