#!/usr/bin/env python3
"""
Tool to generate apps.json from individual release files.

This tool reads all release files from the releases/ directory and creates
a consolidated apps.json file that matches the original format.
"""

import json
import os
import argparse
from pathlib import Path
from typing import Dict, Any


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
            if file_name.endswith('.json'):
                version = file_name[:-5]  # Remove .json extension
                file_path = os.path.join(container_path, file_name)
                containers[container_dir].append((version, file_path))
        
        # Sort by version for consistent ordering
        containers[container_dir].sort(key=lambda x: x[0])
    
    return containers


def load_release_file(file_path: str) -> Dict[str, Any]:
    """Load a single release file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {"apps": {}, "categories": []}


def merge_container_releases(container_name: str, release_files: list) -> Dict[str, Any]:
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
    
    for version, file_path in release_files:
        print(f"  Processing {container_name} {version}")
        
        release_data = load_release_file(file_path)
        
        # Merge apps
        apps = release_data.get('apps', {})
        for app_name, app_data in apps.items():
            merged_apps[app_name] = app_data
        
        # Merge categories
        categories = release_data.get('categories', [])
        merged_categories.update(categories)
    
    return {
        "apps": merged_apps,
        "categories": sorted(list(merged_categories))
    }


def generate_apps_json(releases_dir: str, output_file: str):
    """
    Generate apps.json from all release files.
    
    Args:
        releases_dir: Directory containing release files
        output_file: Path to write the generated apps.json
    """
    print(f"Collecting release files from: {releases_dir}")
    
    # Collect all release files
    containers = collect_release_files(releases_dir)
    
    if not containers:
        print("No release files found!")
        return
    
    print(f"Found {len(containers)} containers")
    
    # Generate consolidated apps.json
    apps_json = {}
    
    for container_name in sorted(containers.keys()):
        print(f"Processing container: {container_name}")
        release_files = containers[container_name]
        
        if not release_files:
            print(f"  Warning: No release files for {container_name}")
            continue
        
        print(f"  Found {len(release_files)} releases")
        
        # Merge all releases for this container
        container_data = merge_container_releases(container_name, release_files)
        apps_json[container_name] = container_data
    
    # Write the generated apps.json
    print(f"Writing apps.json to: {output_file}")
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(apps_json, f, indent=4)
    
    # Print summary
    total_apps = sum(len(container_data["apps"]) for container_data in apps_json.values())
    print(f"Generated apps.json successfully!")
    print(f"  Containers: {len(apps_json)}")
    print(f"  Total apps: {total_apps}")


def main():
    parser = argparse.ArgumentParser(description="Generate apps.json from release files")
    parser.add_argument(
        "--releases-dir",
        default="releases",
        help="Directory containing release files"
    )
    parser.add_argument(
        "--output",
        default="apps.json",
        help="Output path for generated apps.json"
    )
    
    args = parser.parse_args()
    
    # Resolve paths
    releases_dir = os.path.abspath(args.releases_dir)
    output_file = os.path.abspath(args.output)
    
    generate_apps_json(releases_dir, output_file)
    
    return 0


if __name__ == "__main__":
    exit(main())