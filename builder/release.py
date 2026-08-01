from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
from pathlib import Path
from typing import Any


ARCHITECTURE_ALIASES = {
    "x86_64": "x86_64",
    "AMD64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
    "ARM64": "aarch64",
}


def build_date_for_recipe(repo_root: Path, recipe_dir: Path) -> str:
    if os.environ.get("BUILDDATE"):
        return os.environ["BUILDDATE"]
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ad", "--date=format:%Y%m%d", "--", str(recipe_dir / "build.yaml")],
            cwd=repo_root,
            check=True,
            text=True,
            capture_output=True,
        )
        value = result.stdout.strip()
        if value:
            return value
    except (OSError, subprocess.CalledProcessError):
        pass
    return _dt.datetime.now().strftime("%Y%m%d")


def normalize_architecture(architecture: str | None) -> str:
    return ARCHITECTURE_ALIASES.get(architecture or "x86_64", architecture or "x86_64")


def release_version(version: str, architecture: str | None, variant: str | None = None) -> str:
    if variant:
        return version
    return f"{version}-arm64" if normalize_architecture(architecture) == "aarch64" else version


def release_data(
    name: str,
    version: str,
    recipe: dict[str, Any],
    build_date: str,
    architecture: str | None = None,
    variant: str | None = None,
) -> dict[str, Any]:
    apptainer_args = recipe.get("apptainer_args", [])
    normalized_architecture = normalize_architecture(architecture)
    is_named_variant = bool(variant)
    is_arm64 = normalized_architecture == "aarch64" and not is_named_variant
    app_version = f"{version} arm64" if is_arm64 else version
    image_suffix = "_arm64" if is_arm64 else ""
    image = f"{name}_{version}{image_suffix}"
    app_data: dict[str, Any] = {
        "version": build_date,
        "exec": "",
        "apptainer_args": apptainer_args,
    }
    if is_arm64:
        app_data.update({"architecture": normalized_architecture, "image": image})
    data: dict[str, Any] = {
        "apps": {
            f"{name} {app_version}": app_data
        },
        "categories": recipe.get("categories", ["other"]),
    }
    if is_named_variant:
        data["variant"] = variant
        data["architecture"] = normalized_architecture
    for visibility_field in ("show_in_menu", "show_in_applist"):
        if recipe.get(visibility_field) is not None:
            data[visibility_field] = recipe[visibility_field]
    if is_arm64:
        data["architecture"] = normalized_architecture
    for gui_app in recipe.get("gui_apps", []) or []:
        gui_app_data: dict[str, Any] = {
            "version": build_date,
            "exec": gui_app["exec"],
            "apptainer_args": apptainer_args,
        }
        if is_arm64:
            gui_app_data.update({"architecture": normalized_architecture, "image": image})
        data["apps"][f"{gui_app['name']}-{name} {app_version}"] = gui_app_data
    return data


def write_github_release_outputs(name: str, version: str, data: dict[str, Any]) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        return
    release_json = json.dumps(data, indent=2)
    with Path(github_output).open("a") as handle:
        handle.write(f"container_name={name}\n")
        handle.write(f"container_version={version}\n")
        handle.write(f"release_file_content<<EOF\n{release_json}\nEOF\n")


def write_release_file(repo_root: Path, name: str, version: str, data: dict[str, Any]) -> Path:
    path = repo_root / "releases" / name / f"{version}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    return path
