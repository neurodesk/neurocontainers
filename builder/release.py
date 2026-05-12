from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
from pathlib import Path
from typing import Any


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


def release_data(name: str, version: str, recipe: dict[str, Any], build_date: str) -> dict[str, Any]:
    apptainer_args = recipe.get("apptainer_args", [])
    data: dict[str, Any] = {
        "apps": {
            f"{name} {version}": {
                "version": build_date,
                "exec": "",
                "apptainer_args": apptainer_args,
            }
        },
        "categories": recipe.get("categories", ["other"]),
    }
    for gui_app in recipe.get("gui_apps", []) or []:
        data["apps"][f"{gui_app['name']}-{name} {version}"] = {
            "version": build_date,
            "exec": gui_app["exec"],
            "apptainer_args": apptainer_args,
        }
    return data


def write_release_file(repo_root: Path, name: str, version: str, data: dict[str, Any]) -> Path:
    path = repo_root / "releases" / name / f"{version}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    return path
