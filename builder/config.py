from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BuildConfig:
    repo_root: Path
    recipe_roots: tuple[Path, ...]
    include_dirs: tuple[Path, ...]
    output_root: Path


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "recipes").is_dir() and (candidate / "pyproject.toml").is_file():
            return candidate
    raise FileNotFoundError("could not find neurocontainers repository root")


def default_config(start: Path | None = None) -> BuildConfig:
    repo_root = find_repo_root(start)
    return BuildConfig(
        repo_root=repo_root,
        recipe_roots=(repo_root / "recipes",),
        include_dirs=(repo_root,),
        output_root=repo_root / "build",
    )


def resolve_recipe(config: BuildConfig, recipe: str) -> Path:
    path = Path(recipe)
    if path.is_absolute() or "/" in recipe or path.name == "build.yaml":
        candidate = path if path.name != "build.yaml" else path.parent
        if (candidate / "build.yaml").is_file():
            return candidate.resolve()
        raise FileNotFoundError(f"recipe not found: {recipe}")

    for root in config.recipe_roots:
        candidate = root / recipe
        if (candidate / "build.yaml").is_file():
            return candidate.resolve()
    raise FileNotFoundError(f"recipe not found: {recipe}")
