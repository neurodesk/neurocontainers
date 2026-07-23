from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from builder.config import default_config, resolve_recipe
from builder.recipe import RecipeFile, compile_recipe, load_recipe, load_recipe_file


def write_minimal_recipe(recipe_dir: Path, **metadata: object) -> None:
    recipe_dir.mkdir()
    recipe = {
        "name": "readme-test",
        "version": "1.2.3",
        "architectures": ["x86_64"],
        "categories": ["programming"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        **metadata,
    }
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump(recipe, sort_keys=False),
        encoding="utf-8",
    )


def test_loads_existing_recipe() -> None:
    config = default_config()
    recipe_dir = resolve_recipe(config, "dcm2niix")
    recipe = load_recipe(recipe_dir)
    assert recipe["name"] == "dcm2niix"
    assert recipe["version"] == "v1.0.20240202"


def test_loads_typed_recipe_file() -> None:
    config = default_config()
    recipe_file = load_recipe_file(resolve_recipe(config, "dcm2niix"))
    assert isinstance(recipe_file, RecipeFile)
    assert recipe_file.name == "dcm2niix"
    assert recipe_file.build["kind"] == "neurodocker"


def test_compile_records_metadata() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    assert compiled.name == "dcm2niix"
    assert compiled.architecture == "x86_64"
    assert "dcm2niix/v1.0.20240202" in compiled.readme
    assert "downloaded_file" in compiled.staging_plan.files


def test_compile_renders_structured_readme(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        structured_readme={
            "description": "Readme test {{ context.version }} description.",
            "example": "readme-test --version",
            "documentation": "https://example.com/docs",
            "citation": "Example citation.",
        },
    )

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert "## readme-test/1.2.3 ##" in compiled.readme
    assert "Readme test 1.2.3 description." in compiled.readme
    assert "Example:\n```\nreadme-test --version\n```" in compiled.readme
    assert (
        "More documentation can be found here: https://example.com/docs"
        in compiled.readme
    )
    assert "Citation:\n```\nExample citation.\n```" in compiled.readme
    assert "ml readme-test/1.2.3" in compiled.readme


def test_compile_preserves_top_level_readme_precedence(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        readme="Top-level readme for {{ context.version }}.",
        structured_readme={
            "description": "Structured description.",
            "example": "readme-test --help",
        },
    )

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert compiled.readme == "Top-level readme for 1.2.3."


def test_compile_loads_readme_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(
        recipe_dir,
        readme_url="https://example.com/readme-{{ context.version }}.md",
    )
    requested_urls: list[str] = []

    def fake_read_readme_url(url: str) -> str:
        requested_urls.append(url)
        return "Remote readme."

    monkeypatch.setattr("builder.recipe._read_readme_url", fake_read_readme_url)

    compiled = compile_recipe(recipe_dir, architecture="x86_64")

    assert compiled.readme == "Remote readme."
    assert requested_urls == ["https://example.com/readme-1.2.3.md"]


def test_compile_rejects_empty_readme(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "readme-test"
    write_minimal_recipe(recipe_dir, readme="   \n")

    with pytest.raises(ValueError, match="README.*cannot be empty"):
        compile_recipe(recipe_dir, architecture="x86_64")
