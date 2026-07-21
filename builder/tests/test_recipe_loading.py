from __future__ import annotations

from pathlib import Path

import pytest

from builder.config import default_config, resolve_recipe
from builder.recipe import RecipeFile, compile_recipe, load_recipe, load_recipe_file


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


def test_compile_named_variant_uses_concrete_container_identity() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "workshopdemo"),
        variant="arm64",
        include_dirs=config.include_dirs,
    )

    assert compiled.base_name == "workshopdemo"
    assert compiled.name == "workshopdemo_arm64"
    assert compiled.variant == "arm64"
    assert compiled.architecture == "aarch64"
    assert compiled.tag == "workshopdemo_arm64:1.0.0"


def test_compile_named_variant_rejects_wrong_architecture() -> None:
    config = default_config()
    with pytest.raises(ValueError, match="requires architecture aarch64"):
        compile_recipe(
            resolve_recipe(config, "workshopdemo"),
            variant="arm64",
            architecture="x86_64",
            include_dirs=config.include_dirs,
        )
