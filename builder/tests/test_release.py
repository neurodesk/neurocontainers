from __future__ import annotations

from builder.config import default_config, resolve_recipe
from builder.recipe import compile_recipe
from builder.release import release_data, release_version


def test_release_shape_matches_current_contract() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    data = release_data(compiled.name, compiled.version, compiled.recipe, "20260102")
    assert data["categories"] == ["data organisation"]
    assert data["apps"]["dcm2niix v1.0.20240202"]["version"] == "20260102"
    assert data["apps"]["dcm2niix v1.0.20240202"]["exec"] == ""


def test_arm64_release_shape_matches_current_contract() -> None:
    data = release_data(
        "tool",
        "1.2.3",
        {"categories": ["workflows"], "apptainer_args": ["--cleanenv"]},
        "20260102",
        "aarch64",
    )
    assert release_version("1.2.3", "aarch64") == "1.2.3-arm64"
    assert data["architecture"] == "aarch64"
    assert data["apps"]["tool 1.2.3 arm64"]["architecture"] == "aarch64"
    assert data["apps"]["tool 1.2.3 arm64"]["image"] == "tool_1.2.3_arm64"
