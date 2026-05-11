from __future__ import annotations

from build3.config import default_config, resolve_recipe
from build3.recipe import compile_recipe
from build3.release import release_data


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
