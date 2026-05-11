from __future__ import annotations

import json

from build3.config import default_config, resolve_recipe
from build3.dockerfile import render_dockerfile
from build3.recipe import compile_recipe
from build3.release import release_data


def test_dcm2niix_release_contract_matches_existing_metadata_keys() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "dcm2niix"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    generated = release_data(compiled.name, compiled.version, compiled.recipe, "20241125")
    existing = json.loads((config.repo_root / "releases/dcm2niix/v1.0.20240202.json").read_text())
    app_name = "dcm2niix v1.0.20240202"
    assert generated["categories"] == existing["categories"]
    assert generated["apps"][app_name]["version"] == existing["apps"][app_name]["version"]
    assert generated["apps"][app_name]["exec"] == existing["apps"][app_name]["exec"]


def test_supported_recipe_contracts_have_deploy_and_embedded_metadata() -> None:
    config = default_config()
    for name in (
        "template",
        "dcm2niix",
        "ants",
        "afni",
        "connectomeworkbench",
        "bidscoin",
        "neurodesktop",
    ):
        compiled = compile_recipe(
            resolve_recipe(config, name),
            architecture="x86_64",
            include_dirs=config.include_dirs,
        )
        dockerfile = render_dockerfile(compiled.definition)
        assert "DEPLOY_" in dockerfile
        assert 'COPY ["README.md", \\\n      "/README.md"]' in dockerfile
        assert 'COPY ["build.yaml", \\\n      "/build.yaml"]' in dockerfile
