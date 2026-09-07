import copy
from pathlib import Path

import pytest
import yaml

from builder.config import default_config
from builder.dockerfile import render_dockerfile
from builder.openrecon_updates import (
    OPENRECON_PINS,
    OPENRECON_SOURCES,
    uses_openrecon,
    validate_openrecon_policy,
)
from builder.recipe import compile_recipe
from builder.update_observations import SourceObservation
from builder.update_plan import plan_sources


@pytest.fixture
def shared_recipe(tmp_path):
    recipe = {
        "name": "shared-example", "version": "1.0.0", "architectures": ["x86_64"],
        "readme": "Shared acquisition fixture.",
        "categories": ["image reconstruction"], "icon": "data:image/png;base64,aGVsbG8=",
        "build": {"kind": "neurodocker", "base-image": "ubuntu:24.04", "pkg-manager": "apt",
                  "directives": [{"include": "macros/openrecon/neurodocker.yaml"}]},
        "variables": dict(OPENRECON_PINS),
        "auto_update": {"method": "sources", "sources": copy.deepcopy(list(OPENRECON_SOURCES))},
    }
    (tmp_path / "build.yaml").write_text(yaml.safe_dump(recipe, sort_keys=False))
    (tmp_path / "fulltest.yaml").write_text("name: shared-example\nversion: 1.0.0\ntests: []\n")
    return tmp_path, recipe


def test_shared_dependencies_cannot_be_omitted_or_bound_to_another_repo(shared_recipe):
    _, recipe = shared_recipe
    assert uses_openrecon(recipe)
    validate_openrecon_policy(recipe)
    recipe["auto_update"]["sources"].pop()
    with pytest.raises(ValueError, match="openrecon_server_commit"):
        validate_openrecon_policy(recipe)
    recipe["auto_update"]["sources"] = copy.deepcopy(list(OPENRECON_SOURCES))
    recipe["auto_update"]["sources"][0]["repo"] = "unrelated/project"
    with pytest.raises(ValueError, match="requires repo"):
        validate_openrecon_policy(recipe)


def test_nested_group_requires_shared_policy(shared_recipe):
    _, recipe = shared_recipe
    recipe["build"]["directives"] = [{"group": recipe["build"]["directives"]}]
    recipe["auto_update"] = {"method": "manual", "reason": "local example"}
    with pytest.raises(ValueError, match="explicit shared dependency"):
        validate_openrecon_policy(recipe)
    recipe["build"]["directives"] = []
    validate_openrecon_policy(recipe)


@pytest.mark.parametrize("selected", range(4))
def test_shared_observation_changes_generated_install_and_replays(shared_recipe, selected):
    path, recipe = shared_recipe
    config = default_config()
    current = render_dockerfile(compile_recipe(path, architecture="x86_64", include_dirs=config.include_dirs).definition)
    sources = recipe["auto_update"]["sources"]
    observed = {source["id"]: SourceObservation(recipe["variables"][source["target"]["variable"]],
                                               "https://example.com/source", version=recipe["variables"][source["target"]["variable"]])
                for source in sources}
    assert plan_sources(path / "build.yaml", observations=observed) is None
    source = sources[selected]
    value = "2.0.0" if selected == 0 else "a" * 40
    observed[source["id"]] = SourceObservation(value, "https://example.com/next", version=value if selected == 0 else None)
    plan = plan_sources(path / "build.yaml", observations=observed)
    assert plan is not None
    plan.apply()
    generated = render_dockerfile(compile_recipe(path, architecture="x86_64", include_dirs=config.include_dirs).definition)
    assert generated != current
    assert value in generated
    if selected == 0:
        assert "git checkout v2.0.0" in generated
        assert "ismrmrd==2.0.0" in generated
    assert yaml.safe_load((path / "fulltest.yaml").read_text())["version"] == plan.next_version
    assert plan_sources(path / "build.yaml", observations=observed) is None


def test_macro_fallback_matches_migration_baseline(shared_recipe):
    path, recipe = shared_recipe
    del recipe["variables"]
    (path / "build.yaml").write_text(yaml.safe_dump(recipe))
    generated = render_dockerfile(compile_recipe(path, architecture="x86_64", include_dirs=default_config().include_dirs).definition)
    assert all(value in generated for value in OPENRECON_PINS.values())
