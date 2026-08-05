from __future__ import annotations

import attrs

from builder.release_plan import TOP_LEVEL_FIELD_TIERS, plan_recipe_changes
from builder.validation import ContainerRecipe


def recipe(**updates: object) -> dict[str, object]:
    data: dict[str, object] = {
        "name": "demo",
        "version": "1.2.3",
        "architectures": ["x86_64"],
        "build": {
            "kind": "neurodocker",
            "base-image": "ubuntu:24.04",
            "pkg-manager": "apt",
            "directives": [],
        },
        "categories": ["programming"],
    }
    data.update(updates)
    return data


def test_every_validated_top_level_field_has_a_release_tier() -> None:
    accepted = {field.name for field in attrs.fields(ContainerRecipe)}

    assert set(TOP_LEVEL_FIELD_TIERS) == accepted


def test_auto_update_only_change_is_source_only() -> None:
    base = recipe()
    head = recipe(
        auto_update={"method": "github_release", "repo": "example/demo"}
    )

    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": base}, {"demo": head}
    )

    assert plan.candidate_recipes == []
    assert plan.source_only_recipes == ["demo"]
    assert plan.decisions[0].reasons == ("auto-update-only",)


def test_semantically_unchanged_yaml_is_source_only() -> None:
    data = recipe()

    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": data}, {"demo": dict(data)}
    )

    assert plan.candidate_recipes == []
    assert plan.decisions[0].reasons == ("yaml-only-change",)


def test_runtime_recipe_field_change_requires_candidate() -> None:
    base = recipe()
    head = recipe(version="1.2.4")

    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": base}, {"demo": head}
    )

    assert plan.candidate_recipes == ["demo"]
    assert plan.decisions[0].reasons == ("recipe-definition-changed",)


def test_recipe_local_helper_change_requires_candidate_without_build_yaml() -> None:
    data = recipe()

    plan = plan_recipe_changes(
        ["recipes/demo/install.sh"], {"demo": data}, {"demo": data}
    )

    assert plan.candidate_recipes == ["demo"]
    assert plan.decisions[0].reasons == ("recipe-file-changed",)


def test_fulltest_only_change_does_not_rebuild_image() -> None:
    data = recipe()

    plan = plan_recipe_changes(
        ["recipes/demo/fulltest.yaml"], {"demo": data}, {"demo": data}
    )

    assert plan.candidate_recipes == []
    assert plan.decisions[0].reasons == ("test-only",)


def test_unclassified_field_fails_closed_to_candidate() -> None:
    base = recipe()
    # A null-valued addition must still count as a field change; comparing
    # Mapping.get() values would incorrectly equate it with an absent key.
    head = recipe(future_runtime_field=None)

    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": base}, {"demo": head}
    )

    assert plan.candidate_recipes == ["demo"]
    assert plan.decisions[0].reasons == ("unclassified-field",)
