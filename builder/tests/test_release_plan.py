from __future__ import annotations

import attrs
import pytest

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


@pytest.mark.parametrize(
    "updates,reason",
    [
        (
            {"copyright": [{"license": "MIT", "url": "https://example.com/license"}]},
            "source-metadata-only",
        ),
        ({"icon": "data:image/png;base64,aGVsbG8="}, "catalog-only"),
        ({"draft": True}, "source-metadata-only"),
    ],
)
def test_non_image_recipe_metadata_is_source_only(updates, reason) -> None:
    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": recipe()}, {"demo": recipe(**updates)}
    )

    assert plan.candidate_recipes == []
    assert plan.source_only_recipes == ["demo"]
    assert plan.decisions[0].reasons == (reason,)


def test_semantically_unchanged_yaml_is_source_only() -> None:
    data = recipe()

    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": data}, {"demo": dict(data)}
    )

    assert plan.candidate_recipes == []
    assert plan.decisions[0].reasons == ("yaml-only-change",)


@pytest.mark.parametrize("updates", [
    {"readme": "Updated instructions for {{ context.name }}/{{ context.version }}"},
    {"readme_url": "https://example.com/guide"},
    {"structured_readme": {"description": "Updated instructions"}},
    {"categories": ["workflows"]},
    {"readme": "New help", "categories": ["workflows"], "auto_update": {}},
])
def test_documentation_and_categories_preserve_image(updates) -> None:
    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml"], {"demo": recipe()}, {"demo": recipe(**updates)}
    )

    assert plan.candidate_recipes == []
    assert plan.source_only_recipes == ["demo"]


@pytest.mark.parametrize("updates", [
    {"readme": '{{ get_file("asset") }}'},
    {"categories": ["{{ context.category }}"]},
    {"readme": "Help", "version": "2.0"},
    {"readme": "Help", "files": [{"name": "config", "contents": "changed"}]},
    {"readme": "Help", "deploy": {"bins": ["new-command"]}},
])
def test_uncertain_templates_and_build_inputs_still_require_candidate(updates) -> None:
    # Check both directions, including removal of a template with side effects.
    for base, head in ((recipe(), recipe(**updates)), (recipe(**updates), recipe())):
        plan = plan_recipe_changes(
            ["recipes/demo/build.yaml"], {"demo": base}, {"demo": head}
        )
        assert plan.candidate_recipes == ["demo"]


def test_documentation_with_staged_file_change_requires_candidate() -> None:
    plan = plan_recipe_changes(
        ["recipes/demo/build.yaml", "recipes/demo/config.txt"],
        {"demo": recipe(readme="Old help")},
        {"demo": recipe(readme="New help")},
    )

    assert plan.candidate_recipes == ["demo"]


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


def test_shared_macro_rebuilds_all_consumers_without_recipe_edits():
    dependent = recipe(build={"directives": [{"include": "macros/shared/tool.yaml"}]})
    unrelated = recipe()
    data = {"first": dependent, "second": dependent, "other": unrelated}
    plan = plan_recipe_changes(["macros/shared/tool.yaml"], data, data)
    assert plan.candidate_recipes == ["first", "second"]
    assert all(d.reasons == ("shared-build-input-changed",) for d in plan.decisions)


def test_shared_watch_uses_path_boundaries():
    watched = recipe(auto_update={"method": "sources", "local": ["macros/shared"]})
    data = {"demo": watched}
    assert not plan_recipe_changes(["macros/shared-other/code.py"], data, data).decisions
    assert plan_recipe_changes(["macros/shared/code.py"], data, data).candidate_recipes == ["demo"]
