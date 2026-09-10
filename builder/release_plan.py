from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Mapping


# Every top-level field accepted by ContainerRecipe must be assigned a role here.
# The focused completeness test makes schema growth an explicit release-policy
# decision instead of silently treating a new field as metadata-only.
TOP_LEVEL_FIELD_TIERS: dict[str, frozenset[str]] = {
    "name": frozenset({"candidate"}),
    "version": frozenset({"candidate"}),
    "architectures": frozenset({"candidate"}),
    "build": frozenset({"candidate"}),
    "variants": frozenset({"candidate"}),
    "auto_update": frozenset({"source_only"}),
    "icon": frozenset({"catalog"}),
    "copyright": frozenset({"source_only"}),
    "readme": frozenset({"source_only"}),
    "readme_url": frozenset({"source_only"}),
    "structured_readme": frozenset({"source_only"}),
    "files": frozenset({"candidate"}),
    "deploy": frozenset({"candidate", "catalog"}),
    "tests": frozenset({"candidate"}),
    "categories": frozenset({"metadata"}),
    "show_in_menu": frozenset({"metadata"}),
    "show_in_applist": frozenset({"metadata"}),
    "gui_apps": frozenset({"metadata"}),
    "apptainer_args": frozenset({"metadata"}),
    "draft": frozenset({"source_only"}),
    "description": frozenset({"source_only"}),
    "options": frozenset({"candidate"}),
    "variables": frozenset({"candidate"}),
    "epoch": frozenset({"candidate"}),
}

# Fields outside these proven non-image groups remain candidate-required.
ENABLED_SOURCE_ONLY_FIELDS = frozenset({"auto_update", "copyright", "draft"})
ENABLED_CATALOG_FIELDS = frozenset({"icon"})
DOCUMENTATION_FIELDS = frozenset({"readme", "readme_url", "structured_readme"})
KNOWN_NON_IMAGE_FILES = frozenset({"fulltest.yaml"})


def literal_categories(recipe: Mapping[str, object]) -> list[str] | None:
    """Return categories that the catalog can publish without executing Jinja."""
    value = recipe.get("categories", [])
    if not isinstance(value, list) or not all(
        isinstance(item, str)
        and not any(token in item for token in ("{{", "{%", "{#"))
        for item in value
    ):
        return None
    return value


def _passive_documentation(value: object) -> bool:
    """Allow prose and simple context substitutions, not template side effects."""
    if isinstance(value, str):
        prose = re.sub(
            r"{{\s*(?:context\.(?:name|version|original_version|arch|variant)|arch)\s*}}",
            "",
            value,
        )
        return not any(token in prose for token in ("{{", "{%", "{#"))
    if isinstance(value, Mapping):
        return all(
            _passive_documentation(key) and _passive_documentation(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return all(_passive_documentation(item) for item in value)
    return True


@dataclass(frozen=True)
class RecipeDecision:
    recipe: str
    action: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ReleasePlan:
    decisions: tuple[RecipeDecision, ...]

    @property
    def changed_recipes(self) -> list[str]:
        return [decision.recipe for decision in self.decisions]

    @property
    def candidate_recipes(self) -> list[str]:
        return [
            decision.recipe
            for decision in self.decisions
            if decision.action == "candidate"
        ]

    @property
    def source_only_recipes(self) -> list[str]:
        return [
            decision.recipe
            for decision in self.decisions
            if decision.action == "source_only"
        ]

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "changed_recipes": self.changed_recipes,
            "candidate_recipes": self.candidate_recipes,
            "source_only_recipes": self.source_only_recipes,
            "decisions": [
                {
                    "recipe": decision.recipe,
                    "action": decision.action,
                    "reasons": list(decision.reasons),
                }
                for decision in self.decisions
            ],
        }


def recipe_names_from_paths(paths: list[str]) -> list[str]:
    recipes = {
        parts[1]
        for path in paths
        if len(parts := PurePosixPath(path).parts) >= 3
        and parts[0] == "recipes"
    }
    return sorted(recipes)


def shared_recipe_paths(recipe: Mapping[str, object] | None) -> tuple[str, ...]:
    """Return declared shared build inputs without evaluating recipe templates."""
    if recipe is None:
        return ()
    paths: set[str] = set()
    policy = recipe.get("auto_update")
    if isinstance(policy, dict):
        paths.update(policy.get("local", []))

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("include"), str):
                path = node["include"]
                paths.add(path if path.startswith("macros/") else "macros/" + path)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(recipe.get("build", {}))
    for path in paths:
        if not isinstance(path, str) or not path.startswith("macros/") or ".." in PurePosixPath(path).parts:
            raise ValueError("shared recipe inputs must be paths under macros/")
    return tuple(sorted(paths))


def path_is_shared_input(path: str, recipe: Mapping[str, object] | None) -> bool:
    return any(path == shared or path.startswith(shared.rstrip("/") + "/") for shared in shared_recipe_paths(recipe))


def _changed_top_level_fields(
    base: Mapping[str, object], head: Mapping[str, object]
) -> set[str]:
    return {
        key
        for key in set(base) | set(head)
        if key not in base or key not in head or base[key] != head[key]
    }


def plan_recipe_changes(
    changed_paths: list[str],
    base_recipes: Mapping[str, Mapping[str, object] | None],
    head_recipes: Mapping[str, Mapping[str, object] | None],
) -> ReleasePlan:
    """Classify recipe changes without rendering or executing head-authored data.

    Projection equality here means behavioural equivalence for release policy,
    not byte equality of a hypothetical rebuilt image. The current image embeds
    raw build.yaml and README.md; a source-only verdict deliberately preserves
    the already-published artifact instead of rebuilding those provenance files.
    """
    decisions: list[RecipeDecision] = []
    affected = set(recipe_names_from_paths(changed_paths))
    affected.update(
        name for name, recipe in head_recipes.items()
        if any(path_is_shared_input(path, recipe) for path in changed_paths)
    )
    for recipe in sorted(affected):
        recipe_prefix = f"recipes/{recipe}/"
        relative_paths = {
            path.removeprefix(recipe_prefix)
            for path in changed_paths
            if path.startswith(recipe_prefix)
        }
        base = base_recipes.get(recipe)
        head = head_recipes.get(recipe)
        if head is None:
            raise ValueError(
                f"Recipe removal or a missing recipes/{recipe}/build.yaml "
                "requires an explicit migration"
            )

        candidate_reasons: list[str] = []
        if any(path_is_shared_input(path, head) for path in changed_paths):
            candidate_reasons.append("shared-build-input-changed")
        source_reasons: list[str] = []
        build_yaml_changed = "build.yaml" in relative_paths
        other_paths = relative_paths - {"build.yaml"} - KNOWN_NON_IMAGE_FILES

        if other_paths:
            candidate_reasons.append("recipe-file-changed")

        if build_yaml_changed:
            if base is None:
                candidate_reasons.append("new-recipe")
            else:
                changed_fields = _changed_top_level_fields(base, head)
                non_image_fields = set(
                    ENABLED_SOURCE_ONLY_FIELDS | ENABLED_CATALOG_FIELDS
                )
                for field in DOCUMENTATION_FIELDS:
                    if all(
                        _passive_documentation(data.get(field))
                        for data in (base, head)
                    ):
                        non_image_fields.add(field)
                if all(literal_categories(data) is not None for data in (base, head)):
                    non_image_fields.add("categories")
                unknown = changed_fields - TOP_LEVEL_FIELD_TIERS.keys()
                if unknown:
                    candidate_reasons.append("unclassified-field")
                elif not changed_fields:
                    source_reasons.append("yaml-only-change")
                elif changed_fields == {"auto_update"}:
                    source_reasons.append("auto-update-only")
                elif changed_fields <= ENABLED_SOURCE_ONLY_FIELDS:
                    source_reasons.append("source-metadata-only")
                elif changed_fields <= ENABLED_CATALOG_FIELDS:
                    source_reasons.append("catalog-only")
                elif changed_fields <= non_image_fields:
                    source_reasons.append("documentation-or-catalog-only")
                else:
                    candidate_reasons.append("recipe-definition-changed")

        if candidate_reasons:
            decisions.append(
                RecipeDecision(recipe, "candidate", tuple(sorted(candidate_reasons)))
            )
        else:
            if relative_paths <= KNOWN_NON_IMAGE_FILES:
                source_reasons.append("test-only")
            decisions.append(
                RecipeDecision(
                    recipe,
                    "source_only",
                    tuple(sorted(set(source_reasons) or {"non-image-change"})),
                )
            )

    return ReleasePlan(tuple(decisions))
