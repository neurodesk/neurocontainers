from __future__ import annotations

import json

import pytest
import yaml

from tools.generate_apps_json import generate_apps_json, merge_container_releases


@pytest.mark.parametrize("container,source", [("demo", None), ("demo_gpu_arm64", "demo")])
def test_catalog_refresh_preserves_published_artifacts(tmp_path, container, source) -> None:
    release_dir = tmp_path / "releases" / container
    release_dir.mkdir(parents=True)
    release = {
        "apps": {f"{container} 1.0": {"version": "20260101", "exec": "demo"}},
        "categories": ["programming"],
    }
    if source:
        release.update(recipe=source, variant="gpu_arm64", architecture="aarch64")
    release_path = release_dir / "1.0.json"
    original = json.dumps(release)
    release_path.write_text(original)
    recipe_dir = tmp_path / "recipes" / (source or container)
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "build.yaml").write_text(yaml.safe_dump({"categories": ["workflows"]}))
    output = tmp_path / "apps.json"

    generate_apps_json(str(release_dir.parent), str(output))

    catalog = json.loads(output.read_text())[container]
    assert catalog["categories"] == ["workflows"]
    assert catalog["apps"] == release["apps"]
    assert release_path.read_text() == original


@pytest.mark.parametrize("recipe", [None, {}, {"categories": ["{{ context.category }}"]}])
def test_catalog_retains_release_categories_when_recipe_cannot_supply_them(tmp_path, recipe) -> None:
    release_dir = tmp_path / "releases" / "demo"
    release_dir.mkdir(parents=True)
    (release_dir / "1.0.json").write_text(json.dumps({
        "apps": {"demo 1.0": {"version": "20260101"}},
        "categories": ["programming"],
    }))
    if recipe is not None:
        recipe_dir = tmp_path / "recipes" / "demo"
        recipe_dir.mkdir(parents=True)
        (recipe_dir / "build.yaml").write_text(yaml.safe_dump(recipe))
    output = tmp_path / "apps.json"

    generate_apps_json(str(release_dir.parent), str(output))

    assert json.loads(output.read_text())["demo"]["categories"] == ["programming"]


def test_merge_container_releases_preserves_visibility_flags(tmp_path) -> None:
    release_path = tmp_path / "1.0.0.json"
    release_path.write_text(
        json.dumps(
            {
                "show_in_menu": False,
                "show_in_applist": False,
                "apps": {
                    "tool 1.0.0": {
                        "version": "20260102",
                        "exec": "",
                        "apptainer_args": [],
                    }
                },
                "categories": ["workflows"],
            }
        )
    )

    merged = merge_container_releases("tool", [("1.0.0", str(release_path))])

    assert merged["show_in_menu"] is False
    assert merged["show_in_applist"] is False
    assert merged["apps"]["tool 1.0.0"]["version"] == "20260102"
    assert merged["categories"] == ["workflows"]


def test_generate_apps_json_rejects_duplicate_app_identity(tmp_path) -> None:
    releases_dir = tmp_path / "releases"
    for container, build_date in (
        ("legacy-container", "20240101"),
        ("canonical-container", "20260102"),
    ):
        release_dir = releases_dir / container
        release_dir.mkdir(parents=True)
        (release_dir / "latest.json").write_text(
            json.dumps(
                {
                    "apps": {
                        "rolling-tool latest": {
                            "version": build_date,
                            "exec": "",
                        }
                    },
                    "categories": ["workflows"],
                }
            )
        )

    with pytest.raises(
        ValueError,
        match=(
            "Duplicate app identity 'rolling-tool latest' found in release "
            "containers: canonical-container, legacy-container"
        ),
    ):
        generate_apps_json(str(releases_dir), str(tmp_path / "apps.json"))


def test_merge_includes_named_arm64_variant(tmp_path) -> None:
    release_path = tmp_path / "1.0.0.json"
    release_path.write_text(
        json.dumps(
            {
                "variant": "arm64",
                "architecture": "aarch64",
                "apps": {
                    "tool_arm64 1.0.0": {
                        "version": "20260102",
                        "exec": "",
                        "apptainer_args": [],
                    }
                },
                "categories": ["workflows"],
            }
        )
    )

    merged = merge_container_releases("tool_arm64", [("1.0.0", str(release_path))])

    assert merged["apps"]["tool_arm64 1.0.0"]["version"] == "20260102"
