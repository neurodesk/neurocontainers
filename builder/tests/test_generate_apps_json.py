from __future__ import annotations

import json

import pytest
import yaml

from tools.generate_apps_json import generate_apps_json, merge_container_releases


@pytest.mark.parametrize(
    "container,source", [("demo", None), ("demo_gpu_arm64", "demo")]
)
def test_catalog_refresh_preserves_published_artifacts(
    tmp_path, container, source
) -> None:
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
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump({"categories": ["workflows"]})
    )
    output = tmp_path / "apps.json"

    generate_apps_json(str(release_dir.parent), str(output))

    catalog = json.loads(output.read_text())[container]
    assert catalog["categories"] == ["workflows"]
    assert catalog["apps"] == release["apps"]
    assert release_path.read_text() == original


@pytest.mark.parametrize(
    "recipe", [None, {}, {"categories": ["{{ context.category }}"]}]
)
def test_catalog_retains_release_categories_when_recipe_cannot_supply_them(
    tmp_path, recipe
) -> None:
    release_dir = tmp_path / "releases" / "demo"
    release_dir.mkdir(parents=True)
    (release_dir / "1.0.json").write_text(
        json.dumps(
            {
                "apps": {"demo 1.0": {"version": "20260101"}},
                "categories": ["programming"],
            }
        )
    )
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


def write_catalog_source(
    tmp_path, recipe_text: str, *, container: str = "demo", release: dict | None = None
) -> None:
    release_dir = tmp_path / "releases" / container
    release_dir.mkdir(parents=True)
    (release_dir / "1.0.0.json").write_text(
        json.dumps(
            {
                "apps": {f"{container} 1.0.0": {"version": "20260101"}},
                "categories": ["programming"],
                **(release or {}),
            }
        )
    )
    recipe_dir = tmp_path / "recipes" / "demo"
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "build.yaml").write_text(
        """\
name: demo
version: 1.0.0
architectures: [x86_64, aarch64]
build:
  kind: neurodocker
  base-image: ubuntu:24.04
  pkg-manager: apt
  directives: []
categories: [programming]
domains: [neuroimaging]
icon: "data:image/png;base64,iVBORw0KGgo="
"""
        + recipe_text
    )


def catalog_entry(tmp_path, container: str = "demo") -> dict:
    output = tmp_path / "apps.json"
    generate_apps_json(str(tmp_path / "releases"), str(output))
    return json.loads(output.read_text())[container]


@pytest.mark.parametrize(
    "container,release",
    [("demo", {}), ("demo_arm64", {"recipe": "demo", "variant": "arm64"})],
)
def test_catalog_carries_structured_readme_over_readme(
    tmp_path, container, release
) -> None:
    write_catalog_source(
        tmp_path,
        """\
structured_readme:
  example: demo --help
  description: "Demo {{ context.version }}\\u2028 converts data."
  citation: "Doe J. Demo. 2020.\\nRoe R.\\u2028 Demo again. 2021."
readme: |
  ## demo/{{ context.version }} ##
  Readme description.

  Citation:
  ```
  Readme citation.
  ```
""",
        container=container,
        release=release,
    )

    entry = catalog_entry(tmp_path, container)

    assert entry["citation"] == "Doe J. Demo. 2020.\nRoe R. Demo again. 2021."
    assert entry["description"] == "Demo 1.0.0 converts data."


CITATION = "Avants BB, et al. Neuroimage. 2011.\ndoi: 10.1016/j.neuroimage.2010.09.025."


@pytest.mark.parametrize(
    "readme",
    [
        "  Citation:\n  ```\n  Avants BB, et al. Neuroimage. 2011.\n"
        "  doi: 10.1016/j.neuroimage.2010.09.025.\n  ```\n\n"
        "  License: Apache 2 License\n",
        "  Citations:\n  Avants BB, et al. Neuroimage. 2011.\n"
        "  doi: 10.1016/j.neuroimage.2010.09.025.\n\n  To run: ml demo\n",
        "  **Citation:** Avants BB, et al. Neuroimage. 2011.\n"
        "  doi: 10.1016/j.neuroimage.2010.09.025.\n  ---\n",
        "  ### Citation\n  \\`\\`\\`\n  Avants BB, et al. Neuroimage. 2011.\n"
        "  doi: 10.1016/j.neuroimage.2010.09.025.\n  \\`\\`\\`\n  ## Usage\n",
        "  Citation:\n  ```{% raw %}\n  Avants BB, et al. Neuroimage. 2011.\n"
        "  doi: 10.1016/j.neuroimage.2010.09.025.\n  {% endraw %}```\n",
    ],
    ids=["fenced", "plural", "bold", "heading", "jinja-raw"],
)
def test_catalog_reads_citation_from_readme_section(tmp_path, readme) -> None:
    write_catalog_source(
        tmp_path,
        "readme: |2-\n    ## demo/{{ context.version }} ##\n"
        "    Demo converts data.\n\n" + readme,
    )

    entry = catalog_entry(tmp_path)

    assert entry["citation"] == CITATION
    assert entry["description"] == "Demo converts data."


def test_catalog_prefers_readme_file_with_underlined_citation(tmp_path) -> None:
    write_catalog_source(
        tmp_path,
        "readme: |\n  ## demo/1.0.0 ##\n  Ignored.\n\n  Citation:\n  Ignored.\n",
    )
    (tmp_path / "recipes" / "demo" / "README.md").write_text(
        """\
----------------------------------
## demo/1.0.0 ##
Demo with a toolbox preinstalled.

Licensing
---------

Commercial.

Citation
--------

  (Demo, Example Inc.)

----------------------------------
"""
    )

    entry = catalog_entry(tmp_path)

    assert entry["citation"] == "(Demo, Example Inc.)"
    assert entry["description"] == "Demo with a toolbox preinstalled."


def test_catalog_renders_readme_templates(tmp_path) -> None:
    write_catalog_source(
        tmp_path,
        """\
variables:
  upstream_version: "7.3"
readme: |
  ## demo/{{ context.version }} ##
  Packages upstream {{ context.upstream_version }}.
  {% if context.arch == "aarch64" %}ARM build.{% else %}Intel build.{% endif %}
""",
    )

    assert catalog_entry(tmp_path)["description"] == (
        "Packages upstream 7.3. Intel build."
    )


@pytest.mark.parametrize(
    "recipe_text,release",
    [
        (
            "structured_readme:\n  example: demo\n  citation: ''\nreadme: |\n"
            "  ## demo/1.0.0 ##\n\n  Citation:\n  ```\n\n  ```\n\n"
            "  To run container outside of this environment: ml demo\n",
            {},
        ),
        (
            "readme: |\n  ## demo/1.0.0 ##\n  Demo.\n\n  Citation: Demo 2020.\n",
            {"recipe": "demo", "variant": "retired"},
        ),
    ],
    ids=["empty-citation", "undeclared-variant"],
)
def test_catalog_omits_citation_it_cannot_find(tmp_path, recipe_text, release) -> None:
    write_catalog_source(tmp_path, recipe_text, release=release)

    entry = catalog_entry(tmp_path)

    assert "citation" not in entry
    assert entry["apps"] == {"demo 1.0.0": {"version": "20260101"}}
