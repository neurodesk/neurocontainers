from pathlib import Path

import pytest
import yaml

from builder.audit_updates import audit, validate_update_policy


def test_new_recipe_cannot_silently_escape_tracking(tmp_path: Path) -> None:
    recipe_dir = tmp_path / "new-tool"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text("name: new-tool\nversion: 1.0\n")
    rows = audit(tmp_path)
    assert len(rows) == 1
    assert rows[0]["status"] == "error"
    assert "auto_update is required" in rows[0]["reason"]


def test_readme_version_does_not_count_as_install_binding() -> None:
    recipe = {
        "auto_update": {"method": "pypi", "package": "demo"},
        "readme": "Demo {{ context.version }}",
        "build": {"directives": [{"run": "pip install demo==1.0"}]},
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)

    recipe["build"]["directives"][0]["run"] = "pip install demo=={{ context.version }}"
    validate_update_policy(recipe)


def test_unused_raw_tag_variable_cannot_fall_back_to_another_version_binding():
    recipe = {
        "name": "demo",
        "version": "1.0",
        "variables": {"upstream_tag": "v1.0"},
        "auto_update": {
            "method": "github_tags",
            "repo": "org/demo",
            "tag_variable": "upstream_tag",
        },
        "build": {
            "directives": [
                {
                    "run": "git clone --branch {{ context.version }} https://github.com/org/demo"
                }
            ]
        },
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)
    recipe["build"]["directives"][0]["run"] += " /opt/demo-{{ context.upstream_tag }}"
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


@pytest.mark.parametrize("policy", [
    {"method": "manual", "reason": "Locally maintained example without an upstream software release."},
    {"method": "pypi", "package": "example", "mode": "notify",
     "reason": "The package has an incompatible release process."},
])
def test_manual_and_notification_only_policies_fail_audit(
    tmp_path: Path, policy: dict,
) -> None:
    recipe_dir = tmp_path / "bundle"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(
        yaml.safe_dump(
            {
                "auto_update": policy,
            }
        )
    )
    row = audit(tmp_path)[0]
    assert row["status"] == "error"
    assert "requires automatic updates" in row["reason"]


def test_locally_maintained_recipe_can_track_repository_inputs(tmp_path):
    recipe_dir = tmp_path / "local-example"
    recipe_dir.mkdir()
    (recipe_dir / "build.yaml").write_text(yaml.safe_dump({
        "name": "local-example", "version": "1.0.0",
        "auto_update": {"method": "sources", "sources": [], "local": []},
    }))
    (recipe_dir / "fulltest.yaml").write_text("name: local-example\nversion: 1.0.0\ntests: []\n")
    assert audit(tmp_path)[0]["status"] == "automatic"


@pytest.mark.parametrize(
    "extra",
    [
        {"deploy": {"path": ["/opt/demo-{{ context.version }}"]}},
        {"test": {"name": "fake version", "script": "echo {{ context.version }}"}},
        {"file": {"name": "unrelated.txt", "contents": "{{ context.version }}"}},
        {"run": "echo {{ context.version }}"},
    ],
)
def test_runtime_metadata_does_not_make_an_unpinned_install_safe(extra):
    recipe = {
        "name": "demo",
        "auto_update": {"method": "pypi", "package": "demo"},
        "build": {"directives": [{"run": "pip install demo"}, extra]},
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


def test_versioned_dependency_does_not_count_as_the_tracked_package():
    recipe = {
        "name": "demo",
        "auto_update": {"method": "pypi", "package": "demo"},
        "build": {
            "directives": [{"run": "pip install helper=={{ context.version }} demo"}]
        },
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


def test_unused_requirements_cannot_mask_an_unpinned_install():
    recipe = {
        "name": "demo",
        "auto_update": {"method": "pypi", "package": "demo"},
        "files": [
            {"name": "requirements.txt", "contents": "demo=={{ context.version }}"}
        ],
        "build": {"directives": [{"run": "pip install demo"}]},
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)
    recipe["build"]["directives"][0][
        "run"
    ] = 'pip install -r {{ get_file("requirements.txt") }}'
    validate_update_policy(recipe)


def test_enabling_automatic_updates_requires_runtime_tests(tmp_path):
    root = tmp_path / "demo"
    root.mkdir()
    recipe = {
        "name": "demo",
        "auto_update": {"method": "pypi", "package": "demo"},
        "build": {"directives": [{"run": "pip install demo=={{ context.version }}"}]},
    }
    with pytest.raises(ValueError, match="fulltest.yaml"):
        validate_update_policy(recipe, recipe_path=root / "build.yaml")


def test_asset_mapping_must_reference_a_declared_download():
    recipe = {
        "name": "demo",
        "auto_update": {
            "method": "github_release",
            "repo": "org/demo",
            "assets": {"missing": r"demo.zip"},
        },
        "files": [
            {
                "name": "actual",
                "url": "https://github.com/org/demo/archive/{{ context.version }}.zip",
            }
        ],
    }
    with pytest.raises(ValueError, match="declared URL file named missing"):
        validate_update_policy(recipe)


def test_printing_requirements_does_not_bind_an_install():
    recipe = {
        "name": "demo",
        "auto_update": {"method": "pypi", "package": "demo"},
        "files": [
            {"name": "requirements.txt", "contents": "demo=={{ context.version }}"}
        ],
        "build": {
            "directives": [
                {"run": ['cat {{ get_file("requirements.txt") }}', "pip install demo"]}
            ]
        },
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


@pytest.mark.parametrize(
    "commands",
    [
        [
            "git clone https://github.com/org/demo /opt/demo",
            "git clone https://github.com/org/helper /opt/helper",
            "cd /opt/helper",
            "git checkout {{ context.version }}",
        ],
        [
            "git clone https://github.com/org/demo /opt/demo && "
            "git clone --branch {{ context.version }} https://github.com/org/helper"
        ],
        ["git clone https://github.com/org/demo /opt/demo-{{ context.version }}"],
        [
            "git clone --branch fixed https://github.com/org/demo "
            "/opt/demo-{{ context.version }}"
        ],
    ],
)
def test_git_binding_must_select_the_tracked_repository_version(commands):
    recipe = {
        "name": "demo",
        "auto_update": {"method": "github_tags", "repo": "org/demo"},
        "build": {"directives": [{"run": commands}]},
    }
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


@pytest.mark.parametrize("branch", ["--branch v", "--branch=v", "-b v"])
def test_versioned_clone_binds_the_tracked_repository(branch):
    validate_update_policy(
        {
            "name": "demo",
            "auto_update": {"method": "github_tags", "repo": "org/demo"},
            "build": {
                "directives": [
                    {
                        "run": f"git clone {branch}{{{{ context.version }}}} "
                        "https://github.com/org/demo.git /opt/demo"
                    }
                ]
            },
        }
    )


def test_raw_tag_variable_must_exist_and_bind_installation():
    recipe = {
        "name": "demo",
        "version": "1.0",
        "auto_update": {
            "method": "github_tags",
            "repo": "org/demo",
            "tag_variable": "upstream_tag",
        },
        "build": {
            "directives": [
                {
                    "run": "git clone --branch {{ context.upstream_tag }} "
                    "https://github.com/org/demo.git"
                }
            ]
        },
    }
    with pytest.raises(ValueError, match="current source ref"):
        validate_update_policy(recipe)
    recipe["variables"] = {"upstream_tag": "v1.0"}
    validate_update_policy(recipe)
    recipe["variables"]["upstream_tag"] = "v9.9"
    with pytest.raises(ValueError, match="current recipe version"):
        validate_update_policy(recipe)
    recipe["variables"]["upstream_tag"] = "v1.0"
    recipe["build"]["directives"][0]["run"] = "git clone https://github.com/org/demo"
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


def test_github_branch_feed_must_match_the_cloned_repository():
    recipe = {
        "name": "demo",
        "auto_update": {
            "method": "webpage",
            "url": "https://api.github.com/repos/org/demo/branches",
            "version_regex": r"release-(?P<version>\d+\.\d+)",
        },
        "build": {
            "directives": [
                {
                    "run": "git clone --branch release-{{ context.version }} "
                    "https://github.com/org/demo.git"
                }
            ]
        },
    }
    validate_update_policy(recipe)
    recipe["auto_update"]["url"] = "https://api.github.com/repos/org/helper/branches"
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)


def test_raw_tag_binding_survives_filename_transformations():
    validate_update_policy(
        {
            "name": "demo",
            "version": "2023.12.1",
            "variables": {"upstream_tag": "v2023.12.1+402"},
            "auto_update": {
                "method": "github_tags",
                "repo": "org/demo",
                "tag_variable": "upstream_tag",
                "version_regex": r"v(?P<version>\d{4}\.\d+\.\d+)\+\d+",
            },
            "files": [
                {
                    "name": "installer",
                    "url": "https://example.org/demo-{{ context.upstream_tag[1:] | replace('+', '-') }}.deb",
                }
            ],
        }
    )


def test_oci_tracking_must_match_the_imported_image():
    recipe = {
        "auto_update": {
            "method": "oci",
            "url": "https://registry.example/v2/vendor/runtime/tags/list",
        },
        "build": {
            "base-image": "registry.example/vendor/runtime:{{ context.version }}"
        },
    }
    validate_update_policy(recipe)
    recipe["build"]["base-image"] = "registry.example/unrelated:{{ context.version }}"
    with pytest.raises(ValueError, match="source download"):
        validate_update_policy(recipe)
