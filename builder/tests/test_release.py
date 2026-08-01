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


def test_release_renders_gui_app_exec_from_recipe_context() -> None:
    config = default_config()
    compiled = compile_recipe(
        resolve_recipe(config, "cat12"),
        architecture="x86_64",
        include_dirs=config.include_dirs,
    )
    data = release_data(compiled.name, compiled.version, compiled.recipe, "20260519")
    exec_command = data["apps"]["cat12GUI-cat12 26.0.rc3"]["exec"]

    assert exec_command == "bash run_spm25.sh /opt/mcr/R2023b/"
    assert "{{" not in exec_command


def test_release_preserves_container_visibility_flags() -> None:
    data = release_data(
        "tool",
        "1.2.3",
        {
            "categories": ["workflows"],
            "show_in_menu": False,
            "show_in_applist": False,
        },
        "20260102",
    )

    assert data["show_in_menu"] is False
    assert data["show_in_applist"] is False


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


def test_named_arm64_variant_is_a_normal_container_release() -> None:
    data = release_data(
        "tool_arm64",
        "1.2.3",
        {"categories": ["workflows"], "apptainer_args": ["--cleanenv"]},
        "20260102",
        "aarch64",
        "arm64",
    )

    assert release_version("1.2.3", "aarch64", "arm64") == "1.2.3"
    assert data["variant"] == "arm64"
    assert data["architecture"] == "aarch64"
    assert data["apps"] == {
        "tool_arm64 1.2.3": {
            "version": "20260102",
            "exec": "",
            "apptainer_args": ["--cleanenv"],
        }
    }
