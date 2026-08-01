from __future__ import annotations

from builder import run_tests


def test_container_runtime_command_prefers_apptainer(monkeypatch) -> None:
    def fake_which(command: str) -> str | None:
        return f"/usr/bin/{command}" if command in {"apptainer", "singularity"} else None

    monkeypatch.setattr(run_tests.shutil, "which", fake_which)

    assert run_tests.container_runtime_command() == "apptainer"


def test_container_runtime_command_falls_back_to_singularity(monkeypatch) -> None:
    def fake_which(command: str) -> str | None:
        return "/usr/bin/singularity" if command == "singularity" else None

    monkeypatch.setattr(run_tests.shutil, "which", fake_which)

    assert run_tests.container_runtime_command() == "singularity"


def test_container_runtime_command_keeps_existing_error_path(monkeypatch) -> None:
    monkeypatch.setattr(run_tests.shutil, "which", lambda command: None)

    assert run_tests.container_runtime_command() == "apptainer"


def test_substitute_variables_expands_nested_values() -> None:
    variables = {
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-${tool_version}",
    }

    assert (
        run_tests.substitute_variables("${tool_dir}/bin/tool", variables)
        == "/opt/tool-3.2.8/bin/tool"
    )


def test_top_level_variables_support_container_patterns() -> None:
    config = {
        "name": "tool",
        "version": "${tool_version}",
        "container": "tool_${tool_version}_*.simg",
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-${tool_version}",
        "tests": [],
    }

    variables = run_tests.collect_top_level_variables(config)

    assert variables == {
        "name": "tool",
        "version": "3.2.8",
        "tool_version": "3.2.8",
        "tool_dir": "/opt/tool-3.2.8",
    }
    assert (
        run_tests.substitute_variables(config["container"], variables)
        == "tool_3.2.8_*.simg"
    )


def test_container_variables_expand_literal_recipe_version() -> None:
    config = {
        "name": "deeplabcut",
        "version": "2.3.11",
        "container": "${name}_${version}_REFERENCE.simg",
    }

    variables = run_tests.collect_top_level_variables(config)

    assert (
        run_tests.substitute_variables(config["container"], variables)
        == "deeplabcut_2.3.11_REFERENCE.simg"
    )
