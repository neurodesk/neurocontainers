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
