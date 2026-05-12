from __future__ import annotations

from pathlib import Path

from builder.adapters import BuildInputs, BuildKitAdapter, DockerAdapter, SifAdapter
from builder.tester import ContainerTesterAdapter, TestRequest


def inputs(tmp_path: Path) -> BuildInputs:
    return BuildInputs(
        name="tool",
        version="1.0",
        tag="tool:1.0",
        architecture="x86_64",
        build_dir=tmp_path,
        dockerfile_path=tmp_path / "Dockerfile",
    )


def test_docker_adapter_command(tmp_path: Path) -> None:
    command = DockerAdapter().command(inputs(tmp_path))
    assert command[:3] == ["docker", "buildx", "build"]
    assert "--build-context" in command
    assert "linux/amd64" in command


def test_buildkit_adapter_command(tmp_path: Path) -> None:
    command = BuildKitAdapter().command(inputs(tmp_path), tmp_path / "image.tar")
    assert command[:2] == ["buildctl", "build"]
    assert "platform=linux/amd64" in command


def test_sif_adapter_dry_command(tmp_path: Path) -> None:
    command = SifAdapter().command(tmp_path / "image.tar", tmp_path / "tool.sif")
    assert command[1:3] == ["build", "--force"]
    assert command[-1].startswith("docker-archive://")


def test_container_tester_command() -> None:
    command = ContainerTesterAdapter().command(
        TestRequest(tag="tool:1.0", architecture="x86_64", offline_mode=True)
    )
    assert command[:2] == ["docker", "run"]
    assert "--network" in command
    assert "tool:1.0" in command
