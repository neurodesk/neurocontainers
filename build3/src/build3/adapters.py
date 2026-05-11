from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


def platform_for_architecture(architecture: str) -> str:
    if architecture == "x86_64":
        return "linux/amd64"
    if architecture == "aarch64":
        return "linux/arm64"
    raise ValueError(f"unsupported architecture: {architecture}")


@dataclass(frozen=True)
class BuildInputs:
    name: str
    version: str
    tag: str
    architecture: str
    build_dir: Path
    dockerfile_path: Path


class DockerAdapter:
    def command(self, inputs: BuildInputs) -> list[str]:
        return [
            "docker",
            "buildx",
            "build",
            "--load",
            "--platform",
            platform_for_architecture(inputs.architecture),
            "-f",
            str(inputs.dockerfile_path),
            "-t",
            inputs.tag,
            "--build-context",
            f"cache={inputs.build_dir / 'cache'}",
            str(inputs.build_dir),
        ]

    def run(self, inputs: BuildInputs, *, dry_run: bool = False) -> list[str]:
        command = self.command(inputs)
        if dry_run:
            return command
        if not shutil.which("docker"):
            raise RuntimeError("docker CLI not found")
        env = os.environ.copy()
        env.setdefault("DOCKER_BUILDKIT", "1")
        subprocess.check_call(command, env=env)
        return command


class BuildKitAdapter:
    def command(self, inputs: BuildInputs, output_tar: Path) -> list[str]:
        return [
            "buildctl",
            "build",
            "--frontend=dockerfile.v0",
            "--local",
            f"context={inputs.build_dir}",
            "--local",
            f"dockerfile={inputs.build_dir}",
            "--local",
            f"cache={inputs.build_dir / 'cache'}",
            "--opt",
            f"filename={inputs.dockerfile_path.name}",
            "--opt",
            f"platform={platform_for_architecture(inputs.architecture)}",
            "--output",
            f"type=docker,name={inputs.tag},dest={output_tar}",
        ]

    def run(self, inputs: BuildInputs, output_tar: Path, *, dry_run: bool = False) -> list[str]:
        command = self.command(inputs, output_tar)
        if dry_run:
            return command
        if not shutil.which("buildctl"):
            raise RuntimeError("buildctl CLI not found")
        subprocess.check_call(command)
        return command


class SifAdapter:
    def command(self, docker_archive: Path, output_sif: Path) -> list[str]:
        runtime = shutil.which("apptainer") or shutil.which("singularity") or "apptainer"
        return [
            runtime,
            "build",
            "--force",
            str(output_sif),
            "docker-archive://" + str(docker_archive),
        ]

    def run(self, docker_archive: Path, output_sif: Path, *, dry_run: bool = False) -> list[str]:
        command = self.command(docker_archive, output_sif)
        if dry_run:
            return command
        if not shutil.which(command[0]):
            raise RuntimeError("apptainer or singularity CLI not found")
        subprocess.check_call(command)
        return command
