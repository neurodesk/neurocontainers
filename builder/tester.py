from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass

from .adapters import platform_for_architecture


@dataclass(frozen=True)
class TestRequest:
    __test__ = False

    tag: str
    architecture: str
    offline_mode: bool = False


def describe_test_request(request: TestRequest) -> dict[str, str | bool]:
    return {
        "tag": request.tag,
        "architecture": request.architecture,
        "offline_mode": request.offline_mode,
    }


class ContainerTesterAdapter:
    def command(self, request: TestRequest) -> list[str]:
        command = [
            "docker",
            "run",
            "--rm",
            "--platform",
            platform_for_architecture(request.architecture),
        ]
        if request.offline_mode:
            command.extend(["--network", "none"])
        command.append(request.tag)
        command.extend(["/bin/sh", "-lc", "test -n \"$DEPLOY_BINS$DEPLOY_PATH\""])
        return command

    def run(self, request: TestRequest, *, dry_run: bool = False) -> list[str]:
        command = self.command(request)
        if dry_run:
            return command
        if not shutil.which("docker"):
            raise RuntimeError("docker CLI not found")
        subprocess.check_call(command)
        return command
