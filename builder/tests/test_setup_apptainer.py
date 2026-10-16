"""Execute the composite action's installer with package/network boundaries stubbed."""

import os
from pathlib import Path
import subprocess

import pytest
import yaml


ACTION = Path(".github/actions/setup-apptainer").resolve()


@pytest.fixture
def installer(tmp_path):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    output = tmp_path / "output"
    log = tmp_path / "commands"
    for name, body in {
        "uname": 'if [[ "$1" == -s ]]; then echo Linux; else echo "$TEST_MACHINE"; fi',
        "dpkg": 'echo "$TEST_ARCH"',
        "sudo": 'printf "%s\\n" "$*" >> "$TEST_LOG"',
        "curl": 'while [[ "$1" != -o ]]; do shift; done; printf package > "$2"',
        "apptainer": 'echo "apptainer version $TEST_INSTALLED_VERSION"',
    }.items():
        path = binaries / name
        path.write_text(f"#!/bin/bash\nset -eu\n{body}\n")
        path.chmod(0o755)

    def run(arch="amd64", installed="1.4.3"):
        action = yaml.safe_load((ACTION / "action.yml").read_text())
        step = next(step for step in action["runs"]["steps"] if step.get("id") == "install")
        script = step["run"].replace("${{ inputs.apptainer-version }}", "1.4.3")
        script = script.replace("${{ github.action_path }}", str(ACTION))
        environment = {
            **os.environ,
            "PATH": f"{binaries}:{os.environ['PATH']}",
            "RUNNER_TEMP": str(tmp_path),
            "GITHUB_OUTPUT": str(output),
            "APPTAINER_VERSION": "1.4.3",
            "TEST_ARCH": arch,
            "TEST_MACHINE": "aarch64" if arch == "arm64" else "x86_64",
            "TEST_INSTALLED_VERSION": installed,
            "TEST_LOG": str(log),
        }
        result = subprocess.run(
            ["bash", "-e", "-c", script],
            env=environment,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result, output.read_text() if output.exists() else ""

    return run


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_installed_version_must_match_before_output(installer, arch):
    result, output = installer(arch=arch, installed="1.5.4")
    assert result.returncode != 0, f"Accepted wrong installed version: {output!r}"
    assert output == ""


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
def test_matching_version_output(installer, arch):
    result, output = installer(arch=arch)
    assert result.returncode == 0, result.stderr
    assert output == "apptainer-version=1.4.3\n"
