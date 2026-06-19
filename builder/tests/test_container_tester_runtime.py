from __future__ import annotations

import subprocess
from types import SimpleNamespace

from workflows.container_tester import ApptainerRuntime, ContainerTester


def test_apptainer_runtime_uses_cleanenv_when_requested(monkeypatch) -> None:
    commands: list[list[str]] = []
    envs: list[dict[str, str] | None] = []

    monkeypatch.setattr("workflows.container_tester.shutil.which", lambda name: name)
    monkeypatch.setenv("APPTAINER_BINDPATH", "/opt:/opt")
    monkeypatch.setenv("SINGULARITY_BINDPATH", "/opt:/opt")

    def fake_run(command, **kwargs):
        commands.append(command)
        envs.append(kwargs.get("env"))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("workflows.container_tester.subprocess.run", fake_run)

    runtime = ApptainerRuntime()
    runtime.run_test("container.simg", "true", clean_env=True)

    assert commands == [
        [
            "apptainer",
            "exec",
            "--cleanenv",
            "--no-mount",
            "hostfs",
            "container.simg",
            "bash",
            "-c",
            "true",
        ]
    ]
    assert envs[0] is not None
    assert "APPTAINER_BINDPATH" not in envs[0]
    assert "SINGULARITY_BINDPATH" not in envs[0]


def test_builtin_tests_run_with_clean_apptainer_environment(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeRuntime:
        name = "apptainer"

        def run_test(self, container_ref, test_script, volumes=None, gpu=False, working_dir="/test", clean_env=False):
            calls.append(
                {
                    "container_ref": container_ref,
                    "volumes": volumes,
                    "gpu": gpu,
                    "working_dir": working_dir,
                    "clean_env": clean_env,
                }
            )
            return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    tester = ContainerTester()
    tester.selected_runtime = FakeRuntime()
    result = tester._run_builtin_test(
        "container.simg",
        {"name": "Simple Deploy Bins/Path Test", "builtin": "test_deploy.sh"},
    )

    assert result["status"] == "passed"
    assert calls == [
        {
            "container_ref": "container.simg",
            "volumes": [],
            "gpu": False,
            "working_dir": "/test",
            "clean_env": True,
        }
    ]
