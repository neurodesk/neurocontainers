import os
import subprocess
from pathlib import Path


RUNTIME_SCRIPT = "/opt/neurodesktop/nested_container_runtime.sh"


def run_bash(script, env=None):
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        ["/bin/bash", "-lc", script],
        capture_output=True,
        text=True,
        timeout=30,
        env=merged_env,
    )


def test_nested_container_runtime_script_installed():
    path = Path(RUNTIME_SCRIPT)
    assert path.is_file(), f"{RUNTIME_SCRIPT} is missing"
    assert os.access(path, os.X_OK), f"{RUNTIME_SCRIPT} is not executable"


def test_nested_runtime_sets_matching_bindpaths_by_default():
    result = run_bash(
        f"""
        unset APPTAINER_BINDPATH SINGULARITY_BINDPATH
        source {RUNTIME_SCRIPT}
        printf '%s\\n%s\\n%s\\n' \
            "$APPTAINER_BINDPATH" \
            "$SINGULARITY_BINDPATH" \
            "$NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE"
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "/data,/mnt,/neurodesktop-storage,/tmp,/cvmfs",
        "/data,/mnt,/neurodesktop-storage,/tmp,/cvmfs",
        "image",
    ]


def test_nested_runtime_uses_bound_host_singularity(tmp_path):
    host_bin_dir = tmp_path / "host-bin"
    host_bin_dir.mkdir()
    host_singularity = host_bin_dir / "singularity"
    host_singularity.write_text("#!/bin/sh\nexit 0\n")
    host_singularity.chmod(0o755)

    result = run_bash(
        f"""
        source {RUNTIME_SCRIPT}
        printf '%s\\n%s\\n' "$NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE" "$PATH"
        """,
        env={
            "NEURODESKTOP_HOST_SINGULARITY_BIN": str(host_singularity),
            "NEURODESKTOP_NESTED_CONTAINER_RUNTIME": "auto",
        },
    )
    assert result.returncode == 0, result.stderr
    active, path = result.stdout.splitlines()
    assert active == "host"
    assert str(host_bin_dir) in path.split(":")


def test_nested_runtime_warns_when_no_new_privs_blocks_nested_userns(tmp_path):
    status_file = tmp_path / "status"
    status_file.write_text("Name:\tbash\nNoNewPrivs:\t1\n")

    result = run_bash(
        f"""
        source {RUNTIME_SCRIPT}
        printf '%s\\n%s\\n' "$NEURODESKTOP_NESTED_CONTAINER_RUNTIME_ACTIVE" "$NEURODESKTOP_NESTED_CONTAINER_WARNING"
        """,
        env={
            "NEURODESKTOP_PROC_STATUS": str(status_file),
            "NEURODESKTOP_NESTED_CONTAINER_RUNTIME": "image",
        },
    )
    assert result.returncode == 0, result.stderr
    active, warning = result.stdout.splitlines()
    assert active == "image"
    assert "NoNewPrivs=1" in warning
