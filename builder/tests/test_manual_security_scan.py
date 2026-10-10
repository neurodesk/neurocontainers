import os
from pathlib import Path
import subprocess

import pytest
import yaml


@pytest.mark.parametrize(
    "mode",
    [
        "export-failure",
        "partial-export-failure",
        "scan-failure",
        "missing-report",
        "invalid-report",
        "clean",
        "findings",
    ],
)
def test_semgrep_step_does_not_report_execution_failures_as_clean(tmp_path, mode):
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github/workflows/manual-container-security-scan.yml"
    )
    steps = yaml.safe_load(workflow.read_text())["jobs"]["container-security-scan"][
        "steps"
    ]
    script = next(step["run"] for step in steps if step.get("id") == "semgrep")
    for key, value in {
        "inputs.container_image": "fixture",
        "env.TEMP_DIR": "temporary",
        "env.SCAN_OUTPUT_DIR": "reports",
    }.items():
        script = script.replace("${{ " + key + " }}", value)
    (tmp_path / "reports").mkdir()
    docker = tmp_path / "docker"
    docker.write_text("""#!/bin/bash
set -eu
case "$1" in
  create) echo fixture-container ;;
  export)
    if [ "$MODE" = export-failure ]; then exit 2; fi
    tar -cf - --files-from /dev/null
    if [ "$MODE" = partial-export-failure ]; then exit 2; fi ;;
  run)
    case "$MODE" in
      scan-failure) exit 2 ;;
      missing-report) exit 0 ;;
      invalid-report) echo '{}' > reports/semgrep_results.json ;;
      clean) echo '{"results": []}' > reports/semgrep_results.json ;;
      findings) echo '{"results": [{"check_id": "fixture"}]}' > reports/semgrep_results.json ;;
    esac ;;
  rm) touch cleanup-ran ;;
  *) exit 3 ;;
esac
""")
    docker.chmod(0o755)
    output = tmp_path / "outputs"
    result = subprocess.run(
        ["bash", "-e", "-c", script],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "MODE": mode,
            "GITHUB_OUTPUT": str(output),
        },
        capture_output=True,
        text=True,
    )
    actual = output.read_text() if output.exists() else ""
    if mode in ("clean", "findings"):
        assert result.returncode == 0, result.stderr
        assert ("found=false" if mode == "clean" else "found=true") in actual
    else:
        assert result.returncode != 0
        assert "found=false" not in actual
    assert (tmp_path / "cleanup-ran").exists()
