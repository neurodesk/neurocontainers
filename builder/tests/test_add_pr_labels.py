from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPT = Path("workflows/add_pr_labels_with_retry.sh")


def _write_fake_gh(tmp_path: Path, *, failures: int) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    attempts_file = tmp_path / "attempts"
    gh = bin_dir / "gh"
    gh.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
attempts=0
if [[ -f "$ATTEMPTS_FILE" ]]; then
  attempts="$(cat "$ATTEMPTS_FILE")"
fi
attempts=$((attempts + 1))
printf '%s' "$attempts" > "$ATTEMPTS_FILE"
printf '%s\\n' "$*" >> "$GH_LOG_FILE"
if (( attempts <= FAILURES )); then
  exit 1
fi
"""
    )
    gh.chmod(0o755)
    return bin_dir, attempts_file


def _run_script(
    tmp_path: Path, *, failures: int, max_attempts: int = 5
) -> subprocess.CompletedProcess[str]:
    bin_dir, attempts_file = _write_fake_gh(tmp_path, failures=failures)
    env = os.environ.copy()
    env.update(
        {
            "ATTEMPTS_FILE": str(attempts_file),
            "FAILURES": str(failures),
            "GH_API_MAX_ATTEMPTS": str(max_attempts),
            "GH_API_RETRY_DELAY_SECONDS": "0",
            "GH_LOG_FILE": str(tmp_path / "gh.log"),
            "GITHUB_REPOSITORY": "neurodesk/neurocontainers",
            "PATH": f"{bin_dir}{os.pathsep}{env['PATH']}",
        }
    )
    return subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "https://github.com/neurodesk/neurocontainers/pull/2915",
            "automated",
            "release",
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def test_add_pr_labels_retries_only_the_idempotent_rest_update(
    tmp_path: Path,
) -> None:
    result = _run_script(tmp_path, failures=2)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "attempts").read_text() == "3"
    calls = (tmp_path / "gh.log").read_text().splitlines()
    assert calls == [
        "api --method POST repos/neurodesk/neurocontainers/issues/2915/labels "
        "--raw-field labels[]=automated --raw-field labels[]=release"
    ] * 3


def test_add_pr_labels_fails_after_bounded_attempts(tmp_path: Path) -> None:
    result = _run_script(tmp_path, failures=5, max_attempts=3)

    assert result.returncode != 0
    assert (tmp_path / "attempts").read_text() == "3"
    assert "Failed to add PR labels after 3 attempts" in result.stderr


def test_create_pr_workflow_labels_after_pr_creation() -> None:
    workflow = Path(".github/workflows/build-app.yml").read_text()
    create_pr_step = workflow.split("    - name: Create Release File Pull Request", 1)[1].split(
        "    - name: Detect OpenReconLabel.json", 1
    )[0]
    create_command = create_pr_step.split("gh pr create", 1)[1].split("PR_URL", 1)[0]

    assert '--label "automated"' not in create_command
    assert '--label "release"' not in create_command
    assert 'bash workflows/add_pr_labels_with_retry.sh "$PR_URL" automated release' in create_pr_step
