import json
import os
from pathlib import Path
import subprocess

import yaml


WORKFLOW = (
    Path(__file__).resolve().parents[2] / ".github/workflows/update-webapps-json.yml"
)


def test_webapps_updates_request_the_single_downstream_queue(tmp_path: Path) -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert set(triggers) == {"push", "workflow_dispatch"}
    assert "releases/**/*.json" in triggers["push"]["paths"]
    assert "recipes/**/build.yaml" in triggers["push"]["paths"]
    calls = tmp_path / "calls.jsonl"
    gh = tmp_path / "gh"
    gh.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "with open(os.environ['GH_CALLS'], 'a') as calls:\n"
        "    calls.write(json.dumps(sys.argv[1:]) + '\\n')\n"
    )
    gh.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "GH_CALLS": str(calls),
    }
    steps = workflow["jobs"]["update-webapps-json"]["steps"]
    for _ in range(2):
        for step in steps:
            subprocess.run(["bash", "-e", "-c", step["run"]], env=env, check=True)
    assert [json.loads(line) for line in calls.read_text().splitlines()] == [
        [
            "api",
            "--method",
            "POST",
            "repos/neurodesk/neurocommand/dispatches",
            "-f",
            "event_type=webapps-update",
        ]
    ] * 2
