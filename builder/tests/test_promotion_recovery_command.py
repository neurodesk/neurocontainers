import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
COMMAND = (
    "gh workflow run promote-container-candidate.yml "
    "--repo neurodesk/neurocontainers --ref main "
    "-f pr_number=3769 -f staged_run_id=40427710945"
)
COMMENT = (
    "<!-- one-pr-container-candidate -->\n"
    "<!-- container-release-identity: surfice 1.0.20211006 -->\n"
    "<!-- container-release-status:start -->\n"
    "## promotion running\n"
    "<!-- container-release-status:end -->"
)


def run_report(event, publish, finalize, comment=COMMENT):
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/promote-container-candidate.yml").read_text()
    )
    script = workflow["jobs"]["report_promotion"]["steps"][0]["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const observed = {errors: [], summary: []};
const summary = {
  addHeading(text) { observed.summary.push(text); return summary; },
  addRaw(text) { observed.summary.push(text); return summary; },
  addCodeBlock(text) { observed.summary.push(text); return summary; },
  addLink(text, href) { observed.summary.push(href); return summary; },
  async write() {},
};
const core = {
  error: message => observed.errors.push(message),
  notice: () => {},
  setFailed: message => { throw new Error(message); },
  summary,
};
const github = {
  rest: {issues: {
    listComments: 'comments',
    updateComment: async args => { observed.updated = args.body; },
  }},
  paginate: async () => input.comment
    ? [{id: 1, user: {type: 'Bot'}, body: input.comment}]
    : [],
};
const context = {
  eventName: input.event, runId: 40427710945, runNumber: 77,
  repo: {owner: 'neurodesk', repo: 'neurocontainers'},
};
(async () => {
  await new AsyncFunction('github', 'context', 'core', input.script)(github, context, core);
  process.stdout.write(JSON.stringify(observed));
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    env = {
        **os.environ,
        "PR_NUMBER": "3769",
        "AWAIT_RESULT": "success",
        "PUBLISH_RESULT": publish,
        "FINALIZE_RESULT": finalize,
        "SYNC_RESULT": "skipped",
        "SKIP_REASON": "",
        "FINALIZED_CONTAINERS": "",
    }
    result = subprocess.run(
        [
            "node",
            "-e",
            harness,
            json.dumps({"script": script, "event": event, "comment": comment}),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_finalize_only_failure_reports_the_exact_recovery_command():
    observed = run_report("push", "success", "failure")

    assert "finalization failed" in observed["updated"]
    assert f"```bash\n{COMMAND}\n```" in observed["updated"]
    assert any(COMMAND in error for error in observed["errors"])
    assert COMMAND in observed["summary"]


@pytest.mark.parametrize(
    ("event", "publish"),
    [
        # A failed publish job has no complete staged set to recover.
        ("push", "failure"),
        # Recovery itself only accepts a failed push run as its source.
        ("workflow_dispatch", "success"),
    ],
)
def test_unrecoverable_failures_do_not_offer_recovery(event, publish):
    observed = run_report(event, publish, "failure")

    assert "staged_run_id" not in observed["updated"]
    assert "staged recovery" in observed["updated"]
    assert observed["errors"] == []
    assert observed["summary"] == []


def test_recovery_command_reaches_the_run_without_a_candidate_comment():
    observed = run_report("push", "success", "failure", comment=None)

    assert "updated" not in observed
    assert any(COMMAND in error for error in observed["errors"])
    assert COMMAND in observed["summary"]
