"""Exercise the actual promotion script with superseded exact-head runs."""

import json
from pathlib import Path
import subprocess

import pytest
import yaml


HEAD = "a" * 40


def run_record(run_id, conclusion, status="completed", head=HEAD):
    return {
        "id": run_id,
        "status": status,
        "conclusion": conclusion,
        "head_sha": head,
        "event": "pull_request",
    }


@pytest.mark.parametrize(
    "responses, expected_id, expected_error, expected_polls",
    [
        # Same-head events may appear oldest first, including cancelled runs.
        ([[run_record(10, "cancelled"), run_record(11, "success")]], 11, None, 1),
        # Do not fall back to older green artifacts when the latest run fails.
        (
            [[run_record(11, "failure"), run_record(10, "success")]],
            None,
            "Candidate run 11 completed with failure",
            1,
        ),
        # Wait for the latest running attempt despite an older completed run.
        (
            [
                [run_record(10, "success"), run_record(11, None, "in_progress")],
                [run_record(10, "success"), run_record(11, "success")],
            ],
            11,
            None,
            2,
        ),
        # An unrelated newer head can never supply this PR's artifacts.
        (
            [[run_record(12, "success", head="b" * 40), run_record(11, "success")]],
            11,
            None,
            1,
        ),
    ],
)
def test_promotion_uses_latest_exact_head(
    responses, expected_id, expected_error, expected_polls
):
    workflow = yaml.safe_load(
        Path(".github/workflows/promote-container-candidate.yml").read_text()
    )
    script = workflow["jobs"]["await_candidate"]["steps"][0]["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const outputs = {};
let polls = 0;
let artifactRun = null;
const github = {
  rest: {actions: {
    listRepoWorkflows: async () => ({data: {workflows: [{
      id: 42, path: '.github/workflows/pr-container-candidate.yml'
    }]}}),
    listWorkflowRuns: async query => {
      if (query.head_sha !== input.head || query.event !== 'pull_request')
        throw new Error('Source identity filter missing');
      const runs = input.responses[polls++];
      if (!runs) throw new Error('Unexpected extra poll');
      return {data: {workflow_runs: runs}};
    },
    listWorkflowRunArtifacts: () => {}
  }},
  paginate: async (method, query) => {
    artifactRun = query.run_id;
    return [
      {id: 100, name: 'candidate-demo-' + input.head, expired: false},
      {id: 101, name: 'candidate-report-demo-' + input.head, expired: false},
      {id: 102, name: 'candidate-demo-' + 'b'.repeat(40), expired: false},
      {id: 103, name: 'candidate-expired-' + input.head, expired: true}
    ];
  }
};
global.setTimeout = callback => { callback(); return 0; };
(async () => {
  let error = null;
  try {
    await new AsyncFunction('github', 'context', 'core', 'process', input.script)(
      github, {repo: {owner: 'example', repo: 'containers'}},
      {info: () => {}, setOutput: (key, value) => { outputs[key] = value; }},
      {env: {HEAD_SHA: input.head}}
    );
  } catch (failure) { error = failure.message; }
  process.stdout.write(JSON.stringify({outputs, polls, artifactRun, error}));
})();
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            harness,
            json.dumps(
                {
                    "head": HEAD,
                    "responses": responses,
                    "script": script,
                }
            ),
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=10,
    )
    actual = json.loads(result.stdout)
    assert actual["error"] == expected_error
    assert actual["polls"] == expected_polls
    assert actual["artifactRun"] == expected_id
    if expected_error is None:
        assert json.loads(actual["outputs"]["artifacts"]) == [
            {"id": 100, "name": "candidate-demo-" + HEAD}
        ]
    else:
        assert actual["outputs"] == {}
