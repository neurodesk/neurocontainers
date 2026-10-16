"""Drive the workflow's recovery authorization with GitHub-shaped responses."""

import copy
import json
from pathlib import Path
import subprocess

import pytest
import yaml


HEAD = "a" * 40
MERGE = "b" * 40


@pytest.fixture
def recovery():
    return {
        "run": {
            "repository": {"full_name": "example/containers"},
            "head_repository": {"full_name": "example/containers"},
            "path": ".github/workflows/promote-container-candidate.yml",
            "event": "push",
            "head_branch": "main",
            "head_sha": MERGE,
            "status": "completed",
            "conclusion": "failure",
        },
        "jobs": [
            {"name": f"publish (candidate-{name}-{HEAD})", "conclusion": "success"}
            for name in ("first", "second")
        ]
        + [{"name": "finalize", "conclusion": "failure"}],
        "artifacts": [
            {"name": f"promotion-metadata-{name}-{HEAD}", "id": index, "expired": False}
            for index, name in enumerate(("first", "second"), start=1)
        ],
    }


def run_recovery(recovery):
    workflow = yaml.safe_load(
        Path(".github/workflows/promote-container-candidate.yml").read_text()
    )
    script = workflow["jobs"]["await_candidate"]["steps"][0]["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const outputs = {};
const actions = {
  getWorkflowRun: async args => {
    if (args.run_id !== 123) throw new Error('Wrong run');
    return {data: input.run};
  },
  listJobsForWorkflowRun: 'jobs', listWorkflowRunArtifacts: 'artifacts'
};
const github = {rest: {actions}, paginate: async (method, args) => {
  if (args.run_id !== 123) throw new Error('Wrong run');
  if (method === 'jobs' && args.filter !== 'latest') throw new Error('Wrong attempt');
  return input[method];
}};
(async () => {
  let error = null;
  try {
    await new AsyncFunction('github', 'context', 'core', 'process', input.script)(
      github, {repo: {owner: 'example', repo: 'containers'}, eventName: 'workflow_dispatch'},
      {setOutput: (key, value) => {outputs[key] = value;}},
      {env: {HEAD_SHA: input.head, MERGE_SHA: input.merge, STAGED_RUN_ID: '123'}}
    );
  } catch (failure) {error = failure.message;}
  process.stdout.write(JSON.stringify({outputs, error}));
})();
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            harness,
            json.dumps(
                {
                    **recovery,
                    "script": script,
                    "head": HEAD,
                    "merge": MERGE,
                }
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_recovery_uses_only_metadata_from_complete_trusted_staging(recovery):
    result = run_recovery(recovery)
    assert result["error"] is None
    assert json.loads(result["outputs"]["artifacts"]) == [
        {"id": item["id"], "name": item["name"]} for item in recovery["artifacts"]
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("event", "pull_request"),
        ("head_branch", "feature"),
        ("head_sha", "c" * 40),
        ("path", ".github/workflows/untrusted.yml"),
        ("conclusion", "success"),
        ("status", "in_progress"),
        ("repository", {"full_name": "other/containers"}),
        ("head_repository", {"full_name": "fork/containers"}),
    ],
)
def test_recovery_rejects_untrusted_or_wrong_origin(recovery, field, value):
    recovery["run"][field] = value
    result = run_recovery(recovery)
    assert result["error"] is not None
    assert result["outputs"] == {}


@pytest.mark.parametrize(
    "fault",
    [
        "publish_failed",
        "publish_missing",
        "wrong_head",
        "finalize_success",
        "missing",
        "expired",
        "unexpected",
        "duplicate",
    ],
)
def test_recovery_rejects_incomplete_staging(recovery, fault):
    if fault == "publish_failed":
        recovery["jobs"][0]["conclusion"] = "failure"
    elif fault == "publish_missing":
        recovery["jobs"] = recovery["jobs"][-1:]
    elif fault == "wrong_head":
        recovery["jobs"][0]["name"] = f"publish (candidate-first-{'c' * 40})"
    elif fault == "finalize_success":
        recovery["jobs"][-1]["conclusion"] = "success"
    elif fault == "missing":
        recovery["artifacts"].pop()
    elif fault == "expired":
        recovery["artifacts"][0]["expired"] = True
    elif fault == "unexpected":
        recovery["artifacts"][0]["name"] = f"promotion-metadata-other-{HEAD}"
    else:
        recovery["artifacts"][1] = copy.deepcopy(recovery["artifacts"][0])
    result = run_recovery(recovery)
    assert result["error"] is not None
    assert result["outputs"] == {}
