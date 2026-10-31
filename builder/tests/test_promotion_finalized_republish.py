"""Run await_candidate's script for dispatches of already finalized merges."""

import json
from pathlib import Path
import subprocess

import pytest
import yaml


HEAD = "a" * 40
MERGE = "b" * 40
REPOSITORY = "neurodesk/neurocontainers"
WORKFLOW = ".github/workflows/promote-container-candidate.yml"


def push_run(run_id, status="completed", head=MERGE, path=WORKFLOW):
    return {
        "id": run_id,
        "status": status,
        "head_sha": head,
        "event": "push",
        "head_branch": "main",
        "path": path,
        "repository": {"full_name": REPOSITORY},
        "head_repository": {"full_name": REPOSITORY},
    }


def run_await(event, pushes, finalize, bundles=True):
    workflow = yaml.safe_load(Path(WORKFLOW).read_text())
    script = workflow["jobs"]["await_candidate"]["steps"][0]["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const outputs = {};
const jobsFor = [];
const github = {
  rest: {actions: {
    listRepoWorkflows: async () => ({data: {workflows: [{
      id: 42, path: '.github/workflows/pr-container-candidate.yml'
    }]}}),
    listWorkflowRuns: async query => query.event === 'push'
      ? {data: {workflow_runs: input.pushes}}
      : {data: {workflow_runs: [{
          id: 11, status: 'completed', conclusion: 'success',
          head_sha: input.head, event: 'pull_request'
        }]}},
    listJobsForWorkflowRun: 'jobs',
    listWorkflowRunArtifacts: 'artifacts',
  }},
  paginate: async (method, query) => {
    if (method === 'jobs') {
      jobsFor.push(query.run_id);
      return [{name: 'finalize', conclusion: input.finalize}];
    }
    return input.bundles
      ? [{id: 100, name: 'candidate-demo-' + input.head, expired: false}]
      : [{id: 101, name: 'candidate-report-demo-' + input.head, expired: false}];
  }
};
(async () => {
  let error = null;
  try {
    await new AsyncFunction('github', 'context', 'core', 'process', input.script)(
      github,
      {eventName: input.event, repo: {owner: 'neurodesk', repo: 'neurocontainers'}},
      {info: () => {}, setOutput: (key, value) => { outputs[key] = value; }},
      {env: {HEAD_SHA: input.head, MERGE_SHA: input.merge}}
    );
  } catch (failure) { error = failure.message; }
  process.stdout.write(JSON.stringify({outputs, jobsFor, error}));
})();
"""
    payload = {
        "script": script,
        "event": event,
        "pushes": pushes,
        "finalize": finalize,
        "bundles": bundles,
        "head": HEAD,
        "merge": MERGE,
    }
    result = subprocess.run(
        ["node", "-e", harness, json.dumps(payload)],
        check=True,
        text=True,
        capture_output=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_dispatch_refuses_a_merge_whose_push_promotion_finalized():
    observed = run_await("workflow_dispatch", [push_run(5), push_run(9)], "success")

    assert "already finalized" in observed["error"]
    assert observed["jobsFor"] == [9]
    assert observed["outputs"] == {"unavailable": "finalized"}


@pytest.mark.parametrize(
    "pushes, finalize",
    [
        ([push_run(9)], "failure"),
        ([push_run(9)], "skipped"),
        ([push_run(9, status="in_progress")], "success"),
        ([], "success"),
        ([push_run(9, head="c" * 40)], "success"),
        ([push_run(9, path=".github/workflows/manual-build.yml")], "success"),
    ],
)
def test_dispatch_still_recovers_a_merge_that_did_not_finalize(pushes, finalize):
    observed = run_await("workflow_dispatch", pushes, finalize)

    assert observed["error"] is None
    assert json.loads(observed["outputs"]["artifacts"]) == [
        {"id": 100, "name": "candidate-demo-" + HEAD}
    ]


def test_push_promotion_never_checks_for_an_earlier_finalization():
    observed = run_await("push", [push_run(9)], "success")

    assert observed["error"] is None
    assert observed["jobsFor"] == []


def test_missing_bundles_are_reported_as_unavailable():
    observed = run_await("push", [], "success", bundles=False)

    assert "manual-build" in observed["error"]
    assert observed["outputs"] == {"unavailable": "missing"}
