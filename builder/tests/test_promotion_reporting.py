import json
from pathlib import Path
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
HEAD = "a" * 40
MERGE = "b" * 40
CURRENT = "c" * 40
REPOSITORY = "neurodesk/neurocontainers"
WORKFLOW = ".github/workflows/promote-container-candidate.yml"


def promotion(run_id, head, event, conclusion="failure"):
    return {
        "id": run_id,
        "run_number": run_id,
        "run_attempt": 1,
        "head_sha": head,
        "head_branch": "main",
        "event": event,
        "path": WORKFLOW,
        "repository": {"full_name": REPOSITORY},
        "head_repository": {"full_name": REPOSITORY},
        "status": "completed" if conclusion else "in_progress",
        "conclusion": conclusion,
        "html_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}",
        "created_at": f"2026-10-16T18:{run_id - 900:02}:00Z",
        "run_started_at": f"2026-10-16T18:{run_id - 900:02}:00Z",
    }


def identity(run_id=901, pr=3661, head=HEAD, merge=MERGE, source=900, attempt=1):
    return {
        "id": 1000 + run_id,
        "name": f"promotion-identity-v1-{pr}-{head}-{merge}-{source}-{attempt}",
        "expired": False,
        "workflow_run": {"id": run_id, "head_sha": CURRENT, "head_branch": "main"},
    }


@pytest.fixture
def recovery():
    return {
        "runs": [
            promotion(901, CURRENT, "workflow_dispatch", "success"),
            promotion(900, MERGE, "push"),
        ],
        "identities": {"901": [identity()]},
        "promotion_jobs": {"901": [{"name": "finalize", "conclusion": "success"}]},
        "existing": (
            "<!-- one-pr-container-candidate -->\n"
            "<!-- container-release-outcome: passed -->\n"
            "<!-- container-release-status:start -->\n"
            "## promotion finalized\n"
            "**Promotion:** [run 901](https://example.test/recovery) success\n"
            "<!-- container-release-status:end -->"
        ),
    }


def run_reporter(tmp_path, recovery):
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/report-container-candidate.yml").read_text()
    )
    script = workflow["jobs"]["report"]["steps"][1]["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const observed = {calls: [], warnings: []};
const pr = {
  number: 3661, head: {sha: input.head}, state: 'closed',
  merged_at: '2026-10-16T18:00:00Z', merge_commit_sha: input.merge,
  title: 'fixtures', user: {login: 'test'}, html_url: 'https://example.test/pr',
  changed_files: 1, additions: 1, deletions: 0,
  ...input.pr,
};
const candidate = {
  id: 800, run_number: 800, head_sha: input.head, conclusion: 'success',
  pull_requests: [{number: 3661}], html_url: 'https://example.test/candidate',
};
const existing = {id: 1, user: {type: 'Bot'}, body: input.existing};
const runs = args => input.runs.filter(run =>
  (!args.head_sha || run.head_sha === args.head_sha) &&
  (!args.event || run.event === args.event)
);
const actions = {
  listJobsForWorkflowRun: 'jobs', listWorkflowRunArtifacts: 'artifacts',
  listRepoWorkflows: async () => ({data: {workflows: [{id: 10, path: input.workflow}]}}),
  listWorkflowRuns: async args => ({data: {workflow_runs: runs(args)}}),
  getWorkflowRun: async args => {
    observed.calls.push({method: 'getRun', ...args});
    const run = input.runs.find(run => run.id === args.run_id);
    if (!run) throw new Error('Run not found');
    return {data: run};
  },
};
const github = {
  rest: {
    actions,
    pulls: {get: async () => ({data: pr}), listFiles: 'files'},
    issues: {
      listComments: 'comments',
      updateComment: async args => {observed.updated = args.body;},
      createComment: async args => {observed.created = args.body;},
    },
  },
  paginate: async (method, args) => {
    observed.calls.push({method: method === actions.listWorkflowRuns ? 'runs' : method, ...args});
    if (method === actions.listWorkflowRuns) return runs(args);
    if (method === 'files') return [{filename: 'recipes/qsmxt/build.yaml'}];
    if (method === 'comments') return input.existing ? [existing] : [];
    if (method === 'artifacts') {
      if (input.artifact_error && args.run_id !== 800) throw new Error('Artifacts unavailable');
      return input.identities[args.run_id] || [];
    }
    if (method === 'jobs') return args.run_id === 800
      ? [{name: 'candidate (qsmxt | 10.0.0 | x86_64)', steps: []}]
      : input.promotion_jobs[args.run_id] || [];
    throw new Error('Unexpected API call');
  },
};
(async () => {
  await new AsyncFunction('github', 'context', 'core', 'require', input.script)(
    github, {repo: {owner: 'neurodesk', repo: 'neurocontainers'}, payload: {workflow_run: candidate}},
    {info: () => {}, warning: message => observed.warnings.push(message), setFailed: message => {throw Error(message);}},
    require
  );
  process.stdout.write(JSON.stringify(observed));
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
    result = subprocess.run(
        [
            "node", "-e", harness,
            json.dumps({**recovery, "script": script, "head": HEAD, "merge": MERGE, "workflow": WORKFLOW}),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_delayed_candidate_reporter_uses_recovery_on_newer_main(tmp_path, recovery):
    result = run_reporter(tmp_path, recovery)
    assert "promotion completed" in result["updated"]
    assert "[run 901]" in result["updated"]
    assert "[run 900]" not in result["updated"]
    assert "### What was tested" in result["updated"]
    assert "qsmxt 10.0.0 (x86_64)" in result["updated"]
