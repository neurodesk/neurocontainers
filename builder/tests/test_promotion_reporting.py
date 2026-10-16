from datetime import datetime, timedelta, timezone
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
    started = (
        datetime(2026, 10, 16, 18, tzinfo=timezone.utc)
        + timedelta(minutes=run_id - 900)
    ).isoformat()
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
        "created_at": started,
        "run_started_at": started,
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
  listWorkflowRuns: async args => ({data: {workflow_runs: runs(args).slice(0, args.per_page || 30)}}),
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
    if (method === 'jobs') {
      if (args.run_id === 800) return [{name: 'candidate (qsmxt | 10.0.0 | x86_64)', steps: []}];
      if (args.filter !== 'latest') throw new Error('Must read the latest job attempts');
      return input.promotion_jobs[args.run_id] || [];
    }
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
            "node",
            "-e",
            harness,
            json.dumps(
                {
                    **recovery,
                    "script": script,
                    "head": HEAD,
                    "merge": MERGE,
                    "workflow": WORKFLOW,
                }
            ),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_delayed_candidate_reporter_uses_recovery_on_newer_main(tmp_path, recovery):
    reports = tmp_path / "candidate-reports"
    reports.mkdir()
    (reports / "candidate-report.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "container": "qsmxt",
                "recipe": "qsmxt",
                "version": "10.0.0",
                "architecture": "x86_64",
                "pr_number": 3661,
                "head_sha": HEAD,
                "candidate_tag": f"nd-candidate-qsmxt:{HEAD[:12]}",
                "tests": {
                    "total": 3,
                    "passed": 3,
                    "fulltest_total": 2,
                    "fulltest_passed": 2,
                    "suites_total": 1,
                    "suites_passed": 1,
                    "passed_tests": ["runtime smoke"],
                },
                "dive": {"status": "success"},
            }
        )
    )
    result = run_reporter(tmp_path, recovery)
    assert "promotion completed" in result["updated"]
    assert "[run 901]" in result["updated"]
    assert "[run 900]" not in result["updated"]
    assert "### What was tested" in result["updated"]
    assert "qsmxt 10.0.0 (x86_64)" in result["updated"]
    assert "Deploy check: **1/1 passed**" in result["updated"]
    assert "Runtime/fulltest: **2/2 passed in 1/1 suite(s)**" in result["updated"]
    assert "runtime smoke" in result["updated"]


@pytest.mark.parametrize(
    "conclusion,status",
    [
        ("failure", "completed"),
        ("cancelled", "completed"),
        (None, "queued"),
        (None, "waiting"),
        (None, "in_progress"),
    ],
)
def test_newest_matching_operation_replaces_old_success(
    tmp_path, recovery, conclusion, status
):
    newer = promotion(902, CURRENT, "workflow_dispatch", conclusion)
    newer["status"] = status
    recovery["runs"].append(newer)
    recovery["identities"]["902"] = [identity(run_id=902)]
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 902]" in body
    assert "promotion completed" not in body
    assert ("promotion is running" if conclusion is None else f"— {conclusion}") in body


@pytest.mark.parametrize(
    "field,value",
    [
        ("pr", 3662),
        ("head", "d" * 40),
        ("merge", "e" * 40),
        ("source", 999),
        ("attempt", 2),
    ],
)
def test_identity_must_match_the_merged_candidate(tmp_path, recovery, field, value):
    recovery["identities"]["901"] = [identity(**{field: value})]
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "promotion completed" not in body


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", ".github/workflows/untrusted.yml"),
        ("head_branch", "feature"),
        ("event", "pull_request"),
        ("repository", {"full_name": "fork/containers"}),
        ("head_repository", {"full_name": "fork/containers"}),
    ],
)
def test_dispatch_must_be_the_real_main_workflow(tmp_path, recovery, field, value):
    recovery["runs"][0][field] = value
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "promotion completed" not in body


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 999),
        ("head_sha", "d" * 40),
        ("head_branch", "feature"),
        ("path", ".github/workflows/untrusted.yml"),
        ("event", "workflow_dispatch"),
        ("repository", {"full_name": "fork/containers"}),
        ("head_repository", {"full_name": "fork/containers"}),
    ],
)
def test_staged_source_requires_an_authenticated_original_push(
    tmp_path, recovery, field, value
):
    recovery["runs"][1][field] = value
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 901]" not in body
    assert "promotion completed" not in body


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "expired",
        "malformed",
        "wrong_owner",
        "wrong_sha",
        "wrong_branch",
        "contradiction",
        "api_failure",
    ],
)
def test_unusable_evidence_never_preserves_a_success_comment(tmp_path, recovery, fault):
    artifact = recovery["identities"]["901"][0]
    if fault == "missing":
        recovery["identities"] = {}
    elif fault == "expired":
        artifact["expired"] = True
    elif fault == "malformed":
        artifact["name"] = "promotion-identity-v1-malformed"
    elif fault == "wrong_owner":
        artifact["workflow_run"]["id"] = 800
    elif fault == "wrong_sha":
        artifact["workflow_run"]["head_sha"] = HEAD
    elif fault == "wrong_branch":
        artifact["workflow_run"]["head_branch"] = "feature"
    elif fault == "contradiction":
        recovery["identities"]["901"].append(identity(pr=3662))
    else:
        recovery["artifact_error"] = True
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "promotion completed" not in body
    assert "Reporting note:" in body


def test_latest_attempt_uses_immutable_prior_attempt_identity(tmp_path, recovery):
    recovery["runs"][0].update(run_attempt=2, status="in_progress", conclusion=None)
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 901]" in body
    assert "promotion is running" in body
    assert "promotion completed" not in body


def test_new_attempt_can_replace_expired_identity(tmp_path, recovery):
    recovery["runs"][0]["run_attempt"] = 2
    recovery["identities"]["901"][0]["expired"] = True
    recovery["identities"]["901"].append(identity(attempt=2))
    assert "promotion completed" in run_reporter(tmp_path, recovery)["updated"]


def test_latest_started_attempt_wins_over_run_creation_order(tmp_path, recovery):
    recovery["runs"][1].update(run_attempt=2, run_started_at="2026-10-16T19:00:00Z")
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "promotion failed" in body


def test_success_without_a_successful_finalizer_is_not_publication(tmp_path, recovery):
    recovery["promotion_jobs"]["901"] = [{"name": "finalize", "conclusion": "skipped"}]
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 901]" in body
    assert "finalization is unverified" in body
    assert "promotion completed" not in body


def test_dispatch_without_staged_source_and_without_existing_comment(
    tmp_path, recovery
):
    recovery["identities"]["901"] = [identity(source=0)]
    recovery["existing"] = ""
    result = run_reporter(tmp_path, recovery)
    assert "promotion completed" in result["created"]
    assert "[run 901]" in result["created"]


@pytest.mark.parametrize("conclusion", ["failure", "success", None])
def test_legacy_push_needs_no_identity_artifact(tmp_path, recovery, conclusion):
    recovery["runs"] = [promotion(900, MERGE, "push", conclusion)]
    recovery["identities"] = {}
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "Reporting note:" not in body
    if conclusion == "success":
        assert "promotion completed" in body


def test_legacy_dispatch_is_not_associated_from_title_or_comment(tmp_path, recovery):
    recovery["identities"] = {}
    recovery["runs"][0]["display_title"] = f"Recover PR #3661 {HEAD} {MERGE} source 900"
    recovery["runs"][0]["head_sha"] = MERGE
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 900]" in body
    assert "promotion completed" not in body


def test_paginated_runs_and_artifacts_include_late_matching_records(tmp_path, recovery):
    unrelated = [
        promotion(910 + i, CURRENT, "workflow_dispatch", "success") for i in range(101)
    ]
    for run in unrelated:
        recovery["identities"][str(run["id"])] = [identity(run_id=run["id"], pr=3662)]
    recovery["runs"] = unrelated + recovery["runs"]
    recovery["identities"]["901"] = [{"name": f"other-{i}"} for i in range(101)] + [
        identity()
    ]
    body = run_reporter(tmp_path, recovery)["updated"]
    assert "[run 901]" in body
    assert "promotion completed" in body


def test_candidate_head_mismatch_still_prevents_rewrite(tmp_path, recovery):
    recovery["pr"] = {"head": {"sha": "d" * 40}}
    result = run_reporter(tmp_path, recovery)
    assert "updated" not in result
    assert "created" not in result


def test_dispatch_identity_is_emitted_by_actual_resolver_step(tmp_path):
    workflow = yaml.safe_load((ROOT / WORKFLOW).read_text())
    steps = workflow["jobs"]["resolve"]["steps"]
    step = next(step for step in steps if step.get("id") == "reporting_identity")
    script = step["with"]["script"]
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
(async () => {
  const output = {};
  await new AsyncFunction('process', 'core', 'require', input.script)(
    {env: input.env}, {setOutput: (key, value) => {output[key] = value;}}, require
  );
  process.stdout.write(JSON.stringify(output));
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            harness,
            json.dumps(
                {
                    "script": script,
                    "env": {
                        "PR_NUMBER": "3661",
                        "HEAD_SHA": HEAD,
                        "MERGE_SHA": MERGE,
                        "STAGED_RUN_ID": "900",
                        "GITHUB_RUN_ATTEMPT": "1",
                        "RUNNER_TEMP": str(tmp_path),
                    },
                }
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    name = json.loads(result.stdout)["name"]
    assert name == identity()["name"]
    assert (tmp_path / "promotion-identity.txt").read_text() == name + "\n"
    assert step["continue-on-error"] is True
    upload = next(
        step for step in steps if step.get("name") == "Retain the dispatch association"
    )
    assert upload["continue-on-error"] is True
