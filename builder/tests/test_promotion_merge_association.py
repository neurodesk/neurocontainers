"""Run the actual promotion resolver against delayed GitHub associations."""

import json
from pathlib import Path
import subprocess

import pytest
import yaml


MERGE = "b" * 40
HEAD = "a" * 40


def pull(number=7, merge=MERGE, merged=True, base="main"):
    return {
        "number": number,
        "merge_commit_sha": merge,
        "merged_at": "2026-10-14T05:55:39Z" if merged else None,
        "base": {"ref": base},
        "head": {"sha": HEAD},
    }


def resolve(responses, event="push", api_error=None, dispatch=None):
    workflow = yaml.safe_load(
        Path(".github/workflows/promote-container-candidate.yml").read_text()
    )
    step = next(s for s in workflow["jobs"]["resolve"]["steps"] if s.get("id") == "pr")
    harness = r"""
const input = JSON.parse(process.argv[1]);
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
const outputs = {};
const delays = [];
let polls = 0;
let gets = 0;
const github = {rest: {
  repos: {listPullRequestsAssociatedWithCommit: async query => {
    if (query.commit_sha !== input.merge || query.owner !== 'example' || query.repo !== 'containers')
      throw new Error('Merge identity filter missing');
    polls++;
    if (input.api_error) throw new Error(input.api_error);
    return {data: input.responses[Math.min(polls - 1, input.responses.length - 1)]};
  }},
  pulls: {get: async query => {
    gets++;
    if (query.pull_number !== 7) throw new Error('Unexpected dispatch PR');
    return {data: input.dispatch};
  }}
}};
global.setTimeout = (callback, delay) => {delays.push(delay); callback(); return 0;};
(async () => {
  let error = null;
  try {
    await new AsyncFunction('github', 'context', 'core', input.script)(github, {
      eventName: input.event, sha: input.merge,
      repo: {owner: 'example', repo: 'containers'},
      payload: {inputs: {pr_number: '7'}}
    }, {info: () => {}, notice: () => {}, setOutput: (k, v) => {outputs[k] = v;}});
  } catch (failure) {error = failure.message;}
  process.stdout.write(JSON.stringify({outputs, polls, gets, delays, error}));
})();
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            harness,
            json.dumps(
                {
                    "responses": responses,
                    "merge": MERGE,
                    "event": event,
                    "api_error": api_error,
                    "dispatch": dispatch,
                    "script": step["with"]["script"],
                }
            ),
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=10,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize(
    "responses, polls",
    [
        ([[], [], [pull()]], 3),
        ([[pull(merged=False)], [pull()]], 2),
        ([[pull(base="other")], [pull()]], 2),
        ([[pull(8, merge="c" * 40), pull()]], 1),
    ],
)
def test_delayed_merge_is_promoted_with_exact_candidate_identity(responses, polls):
    actual = resolve(responses)
    assert actual["error"] is None
    assert actual["polls"] == polls
    assert actual["delays"] == [10000] * (polls - 1)
    assert actual["outputs"] == {
        "should_promote": "true",
        "pr_number": "7",
        "head_sha": HEAD,
        "merge_sha": MERGE,
    }


def test_no_merge_association_remains_a_bounded_safe_skip():
    actual = resolve([[]])
    assert actual["error"] is None
    assert actual["polls"] == 6
    assert actual["delays"] == [10000] * 5
    assert actual["outputs"] == {"should_promote": "false"}


def test_ambiguous_merge_is_still_rejected():
    actual = resolve([[pull(8, merge="c" * 40), pull(9, merge="d" * 40)]])
    assert "Could not uniquely resolve" in actual["error"]
    assert actual["polls"] == 1
    assert actual["delays"] == []
    assert actual["outputs"] == {}


def test_api_errors_are_not_hidden_as_missing_associations():
    actual = resolve([[]], api_error="Forbidden")
    assert actual["error"] == "Forbidden"
    assert actual["polls"] == 1
    assert actual["delays"] == []
    assert actual["outputs"] == {}


def test_explicit_recovery_keeps_its_existing_merged_pr_checks():
    actual = resolve([], event="workflow_dispatch", dispatch=pull())
    assert actual["error"] is None
    assert actual["gets"] == 1 and actual["polls"] == 0
    assert actual["delays"] == []
    assert actual["outputs"]["head_sha"] == HEAD
    rejected = resolve([], event="workflow_dispatch", dispatch=pull(merged=False))
    assert "is not merged into main" in rejected["error"]
    assert rejected["outputs"] == {}
