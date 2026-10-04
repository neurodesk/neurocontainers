# Periodic cold validation

The daily workflow selects at most eight concrete recipe, variant and
architecture targets. It uses native runners, stages the current inputs, builds
with `--no-cache --pull`, converts the candidate to SIF, and executes deploy
checks and the fulltest. It has read-only repository permissions and no registry
login, push, release metadata generation or version update.

`cold-build-policy.json` records Ubuntu LTS base image lifecycles and selects
only versions under standard security maintenance. Canonical lists end months at
<https://ubuntu.com/about/release-cycle>. The policy expires at the start of each
listed end month to avoid implying an exact vendor support day. It does not
assume Ubuntu Pro coverage or infer support for derivatives. Other bases and
missing fulltests appear as explicit exclusions. Digest-qualified Ubuntu images
retain their tag's lifecycle policy. A lifecycle policy does not prove that every
installed package receives security maintenance.

```bash
python -m workflows.cold_build plan --state /tmp/state.json \
  --output /tmp/cold-plan.json --today 2026-10-03
python -m workflows.cold_build run --target /tmp/one-target.json \
  --output /tmp/cold-result
```

Extract a target object from `cold-plan.json` into `one-target.json` before the
second command. Local execution needs Docker Buildx, Go, Apptainer, uv, the
installed builder and the fulltest's data tools. Plan generation needs PyYAML.

Successful targets become due after 45 days; failures become due for retry
after seven days.
Selection prefers the oldest attempt so repeated failures do not consume every
batch. The report lists targets without a current successful build or with a
success older than 60 days. It flags inventory growth beyond the theoretical
60-day batch capacity. That is an alert, not a promise that failing recipes can
be made successful. A failed runner setup also records an unsuccessful attempt.

Each observation records the commit, recipe and Dockerfile hashes, staged input
hashes, built image ID, time, machine and result. CI retains observations and
failure diagnostics for 90 days and restores state from prior completed runs.
If retained state is unavailable, it warns and restarts coverage. Operators
should archive the state artifact for longer retention. The jobs do not start on
untrusted pull requests. Disabling the workflow rolls back scheduled validation.

Issue #2593 remains open for CVMFS retention policy and the module lockfile's
consumer, schema and digest semantics. ARM client acceptance belongs to #1252
and CVMFS scan deployment to #61. This PR supplies the periodic validation unit;
it does not decide those infrastructure policies.
