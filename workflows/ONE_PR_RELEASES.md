# One-PR container releases

Recipe updates now build and test before merge, then promote the exact tested
artifacts after merge. They no longer create a second release-metadata PR.

## Flow

1. A recipe-only PR runs `PR container candidate` with `contents: read` and no
   secrets on an ephemeral ARC runner.
2. It builds a Docker archive and SIF, runs the deploy/fulltest and Dive checks,
   generates the release JSON preview, and stores everything for 30 days.
3. A trusted `workflow_run` posts download/testing instructions on the PR but
   never opens or executes its artifacts.
4. `Container release gate` is the stable required check for branch rules.
5. After merge, `Promote merged container candidate` selects the successful run
   for the exact PR head SHA. It verifies the PR number, recipe fingerprint,
   artifact hashes, and release metadata before publishing the tested files.
6. The promoter commits the generated JSON to `releases/` on `main`. That push
   triggers the existing apps/webapps update workflows.

Manual builds remain available as a recovery path. The old push-to-main
`auto-build` workflow is removed so recipe changes cannot start an untested
second build.

## Required repository configuration

- Make `Container release gate` a required pull-request check.
- Create a GitHub App with repository `Contents: read/write`, install it only on
  this repository, and add the App as the ruleset bypass actor for `main`.
- Store its credentials as `NEUROCONTAINERS_RELEASE_APP_ID` and
  `NEUROCONTAINERS_RELEASE_APP_PRIVATE_KEY`.
- Store the Nectar registry account name as
  `REGISTRY_RC_NECTAR_ORG_AU_USERNAME`; its key remains in
  `REGISTRY_RC_NECTAR_ORG_AU_CLI_KEY`.
- Keep ARC runners ephemeral. Fork approval remains the point where maintainers
  decide whether untrusted recipe build commands may run.

Registry and object-storage credentials are the same secrets used by the legacy
build workflow. GHCR and S3 are release-critical; Docker Hub, Nectar, and Quay
remain best-effort mirrors.
