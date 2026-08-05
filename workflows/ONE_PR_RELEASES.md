# One-PR container releases

Recipe updates now build and test before merge, then promote the exact tested
artifacts after merge. They no longer create a second release-metadata PR.

## Flow

1. A recipe-only PR runs `PR container candidate` with `contents: read` and no
   secrets on an ephemeral ARC runner. Each recipe fans out into one candidate
   per concrete container it declares, so a recipe listing both architectures
   builds `<name>` on the ARC pool and `<name>_arm64` on an ephemeral ARM64
   runner.
2. Each candidate builds a Docker archive and SIF, runs the deploy/fulltest and
   Dive checks, generates the release JSON preview, and stores everything for
   30 days under its own container identity.
3. A trusted `workflow_run` posts one approval summary on recipe PRs. It shows
   the exact recipe, container, version, architecture, build result,
   deploy/fulltest counts, Dive result, candidate bundle, and post-merge publish
   destinations. Later runs update that comment in place. The reporter downloads
   only compact, schema-checked JSON summaries; it never opens candidate images
   or test logs.
4. `Container release gate` is the stable required check for branch rules.
5. After merge, `Promote merged container candidate` runs the same trusted
   release planner over the merge commit. It exits successfully when no
   candidate was required; otherwise it selects the successful run
   for the exact PR head SHA. It verifies the PR number, recipe fingerprint,
   artifact hashes, and release metadata before publishing the tested files.
   The variant a candidate claims is re-resolved against the merged recipe, so a
   candidate cannot promote itself into an identity the recipe never declared.
6. The promoter commits the generated JSON to `releases/` on `main`. That push
   triggers the existing apps/webapps update workflows.
7. For each default x86_64 candidate with an `OpenReconLabel.json`, the promoter
   opens or reuses an OpenRecon metadata PR after the release metadata push.

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
- Keep `QUAY_API_TOKEN` configured with permission to change repository
  visibility. Quay creates new repositories as private, so promotion uses this
  token to preserve anonymous pulls after the first push.
- Keep ARC runners ephemeral. Fork approval remains the point where maintainers
  decide whether untrusted recipe build commands may run.

## Release planning

The planner reads the base and head `build.yaml` files as YAML data. It does not
render Jinja or execute any code from the pull request. Changes that affect only
`auto_update`, semantically unchanged YAML, or `fulltest.yaml` are currently
classified as source-only: they are validated, but the existing container is
preserved and no candidate is built or promoted. This is a behavioural release
projection, not a claim that rebuilding would produce byte-identical images;
the current image still embeds the raw `build.yaml` and README.

Every other recipe definition change is deliberately fail-closed and requires
a candidate. A changed recipe-local file such as `install.sh` also requires a
candidate even when `build.yaml` itself is unchanged. The explicit top-level
field policy is checked against the accepted recipe schema so adding a field
cannot silently broaden the source-only tier.

Registry and object-storage credentials are the same secrets used by the legacy
build workflow. GHCR and S3 are release-critical; Docker Hub, Nectar, and Quay
remain best-effort mirrors.

`Validate Recipe YAML` remains a separate status check but no longer comments on
the PR. Its schema validation is also part of the container candidate gate, so
the single approval summary covers it without sending a second notification.
When a version bump changes both `build.yaml` and `fulltest.yaml`, the legacy
released-container test workflow also defers to the candidate gate instead of
retesting the previous published image and posting its own result comments.
