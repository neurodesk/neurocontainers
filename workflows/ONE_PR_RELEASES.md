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
   30 days under its own container identity. Dive's wasted-percentage rule is
   applied only once the absolute waste is worth judging: below
   `DIVE_RATIO_FLOOR_BYTES` that ratio measures the shared neurodocker
   preamble rather than the recipe, so `one_pr_release.py dive-gate` waives
   that one rule and the step says so. Every other Dive rule, and this one
   above the floor, still fails the candidate.
3. A trusted `workflow_run` posts one action-first lifecycle summary on recipe
   PRs. The heading names the exact container, version, and architecture and
   tells the maintainer whether to merge, fix a candidate, wait for promotion,
   or retry a failed promotion. The summary separates deploy and fulltest
   counts, names the passed or failed checks, reports Dive findings, and gives
   artifact-specific download and smoke-test commands with size and expiry.
   Later candidate and promotion runs update that comment in place. When the
   candidate outcome materially changes from failed to passed or passed to
   failed, the reporter also posts a short, commit-linked PR notification. This
   makes GitHub notify subscribers without duplicating the detailed report on
   every push. Reruns of the same outcome do not create another notification,
   and completed runs for superseded PR heads are ignored.
   Fork PRs carry their PR number and head SHA in the compact report; the trusted
   reporter fetches that PR and verifies its repository, branch, and SHA before
   commenting. The reporter downloads only compact, schema-checked JSON summaries;
   it never opens candidate images or test logs.
4. `Container release gate` is the stable required check for branch rules.
5. After merge, `Promote merged container candidate` runs the same trusted
   release planner over the merge commit. It exits successfully when no
   candidate was required; otherwise a GitHub-hosted job waits for the candidate
   run for the exact PR head SHA to finish before allocating the ARC publishing
   runner. It verifies the PR number, recipe fingerprint, artifact hashes, and
   release metadata before publishing the tested files.
   The variant a candidate claims is re-resolved against the merged recipe, so a
   candidate cannot promote itself into an identity the recipe never declared.
6. The promoter commits the generated JSON to `releases/` on `main`. That push
   triggers the existing apps/webapps update workflows.
7. For each default x86_64 candidate with an `OpenReconLabel.json`, the promoter
   opens or reuses an OpenRecon metadata PR after the release metadata push. If
   the version, label, and README are unchanged, it dispatches the OpenRecon
   build directly so same-version container rebuilds still propagate.
   An OpenRecon `params.sh` that pins a dated image tag is repointed at the build
   date this release published, so the OpenRecon build pulls the image the
   promoter just pushed. When no release metadata resolves that date, the tag is
   left alone and the PR body says so.

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
`auto_update`, `copyright`, `draft`, `icon`, documentation (`readme`, `readme_url`,
`structured_readme`), literal `categories`, semantically unchanged YAML, or
`fulltest.yaml` are currently classified as source-only. The workflow validates
them, but preserves the existing container and builds or promotes no candidate.
This is a behavioural release projection, not a claim that rebuilding would
produce byte-identical images; the current image still embeds the raw
`build.yaml` and README.

Documentation may use simple context substitutions such as
`{{ context.version }}`. More complex templates remain candidate-required because
rendering can have side effects. Documentation inside an existing image remains
the version shipped with that image; updated documentation is available in the
recipe source until the next image release.

The apps.json workflow also runs on recipe definition changes. It reads literal
categories from the current source recipe (including the source recorded for
named variants), replacing the historical category union in the catalog. It
preserves all published app identities and build dates and does not rewrite
release JSON or publish images. Missing recipes and templated categories retain
the categories from release metadata.

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
