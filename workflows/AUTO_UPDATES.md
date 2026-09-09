# Automatic recipe updates

The daily `Auto Update Recipes` workflow reads every recipe's `auto_update` policy.
It opens at most five new PRs per run. Each PR updates `build.yaml` and the sibling
`fulltest.yaml`, validates the recipe, and generates its Dockerfile before pushing.
Container CI then builds and tests the proposed version. Upstream changes can
still require recipe fixes, such as a renamed release archive or a new dependency.

Changes confined to `auto_update` use the existing source-only release path and
skip container builds. Changes to install commands still require candidate builds.

## Configure a recipe

Every recipe requires an explicit policy. `sf-init` inserts a GitHub source
placeholder; replace it before validation. CI checks the whole recipe collection,
so a new recipe cannot silently escape update tracking.

Select the source that publishes the software the recipe actually installs.
For example, a package installed from PyPI should normally track PyPI rather than
a repository's tags. Use `{{ context.version }}` in its install command, base image,
or declared download URL. Update paths and launchers that contain that version too.
A fixed source commit, an unversioned install, or a dependency's version does not
establish a working update path for the main software. Validation checks source
acquisition inputs and rejects version references confined to runtime paths,
tests, or unused variables. For Git installs, use `git clone --branch` with the
version and repository together; a separate checkout cannot establish which
repository receives the tag. This is a static check for common mistakes, not
proof that arbitrary shell code installs the right program. Reviewers still
confirm that the selected upstream, archive layout, and runtime tests match the
main software.

```yaml
auto_update:
  method: github_release
  repo: QSMxT/QSMxT
```

Supported methods and source fields:

| Method | Required source | Selection |
| --- | --- | --- |
| `github_release` | `repo: owner/project` | Highest stable release; falls back to tags only when there are no releases |
| `github_tags` | `repo: owner/project` | Highest stable tag |
| `pypi` | `package: project-name` | Highest stable version with a non-yanked distribution |
| `dockerhub` | `repo: owner/image` | Highest stable image tag |
| `oci` | `url: https://registry/v2/repository/tags/list` | Highest stable image tag, with anonymous registry authentication |
| `npm` | `package: package-name` | Stable `latest` dist-tag, or highest stable version when unavailable |
| `webpage` | `url` and `version_regex` | Highest stable version extracted from an HTTPS page or JSON feed |

Use `version_regex` to restrict a tag family and extract the recipe version.
It must contain a named `version` group. Tag providers match the whole tag;
`webpage` searches the response body.

```yaml
auto_update:
  method: dockerhub
  repo: deepmi/fastsurfer
  version_regex: 'cpu-v(?P<version>\d+\.\d+\.\d+)'
```

For GitHub release filenames that contain hashes or change between releases,
add an `assets` mapping. Each key is a declared `files[].name`; its regex must
match exactly one asset in the selected release.

```yaml
auto_update:
  method: github_release
  repo: SuperElastix/elastix
  assets:
    downloaded_file: 'elastix-[0-9.]+-(?:linux|ubuntu)\.zip'
```

The updater rewrites that file's URL from the selected release, preserving
`{{ context.version }}` in the tag and, when present, the filename. Missing or ambiguous matches
fail the update. Files with pinned checksums are rejected until their checksum
handling is updated. The main install command continues to use `get_file`.

When GitHub tags change prefixes between releases, persist the exact source ref
in a recipe variable and name it with `tag_variable`. The updater replaces it
with the selected upstream tag while updating the recipe version. For example:

```yaml
variables:
  upstream_tag: '1.9.3'
auto_update:
  method: github_release
  repo: CyclotronResearchCentre/bidsme
  tag_variable: upstream_tag
```

Use `git clone --branch {{ context.upstream_tag }}` for that repository. A release
tagged `v1.9.6` then sets `version: 1.9.6` and `upstream_tag: v1.9.6` together.
The initial tag must map to the current recipe version using the configured
regex. Keep the variable as a plain or quoted scalar, without YAML anchors.
Selection compares software versions; a new build suffix for the same software
version does not trigger another PR.

The default numeric scheme excludes prereleases and development versions.
For software distributed only as prereleases, such as DAFNE, explicitly set
`include_prereleases: true`. GitHub drafts remain excluded.
For vendor releases such as MATLAB's `2025b` or BrainSuite's `23a`, set
`version_scheme: year_letter`. GitHub draft and prerelease flags still apply.
Recipes already storing a leading `v` keep that prefix when bumped.

## Independent software and container versions

Use `method: sources` when a container combines packages, installs a source
snapshot, or has a container version that differs from the installed software.
Each source targets the variable or declared file that selects its actual bytes.
A source version is never substituted for an unrelated container label.

```yaml
version: 1.0.0
variables:
  upstream_version: 2.4.0
  model_commit: 0123456789abcdef0123456789abcdef01234567

auto_update:
  method: sources
  local: []
  sources:
    - id: application
      method: pypi
      package: example-tool
      target:
        variable: upstream_version
        fulltest_variable: upstream_version
    - id: models
      method: git_commit
      url: https://huggingface.co/example/models
      ref: refs/heads/main
      target:
        variable: model_commit
```

The install command must use `{{ context.upstream_version }}` and the model
checkout must use `{{ context.model_commit }}`. A sibling `fulltest.yaml` is
required. Its `upstream_version` scalar must match the recipe variable; assertions
use `${upstream_version}` when checking software versions and `${version}` when
checking the container version.

All source observations resolve before the updater writes files. One or several
changed inputs increment the container revision once, such as `1.0.0.post1`.
Opaque container labels use `.r1`. The plan updates the sibling test version and
mapped test variables together. Repeating the same observation produces no
further change. Stable release and mapped software versions cannot move backward.
GitHub commit updates require a descendant of the current commit.

The source list supports the release providers above and these providers:

| Method | Required source | Installed identity |
| --- | --- | --- |
| `github_commit` | `repo`, optional `ref` | Full commit SHA; optional `version_file` supplies version metadata from that commit |
| `github_release_asset` | `repo`, exact `asset` filename | One release's tagged asset URL, version and verified SHA-256 |
| `git_commit` | HTTPS `url`, full `ref` | Full commit SHA from an ordinary Git server |
| `oci_digest` | `image`, `tag` | Immutable registry manifest or image-index digest |
| `http_digest` | HTTPS `url` | SHA-256 of the downloaded bytes, including mutable snapshot URLs |
| `artifact_listing` | `url`, `download_base`, `version_regex` | Published artifact URL and computed SHA-256 |
| `apt` | `package`, `urls` | Package version from the selected distribution's `Packages.gz` or `Packages.xz` indexes |
| `libreoffice_release` | Official stable release and source listings | Released four-part build and matching archived SHA-256s for both Linux architectures |
| `zenodo` | Published `record` ID | Latest published record in that record's version family |
| `slicer_release` | See Slicer recipes | Slicer binary and extension from the same build revision |
| `freesurfer_release` | See SynthSeg recipe | FreeSurfer release and its corresponding model bundle |

LibreOffice uses `libreoffice_release` with a variable target for its four-part
`upstream_version` and `target.variables` mappings for `x86_64_sha256` and
`aarch64_sha256`. The provider selects a three-part release from the official
stable listing, reads its exact build from the corresponding source listing,
and requires both archived binary checksums to match the stable downloads.
Declared files use the permanent `downloadarchive.documentfoundation.org`
URLs and those digest variables. Missing architectures, ambiguous builds, or
checksum differences fail the observation before any recipe edits. Release
candidates in the archive listing are never used to discover updates.

A `target.variable` receives the observed identity by default. Set `value: tag`
for exact release tags or `value: version` for parsed release versions.
`target.variables` maps additional recipe variables to observation fields such
as `version` and `tag`, or to metadata fields from the
same observation. MuscleMap uses this to update a model record and its version
together. Slicer uses it to keep the application and extension compatible.

For downloaded binaries, use `target.file` with the existing declared file name:

```yaml
auto_update:
  method: sources
  sources:
    - id: standalone
      method: artifact_listing
      url: https://example.org/downloads/
      download_base: https://example.org/downloads/
      version_regex: 'tool-(?P<version>\d+\.\d+)\.tar\.gz'
      target:
        file: tool_archive
        variables:
          upstream_version: version
```

The current declared file needs a verified `sha256`. The updater replaces its URL
and SHA-256 together. The builder verifies both cached and newly downloaded
bytes against that digest. Runtime versions can come from named filename groups
or an exact `matlab_readme` member in the archive. Failed or ambiguous observations
leave the recipe unchanged and fail the update check.

Some vendor pages publish root-relative download links that omit their project
path. Set `rebase_root_relative_links: true` and include the project path in
`download_base` to resolve those links within that project. Links already inside
the configured base use normal URL resolution; external hosts and directory
traversal still fail validation.

For GitHub release binaries, select the release and its asset together:

```yaml
- id: application
  method: github_release_asset
  repo: example/tool
  asset: tool-linux.zip
  target:
    file: tool_archive
    variables:
      upstream_version: version
```

This provider selects the highest suitable release using the same stable-version
rules as `github_release`. It downloads the named asset from that exact tag and
verifies its size and any published SHA-256. The declared file keeps an explicit
tagged URL even when GitHub redirects downloads to a signed CDN URL. Each check
reads the asset bytes again, so replacements under an unchanged release tag also
produce an update. Missing, duplicate or incomplete assets fail the check. Do not
combine an independent release selector with a `releases/latest` digest source;
those selectors can resolve different releases.

Snapshot hashing uses conditional HTTP requests when the server supplies an ETag
or Last-Modified value. Only an explicit `304 Not Modified` reuses the previous
verified digest. Servers without validators are downloaded and hashed again.
The scheduled workflow preserves this metadata cache between runs.

## Local code and compiled binaries

Recipes consisting of repository-local scripts use `method: sources` with
`local: []`. Changes to their own recipe files already enter candidate build and
runtime-test CI. Shared inputs are declared as repository-relative macro paths,
for example `local: [macros/openrecon]`. A change to a shared macro selects all
consuming recipes and changes their build fingerprint. OpenRecon consumers must
also track the four installed shared dependencies; the audit enforces this for
new consumers.

Manual and notification-only policies fail the audit. A new recipe must select
its installed source, a published binary feed, or repository-local build inputs.
A policy that only changes a label, unused variable, or runtime directory fails
validation.

FieldTrip, PhysIO, and SamSrfX track their published standalone binaries. A newer
MATLAB source release does not imply a newer standalone binary exists. Producing
those binaries requires MATLAB Compiler and the relevant licensed toolboxes.
The container updater cannot supply a compiler license or claim an unpublished
source release is already available as a standalone artifact. Publication and
runtime verification must complete before the binary feed advances.
See [MATLAB standalone readiness](MATLAB_STANDALONE.md) for the prepared
FieldTrip/PhysIO candidate workflow and the remaining SamSrfX compilation work.

## Verify and inspect results

Run the coverage check without network access:

```bash
python -m builder.audit_updates --json update-coverage.json
```

Preview one recipe or the entire collection:

```bash
python -m builder.check_version qsmxt --dry-run --json update-results.json
python -m builder.check_version --dry-run --max-prs 0 --json update-results.json
```

`--dry-run` reads upstream services and, when `GITHUB_REPOSITORY` is set, existing
PRs. It does not modify recipes, push branches, open issues, or create PRs.
Use an authenticated `GITHUB_TOKEN` for a full scan to avoid GitHub's low anonymous
request limit. The GitHub token is confined to the GitHub API session; public
package and vendor lookups use separate sessions.
OCI registries use the [registry Bearer-token handshake](https://distribution.github.io/distribution/spec/auth/token/)
without GitHub credentials. Anonymous registry tokens stay within that registry's
tag requests; pagination cannot send them to another host.

Every scheduled run publishes a job summary and uploads `recipe-update-results`.
The report distinguishes current packages, proposed or opened PRs, existing PRs,
deferred updates, repository-local packages, and lookup errors.
Read requests retry temporary server errors up to three times. Persistent
lookup errors and invalid policies fail the job after it has checked the other
recipes. They no longer look like a successful run with no updates.

An open or merged update PR suppresses duplicates. For independent-source plans,
an open PR for the same container revision also suppresses additional PRs when
a tracked branch advances again. A closed, unmerged PR is
reported as `pr-closed`; reopen it to retry that version. This respects a
maintainer's decision to decline an update. A later upstream version gets its own
branch and can produce a new PR. Existing PRs do not consume the new-PR limit.

The workflow requires the organization or repository secret
`NEURODESK_GITHUB_TOKEN_ISSUE_AUTOMATION` for writes. It fails clearly if the secret
is absent, because PRs created with the default workflow token do not trigger the
container CI workflow. Concurrent scheduled/manual updater runs are serialized.
