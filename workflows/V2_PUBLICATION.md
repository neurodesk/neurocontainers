# Pull and verify v2 container artifacts

The v2 image tag identifies a Docker image. Its SIF is an OCI referrer, so
Apptainer's `oras://` transport needs the SIF manifest digest. From this
repository, resolve that digest before pulling:

```bash
python3 -m pip install requests
artifact=$(python3 -m tools.v2_registry resolve quay.io/neurodesk/niftyreg:2.1.2_20261003)
apptainer pull niftyreg.simg "$artifact"
```

The resolver checks anonymous access, the manifest digest, the SIF subject and
the single SIF layer. It rejects conflicting SIF attachments. To require the
checksum of a tested SIF, pass `--expected-sha256 HEX_DIGEST` to `resolve`.
Apptainer verifies the downloaded registry blob against its descriptor digest.

## Inspect migration coverage

```bash
python3 -m tools.v2_registry audit > v2-coverage.json
```

This read-only command checks every release metadata file against Quay and GHCR.
It reports inaccessible tags, absent SIFs and disagreement between mirrored SIF
digests, with a nonzero exit status for any gap. HTTP 401 does not distinguish a
private repository from a missing repository. Correct those gaps through the
normal tested publication path. The command never changes visibility or tags.

## Finalize floating tags

```bash
python3 -m tools.v2_registry finalize quay.io/neurodesk/niftyreg 2.1.2 20261003
```

Without `--apply`, this command prints the planned floating tags. With `--apply`,
it uses authenticated ORAS to tag immutable image digests after anonymous SIF
verification. The version alias selects the greatest build date within that
version. `latest` selects the greatest date across versions. Equal dates use
the lexicographically greatest dated tag to make reruns deterministic.

Promotion and manual v2 publication share the `promote-container-and-metadata`
concurrency group. Operators must not run concurrent manual `--apply` commands
outside that serialized workflow. Registry tags have no compare-and-swap API.
Recovering an older release does not move either alias to an older date.

Quay publication and anonymous SIF verification are required before release
metadata is exposed. The administrative visibility API remains best-effort for
already-public repositories; failed anonymous verification blocks publication.
This does not backfill historical releases or change existing legacy tags.
