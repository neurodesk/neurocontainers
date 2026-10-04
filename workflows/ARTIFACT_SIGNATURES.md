# Verify promoted containers

Artifact signing is disabled by default. After the staging acceptance below,
set the repository Actions variable `ARTIFACT_SIGNING_ENABLED` to the literal
value `true` to enable it. Each promotion resolves this policy once; all other
values preserve the existing unsigned release flow.

When enabled, trusted main-branch promotion signs the immutable GHCR image and SIF referrer
digests with Cosign and GitHub OIDC. It also signs the exact tested SIF bytes and
a promotion acceptance statement. Every signature is verified against the exact
workflow identity before the promotion job can finalize release metadata.

The statement binds the candidate head, merged commit, recipe fingerprint,
artifact hashes and the hash of the candidate test results. It records acceptance
of a merged PR's tested candidate. The candidate comes from the PR workflow, so
this is not evidence that the artifact was built in a trusted build environment.

## Verify a GHCR image

Install [Cosign](https://docs.sigstore.dev/cosign/system_config/installation/),
then use an immutable digest from the release you intend to verify:

```bash
identity='https://github.com/neurodesk/neurocontainers/.github/workflows/promote-container-candidate.yml@refs/heads/main'
issuer='https://token.actions.githubusercontent.com'
cosign verify --certificate-identity "$identity" --certificate-oidc-issuer "$issuer" \
  ghcr.io/neurodesk/CONTAINER@sha256:DIGEST
```

Use the same command with the resolved SIF manifest digest to verify its registry
signature. Signature discovery uses Cosign's registry storage conventions.

## Verify a downloaded SIF

For each `NAME.simg` promoted with signing enabled, the S3 download location also contains:

- `NAME.simg.sigstore.json`: signature bundle for the SIF bytes.
- `NAME.simg.promotion.json`: promotion acceptance statement.
- `NAME.simg.promotion.sigstore.json`: signature bundle for that statement.

Download the SIF and sidecars from the same release location, then run:

```bash
cosign verify-blob --certificate-identity "$identity" --certificate-oidc-issuer "$issuer" \
  --bundle NAME.simg.sigstore.json NAME.simg
cosign verify-blob --certificate-identity "$identity" --certificate-oidc-issuer "$issuer" \
  --bundle NAME.simg.promotion.sigstore.json NAME.simg.promotion.json
```

Check the signed statement's SIF hash against your downloaded file and confirm
the intended version, candidate head and merge commit. The same SIF bundle can
verify identical bytes copied to CVMFS; CVMFS operators must mirror the sidecars
or obtain them from the official S3 location. This PR does not change CVMFS
mirroring or the separate manual build workflow.

Existing releases without bundles remain unsigned. A missing bundle or failed
verification must never be interpreted as a verified release. Rerunning a
trusted promotion may create fresh signatures for the same immutable bytes;
it does not alter the SIF. Signing or required sidecar publication failure stops
release finalization. Fulcio/Rekor and registry availability are required.

Before enabling production signing, run a trusted staging promotion with GitHub
OIDC and test registry/S3 destinations. Keep the repository opt-in unset until
that acceptance succeeds; this workflow does not configure staging destinations. Local key-based fixtures exercise byte tampering
but cannot substitute for that identity and publication round trip. Commit
signing and repository signature rules remain a separate maintainer policy
decision under issue #504.

See [Cosign blob signing](https://docs.sigstore.dev/cosign/signing/signing_with_blobs/)
and [signature verification](https://docs.sigstore.dev/cosign/verifying/verify/).
