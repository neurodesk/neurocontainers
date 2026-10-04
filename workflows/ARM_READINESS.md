# ARM readiness

Run `python -m workflows.arm_readiness --output /tmp/arm-readiness.json` to derive
current declarations and named variants from recipes, fulltests and release
records. Counts follow the checkout. A release record is catalog input, not
proof that a remote image works. Published and native observations default to
`unobserved`; undeclared recipes need a separate upstream feasibility review.

Add `--recipes dcm2niix,niimath,datalad --probe` to check exact version/build-date
Quay tags anonymously. The command checks linux/arm64 image configuration and SIF
referrer presence, records digests, and returns nonzero for probe failures.
It does not fall back to `latest` or interpret an HTTP error as absent support.
For image indexes, it selects the ARM platform and checks SIF referrers on
the original index digest, excluding provenance manifests.

The weekly workflow builds dcm2niix, niimath and datalad on the existing native
ARM runner path, converts each fresh image and executes deploy checks and its
fulltest. Candidate images and observations remain CI artifacts. Download the
native artifacts, place each `native-result.json` under its container identity
and pass that parent directory through `--results` to attach observations to a
report. Results must match the current version, recipe hash and a native ARM machine.
Passed results also require the built image ID and candidate SIF digest.
The command returns nonzero for invalid imported observations.

The report leaves ARM module-client invocation unobserved. An operator still
needs an ARM NeuroDesk client to load each named module and execute it. It also
needs to review each undeclared recipe for upstream binaries, dependencies or
platform restrictions before adding ports. Issue #1252 remains open for those
ports and deployment acceptance. This job does not change shared OCI tags,
publish a release or assert that all ARM declarations are usable.
