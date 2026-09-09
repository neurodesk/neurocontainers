# Automatic update migration verification

The collection contains 246 recipes. All have automatic update policies and a
sibling `fulltest.yaml`. The audit rejects missing policies, manual exceptions,
notification-only policies, unused update targets, and missing OpenRecon source
ownership. Runtime suites must contain executable tests. New recipes pass the
same checks before they can be merged.

116 recipes track a directly installed release version. The other 130 use
independent source targets, covering 372 package, source, model, image, and binary
inputs. Their container revisions are separate from software versions. Shared
macro changes select the consuming recipes for candidate builds.

## What was verified

- Every recipe validated and generated a Dockerfile. The collection declares
  301 architecture targets, all selected for CI and all successful. CI results
  consolidate runs across repair commits and do not represent one full rebuild
  at the final commit.
  [Per-target results](auto-update-build-results.tsv) include the tested commit
  and CI job for each architecture.
- Source update plans were replayed against generated Dockerfiles and staged
  downloads, including 117 component transitions in the 20 composite recipes,
  61 in the 36 source migrations, and 80 shared OpenRecon transitions.
- All 28 binary migrations passed current-observation, changed-observation,
  deterministic-plan, and replay checks across their declared architectures.
- QSMxT update replay verifies that the software pin, download and runtime
  assertion change together, while the container revision advances separately.
  Authenticated live scans subsequently detected `9.16.0` to `9.17.0`. Dry runs
  create no PRs; the five-new-PR limit can defer a detected update to a later run.
- DataLad, GIMP, PalmettoBUG, and MITK Diffusion built as real Docker images.
  DataLad and GIMP reported the selected package versions. PalmettoBUG passed nine
  runtime checks; MITK Diffusion passed 126.
- MuscleMap verified actual installed model weights against the selected Zenodo
  records and loaded its whole-body model. MNE 1.1.0 imported and created a real
  recording on Python 3.12 with the recipe's setuptools compatibility pin.
- MNE Extended passed 110 runtime checks and its full CI build. Neurodesktop
  passed CI on both architectures. NCT passed 65 runtime checks, including real
  atlas overlap with bundled data, and its full CI build. SYNcro completed
  SynthStrip and SynthSR inference offline and passed its full CI suite.
- The SIF converter passed plain/compressed-layer and layered-hardlink tests.
  OpenADS and PALS passed complete CI after the hardlink repair; Micapipe passed
  after conversion stopped keeping duplicate copies of its image archive.
- GOUHFI loaded all three bundled model families and completed finite CPU forward
  passes with their official weights. Archive SHA-256 and Zenodo MD5 checks
  verified the author's Hugging Face mirror. These checks do not establish full
  end-to-end pipeline or physical GPU execution.
- Slicer passed all 68 CI runtime checks after repairing ITK wheel metadata,
  binding a compatible MONAI Label stack, and exposing its server launcher.
- SovaBIDS passed 95 local runtime checks, including a real JSON-RPC roundtrip.
  VesselVio passed 96 checks on its generated image, including GUI initialization
  and synchronous graph analysis of its bundled JIT warmup volume.
- SeedSeg executed all four published ONNX models on CPU with both the CPU package
  and the GPU package using its CPU provider. Physical GPU execution is unverified.
- MRtrix3Tissue passed all 117 CI runtime checks after restoring FFTW and Qt
  support. Its FFT and four color maps also passed checks on real MRI data.
  FSL and HCP-ASL passed their full CI suites after Python environment repairs;
  SeedSeg passed on both architectures with its corrected Python bootstrap.
- Dafne passed all 12 recorded build, deployment, and runtime checks on x86_64
  and aarch64. Its compatibility repair locates the installed `dafne_dl` package
  instead of depending on a versioned Miniconda path, and verifies GUI imports
  and offscreen startup under Python 3.8.
- All 674 builder tests passed. They cover source selection, update bindings,
  metadata/version consistency, checksums, cache behavior, shared-input release
  planning, and PR reuse. An advancing source branch does not create another PR
  while the same container revision already has an open update PR.

Additional residual repairs cover native MRIcron packages and independent
architecture update targets, FatSegNet Python compatibility, FSL ASL extension
imports, JIDT Java linkage, portable CLEARSWI compilation, native Dafne radiomics,
GOUHFI CUDA wheel selection, and validated RABIES atlas downloads. Runtime checks
exercise the installed commands and data. GPU model checks use CPU providers;
physical GPU execution remains unverified.

## Reproduce the checks

Use the repository's installed Python environment:

```bash
python -m builder.audit_updates --json update-coverage.json
pytest builder/tests
./workflows/test_all.sh
python workflows/verify_source_migration.py --report source-replay.json
python workflows/verify_source_migration.py neurodesktop-lite mneextended qsmxt
python workflows/verify_binary_updates.py --help
python -m builder.check_version qsmxt --dry-run --json qsmxt-update.json
```

The replay scripts inject source observations to verify edit and build-input
behavior without pretending that synthetic releases exist upstream.

The final authenticated [full-collection dry run](https://github.com/neurodesk/neurocontainers/actions/runs/34317692369)
completed 245 observations and reported one TGVQSM server timeout. It found
140 current recipes, 99 updates deferred by the five-new-PR limit, five updates
that would open PRs, and one previously closed PR. Dry runs create no PRs.
A targeted TGVQSM retry then completed successfully. Every recipe therefore
completed a live observation across the scan and retry. The final MRSIproc
software/model policy also passed a separate live check after that scan.

BrainLes, RABIES, and MRSIproc select the newest Zenodo record retaining the
filenames required by their installers. Records need no human-readable version
label unless a target binds that label. MRSIproc tracks HD-BET 1.x to match its
five legacy model files, and its runtime suite strictly loads all five weights.
Failed upstream observations remain errors rather than being reported as current.

## Remaining source publication work

FieldTrip, PhysIO, and SamSrfX automatically track published standalone binaries.
They cannot receive new source releases as standalone binaries until those
sources are compiled and published. The FieldTrip/PhysIO candidate workflow is
prepared but has not been executed with MATLAB Compiler. SamSrfX has no verified
upstream compilation entrypoint in this repair.

See [MATLAB standalone readiness](MATLAB_STANDALONE.md) for the required license,
compilation inputs, runtime verification, and publication steps. No new MATLAB
binary has been published by this migration. Recipe update automation takes
effect after these repository changes are merged.
