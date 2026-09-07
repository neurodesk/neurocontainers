# Automatic update migration verification

The collection contains 246 recipes. All have automatic update policies and a
sibling `fulltest.yaml`. The audit rejects missing policies, manual exceptions,
notification-only policies, unused update targets, and missing OpenRecon source
ownership. New recipes pass the same checks.

134 recipes track a directly installed release version. The other 112 use
independent source targets, covering 303 package, source, model, image, and binary
inputs. Their container revisions are separate from software versions. Shared
macro changes select the consuming recipes for candidate builds.

## What was verified

- Every recipe validated and generated a Dockerfile.
- Source update plans were replayed against generated Dockerfiles and staged
  downloads, including 117 component transitions in the 20 composite recipes,
  61 in the 36 source migrations, and 80 shared OpenRecon transitions.
- All 28 binary migrations passed current-observation, changed-observation,
  deterministic-plan, and replay checks across their declared architectures.
- QSMxT's observed `v9.16.0` release produces a plan changing the software pin from
  `9.15.0` to `9.16.0`. Its download and runtime assertion change together, the
  container becomes `9.15.0.post1`, and replay produces no further change.
- DataLad, GIMP, PalmettoBUG, and MITK Diffusion built as real Docker images.
  DataLad and GIMP reported the selected package versions. PalmettoBUG passed nine
  runtime checks; MITK Diffusion passed 126.
- MuscleMap verified actual installed model weights against the selected Zenodo
  records and loaded its whole-body model. MNE 1.1.0 imported and created a real
  recording on Python 3.12 with the recipe's setuptools compatibility pin.
- Conda resolved the complete MNE environment for both 1.1.0 and 1.12.1. Pip
  resolved the pinned desktop Python package set after the widget compatibility
  fix. These resolver checks do not replace full desktop or MNE container builds.
- All 564 builder tests passed. They cover source selection, update bindings, metadata/version
  consistency, checksums, cache behavior, shared-input release planning, and PR
  reuse. An advancing source branch does not create another PR while the same
  container revision already has an open update PR.

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
behavior without pretending that synthetic releases exist upstream. A live
full-collection scan needs the workflow's GitHub API authentication; anonymous
requests reached GitHub's public rate limit during this migration. Public package
and vendor endpoints were also checked directly.

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
