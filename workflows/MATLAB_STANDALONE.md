# MATLAB standalone candidates

[build-matlab-standalone.yml](../.github/workflows/build-matlab-standalone.yml) prepares Linux x86_64 candidates for FieldTrip and PhysIO with SPM12. The workflow has not completed a licensed compilation or a compiled application test. Its Python package validation has been tested without MATLAB. No candidate has been uploaded or promoted.

The recipe updater continues to follow published standalone artifacts. A newer source tag alone does not advance a compiled recipe. Candidate objects use a separate Swift prefix that the current recipe policies do not select.

## Sources and compilation entrypoints

| Software | Official source | Implemented command |
| --- | --- | --- |
| FieldTrip | `fieldtrip/fieldtrip`, exact date tag and full commit SHA | `ft_defaults; ft_compile_standalone(source)` |
| PhysIO | `ComputationalPsychiatry/PhysIO`, exact `vX.Y.Z` tag and full commit SHA | Copy the source into pinned SPM12's `toolbox/PhysIO`, run `tapas_physio_init()`, then `spm_make_standalone(compiled)` |
| SPM12 dependency | `spm/spm12`, full commit SHA | Its own `config/spm_make_standalone.m` compiles the SPM gateway and registered toolboxes |

FieldTrip supplies both [the compiler helper](https://github.com/fieldtrip/fieldtrip/blob/master/utilities/ft_compile_standalone.m) and [the standalone script gateway](https://github.com/fieldtrip/fieldtrip/blob/master/utilities/ft_standalone.m). The helper explicitly includes Signal Processing, Image Processing, Statistics and Machine Learning, Optimization, and Curve Fitting toolboxes.

PhysIO moved from the archived TAPAS repository to [ComputationalPsychiatry/PhysIO](https://github.com/ComputationalPsychiatry/PhysIO). Its [installation instructions](https://github.com/ComputationalPsychiatry/PhysIO/blob/master/README.md#installation) describe placing the source under SPM's toolbox directory. The source exposes `tapas_physio_cfg_matlabbatch.m` at that directory's root. Its [initializer](https://github.com/ComputationalPsychiatry/PhysIO/blob/master/tapas_physio_init.m) returns `isPhysioCorrectlyInitialized`, which the build asserts before compilation.

SPM12's [compiler helper](https://github.com/spm/spm12/blob/main/config/spm_make_standalone.m) discovers root `*_cfg_*.m` files in each toolbox directory. It writes `spm_cfg_static_tools.m`, then calls `mcc` with the SPM source tree. The candidate build checks that this generated registration includes PhysIO.

PhysIO's [requirements](https://github.com/ComputationalPsychiatry/PhysIO/blob/master/requirements.txt) include Signal Processing and Statistics and Machine Learning. SPM12's helper normally enables only Signal Processing with `mcc -N`. The candidate build adds a second `-p` entry for `toolbox/stats` in the checked-out helper. The replacement requires exactly one match of the known declaration and stops if that declaration changes. The manifest records the adjustment. This integration has not been verified by a licensed build.

## Dispatch inputs and credentials

The workflow runs only on the repository's default branch. Its current runner is GitHub-hosted `ubuntu-22.04`; suitability of that runner for the available MATLAB license remains unconfirmed.

| Input | Accepted value |
| --- | --- |
| `software` | `fieldtrip` or `physio` |
| `source_ref` | Full lowercase 40-character commit SHA from the selected official repository |
| `source_version` | Exact FieldTrip `YYYYMMDD` tag or PhysIO `vX.Y.Z` tag |
| `runtime_release` | Exact release such as `R2023b`, at least `R2021a` |
| `spm_ref` | Full `spm/spm12` commit SHA for PhysIO; empty for FieldTrip |
| `publish_candidate` | Defaults to `false`; `true` uploads a validated candidate to Swift |

The workflow verifies the checkout commit and resolves the official source tag to that same commit. It does not accept a branch name as an immutable source reference.

Both MATLAB actions use `v3`. The named repository secret `MATLAB_BATCH_LICENSE_TOKEN` is mapped to `MLM_LICENSE_TOKEN` for MATLAB execution. A missing token stops the job before MATLAB setup. The script also checks MATLAB Compiler entitlement. [MathWorks' action documentation](https://github.com/matlab-actions/setup-matlab/tree/v3#use-matlab-batch-licensing-token) states that public-project automatic licensing excludes transformation products such as MATLAB Compiler. The token's entitlement, toolbox coverage, and suitability for this runner have not been verified.

Candidate publication uses the existing `SWIFT_OS_AUTH_URL`, `SWIFT_OS_APPLICATION_CREDENTIAL_ID`, and `SWIFT_OS_APPLICATION_CREDENTIAL_SECRET` secrets. Those credentials are supplied only to the upload step in a separate job. No new repository variable is required.

## Candidate checks and output

[compile_matlab_standalone.m](compile_matlab_standalone.m) invokes the upstream entrypoints. [builder.matlab_standalone](../builder/matlab_standalone.py) validates and packages their output. Packaging requires a nonempty Linux x86_64 ELF executable, its runtime launcher, a compiler readme naming the requested MATLAB release, and SPM's external CTF when applicable.

The compiled application runs through its generated shell launcher, using the installed MATLAB directory for runtime libraries. FieldTrip must demean a synthetic trace while preserving its shape. PhysIO must create its configuration structure through SPM's compiled `eval` gateway. Each check requires both a zero exit code and an assertion-completion marker. These are focused smoke checks; they do not validate every analysis function or replace recipe container tests.

The archive includes source license files, deterministic archive metadata, and the compiled files under `fieldtrip/` or `spm12/`. SHA256 is computed from the resulting archive bytes. The manifest records the official source, both source revisions where applicable, the MATLAB release, compiler adjustments, archive size, digest, and smoke result.

GitHub Actions retains the archive, manifest, checksum file, and runtime log for review. With `publish_candidate=true`, a separate job rechecks the bytes, manifest, runtime result, and exact destination before uploading into:

```text
matlab-candidates/<software>/<source_ref>/<runtime>/<sha256>/<archive>
```

This workflow has no release-promotion step. A future promotion must verify the real compiled package in the recipe container and reconcile its filename and runtime metadata with the watched artifact feed. In particular, the current PhysIO feed names the older `R2021a` release and a specific SPM revision. A new PhysIO semantic version cannot be presented as that old artifact.

## SamSrfX source limit

The public [SamSrf repository](https://github.com/samsrf/samsrf/tree/519810ae8269ee03afbdf4597853945435dbf168) contains `SamSrfX.m` and publishes a standalone binary. Inspection of the complete source tree at commit `519810ae8269ee03afbdf4597853945435dbf168` found no compiler project, `mcc` build script, standalone compilation helper, or release compilation workflow. The source file alone does not specify all runtime assets and compiler options used for the published application.

The candidate workflow therefore rejects SamSrfX as a source compilation target. Its existing compiled download feed remains automatic. Source publication still needs an upstream-supported build entrypoint or a locally developed compiler project validated with the licensed toolchain; neither has been established here.

## Local verification

The packaging tests execute real shell launcher fixtures and produce real archives without requiring MATLAB or Swift credentials:

```bash
python -m pytest builder/tests/test_matlab_standalone.py -q
```

They cover source and tag validation, runtime and architecture mismatches, missing CTF and license files, failed runtime assertions, deterministic packaging, SHA256 verification, and rejection of changed publication destinations. These tests do not prove that MATLAB Compiler can build either upstream application.
