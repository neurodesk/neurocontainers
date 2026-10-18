# Replacing FSL motion correction in QSMxT with niimath

Implementation date: 2026-10-17.

The QSMxT recipe now uses niimath `v1.0.20261016` for 3D EPI motion correction.
It removes the FSL environment, Conda/Micromamba installation, Nipype, and the
external preprocessing checkout. The container remains QSMxT `10.0.0`; its
build date identifies this dependency rebuild.

## Processing contract

`ep3d_preproc.py` converts magnitude and radian phase to real and imaginary
components. It estimates rigid motion from magnitude relative to repetition 0,
then applies those parameters to both components on the first repetition's grid:

```bash
niimath magnitude.nii -moco -ref 0 -estimate motion.1D
niimath real.nii -gz 0 -resample magnitude.nii -hmc motion.1D real_mc.nii
niimath imag.nii -gz 0 -resample magnitude.nii -hmc motion.1D imag_mc.nii
```

niimath documents [estimate-only motion parameters](https://github.com/rordenlab/niimath/blob/5a5784b89b1e8e252184314e19fb535a9e27587f/README.md#L424-L445)
and [their application with cubic B-spline resampling](https://github.com/rordenlab/niimath/blob/5a5784b89b1e8e252184314e19fb535a9e27587f/README.md#L453-L465).
The resampler uses zero outside the field of view. The recipe pins the
[release](https://github.com/rordenlab/niimath/releases/tag/v1.0.20261016), verifies
its published SHA-256, and tracks it as a dependency in the automatic-update policy.

The driver retains smooth temporal phase matching relative to repetition 0,
with Gaussian sigma 2 voxels, followed by complex averaging. Optional ANTs NLM
runs on the complex average. These numerical operations follow
[Rüdiger Stirnberg's preprocessing functions](https://github.com/mrphysics-bonn/ep3d-highres-preprocessing/blob/8062dc1a6e368f3d1c329c50dbea42c8fdc5c3e8/basic-preprocessing/complex_nifti_tools/nipype_helpers.py#L85-L191),
packaged with the author's permission and attributed in the driver.

## Verification

`verify_ep3d_runtime.py`, invoked by `fulltest.yaml`, runs the actual driver and
QSMxT binary. It checks the absence of Nipype, FSL launchers, and Conda tools;
alternating polarity phase offsets; known translations and rotations; an oblique
anisotropic grid; output geometry; both denoising modes; and finite QSM output.
The updated recipe built successfully. Its container passed this runtime check,
including both NLM modes and a known two-degree rotation on an oblique,
anisotropic grid. The adapter and regional-report suites passed 366 tests.

The real OpenRecon TCP client/server also replayed a supplied reconstructed
magnitude/phase acquisition with two repetitions, a 510 by 540 by 352 matrix,
0.4 mm voxels, TE 19.3 ms, and 7 T field strength. Both sides used the official
QSMxT 10.0.0 binary with ROMEO, native-grid V-SHARP, Tikhonov lambda 0.01 with
identity regularization, mean referencing, and NLM disabled. Each container had
8 CPUs and a 16 GiB memory cap on a shared 32 GiB AMD EPYC-Milan host.

The comparison tests the serial niimath implementation against the optimized
niimath implementation. Timings start at MRD client launch and end after all
returned images are received. Container startup, DICOM-to-MRD preparation,
post-run benchmark validation, and rendering are excluded.

| Pair | Serial, seconds | Optimized, seconds | Execution order |
| --- | ---: | ---: | --- |
| 1 | 1039.166 | 657.750 | Serial, optimized |
| 2 | 847.988 | 627.833 | Optimized, serial |
| 3 | 845.670 | 615.885 | Serial, optimized |
| 4 | 829.067 | 610.866 | Optimized, serial |
| 5 | 824.093 | 597.524 | Serial, optimized |

Median replay time fell from 845.670 to 615.885 seconds, or 27.2% less time.
These timings describe the frozen benchmark versions, before later main-branch
output-metadata updates were integrated for the pull request.
The median paired ratio excluding the first pair was 1.365, which probes
first-pair warm-up, cache effects, and different observed host-load history.
All ten native Chimap voxel arrays, masks, affines, and scientific configurations
were identical. The returned display pixels matched their documented conversion,
and all original magnitude/phase pixels remained unchanged.

The supplied data exercised one inferred DC group. Raw reconstruction, scanner
distortion correction, and a separate ND branch were outside this benchmark.
The fixture normalized DICOM LPS geometry to the adapter's current convention;
it does not validate standard scanner MRD geometry interoperability. The default
mask excluded substantial central tissue, so repeatability does not establish
scientific adequacy in the excluded region.

The motion estimator differs from MCFLIRT, and cubic B-spline interpolation
replaces the previous sinc interpolation. The synthetic checks establish the
runtime contract, not numerical equivalence to FSL. The real-data benchmark
establishes repeatability and timing for this acquisition and configuration.
