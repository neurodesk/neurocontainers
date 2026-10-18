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
it does not validate standard scanner MRD geometry interoperability. The Otsu
mask used by that benchmark excluded substantial central tissue, so repeatability
does not establish scientific adequacy in the excluded region.

The motion estimator differs from MCFLIRT, and cubic B-spline interpolation
replaces the previous sinc interpolation. The synthetic checks establish the
runtime contract, not numerical equivalence to FSL. The real-data benchmark
establishes repeatability and timing for this acquisition and configuration.

## HD-BET default

The 2026-10-18 follow-up changes the OpenRecon mask default to `hd-bet`, both
in `OpenReconLabel.json` and when configuration omits `maskpreset`. The adapter
passes `--mask magnitude,hd-bet,close:1,fill-holes:0` to the unchanged official
QSMxT binary. The model is already packaged and verified against its pinned
manifest, so extraction works without network access. Otsu thresholding remains
available through the explicit `robust-threshold` preset. Saved scanner protocols
that explicitly store a mask preset retain that choice.

This changes the mask and therefore the scientific reconstruction. The earlier
serial-versus-parallel Otsu timings above remain historical measurements; they
do not establish HD-BET performance. The follow-up benchmark retains the supplied
full-resolution MRD fixture, Tikhonov reconstruction, and the 8-CPU/16-GiB limits.

Five complete HD-BET replays passed validation:

| Run | Full replay, seconds |
| --- | ---: |
| H01 | 683.013 |
| H02 | 644.093 |
| H03 | 627.960 |
| H04 | 617.445 |
| H05 | 641.767 |

Median full replay time was 641.767 seconds, with a range of
617.445 to 683.013 seconds. The first run remains included.
Median cgroup memory peak was 13.905 GiB; the maximum was
14.079 GiB. These peaks include filesystem cache. The host was shared
and caches were not flushed. Input checksums ran outside the client timer.

| Step | Median, seconds | Range, seconds |
| --- | ---: | ---: |
| Full OpenRecon replay | 641.767 | 617.445 to 683.013 |
| Original image copy | 7.809 | 2.665 to 12.281 |
| Input diagnostics | 20.571 | 18.992 to 22.445 |
| BIDS preparation, including EPI | 194.880 | 187.961 to 233.874 |
| EPI preprocessing, nested | 119.570 | 115.211 to 140.339 |
| Motion estimation, nested | 36.604 | 35.875 to 39.002 |
| Cartesian resampling wall time, nested | 9.527 | 9.150 to 13.023 |
| Phase matching and complex average, nested | 22.065 | 20.574 to 27.370 |
| Other BIDS preparation | 78.687 | 72.750 to 93.535 |
| Native QSMxT, including stages | 324.257 | 322.723 to 329.353 |
| Phase scaling, nested | 5.511 | 4.333 to 5.769 |
| Magnitude and inhomogeneity correction, nested | 61.099 | 60.403 to 62.348 |
| HD-BET mask and refinements, nested | 126.701 | 125.746 to 128.614 |
| ROMEO unwrapping, nested | 24.112 | 23.815 to 27.047 |
| V-SHARP background removal, nested | 80.962 | 79.120 to 83.013 |
| Tikhonov inversion, nested | 13.443 | 12.602 to 14.268 |
| Mean reference, nested | 7.364 | 6.535 to 8.005 |
| Native loading, setup and other I/O, nested | 5.664 | 5.446 to 5.897 |
| Output diagnostics | 61.942 | 57.904 to 76.715 |
| Derived image packing | 9.197 | 8.024 to 10.400 |
| Output validation | 0.453 | 0.448 to 0.470 |
| Send original and derived images | 4.210 | 3.783 to 5.033 |
| Other client and protocol work | 5.668 | 3.739 to 7.479 |

Nested rows overlap their parent. Native QSMxT accounts for roughly half the
elapsed replay time; HD-BET extraction plus mask refinements is its longest
stage. The measured CPU usage averaged about 1.34 cores despite the 8-CPU
budget, so these timings include substantial serial processing and I/O.

All five full-resolution Chimap arrays, brain masks, affines, and native
scientific configurations matched exactly. Every returned display pixel and
slice position was checked against the native result, and all original
magnitude/phase pixels remained unchanged. The native configuration confirmed
`hd-bet` and Tikhonov lambda 0.01 with identity regularization in every run.
The private MRD copy and canonical original matched the recorded whole-file
SHA-256 before and after each new replay. H01 had explicit copy verification
before launch and a post-run check when its completed container was collected.

An initial read-only input mount failed before reconstruction because the MRD
library opens input files in read/write mode. That setup attempt is retained
separately and excluded from these five successful timings. The private input
copy resolved the access requirement without changing the source data.

On the same axial QC slice used for the earlier Otsu result, HD-BET included
all 12,792 pixels in the largest enclosed gap of the previous mask. This measures
coverage of that specific two-dimensional gap; it is not an anatomical accuracy
score. No expert reference mask was supplied. Residual QSM streak artifacts
remain, and the acquisition and scanner-interoperability limits above still
apply. The rebuilt container also passed 368 adapter/regional-report tests and
all 22 checked-in release test commands offline with `HOME=/nonexistent`.
