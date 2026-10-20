# 3D EPI post-processing

`ep3d` receives repeated, single-echo 3D EPI magnitude and phase volumes through
OpenRecon. Its base image is NeuroContainers QSMxT 10.0.0. Both recipes install
the same preprocessing modules from `macros/ep3d/helpers.yaml`.

The pipeline estimates rigid motion on magnitude relative to the first
repetition, applies that motion to the real and imaginary components, matches
phase between repetitions, and computes the complex average. Optional ANTs
non-local means denoising runs on that average.

The corrected images retain the first repetition's geometry and phase reference.
This pipeline corrects motion and phase differences between repetitions. It does
not estimate a susceptibility distortion field.

## Input

At least two matching magnitude and phase repetitions are required. Each volume
can arrive as a packed 3D MRD image or as a stack of 2D images. The MRD
`repetition`, `set`, and `average` fields identify corresponding volumes.
Multiple echoes are rejected.

The scanner parameters are:

- `config`: `ep3d`.
- `inputseries`: `distortion-corrected`, `non-distortion-corrected`, or `both`.
  The default prefers a complete pair without a separate `ND` token in its name
  and falls back to a complete ND pair. Names do not verify physical correction.
  `both` requires both pairs and processes them separately.
- `denoise`: enables NLM denoising. The default is `false`.
- `phasewrap`: scanner counts per full phase cycle. The default is `4096`.
  Phase already in radians passes through. Count input uses the same phase
  convention as the QSMxT adapter.

## Output

Each selected input pair produces two series:

- `ep3d corrected magnitude DC` or `ND`, in arbitrary units.
- `ep3d corrected phase DC` or `ND`, as wrapped phase in radians.

The adapter returns only these corrected series. It does not run QSM
reconstruction or return original images. The inherited QSMxT tools and models
remain in the base image.

Pixels use the QSMxT adapter's scanner-compatible unsigned 12-bit display
encoding. `RescaleSlope` and `RescaleIntercept` recover physical values with
`value = pixel * RescaleSlope + RescaleIntercept`. Phase images have MRD phase
type and `ComplexImageComponent=PHASE`. Every derived series has a new UID.
`EP3DOutput` and `EP3DUnits` identify the result. The inherited `QSMxTDisplay*`
metadata records the display scaling.

Temporary files reside under `/tmp/share/ep3d` and are removed after each request.

## Run on the command line

For a matching 4D NIfTI magnitude and radian-phase pair, run:

```bash
ep3d-preprocess magnitude.nii.gz phase.nii.gz output output.json --n-procs 4
```

Add `--denoise` to apply NLM denoising. `output.json` records the paths to
`average_magnitude.nii` and `average_phase.nii`. These files contain float32
physical values, with phase in radians.
