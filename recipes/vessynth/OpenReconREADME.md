# VesSynth OpenRecon

This research container segments vessels from reconstructed magnitude images using
VesSynth and its official model bundle. It provides TOF, T2star, HiPCT, OCT and
fibers models. LSFM has no bundled checkpoint and is unsupported.

Run `vessynth -i input.nii.gz -o results -mod TOF`. Relative paths work from the
caller directory. `vessynth --version` reports the dated container build,
not an upstream software release. All models remain under `/opt/VesSynth/models`.
VesSynth uses CUDA when available and otherwise runs on CPU.
CPU inference defaults to two OpenMP threads. Set `OMP_NUM_THREADS` to change
this limit, or `APPTAINERENV_OMP_NUM_THREADS` when running with Apptainer.

Run `openrecon-vessynth --help` for server options. Select `vessynth` as the
MRD server configuration. JSON settings may appear at the top level or inside
`parameters`. Defaults are `modality: TOF`, `threshold: 0.3`, `patchsize: 128`,
`stepsize: 32`, `output: binary`, `sendoriginal: true`, and
`gaussianweights: true`. Output choices are binary, probability and both.
Patch size must be a multiple of 32, at least 32. Stride must divide patch size.
When patch size changes without an explicit stride, stride becomes patch size / 4.

The adapter partitions header series and repeated/contrast volumes before
physical slice ordering. It accepts finite single-channel, single-plane
magnitude images with regular orthonormal geometry. It rejects raw acquisitions,
complex images, duplicate slice positions, gaps and inconsistent geometry.
MRD positions represent image centers. The adapter converts them to voxel-zero
RAS coordinates for NIfTI and checks the inference output shape and affine.
It restores singleton axes removed by upstream only when the squeezed shape
and exact voxel count agree with the input.

Originals receive separate series identities. Binary output uses segmentation
scanner stamps and uint16 values 0 or 1. Probability output uses the Image role
and float32 values between 0 and 1. Binary masks use strict probability greater
than threshold. Derived and original output series never reuse input indices.
The adapter invokes the upstream CLI in a temporary workspace for each volume.
It logs failures through the MRD server and always closes the connection.
GPU execution and physical scanner integration have not been tested. Probability
output is verified through the MRD server but remains unvalidated on a scanner.

`python /opt/VesSynth/verify_runtime.py adapter` checks MRD geometry and error
handling with deterministic inference. `python /opt/VesSynth/verify_runtime.py model`
loads all five official checkpoints against their packaged network definitions
and runs the bundled TOF checkpoint on a small nonconstant CPU volume.
