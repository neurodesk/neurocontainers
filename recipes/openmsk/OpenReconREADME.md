# OpenMSK OpenRecon

`openmsk` packages the KneePipeline knee MRI toolbox as a Siemens OpenRecon
image-to-image module. It expects reconstructed qDESS two-echo Enhanced MR
images arriving as MRD image messages from the scanner.

## Outputs

- Bone and cartilage segmentation as source-geometry derived MRD images.
- Optional cartilage mesh and thickness outputs in the KneePipeline working
  directory when `computethickness` is enabled.
- T2 map MRD images only when the pipeline produced `*_t2map.nii.gz`.

## qDESS And T2 Caveat

OpenRecon receives MRD images, not the original DICOM private tags. For the MRD
path, `openmsk` reconstructs an echo-1 NIfTI and KneePipeline treats it as a
generic NIfTI, so qDESS T2 mapping is skipped. To run the full pipeline,
including T2, run `run_pipeline.py` directly on a qDESS DICOM directory that
still contains the GL/TG private tags.

## Parameters

- `sendoriginal`: return original images before derived outputs.
- `segmodel`: KneePipeline model name (`acl_qdess_bone_july_2024` by default;
  `goyal_sagittal` and `nnunet_knee` are also packaged).
- `computethickness`: run slower mesh/thickness analysis after the segmentation
  has been sent.

## Build And Validate

```bash
source env/bin/activate
python3 builder/validation.py recipes/openmsk/build.yaml
python -m builder generate openmsk --recreate --architecture x86_64
```
