# OpenMSK OpenRecon

`openmsk` packages the KneePipeline knee MRI toolbox as a Siemens OpenRecon
image-to-image module. It expects reconstructed qDESS two-echo Enhanced MR
images arriving as MRD image messages from the scanner.

## Outputs

- Bone and cartilage segmentation as source-geometry derived MRD images.
- Cartilage mesh and thickness outputs in the KneePipeline working directory;
  compact metrics are copied into returned image comments when available.
- T2 map MRD images only when the pipeline produced `*_t2map.nii.gz`.
- Optional NSM reconstruction and BScore JSON files when GPU execution and the
  gated ShapeMedKnee weights are available.

## qDESS And T2 Caveat

OpenRecon receives MRD images, not the original DICOM private tags. For the MRD
path, `openmsk` reconstructs an echo-1 NIfTI and KneePipeline treats it as a
generic NIfTI, so qDESS T2 mapping is skipped. To run the full pipeline,
including T2, run `run_pipeline.py` directly on a qDESS DICOM directory that
still contains the GL/TG private tags.

## Parameters

- `sendoriginal`: return original images before derived outputs.
- `segmodel`: KneePipeline model name (`acl_qdess_bone_july_2024` by default).
- `runnsm`: run GPU-only Neural Shape Model fitting if weights are present.
- `runbscore`: compute BScore after NSM fitting.
- `computethickness`: request cartilage thickness computation.

## Build And Validate

```bash
source env/bin/activate
python3 builder/validation.py recipes/openmsk/build.yaml
python -m builder generate openmsk --recreate --architecture x86_64
```

Manual OpenRecon test procedure is in `recipes/openmsk/test.yaml`. To enable
NSM/BScore at build time, provide a Hugging Face token in `HF_TOKEN` so the
optional `aagatti/ShapeMedKnee` snapshot can be downloaded into
`/opt/NSM_MODELS`.
