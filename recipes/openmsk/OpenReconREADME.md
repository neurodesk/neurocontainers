# OpenMSK OpenRecon

`openmsk` packages the KneePipeline knee MRI toolbox as a Siemens OpenRecon
image-to-image module. It expects reconstructed qDESS two-echo Enhanced MR
images arriving as MRD image messages from the scanner.

## Outputs

- Bone, cartilage, and meniscus segmentation as source-geometry derived MRD
  images using the upstream DOSMA/OpenMSK label values.
- Subregion segmentation as a separate derived MRD segmentation series when
  KneePipeline writes `*_subregions-labels.nii.gz`.
- Optional cartilage mesh and thickness outputs in the KneePipeline working
  directory when `computethickness` is enabled.
- Metrics comments on derived metric-bearing outputs and a burned-in metrics
  report image series when KneePipeline writes metrics JSON/CSV files.
- qDESS T2 map MRD images and per-region T2 metrics when KneePipeline's
  `steps.t2_mapping` writes `*_t2map.nii.gz` and `*_t2_results.json`.

## Segmentation Labels

Returned segmentation and subregion series use the same public labels as the
upstream GitHub monolith:

| Label | Tissue |
| ---: | --- |
| 0 | Background |
| 1 | Patellar cartilage |
| 2 | Femoral cartilage |
| 3 | Medial tibial cartilage |
| 4 | Lateral tibial cartilage |
| 5 | Medial meniscus |
| 6 | Lateral meniscus |
| 7 | Femur |
| 8 | Tibia |
| 9 | Patella |

Femoral-cartilage subregion output retains the upstream labels 11 anterior,
12 medial weight-bearing, 13 lateral weight-bearing, 14 medial posterior, and
15 lateral posterior.

KneePipeline's modular mesh, thickness, T2, and NSM steps use a canonical
label scheme internally. OpenRecon converts to that scheme only after sending
the public segmentation and converts subregion output back before returning it.

## Output Geometry

Derived series are returned as per-slice MRD images resampled onto the source
image grid, and the scanner's DICOM writer defines the stored dimensions,
spacing, origin, and direction. An offline KneePipeline run on exported DICOMs
works on the DICOM-derived grid instead, so the two outputs are not
voxel-for-voxel identical: comparing them (for example computing DSC or ASSD
against an offline run) requires resampling one result onto the other's grid
first. This is inherent to OpenRecon, not a defect in either output.

## qDESS And T2 Caveat

OpenRecon receives MRD images, not the original DICOM. To preserve the qDESS
T2 path, the wrapper keeps both echo groups and writes a minimal two-echo MR
DICOM series with `EchoNumbers`, TR/TE/flip angle, and DOSMA's qDESS private
GL/TG tags. TR/TE/flip are read from the MRD header or image metadata when
available. GL/TG and any missing timing values can be supplied through the
OpenRecon qDESS fallback fields.

The fallback values are only as good as the protocol values entered on the
scanner. Runtime logs report where every qDESS value came from.

## Parameters

- `sendoriginal`: return original images before derived outputs.
- `segmodel`: KneePipeline model name (`acl_qdess_bone_july_2024` by default;
  `goyal_sagittal`, `goyal_coronal`, `goyal_axial`, and `nnunet_knee` are
  also packaged). The packaged `nnunet_knee` path runs nnU-Net with
  scanner-safe single preprocessing/export worker defaults.
- `computethickness`: run slower mesh/thickness analysis after the segmentation
  has been sent.
- `runnsm`, `runbscore`: accepted for legacy scanner protocol compatibility
  only; ignored because the gated ShapeMedKnee assets are not packaged.
- `qdesstrms`, `qdesste1ms`, `qdesste2ms`, `qdessflipangledeg`,
  `qdessglarea`, `qdesstgus`: fallback TR, TE1, TE2, flip angle, GL area, and
  TG values used to synthesize the qDESS DICOM input when MRD metadata is
  incomplete. DOSMA's qDESS fit uses one sequence TE for the S1/S2 signal
  ratio, so a single TE from the MRD sequence header is shared by both
  synthesized volumes instead of inventing TE2 from the GUI.

## Build And Validate

```bash
source env/bin/activate
python3 builder/validation.py recipes/openmsk/build.yaml
python -m builder generate openmsk --recreate --architecture x86_64
```

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/openmsk

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
