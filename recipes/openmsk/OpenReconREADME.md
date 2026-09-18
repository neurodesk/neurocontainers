# OpenMSK OpenRecon

`openmsk` packages the KneePipeline knee MRI toolbox as a Siemens OpenRecon
image-to-image module. It expects reconstructed DESS two-echo Enhanced MR
images arriving as MRD image messages from the scanner.

## Tested protocol

This OpenRecon workflow was tested using the Siemens DESS WIP
`wip_dess_CS_260902` on a MAGNETOM 3.0T X60 running Numaris/X VA61A-092T.
The human-readable protocol PDF and scanner `.pro` export are stored in this
repository and available from GitHub:

- [wip_dess_CS_260902.pdf](https://github.com/NeuroDesk/neurocontainers/blob/main/recipes/openmsk/wip_dess_CS_260902.pdf)
- [wip_dess_CS_260902.pro](https://github.com/NeuroDesk/neurocontainers/blob/main/recipes/openmsk/wip_dess_CS_260902.pro)

The `.pro` file includes the saved OpenRecon algorithm selection and OpenMSK
parameters used for the test.

## Outputs

- Bone, cartilage, and meniscus segmentation as source-geometry derived MRD
  images using the upstream DOSMA/OpenMSK label values.
- Subregion segmentation as a separate derived MRD segmentation series when
  KneePipeline writes `*_subregions-labels.nii.gz`.
- Optional cartilage mesh and thickness outputs in the KneePipeline working
  directory when `computethickness` is enabled.
- Metrics comments on derived metric-bearing outputs and a burned-in metrics
  report image series when KneePipeline writes metrics JSON files.
- DESS T2 map MRD images and per-region T2 metrics when KneePipeline's
  `steps.t2_mapping` writes `*_t2map.nii.gz` and `*_t2_results.json`.

## Segmentation labels

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

## Output geometry

Derived series are returned as per-slice MRD images resampled onto the source
image grid, and the scanner's DICOM writer defines the stored dimensions,
spacing, origin, and direction. An offline KneePipeline run on exported DICOMs
works on the DICOM-derived grid instead, so the two outputs are not
voxel-for-voxel identical: comparing them (for example computing DSC or ASSD
against an offline run) requires resampling one result onto the other's grid
first. This is inherent to OpenRecon, not a defect in either output.

## DESS and T2 caveat

OpenRecon receives MRD images, not the original DICOM. To preserve the DESS
T2 path, the wrapper keeps both echo groups and writes a minimal two-echo MR
DICOM series with `EchoNumbers`, TR/TE/flip angle, and DOSMA-compatible GL/TG
adapter fields. These fields let the pinned DOSMA code read the synthetic
input. They are not documented Siemens qDESS private attributes. For a
three-volume `sequenceName`, `sequenceName_fid`, and
`sequenceName_SE` input, the unsuffixed volume is excluded, segmentation uses
the voxelwise root-sum-of-squares of `_fid` and `_SE`, and fitting uses the two
echoes separately. Because the scanner labels both echoes with TE1, the wrapper
writes the second echo with
`TE2 = 2 * TR - TE1`. TR/TE1/flip are read from the MRD header or image
metadata when available. GL/TG can come from MRD user parameters or image
metadata; otherwise legacy saved-protocol values and then built-in DESS
defaults are used.

### Echo and spoiler metadata

TE1 and TE2 are not separate vendor-neutral DICOM attributes. Classic MR
stores `EchoTime (0018,0081)` on each echo and identifies it with
`EchoNumbers (0018,0086)`. Enhanced MR stores `EffectiveEchoTime (0018,9082)`
inside `MREchoSequence (0018,9114)`. This Siemens sequence sends TE1 for both
echo groups, so the wrapper uses the symmetric qDESS effective timing
`TE2 = 2 * TR - TE1`.

DOSMA reads two GE private user slots for its spoiled-qDESS model:

| DOSMA parameter | GE private element | qDESS meaning |
| --- | --- | --- |
| `GL_AREA` | `(0019,xxB6,"GEMS_ACQU_01")`, normally `(0019,10B6)` | Spoiler-gradient area in the inferred `G/cm * us` units |
| `TG` | `(0019,xxB7,"GEMS_ACQU_01")`, normally `(0019,10B7)` | Spoiler-gradient duration in microseconds |

GE names these fields `User data 15` and `User data 16`; the qDESS meanings
come from the GE research sequence and DOSMA. They are not Siemens private-tag
definitions. A private element must always be interpreted with its private
creator. The wrapper writes the physical `B6/B7` elements only as an adapter
for the pinned DOSMA reader, which does not validate the creator.

For the tested Siemens `%CustomerSeq%\wip_GRE_3D_DRB_dess` protocol,
the `.pro` export contains `sWipMemBlock.adFree[15] = 31.33`. The scanner UI
shows matching read/phase/slice gradient moments of `0.00`, `0.00`, and
`31.33 ms*mT/m`, identifying this sequence-specific WIP field as the spoiler
area. It converts to `GL_AREA = 3133` in DOSMA's inferred units. OpenMSK's
current `3132` fallback differs by `0.01 ms*mT/m`, consistent with rounding.
The export contains `TR = 15.19 ms` and `alTE[0] = 4900 us`, giving
`TE2 = 25.48 ms`, but it does not expose `TG`. OpenMSK therefore retains the
`TG = 1560 us` default. That duration implies a rectangular-gradient amplitude
of about `20.08 mT/m`; it must be confirmed against the
`wip_GRE_3D_DRB_dess` sequence source before treating it as measured metadata.

Standard Enhanced MR `Spoiling (0018,9016)` can report `RF`, `GRADIENT`,
`RF_AND_GRADIENT`, or `NONE`. It cannot encode the spoiler area, duration,
direction, or waveform. DOSMA reduces the scheme to the scalar area and
duration used in `delta-k = gamma * G * TG`.

See [DICOM gradient-tag research](dicom-gradient-tags.md) for the supporting
source trail and the remaining Siemens/TG uncertainties.

Some scanner exports instead interleave the two echoes in one named series as
equal-sized MRD `set=0` and `set=1` groups. OpenMSK keeps those sets together,
segments their root-sum-of-squares, and uses both sets separately for fitting.
LogViewer reports the detected series/set counts, grouping method, segmentation
and fitting routing, received TE labels, and the corrected TE values.

Runtime logs report the resolved value and source for every DESS fitting
parameter.

The `pymskt` right-knee reference used for cartilage subregion registration is
packaged into the container at build time. Subregion and T2-statistics
post-processing therefore does not require GitHub access at scanner runtime.
If subregion generation nevertheless fails and thickness was not requested,
OpenMSK computes global T2 statistics from the remapped femoral, medial tibial,
lateral tibial, and patellar cartilage labels and does not report that recovery
as a failed post-processing run.

## Segmentation models

The menu contains four knee tissue segmentation models. They all return the
nine anatomical labels listed above. T2 fitting, cartilage subregions, meshes,
and thickness measurements are separate KneePipeline steps that operate on the
selected model's segmentation.

| Menu choice | Method | Inference behavior |
| --- | --- | --- |
| `goyal_sagittal` | DOSMA/TensorFlow 2D network, default | Reformats the volume and processes sagittal 512 x 512 slices |
| `goyal_coronal` | DOSMA/TensorFlow 2D network | Reformats the same volume and processes coronal 160 x 512 slices |
| `goyal_axial` | DOSMA/TensorFlow 2D network | Reformats the same volume and processes axial 160 x 512 slices |
| `nnunet_knee` | PyTorch nnU-Net v2 3D residual-encoder network | Processes 3D full-resolution patches using the packaged fold 1 checkpoint |

The Goyal plane names describe the direction in which the 3D input is sliced
for 2D inference. They do not require sagittal, coronal, and axial acquisitions.
For two-echo qDESS input, DOSMA segments the voxelwise root-sum-of-squares
image. The three checkpoints are downloaded from Anthony Gatti's
[`aagatti/dosma_bones`](https://huggingface.co/aagatti/dosma_bones) repository
and run through the DOSMA
[`bone_seg`](https://github.com/gattia/DOSMA/blob/bone_seg/dosma/models/stanford_qdess_bone.py)
implementation.

The closest published description of the sagittal checkpoint is Goyal et al.,
["Automating Imaging Biomarker Analysis for Knee Osteoarthritis Using an
Open-Source MRI-Based Deep Learning Pipeline"](https://doi.org/10.1016/j.ostima.2025.100288).
The paper describes a Keras 2D network trained on 347 sagittal DESS and qDESS
scans: 176 Siemens 3T DESS scans from four sites, 155 GE 3T qDESS scans, and
16 Siemens 3T qDESS scans from subjects with anterior cruciate ligament
reconstruction. It reports optimization for sagittal fat-saturated
gradient-echo knee MRI. The public model repository does not provide a
checkpoint manifest that formally links the published evaluation to the
packaged file. It also does not document the coronal and axial checkpoints'
training cohorts or validation results.

An older menu choice named `acl_qdess_bone_july_2024` pointed to a checkpoint
that is byte-for-byte identical to `goyal_sagittal`, so it is no longer shown.
Saved protocols that still send the old identifier remain compatible and use
the `goyal_sagittal` file.

The `nnunet_knee` weights come from
[`aagatti/nnunet_knee`](https://huggingface.co/aagatti/nnunet_knee). Although
that repository's overview describes a two-stage cascade, this OpenMSK image
packages only the single-stage `3d_fullres` fold 1 `checkpoint_best.pth`. Its
metadata declares 342 training volumes, Z-score normalization, and the same
nine tissue labels. No case manifest is published, so the exact training
cohort and its relationship to the 347-scan Goyal dataset are not established.

## Parameters

- `sendoriginal`: return original images before derived outputs. Distinct source
  volumes are returned as separately labeled scanner series rather than being
  combined into one series.
- `segmodel`: choose `goyal_sagittal` (default), `goyal_coronal`,
  `goyal_axial`, or `nnunet_knee`. The packaged `nnunet_knee` path uses
  single preprocessing and export workers to limit scanner-side memory use.
- `computethickness`: run slower mesh/thickness analysis after the segmentation
  has been sent.

TR, TE1, and flip angle are read from MRD sequence parameters when available;
TE2 is computed as `2 * TR - TE1`. The current scanner MRD header does not
expose GL area or TG, so this protocol uses the built-in defaults. Acquisition
fallback fields and the ignored legacy `runnsm`/`runbscore` flags remain
accepted in saved protocol configurations, but are intentionally hidden from
the GUI.

## Build and validate

```bash
source env/bin/activate
python3 builder/validation.py recipes/openmsk/build.yaml
python -m builder generate openmsk --recreate --architecture x86_64
```

## Open source development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/openmsk

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
