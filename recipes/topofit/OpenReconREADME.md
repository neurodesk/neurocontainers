# TopoFit OpenRecon

This OpenRecon workflow runs BrainNet TopoFit on a reconstructed 3D anatomical
MR image during the scanner session. It generates bilateral white, pial, and
spherical registration surfaces in FreeSurfer geometry format. It then returns
a scanner-visible axial QC series on the exact source image grid.

This is a research workflow, not a complete FreeSurfer reconstruction. It does
not run `recon-all`, cortical parcellation, or longitudinal processing.

## Input and output

The input must be one reconstructed three-dimensional magnitude image series.
MP2RAGE is not required. MPRAGE and other non-MP2RAGE 3D anatomical GRE series
pass through the input selector unchanged. For an MP2RAGE scan, only the
denoised uniform (`UNI-DEN`) contrast is processed; INV1, INV2, UNI, and other
contrasts in the same stream are ignored. An MP2RAGE stream without a
`UNI-DEN` contrast is rejected rather than processed as a different anatomical
contrast.

The adapter sorts slices by physical position, converts center-based MRD/LPS
geometry and reconstructed pixel directions to NIfTI RAS, and writes one
NIfTI volume for BrainNet. By default, BrainNet conforms the data internally
to its expected 1 mm RAS grid.

Each successful run writes these artifacts below `/tmp/share/topofit`:

- `surf/lh.white` and `surf/rh.white`
- `surf/lh.pial` and `surf/rh.pial`
- `surf/lh.registration` and `surf/rh.registration`
- `topofit_qc.nii.gz`
- `topofit_sulcal_middepth_mask.nii.gz` when sulcal analysis is enabled
- `topofit_manifest.json`

The QC volume preserves the input dimensions and affine. It displays the
source anatomy with pial vertices at intensity 3500 and white vertices at
intensity 4095. The surface thickness control sets the in-plane dilation
radius. A value of zero keeps only the projected vertices. A value of one
retains the previous width. OpenRecon sends that volume back as
`<source>_topofit_qc`. When **Keep original images** is enabled, a restamped
`<source>_original` series is sent first.

When **Find flat pial patches** is enabled, the workflow searches each pial
mesh for the 10 mm-radius neighborhood with the lowest area-weighted plane-fit
residual and coherent face normals. It draws both selected patches and their
20 mm outward normals into the QC pixels. The result manifest stores the
centers and normals in NIfTI world RAS. Each QC image comment also contains the
centers in scanner patient-space LPS millimetres and the unit normals in LPS.

Surface artifacts stay in the run workspace so later research analysis can
consume them. Flat-patch coordinates are candidates only. They are not
motion-cleared prescription coordinates.

When **Find sulcal mid-depth voxels** is enabled, the workflow computes signed
cotangent mean curvature on each pial mesh. Negative curvature is concave and
sulcal. Faces whose three vertices meet the configured threshold are moved to
50% cortical depth using the corresponding white and pial vertices. The
workflow writes every source-grid voxel cell intersected by those faces to
`topofit_sulcal_middepth_mask.nii.gz`. Labels 1 and 2 mark the left and right
hemispheres; 3 marks overlap. The mask keeps the source shape and affine.

## Parameters

| GUI label | Parameter | Default | Meaning |
| --- | --- | --- | --- |
| Keep original images | `sendoriginal` | `true` | Return a restamped input series before QC. |
| Inference device | `tfdevice` | `cuda` | Run on the scanner GPU or use CPU for compatibility testing. |
| TopoFit model | `tfmodel` | `t1w_1mm` | Select the T1w or synthetic pretrained model. |
| Conform input | `tfconform` | `true` | Resample internally to the model grid. |
| Find flat pial patches | `tfflatpatches` | `false` | Find one candidate per hemisphere, draw its patch and normal, and write LPS geometry into image comments. |
| Find sulcal mid-depth voxels | `tfsulcalmiddepth` | `false` | Write a source-grid label mask for curvature-defined sulci at 50% cortical depth. |
| Sulcal curvature threshold | `tfsulcalthreshold` | `0.1 mm^-1` | Set the minimum magnitude of negative mean curvature. |
| Surface thickness | `tfoverlaythickness` | `1` voxel | Set the in-plane dilation radius from 0 to 3 voxels. |

## Safety boundary

Every QC image and manifest is marked:

`RESEARCH ONLY - NOT MOTION-CLEARED - NOT FOR PRESCRIPTION`

The manifest explicitly keeps `prescription_coordinates` set to `null`, even
when it records research-only flat-patch candidates. A later stage must assess
motion and surface quality and pass independent validation before any
coordinate can be actionable.

The adapter fails closed: it buffers outputs until every required bilateral
surface has been written and parsed successfully. If validation or inference
fails, it sends an MRD error and no derived image series.

## Standalone test

The container exposes the same workflow as a command-line tool:

```bash
topofit-openrecon mprage.nii.gz output --device cuda
topofit-openrecon mprage.nii.gz transport-test --device cpu --mock
topofit-openrecon mprage.nii.gz flat-patches --find-flat-patches --overlay-thickness 0
topofit-openrecon mprage.nii.gz sulci --find-sulcal-middepth --sulcal-curvature-threshold 0.1
```

The first command runs the real model. The second tests the artifact and QC
path in seconds. The third enables flat-patch analysis and uses the thinnest
surface trace.

## Citation

Please cite the BrainNet/TopoFit publication and software release used by your
study. BrainNet source and release information is available at
https://github.com/simnibs/brainnet.
