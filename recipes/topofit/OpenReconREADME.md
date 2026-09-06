# TopoFit OpenRecon

This OpenRecon workflow runs BrainNet TopoFit on a reconstructed 3D anatomical
MR image during the scanner session. It generates bilateral white, pial, and
spherical registration surfaces in FreeSurfer geometry format. It then returns
scanner-visible QC series on the exact source image grid.

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
- `topofit_patch_qc.nii.gz` when flat-patch analysis is enabled
- `topofit_patch_geometry.npz` with selected ribbon coordinates and local normals
- `topofit_sulcal_middepth_mask.nii.gz` when sulcal analysis is enabled
- `topofit_manifest.json`

The QC volume preserves the input dimensions and affine. It displays the
source anatomy with pial vertices at intensity 3500 and white vertices at
intensity 4095. The surface thickness control sets the in-plane dilation
radius. A value of zero keeps only the projected vertices. A value of one
retains the previous width. OpenRecon sends that volume back as
`<source>_topofit_qc`. When **Keep original images** is enabled, a restamped
`<source>_original` series is sent first.

When **Find cortical patches** is enabled, the workflow searches the middle
of the cortical ribbon: `white + 0.5 * (pial - white)`.
It maps the fsaverage cortex labels through TopoFit's registration spheres,
excludes a 5 mm mesh-edge margin around the medial-wall closure, and rejects
white-to-pial separations below 0.5 mm. These conservative eligibility guards
are not validated tissue-classification or cortical-thickness thresholds.
They do not exclude all medial cortex by a midline coordinate cutoff.

Candidates grow along mesh edges within the configured radius, default 10 mm.
The default search returns up to three non-overlapping patches per hemisphere.
It evaluates up to `max(64, 32 * requested_count)` seeds per hemisphere.
This is a heuristic search, not an exhaustive count of all suitable cortex.
Accepted patches have area-weighted plane-fit RMS at most 0.5 mm, area at least
`0.25 * pi * radius^2`, and signed normal coherence at least 0.9. The default
minimum area is 78.5 mm². RMS and area cutoffs are configurable research settings,
not clinically validated thresholds. Scoring combines RMS and normal coherence.
Ranked candidates that share any mesh vertex with an accepted patch are removed.
Fewer patches, or none, are returned if the criteria cannot be met. A failed
quality search produces `NO_PATCH_MEETS_CRITERIA`, not a relaxed fallback.
An orientation filter retains a connected part of one cortical bank. The
reported center is an actual mid-surface vertex, not a centroid in CSF.
The workflow returns a separate
`<source>_topofit_patch_qc` series with each selected patch filled on the source
grid (intensity 3000), its white boundary (2400), and its pial boundary (2700).
A center ring shows the true in-plane normal component at physical scale (4095).
A dot means that the normal points toward increasing slice positions. A cross
means that it points toward decreasing slice positions. This convention keeps
a through-plane normal visible without drawing a false in-plane arrow. The
result manifest stores the centers and normals in NIfTI world RAS. Each QC
image comment also contains the centers and unit normals in scanner
patient-space LPS.

Patch IDs such as `LH01` and `RH03` appear beside their normal glyphs. They are
ranked within each hemisphere for that run, not longitudinal anatomical labels.
All patches share one source-grid QC series. The JSON manifest records accepted
counts, IDs, scores, areas, RMS errors, normal coherence, and local geometry paths.
The offline preview groups the patches into a ranked contact sheet with cortex zooms.

`topofit_patch_geometry.npz` contains `<patch_id>_vertex_indices`, local triangle
indices in `<patch_id>_faces`, and paired `<patch_id>_white_ras_mm`,
`<patch_id>_mid_ras_mm`, and `<patch_id>_pial_ras_mm` arrays. The
`<patch_id>_normals_ras` array contains unit, area-weighted local mid-surface
normals oriented from white matter toward pial. Diffusion analysis needs these
local normals, not the single representative plane normal drawn for QC.

`--patch-roi roi.nii.gz` restricts selection to positive mask values on the
input NIfTI grid. The affine and shape must match the scan. The OpenRecon
configuration uses `tfpatchregion=roi` and the container-side path `tfpatchroi`.
Whole-cortex mode ignores a previously supplied ROI path. The package does not
upload or register a mask; it must be supplied through a container volume mount.
A unilateral mask returns only that hemisphere. An empty cortical ROI fails
without falling back to whole-brain selection.

McNab et al. (2013), *Surface based analysis of diffusion orientation for
identifying architectonic domains in the in vivo human cortex*, Tian et al.'s
OHBM 2017 poster *In Vivo Identification of Granular Cortices using Whole-brain
Cortical Diffusion MRI Analysis*, and the supplied 2026 FCD draft motivate
ribbon sampling and local normals. This workflow provides the
structural geometry, not a reproduction of their diffusion results. It does
not identify FCD, register diffusion or FLAIR, compute radiality or directional
ADC, or generate matched contralateral controls. Without an ROI, the
automatic patches are independent candidates and need anatomical review.

Surface artifacts stay in the run workspace so later research analysis can
consume them. Flat-patch coordinates are candidates only. They are not
motion-cleared prescription coordinates.

When **Find sulcal mid-depth voxels** is enabled, the workflow computes signed
cotangent mean curvature on each pial mesh. Negative curvature is concave and
sulcal. The same cortical eligibility mask excludes the medial-wall transition
and collapsed ribbon. Faces whose three vertices meet the threshold are moved to
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
| Find cortical patches | `tfflatpatches` | `false` | Find connected mid-cortical candidates and return patch-and-normal QC with LPS geometry in image comments. |
| Maximum patches per hemisphere | `tfpatchcount` | `3` | Return up to 1–10 accepted patches per selected hemisphere. |
| Cortical patch radius | `tfpatchradius` | `10 mm` | Mesh-edge radius, 5–20 mm. |
| Patch hemisphere | `tfpatchhemisphere` | `both` | Both, `lh`, or `rh`. Reconstruction remains bilateral. |
| Patch search region | `tfpatchregion` | `cortex` | Whole eligible cortex or `roi`. |
| Native-space ROI path | `tfpatchroi` | empty | Container-visible NIfTI mask path, used only in ROI mode. |
| Configuration-only maximum plane-fit error | `tfpatchmaxrms` | `0.5 mm` | Advanced acceptance cutoff, 0.01–2 mm RMS. |
| Configuration-only minimum area fraction | `tfpatchminarea` | `0.25` | Advanced area cutoff as a fraction of `pi * radius^2`, 0.1–1. |
| Find sulcal mid-depth voxels | `tfsulcalmiddepth` | `false` | Write a source-grid label mask for curvature-defined sulci at 50% cortical depth. |
| Sulcal curvature threshold | `tfsulcalthreshold` | `0.1 mm^-1` | Set the minimum magnitude of negative mean curvature. |
| Surface thickness | `tfoverlaythickness` | `1` voxel | Set the in-plane dilation radius from 0 to 3 voxels. |

## Safety boundary

The scanner label uses all 14 parameter slots allowed by OpenRecon schema 1.1.0.
The existing controls are retained. `tfpatchmaxrms` and `tfpatchminarea` are
advanced JSON configuration or CLI options, not additional scanner controls.
The schema permits only 0.1 steps for doubles, so it cannot represent an area
fraction of 0.25 as a numeric control without changing the parameter's units.

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
topofit-openrecon mprage.nii.gz multiple-patches --find-flat-patches --patch-count 3 --patch-radius 10 --patch-max-rms 0.5 --patch-min-area 0.25
topofit-openrecon mprage.nii.gz roi-patch --find-flat-patches --patch-roi roi.nii.gz
topofit-openrecon mprage.nii.gz sulci --find-sulcal-middepth --sulcal-curvature-threshold 0.1
```

The first command runs the real model. The second tests the artifact and QC
path in seconds. The third enables flat-patch analysis and uses the thinnest
surface trace.

The repository includes two offline verification scripts. `verify_dicom.py`
reads one Enhanced MR volume, creates MRD frames, and runs the real adapter.
It verifies the returned geometry and pixels before exporting a derived QC
DICOM. It does not test the scanner connection or replace the scanner's DICOM
converter. `preview_patch_qc.py` renders the selected geometry over source
anatomy in a contact sheet with one selected plane and cortex zoom per patch.
It requires Pillow.

The multi-patch manifest uses schema version 2 in `flat_patch_definition`.
Its `flat_patches` dictionary and NPZ arrays are keyed by patch ID rather than
hemisphere. The default count is three when analysis is enabled. A count of one
restores the previous output count without restoring the old manifest schema.

## Citation

Please cite the BrainNet/TopoFit publication and software release used by your
study. BrainNet source and release information is available at
https://github.com/simnibs/brainnet.
