# TopoFit OpenRecon

This OpenRecon workflow runs BrainNet TopoFit on a reconstructed 3D MPRAGE
during the scanner session. It generates bilateral white, pial, and spherical
registration surfaces in FreeSurfer geometry format. It then returns a
scanner-visible axial QC series on the exact source image grid.

This is a research workflow, not a complete FreeSurfer reconstruction. It does
not run `recon-all`, cortical parcellation, or longitudinal processing.

## Input and output

The input must be one reconstructed three-dimensional magnitude image series.
The adapter sorts slices by physical position, converts center-based MRD/LPS
geometry and reconstructed pixel directions to NIfTI RAS, and writes one
NIfTI volume for BrainNet. By default, BrainNet conforms the data internally
to its expected 1 mm RAS grid.

Each successful run writes these artifacts below `/tmp/share/topofit`:

- `surf/lh.white` and `surf/rh.white`
- `surf/lh.pial` and `surf/rh.pial`
- `surf/lh.registration` and `surf/rh.registration`
- `topofit_qc.nii.gz`
- `topofit_manifest.json`

The QC volume preserves the input dimensions and affine. It displays the
source anatomy with pial vertices at intensity 3500 and white vertices at
intensity 4095. OpenRecon sends that volume back as
`<source>_topofit_qc`. When **Keep original images** is enabled, a restamped
`<source>_original` series is sent first.

Surface artifacts stay in the run workspace so a later in-container analysis
stage can consume them. This first milestone does not export prescription
coordinates.

## Parameters

| GUI label | Parameter | Default | Meaning |
| --- | --- | --- | --- |
| Keep original images | `sendoriginal` | `true` | Return a restamped input series before QC. |
| Inference device | `tfdevice` | `cuda` | Run on the scanner GPU or use CPU for compatibility testing. |
| TopoFit model | `tfmodel` | `t1w_1mm` | Select the T1w or synthetic pretrained model. |
| Conform input | `tfconform` | `true` | Resample internally to the model grid. |
| Mock surfaces | `tfdebugmock` | `false` | Exercise MRD transport and geometry without neural inference. |

The mock option follows the full scanner conversion, QC, metadata, and return
path, but creates six tiny synthetic meshes. It is useful for a quick scanner
integration test. Mock artifacts are not analysis results.

## Safety boundary

Every QC image and manifest is marked:

`RESEARCH ONLY - NOT MOTION-CLEARED - NOT FOR PRESCRIPTION`

The manifest explicitly sets `prescription_coordinates` to `null`. A later
stage must define the target, assess motion and surface quality, transform the
result into the scanner frame, and pass independent validation before any
coordinate can be actionable.

The adapter fails closed: it buffers outputs until every required bilateral
surface has been written and parsed successfully. If validation or inference
fails, it sends an MRD error and no derived image series.

## Standalone test

The container exposes the same workflow as a command-line tool:

```bash
topofit-openrecon mprage.nii.gz output --device cuda
topofit-openrecon mprage.nii.gz transport-test --device cpu --mock
```

The first command runs the real model. The second tests the artifact and QC
path in seconds.

## Citation

Please cite the BrainNet/TopoFit publication and software release used by your
study. BrainNet source and release information is available at
https://github.com/simnibs/brainnet.
