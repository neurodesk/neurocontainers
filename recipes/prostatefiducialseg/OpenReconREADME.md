# Prostate Fiducial Segmentation OpenRecon

`prostatefiducialseg` is an OpenRecon image-in/image-out package for prostate
fiducial segmentation. It receives reconstructed T1-weighted prostate images,
preprocesses the volume, runs the bundled `predict3.py` ensemble models, and
returns the source image data with detected fiducial boundaries overlaid.

## Recommended Sequence

Use a 3D T1-weighted prostate acquisition with consistent slice geometry. The
wrapper expects a complete image volume, writes it to NIfTI, applies N4 bias
field correction, runs the bundled fiducial models, and re-slices the result
back into MRD image messages.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `prostatefiducialseg` | Selects the MRD server configuration. |

## Runtime Notes

- The OpenRecon label exposes only `config`; model paths and postprocessing are
  fixed in the wrapper.
- The runtime uses the models staged under `/opt/models/*.pth`.
- Returned images preserve source geometry and add an example ROI metadata
  field.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/prostatefiducialseg

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
