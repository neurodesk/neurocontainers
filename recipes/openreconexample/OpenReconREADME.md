# OpenRecon Example

`openreconexample` is an older OpenRecon image-in/image-out example. It receives
reconstructed magnitude MRD image messages, writes them to NIfTI, runs BET2, and
returns the BET2-processed image stream. It can also return copied original
images before the processed output.

## Recommended Sequence

Use a reconstructed 3D brain magnitude image series with consistent slice
geometry. The wrapper groups images by series index, runs BET2 once per image
group, and then re-slices the processed NIfTI volume back into MRD images.

For new image-to-image development, prefer `openreconi2iexample`, which is the
current reference template and scanner-contract testbed.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `openreconexample` | Selects the MRD server configuration. |
| Send original images | `sendoriginal` | boolean | `true` | Sends copied original images before the BET2 output. |

## Runtime Notes

- Non-magnitude images are passed through with source geometry preserved.
- BET2 is run with a fixed fractional intensity threshold of `0.65`.
- Returned processed images preserve source geometry and add an example ROI
  metadata field.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/openreconexample

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
