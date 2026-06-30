# ARFIProc OpenRecon

`arfiproc` is a fixed-configuration OpenRecon image-in/image-out package for
ARFI processing experiments. The current wrapper receives reconstructed MRD
image messages, writes the magnitude/phase image group to NIfTI, reads it back,
normalizes it to 12-bit integer range, and returns source-geometry MRD images.

## Recommended Sequence

Use reconstructed ARFI image data with consistent slice geometry in one incoming
series. The wrapper groups image messages by `image_series_index`; when a new
series starts, it processes and returns the previous group.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `arfiproc` | Selects the MRD server configuration. |

## Runtime Notes

- The OpenRecon label exposes only `config`; processing settings are fixed in
  the wrapper.
- Output images preserve source geometry with `Keep_image_geometry = 1`.
- The wrapper adds an example ROI metadata field to returned images.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/arfiproc

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
