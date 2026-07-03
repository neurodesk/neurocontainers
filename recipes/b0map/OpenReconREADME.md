# B0 Map OpenRecon

`b0map` is an OpenRecon image-in/image-out package for B0-map development. It
receives reconstructed MRD image messages, keeps source geometry, and returns a
processed image stream using the B0-map wrapper path.

## Recommended Sequence

Use a dual-echo B0 mapping acquisition where the two echo conditions share the
same slice geometry. The label exposes echo-ordering and delta-TE controls for
that workflow. The current wrapper keeps the incoming image geometry and is best
treated as an experimental B0-map adapter.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `b0map` | Selects the MRD server configuration. |
| Send original images | `sendoriginal` | boolean | `false` | Sends copied original images before derived output. |
| Interleaved data | `interleaved` | boolean | `false` | Indicates whether the two echo streams are interleaved. |
| FWHM for mask smoothing | `maskfwhm` | double | `5.0` | Smooths the brain mask in mm. |
| Mask erodes | `masknerode` | int | `2` | Number of erosion operations applied to the mask. |
| Mask dilates | `maskndilate` | int | `4` | Number of dilation operations applied to the mask. |
| Mask threshold | `maskthresh` | double | `0.5` | Threshold applied after mask smoothing. |
| Delta TE | `deltaTe` | double | `5.0` | Echo-time difference for B0-map calculation. |

## Runtime Notes

- Non-magnitude images are ignored by the processing group in the current
  wrapper.
- Returned images use source geometry and `SequenceDescriptionAdditional = AFI
  B1+ Map`, inherited from the older AFI-derived implementation.
- The mask and delta-TE controls document the intended B0 workflow, but verify
  scanner results before relying on this package for production use.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/b0map

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
