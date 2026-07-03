# AFI B1 OpenRecon

`afib1` is an OpenRecon image-in/image-out package for Actual Flip Angle (AFI)
B1 mapping. It receives reconstructed magnitude MRD image messages from an AFI
sequence with two TR conditions, estimates the actual flip angle, and returns
either an actual-flip-angle map or a B1+ percent-units map.

## Recommended Sequence

Use an AFI B1 mapping sequence that sends matching TR1 and TR2 images for the
same slice geometry. Leave `interleaved` disabled when the first half of the
incoming image stream is TR1 and the second half is TR2. Enable `interleaved`
when TR1 and TR2 frames alternate through the stream.

If `brainmask` is enabled, the wrapper writes the TR1 and TR2 volumes to NIfTI,
runs BET2, cleans the resulting mask, and uses it before smoothing the B1 map.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `afib1` | Selects the MRD server configuration. |
| Send original images | `sendoriginal` | boolean | `false` | Sends copied original images before the B1 output. |
| Interleaved data | `interleaved` | boolean | `false` | Treats the stream as alternating TR1/TR2 frames. |
| B1+ map style | `b1output` | choice | `afa` | Sends actual flip angle in degrees or B1+ in percent units. |
| Brain mask? | `brainmask` | boolean | `false` | Applies a BET2-derived brain mask before smoothing. |
| FWHM for mask smoothing | `maskfwhm` | double | `5.0` | Smooths the brain mask in mm. |
| Mask erodes | `masknerode` | int | `2` | Number of erosion operations applied to the mask. |
| Mask dilates | `maskndilate` | int | `4` | Number of dilation operations applied to the mask. |
| Mask threshold | `maskthresh` | double | `0.5` | Threshold applied after mask smoothing. |
| Signal threshold | `signalthresh` | double | `60.0` | Minimum TR1 signal used for stable ratio calculation. |
| FWHM for map smoothing | `b1fwhm` | double | `8.0` | Smooths the B1 map in mm; use `0` to disable. |
| TR Ratio | `trratio` | double | `5.0` | Ratio of TR2 to TR1 used by the AFI equation. |
| Nominal Flip Angle | `nominalfa` | double | `60.0` | Nominal excitation flip angle in degrees. |

## Runtime Notes

- Non-magnitude images are passed through with source geometry preserved.
- The derived B1 output is returned as source-geometry MRD image data with
  `SequenceDescriptionAdditional = AFI B1+ Map`.
- The wrapper writes temporary NIfTI files for BET2 and debugging in the runtime
  working directory, not under `/home`.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/afib1

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
