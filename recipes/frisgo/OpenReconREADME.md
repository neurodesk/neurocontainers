# FRISGO Fuzzy Ripple Correction

`frisgo` is an OpenRecon image-to-image package that mitigates Fuzzy Ripple
artifacts in dual-polarity (3D-)EPI BOLD time series. It waits for the complete
run, assembles all repetitions into a 4D NIfTI time series and runs
`LN2_FRISGO -input timeseries.nii -tshift` from LayNii. LN2_FRISGO estimates
the signal halfway between successive TRs with a cubic interpolation across
four time points, which aligns the trigger timing with the k-space centre and
cancels the polarity-alternating ripple.

It is built for reconstructed magnitude BOLD runs with at least four
repetitions, sent as one 2D image per slice (or partition) and repetition.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages from one or more image series.
- Images are grouped by source series. Within a series they are ordered by
  physical position along the slice normal and by the MRD `repetition`
  counter. If every repetition counter is equal, arrival order per slice is
  used as time order.
- Every repetition must contain every slice; an incomplete grid fails the job
  before anything is sent.
- FRISGO is applied to magnitude series (`IMTYPE_MAGNITUDE` or unset image
  type) with at least four repetitions. Other series are only returned as
  originals.

## Outputs

- Original time series on `image_series_index = 100` when `sendoriginal` is
  true, with new series UIDs and unchanged pixel data.
- `<source>-frisgo`: the LN2_FRISGO corrected time series on
  `image_series_index = 101` when `sendfrisgo` is true, with
  `ImageType = DERIVED\PRIMARY\M\FRISGO`.
- Additional source series use `image_series_index = 102/103`, `104/105`, ...
- Originals are sent first, then the FRISGO series, as separate MRD image
  messages.

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `config` | `frisgo` | MRD server module to run. |
| `sendoriginal` | `true` | Return the original BOLD time series. |
| `sendfrisgo` | `true` | Return the FRISGO corrected BOLD time series. |

## Runtime notes

- Both outputs keep the source 2D image geometry, slice and repetition
  counters, with `Keep_image_geometry = 1` and a one-based `image_index` in
  time order. Series identity, IceMiniHead and storage-field handling follow
  the `openreconi2iexample` reference, including stripping `ImageTypeValue3`.
- Corrected values are rounded to the source pixel type. For unsigned scanner
  images, the rare negative values LN2_FRISGO produces at the background edge
  are clipped to 0.
- The first and last two time points are averages of neighbouring volumes, as
  implemented by `LN2_FRISGO -tshift`.
- No output is produced until the last image of the run arrives.
- Scanner logs contain a `frisgo runtime version=...` marker and the full
  LN2_FRISGO console output.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/frisgo

LN2_FRISGO is part of LayNii: https://github.com/layerfMRI/LAYNII

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
