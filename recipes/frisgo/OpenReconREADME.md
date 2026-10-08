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
- Images are grouped by source series and by echo/contrast, phase, set and
  average counters. Within a series they are ordered by
  physical position along the slice normal and by the MRD `repetition`
  counter. If every repetition counter is equal, arrival order per slice is
  used as time order.
- Correction requires every repetition to contain every slice and nonconstant
  repetition counters to be consecutive and unique per slice. Incomplete grids,
  missing repetitions and duplicate counters skip correction for that group.
  Requested originals are still returned, and other groups are processed.
- FRISGO is applied to magnitude series (`IMTYPE_MAGNITUDE` or unset image
  type) with at least four repetitions. Other series are only returned as
  originals.

## Outputs

- Original time series on `image_series_index = 100` when `sendoriginal` is
  true, with new series UIDs and unchanged pixel data.
- `<source>-frisgo`: the LN2_FRISGO corrected time series on
  `image_series_index = 101` when `sendfrisgo` is true, with
  `ImageType = DERIVED\PRIMARY\M\FRISGO`.
- Additional source groups and chunks use `image_series_index = 102/103`,
  `104/105`, ... Each output series contains at most 65,535 images, with a new
  series UID and image numbering starting at 1 for every chunk. Source slice
  and repetition counters stay intact across chunks.
- Each group's originals are sent before correction, followed by its FRISGO
  series, in batches of at most 128 images.

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
- Output starts after the scanner closes the input stream. The input run is
  buffered in memory, so available scanner memory must cover the source run
  and LayNii's floating-point working arrays. Output copies are sent in small
  batches; the Python input NIfTI array is released before LayNii starts.
- Scanner logs contain a `frisgo runtime version=...` marker and the full
  LN2_FRISGO console output.
- Temporary NIfTI files are stored in `/tmp/share/frisgo`, on the scanner share
  mounted by FIRE, rather than inside its fixed-size chroot image. Set
  `FRISGO_WORKDIR` to use another writable scratch filesystem. There is no
  automatic fallback to `/tmp` if the selected directory is unavailable.
- Before writing, FRISGO checks space for the float32 input and corrected output
  plus 64 MiB of headroom. A 236 x 228 x 208 x 10 run needs approximately 918 MiB
  free. Insufficient space skips correction with a clear log message; requested
  originals are still returned. Temporary files are removed after success or
  failure. Corrected NIfTIs are read without memory mapping so their files are
  closed before deletion on network-mounted shares. Concurrent runs and longer
  sequences need additional free space.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/frisgo

LN2_FRISGO is part of LayNii: https://github.com/layerfMRI/LAYNII

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
