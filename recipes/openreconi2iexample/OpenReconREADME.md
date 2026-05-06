# OpenRecon Image-to-Image Invert Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages, creates inverted copies of magnitude
images, and names those copies from the source scan plus `-inverted`. It also
re-emits the original scan when `sendoriginal` is enabled. When
`sendthresholdmip` is enabled, it thresholds the magnitude volume and sends one
segmentation maximum intensity projection slice. When `sendinterpolated` is
enabled, it sends a double-slice-count through-plane interpolated image series.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages.
- Magnitude images (`IMTYPE_MAGNITUDE` or unset image type) are inverted.
- Magnitude images are thresholded for the segmentation MIP when
  `sendthresholdmip` is enabled.
- Magnitude images are interpolated between slices when `sendinterpolated` is
  enabled.
- All image messages can be returned as the original scan.

## Outputs

- `<source>-inverted`: inverted magnitude images on `image_series_index = 99`.
- Original scan: unmodified input images on `image_series_index = 100` when
  `sendoriginal` is true.
- `<source>-mip`: one thresholded segmentation maximum intensity projection
  slice on `image_series_index = 101` when `sendthresholdmip` is true.
- `<source>-upsampled`: twice as many magnitude images on
  `image_series_index = 102` when `sendinterpolated` is true.

The inverted images keep the source geometry and use the input intensity range:
`inverted = min(input) + max(input) - input`.
The threshold MIP uses `mean(input) + 0.5 * std(input)` as the cutoff, then
projects the binary segmentation across the source image slices.
The interpolated output keeps the in-plane matrix unchanged and doubles the
through-plane sample count by inserting midpoint slices between acquired slices.
The final edge slice is duplicated so the output count is exactly `2 * N`.

## Scanner Notes

- `sendoriginal` is exposed in `OpenReconLabel.json` and defaults to true.
- `sendthresholdmip` is exposed in `OpenReconLabel.json` and defaults to false.
- `sendinterpolated` is exposed in `OpenReconLabel.json` and defaults to false.
- Output names are written to `SeriesDescription`, `SequenceDescription`,
  `ProtocolName`, and `ImageComments`.
- Derived outputs set `SequenceDescriptionAdditional` to an explicit blank
  value so scanners do not append `_None` or duplicate the display name.
- `Keep_image_geometry = 1` is set on all returned image outputs.
