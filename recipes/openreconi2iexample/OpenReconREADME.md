# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation with a colourmap, and compute a maximum intensity projection.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages.
- All image messages can be returned as copied original images when
  `sendoriginal` is enabled.
- Magnitude images (`IMTYPE_MAGNITUDE` or unset image type) are processed by
  `invert`, `upsampled`, `segment`, and `mip`.

## Outputs

- No output is sent by default.
- `<source>-inverted`: inverted magnitude images on `image_series_index = 99`
  when `invert` is true.
- `<source>-original`: copied input images on `image_series_index = 100` when
  `sendoriginal` is true.
- `<source>-segment`: thresholded segmentation images on
  `image_series_index = 101` when `segment` is true. These outputs set
  `LUTFileName = MicroDeltaHotMetal.pal`.
- `<source>-upsampled`: twice as many magnitude images on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one maximum intensity projection image on
  `image_series_index = 103` when `mip` is true.

The inverted images keep the source geometry and use the input intensity range:
`inverted = min(input) + max(input) - input`.
The segment output estimates a bright-foreground threshold across the received
image stack, keeps the largest connected foreground object in each slice, and
stores the result as a colour-mapped binary `uint16` segmentation.
The upsampled output keeps the in-plane matrix unchanged and doubles the
through-plane sample count by sorting source images by physical slice position
and inserting midpoint slices between acquired slices. The final edge slice is
duplicated so the output count is exactly `2 * N`.
The MIP output projects the source magnitude stack across all source slices.

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, and `mip` are exposed in
  `OpenReconLabel.json` and all default to false.
- Scanner protocols saved before these parameters were added may need the OpenRecon algorithm
  reselected once so the parameter schema refreshes.
- Output names are written to `SeriesDescription`, `SequenceDescription`,
  `ProtocolName`, `ImageComments`, `SeriesNumberRangeNameUID`, and
  `SeriesInstanceUID`. Source-geometry outputs patch matching values into
  `IceMiniHead` when source images include one.
- Copied originals are returned as derived scanner outputs, not as reused source
  slices. The wrapper restamps `SOPInstanceUID`, `NumberInSeries`, `SliceNo`,
  `AnatomicalSliceNo`, and `ChronSliceNo` in both MRD Meta and `IceMiniHead`
  before sending.
- Scanner partition counters such as `Actual3DImagePartNumber` and
  `AnatomicalPartitionNo` are kept at zero for returned 2D image series.
- Derived outputs set `SequenceDescriptionAdditional` to `openrecon` so
  scanners do not append `_None` to the display name.
- `Keep_image_geometry = 1` is set on source-geometry outputs. OpenRecon's
  marshaller can size the SLC dimension from the source `IceMiniHead` unless
  explicit `slice_count` and `partition_count` Meta entries are present with
  `Keep_image_geometry = 0`. The upsampled output changes the slice count, so it
  removes the source `IceMiniHead` and sends explicit `slice_count = 2 * N` and
  `partition_count = 1` before send.
- The upsampled series writes position and direction on the MRD `ImageHeader`.
  Matching Meta entries such as `SlicePosLightMarker`, `ImageRowDir`,
  `ImageColumnDir`, and `ImageSliceNormDir` are diagnostic copies and must match
  the header values.
