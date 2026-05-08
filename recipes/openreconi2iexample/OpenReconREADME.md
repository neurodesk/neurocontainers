# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation, optionally add a segmentation colourmap, and compute a maximum
intensity projection.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages.
- All image messages can be returned as copied original images when
  `sendoriginal` is enabled.
- Magnitude images (`IMTYPE_MAGNITUDE` or unset image type) are processed by
  `invert`, `upsampled`, `segment`, and `mip`.

## Outputs

- No output is sent by default.
- `<source>-inverted`: inverted magnitude images on `image_series_index = 99`
  when `invert` is true. If the source geometry advertises fewer slice or
  partition slots than the number of received images, these are packed into one
  explicit volume image instead of returned as individual source-geometry
  frames.
- `<source>-original`: copied input images on `image_series_index = 100` when
  `sendoriginal` is true. Received images are split by their source series
  before restamping, so scanner-generated processed inputs such as inline MIP
  images are returned as their own derived copies instead of being folded into
  a larger source volume. The same explicit-volume fallback is used per source
  group when that group's geometry cannot safely hold every received image.
  When a processing output is enabled and the scanner injects additional source
  groups besides the primary volume group, those auxiliary groups are preserved
  automatically even if `sendoriginal` is false.
- `<source>-segment`: thresholded segmentation images on
  `image_series_index = 101` when `segment` is true. These outputs set
  `LUTFileName = MicroDeltaHotMetal.pal` only when `segmentationcolormap` is
  true. The same explicit-volume fallback is used for source geometry that
  cannot safely hold every received image.
- `<source>-upsampled`: one volume image with twice as many slices on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one maximum intensity projection image on
  `image_series_index = 103` when `mip` is true.

The inverted images normally keep the source geometry and use the input
intensity range: `inverted = min(input) + max(input) - input`.
The segment output estimates a bright-foreground threshold across the received
image stack, keeps the largest connected foreground object in each slice, and
stores the result as a binary `uint16` segmentation.
The upsampled output keeps the in-plane matrix unchanged and doubles the
through-plane sample count by sorting source images by physical slice position
and inserting midpoint slices between acquired slices. The final edge slice is
duplicated so the output volume contains exactly `2 * N` slices in one MRD
image message.
The MIP output projects the source magnitude stack across all source slices.

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentationcolormap`, and
  `mip` are exposed in `OpenReconLabel.json` and all default to false.
- Scanner protocols saved before these parameters were added may need the OpenRecon algorithm
  reselected once so the parameter schema refreshes.
- Output names are written to `SeriesDescription`, `SequenceDescription`,
  `ProtocolName`, `ImageComments`, `SeriesNumberRangeNameUID`, and
  `SeriesInstanceUID`. Source-geometry outputs patch matching values into
  `IceMiniHead` when source images include one. Explicit-volume outputs remove
  the source `IceMiniHead`.
- Copied originals are returned as derived scanner outputs, not as reused source
  slices. If `sendoriginal` receives multiple source series, additional groups
  are assigned separate derived `image_series_index` values so the scanner can
  store them as independent returned series. For source-geometry outputs, the
  wrapper restamps `SOPInstanceUID`, `NumberInSeries`, `SliceNo`,
  `AnatomicalSliceNo`, and `ChronSliceNo` in both MRD Meta and `IceMiniHead`
  before sending.
- Derived processing uses the unique largest received magnitude source group as
  the primary volume when auxiliary scanner-generated groups are present. The
  auxiliary groups are copied back unchanged except for derived output identity
  fields. If there is no unique primary group, all magnitude images are processed
  together as before and no automatic auxiliary split is applied.
- Scanner partition counters such as `Actual3DImagePartNumber` and
  `AnatomicalPartitionNo` are kept at zero for returned image series.
- Derived outputs set `SequenceDescriptionAdditional` to `openrecon` so
  scanners do not append `_None` to the display name.
- `Keep_image_geometry = 1` is set on source-geometry outputs only when the
  output frame count fits within the source slice or partition count. If a
  scanner sequence sends more images than the advertised source geometry slots,
  `invert` and `segment` are packed into one explicit volume image, while
  `original` is split by source group and only the oversized group is packed.
  Packed explicit-volume outputs use `Keep_image_geometry = 0`,
  `matrix_size[2] = N`, `slice_count = NumberOfSlices = N`, and
  `partition_count = 1`. This prevents the marshaller from receiving invented
  source SLC indices such as `416..831` for a 416-partition sequence.
- Explicit-volume packing requires unique projected MRD header positions. Inputs
  with repeated header positions, such as multi-station or multi-contrast
  wholebody data whose header `position` resets between chunks, fail before send
  with a Python-side OpenRecon logging error instead of sending an unsafe output.
- The upsampled output always changes the slice count, so it removes the source
  `IceMiniHead`, sends one MRD volume image with `matrix_size[2] = 2 * N`, and
  stamps `slice_count = NumberOfSlices = 2 * N` plus `partition_count = 1`
  before send.
- The upsampled volume writes origin, direction, and full through-plane field of
  view on the MRD `ImageHeader`. Matching Meta entries such as
  `SlicePosLightMarker`, `ImageRowDir`, `ImageColumnDir`, and
  `ImageSliceNormDir` are diagnostic copies and must match the header values.
