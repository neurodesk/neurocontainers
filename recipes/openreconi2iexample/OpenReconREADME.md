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
- original pass-through: copied input images on `image_series_index = 100` when
  `sendoriginal` is true. Received images are split by source or source-volume
  group and returned as independent 2D streams. The source protocol,
  sequence, image-type, MiniHead, slice, partition, and pixel data are preserved
  as much as possible; only returned-series identity and required safe storage
  fields are changed.
- `<source>-segment`: explicit-volume thresholded segmentation output(s)
  starting on `image_series_index = 101` when `segment` is true. Each output sets
  `Keep_image_geometry = 0` and removes the source `IceMiniHead`, so scanner
  inline MIP handling is not attached to the segmentation when originals are
  also returned. Inputs with repeated physical slice positions are split by
  source volume fields such as MRD `contrast` before packing; additional
  segmentation groups use separate derived series indices. Segmentations set
  `LUTFileName = MicroDeltaHotMetal.pal` only when `segmentationcolormap` is
  true.
- `<source>-upsampled`: one volume image with twice as many slices on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one maximum intensity projection image on
  `image_series_index = 103` when `mip` is true.

The inverted images normally keep the source geometry and use the input
intensity range: `inverted = min(input) + max(input) - input`.
The segment output estimates a bright-foreground threshold per source volume
group, keeps the largest connected foreground object in each slice, and stores
the result as binary `uint16` explicit-volume segmentation image(s).
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
- Derived output names are written to `SeriesDescription`, `SequenceDescription`,
  `ProtocolName`, `ImageComments`, `SeriesNumberRangeNameUID`, and
  `SeriesInstanceUID`. Source-geometry derived outputs patch matching values
  into `IceMiniHead` when source images include one. Explicit-volume outputs
  remove the source `IceMiniHead`.
- Copied originals are returned as source-native 2D pass-through images, not as
  derived packed volumes. If `sendoriginal` receives multiple source or
  source-volume groups, additional groups are assigned separate returned
  `image_series_index`, `SeriesNumberRangeNameUID`, `SeriesInstanceUID`, and
  `SOPInstanceUID` values so the scanner can store them as independent returned
  series while scanner postprocessing still sees source-like images.
- Original pass-through preserves the source MRD header geometry and pixel data.
  If the incoming MRD `image_index` is zero, the returned copy uses a one-based
  per-series `image_index`; missing `IceMiniHead` storage counters are filled
  from source metadata or the source slice number.
- Scanner partition counters such as `Actual3DImagePartNumber` and
  `AnatomicalPartitionNo` are preserved for original pass-through images and
  kept at zero for derived image series.
- Derived outputs set `SequenceDescriptionAdditional` to `openrecon` so
  scanners do not append `_None` to the display name. Original pass-through
  preserves the incoming value and does not synthesize an `openrecon` suffix.
- Derived outputs strip scanner `ImageTypeValue3` from both MRD metadata and
  `IceMiniHead`. Some sequences reject that protocol node during OpenRecon
  conversion. Original pass-through keeps source image-type metadata as intact
  as possible but normalizes returned `ImageTypeValue3` values to `M` when that
  classifier field exists. This preserves Dixon `ImageType` / `ImageTypeValue4`
  subtype metadata while avoiding the scanner's unknown `MAP` classifier path on
  returned fat-fraction originals. Derived output identity is carried by
  `ImageType`, `DicomImageType`, `ComplexImageComponent`, and `ImageTypeValue4`.
- `sendoriginal` outputs are emitted before derived outputs. This keeps scanner
  inline MIP/MPR postprocessing attached to the original series when originals
  and segmentations are enabled together.
- `Keep_image_geometry = 1` is set on original pass-through images. If an
  original source group has more images than the advertised source slice or
  partition count can hold, the job fails before send rather than packing those
  originals as a derived volume. This keeps scanner MIP/MPR postprocessing on
  the same 2D contract as the scanner received.
- `segment` and `upsampled` are always packed as explicit-volume outputs with
  `Keep_image_geometry = 0`, `matrix_size[2] = N`,
  `slice_count = NumberOfSlices = N`, and `partition_count = 1`. This prevents
  the marshaller from receiving invented source SLC indices such as `416..831`
  for a 416-partition sequence.
- Explicit-volume packing requires unique projected MRD header positions. Inputs
  with repeated header positions are split by source volume fields before
  segmentation packing. Multi-contrast wholebody data whose header `position`
  repeats per contrast is returned as one explicit segmentation volume per
  contrast. Inputs that still repeat positions inside one source volume group
  fail before send with a Python-side OpenRecon logging error instead of sending
  an unsafe output.
- The upsampled output always changes the slice count, so it removes the source
  `IceMiniHead`, sends one MRD volume image with `matrix_size[2] = 2 * N`, and
  stamps `slice_count = NumberOfSlices = 2 * N` plus `partition_count = 1`
  before send.
- The upsampled volume writes origin, direction, and full through-plane field of
  view on the MRD `ImageHeader`. Matching Meta entries such as
  `SlicePosLightMarker`, `ImageRowDir`, `ImageColumnDir`, and
  `ImageSliceNormDir` are diagnostic copies and must match the header values.
