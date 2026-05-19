# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation, optionally mark segmentations for scanner postprocessing,
optionally add a segmentation colourmap, and compute a maximum intensity
projection. It can also send foreground-region volume metrics when `sendmetrics`
is enabled.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages.
- All image messages can be returned as copied original images when
  `sendoriginal` is enabled.
- Magnitude images (`IMTYPE_MAGNITUDE` or unset image type) are processed by
  `invert`, `upsampled`, `segment`, `mip`, and `sendmetrics`.

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
- `<source>-segment`: source-geometry thresholded segmentation slice streams
  starting on `image_series_index = 101` when `segment` is true. The recipe sends
  one 2D mask image per source image, sets `Keep_image_geometry = 1`, keeps the
  source MRD slice geometry, restamps returned-stream storage counters, and uses
  separate segment series identity in Meta and `IceMiniHead`.
  With `segmentpostprocessing = false`, masks are stamped as
  `DataRole = Segmentation` with
  `ImageType = DERIVED\PRIMARY\SEGMENTATION\openrecon_segment`,
  `ImageTypeValue4 = openrecon_segment`, and `SegmentSourceGeometry = 1`; they
  are source-geometry images but are not marked as scanner magnitude images for
  inline postprocessing. With `segmentpostprocessing = true`, masks keep the same
  source geometry and source image-type composer classification, are stamped as
  `DataRole = Image`, and carry `SegmentPostProcessing = 1`. Both modes strip
  `ImageTypeValue3` because the scanner converter can reject that protocol node
  on returned images. If `sendoriginal` is also checked with
  `segmentpostprocessing = true`, the original passthrough stream is sent first
  in a separate MRD image message.
- `<source>-upsampled`: one volume image with twice as many slices on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one maximum intensity projection image on
  `image_series_index = 103` when `mip` is true.
- `<source>-metrics`: one derived DICOM image-table page on
  `image_series_index = 120` when `sendmetrics` is true. The table reports the
  segmented foreground region, source name, voxel count, voxel volume, threshold,
  and volume in `mm3` and `mL`. It is sent as a standalone explicit derived
  output so scanner inline postprocessing does not attach to it. The rendered
  pixels are pre-oriented for scanner display and use standalone identity
  geometry instead of inheriting scan orientation.

The inverted images normally keep the source geometry and use the input
intensity range: `inverted = min(input) + max(input) - input`.
The segment output estimates a bright-foreground threshold per source volume
group, keeps the largest connected foreground object in each slice, and stores
the result as binary `uint16` segmentation data. Segmentations are returned as
source-like 2D image streams in both modes. `segmentpostprocessing` only changes
whether those source-geometry masks are marked as scanner-postprocessing
candidate images.
The upsampled output keeps the in-plane matrix unchanged and doubles the
through-plane sample count by sorting source images by physical slice position
and inserting midpoint slices between acquired slices. The final edge slice is
duplicated so the output volume contains exactly `2 * N` slices in one MRD
image message.
The MIP output projects the source magnitude stack across all source slices.
The metrics output reuses the same foreground segmentation logic as `segment`.
When `segment` and `sendmetrics` are both enabled, each segmentation output also
writes the region volume into `ImageComments` and `ImageComment`.

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentpostprocessing`,
  `segmentationcolormap`, `mip`, and `sendmetrics` are exposed in
  `OpenReconLabel.json` and all default to false.
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
  Returned originals use a one-based per-series `image_index`. Scanner storage
  counters in Meta and `IceMiniHead` are restamped consistently from the
  returned stream: `NumberInSeries` follows `image_index`, `ChronSliceNo`
  follows returned order, and `SliceNo` / `ProtocolSliceNumber` follow the
  returned MRD slice.
- Scanner partition counters such as `Actual3DImagePartNumber` and
  `AnatomicalPartitionNo` are kept at zero for original pass-through and derived
  image series, while slice counters such as `SliceNo` and `ChronSliceNo` are
  restamped for the returned stream.
- Derived outputs set `SequenceDescriptionAdditional` to `openrecon` so
  scanners do not append `_None` to the display name. Original pass-through
  preserves the incoming value and does not synthesize an `openrecon` suffix.
- Returned source-geometry outputs strip scanner `ImageTypeValue3` from both MRD
  metadata and `IceMiniHead`. Some sequences reject that protocol node during
  OpenRecon conversion. Original pass-through preserves Dixon subtype identity
  through `ImageType`, `DicomImageType`, `ComplexImageComponent`, and
  `ImageTypeValue4`.
  Segmentation outputs preserve source geometry. With `segmentpostprocessing`
  disabled they use a non-Dixon segment identity so scanner Dixon composing does
  not count masks as extra `FAT`, `WATER`, or `FAT_FRAC` source images. With
  `segmentpostprocessing` enabled they preserve the source composer identity so
  the scanner can compose the masks through the same route as the source scan.
- `sendoriginal` outputs are emitted before derived outputs. When
  `segmentpostprocessing` is enabled with `segment`, the original stream remains
  first and is sent in its own MRD image message; the segment stream follows as a
  separate source-geometry 2D image message. This keeps the scanner composer from
  receiving two source-like returned streams in one image-message group.
- `Keep_image_geometry = 1` is set on original pass-through and segmentation
  images. If a source group has more images than the advertised source slice or
  partition count can hold, the job fails before send rather than packing those
  source-geometry outputs as a derived volume. This keeps scanner MIP/MPR
  postprocessing on the same 2D contract as the scanner received.
- Segmentation outputs are emitted as one source-geometry 2D stream per source
  volume group while preserving source slice geometry. A valid source header
  `image_index` is preserved; zero-valued source image indices are replaced with
  a one-based returned index because returned MRD images must have
  `image_index >= 1`. Returned storage counters in Meta and `IceMiniHead` are
  restamped from the returned segment stream.
- Explicit-volume outputs, such as `upsampled` and fallback packed `invert`
  outputs, use `Keep_image_geometry = 0`, `matrix_size[2] = N`,
  `slice_count = NumberOfSlices = N`, and `partition_count = 1`. This prevents
  the marshaller from receiving invented source SLC indices such as `416..831`
  for a 416-partition sequence.
- Explicit-volume packing requires unique projected MRD header positions. Inputs
  with repeated header positions are split by source volume fields where the
  packed output supports that. Source-geometry original and segmentation outputs
  instead fail before send when the source geometry cannot safely represent the
  returned 2D image stream.
- The upsampled output always changes the slice count, so it removes the source
  `IceMiniHead`, sends one MRD volume image with `matrix_size[2] = 2 * N`, and
  stamps `slice_count = NumberOfSlices = 2 * N` plus `partition_count = 1`
  before send.
- The upsampled volume writes origin, direction, and full through-plane field of
  view on the MRD `ImageHeader`. Matching Meta entries such as
  `SlicePosLightMarker`, `ImageRowDir`, `ImageColumnDir`, and
  `ImageSliceNormDir` are diagnostic copies and must match the header values.
