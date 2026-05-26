# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation, choose one segment output mode that combines geometry and send
order, optionally add a segmentation colourmap, and compute a maximum intensity
projection. It can also send foreground-region volume metrics when
`sendmetrics` is enabled.

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
- `<source>-segment`: thresholded segmentation outputs starting on
  `image_series_index = 101` when `segment` is true. With
  `segmentoutput = 3d_volume` or `3d_volume_detached`, each source volume group
  is packed into one explicit 3D segmentation volume stamped as
  `DataRole = Segmentation`,
  `ImageType = DERIVED\PRIMARY\SEGMENTATION\openrecon_segment`,
  `ImageTypeValue4 = openrecon_segment`, `SegmentExplicitVolume = 1`, and
  `Keep_image_geometry = 0`. Its MRD header uses the center of the sorted source
  slab as the volume position and uses the sorted source spacing for the
  through-plane FOV so the volume overlaps the source stack on the scanner.
  With `segmentoutput = 2d_masks_first` or `2d_masks_after_originals`, the
  recipe sends one source-geometry 2D mask image per source image, sets
  `Keep_image_geometry = 1`, keeps the source MRD slice geometry, restamps
  returned-stream storage counters, stamps `DataRole = Image`, and carries
  `SegmentPostProcessing = 1`. This is the
  scanner-postprocessing path. It preserves source image-type composer identity,
  including Dixon subtype tokens, sorts returned masks by projected slice
  position, and restamps returned-stream header, Meta, and MiniHead storage
  counters so wrapped source slice numbers do not collide during composing. It
  also resets source grouping header fields such as contrast, phase, repetition,
  set, and average to zero so the returned masks form one derived stream rather
  than clashing with source children. `2d_masks_first` sends masks before
  originals so scanner MRA/MIP postprocessing attaches to masks;
  `2d_masks_after_originals` sends originals first and masks second.
  `3d_volume_detached` sends source-geometry outputs first and explicit 3D data
  later in a separate MRD image message, so scanner composition can attach to
  originals while 3D segmentation remains a detached derived output. All modes
  strip `ImageTypeValue3` because the scanner converter can reject that protocol
  node on returned images.
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
the result as binary `uint16` segmentation data. `segmentoutput` controls
whether segmentations are packed into explicit 3D volumes, detached from
source-geometry streams, or returned as source-like 2D image streams for scanner
postprocessing.
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

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentoutput`,
  `segmentationcolormap`, `mip`, and `sendmetrics` are exposed in
  `OpenReconLabel.json`. Boolean outputs default to false. `segmentoutput`
  defaults to `3d_volume`.
- Scanner protocols saved before these parameters were added may need the OpenRecon algorithm
  reselected once so the parameter schema refreshes.
- Runtime logs include an `openreconi2iexample runtime version=...` marker from
  the container environment. If this does not match the recipe version, the
  scanner is still using an older deployed image or cached protocol config.
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
  returned MRD slice. When `sendoriginal` is paired with `segment` and a 3D
  segment output mode, original pass-through images are sorted by projected
  physical slice position before restamping so original frame numbers align with
  the packed 3D segmentation volume frame order.
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
  The 3D segmentation path uses a non-Dixon segment identity and removes the
  source `IceMiniHead`. The 2D mask path preserves source geometry and source
  composer identity, including Dixon
  subtype tokens, so the scanner can compose the masks through the same route as
  the source scan.
- `sendoriginal` outputs are normally emitted before derived outputs.
  `segmentoutput = 2d_masks_first` sends the 2D segment stream first in its own
  MRD image message and the original stream follows.
  `segmentoutput = 2d_masks_after_originals` keeps the original stream first and
  sends the segment stream second. `segmentoutput = 3d_volume_detached` sends
  detachable 3D data in later MRD image messages after source-geometry images.
  This is the scanner-composition path for 3D segmentation: originals are sent
  first for Dixon or other source-side composing, and explicit 3D segmentation
  follows as detached derived data. The mode also applies to other explicit 3D
  outputs such as `upsampled` and packed-volume fallback outputs.
- Runtime logs include `OPENRECONI2I_POSTPROCESSING target=...` and one
  `OPENRECONI2I_BATCH ...` line before every MRD image send. Use these markers
  to confirm whether the scanner composer is expected to see `segment_2d`,
  `originals`, or no explicit-volume segmentation postprocessing target.
- `Keep_image_geometry = 1` is set on original pass-through and 2D mask
  segmentation images. If a source group has
  more images than the advertised source slice or partition count can hold, the
  2D source-geometry paths fail before send rather than packing those outputs as
  a derived volume. This keeps scanner MIP/MPR postprocessing on the same 2D
  contract as the scanner received.
- 2D mask segmentation outputs are emitted as one
  source-geometry 2D stream per source volume group while preserving source
  physical geometry. The stream is sorted by projected slice position before
  send, then returned header `image_index` and `slice` are restamped to
  contiguous `1..N` and `0..N-1` values. Returned storage counters in Meta and
  `IceMiniHead` are restamped from the returned segment stream, and source
  grouping header fields such as contrast, phase, repetition, set, and average
  are reset to zero to avoid composer child clashes.
- Explicit-volume outputs, such as 3D segmentation,
  `upsampled`, and fallback packed `invert` outputs, use
  `Keep_image_geometry = 0`, `matrix_size[2] = N`,
  `slice_count = NumberOfSlices = N`, and `partition_count = 1`. This prevents
  the marshaller from receiving invented source SLC indices such as `416..831`
  for a 416-partition sequence.
- Packed explicit-volume outputs place the MRD header position at the center of
  the sorted source slab. The voxel data still starts with the first sorted
  source slice; only the volume-level position is slab-centered so scanner 3D
  display and MIP/MPR geometry match the source stack.
- Explicit-volume packing requires unique projected MRD header positions. Inputs
  with repeated header positions are split by source volume fields where the
  packed output supports that. Source-geometry original and
  2D source-geometry segmentation outputs instead fail before send when the
  source geometry cannot safely represent the returned 2D image stream.
- The upsampled output always changes the slice count, so it removes the source
  `IceMiniHead`, sends one MRD volume image with `matrix_size[2] = 2 * N`, and
  stamps `slice_count = NumberOfSlices = 2 * N` plus `partition_count = 1`
  before send.
- The upsampled volume writes origin, direction, and full through-plane field of
  view on the MRD `ImageHeader`. Matching Meta entries such as
  `SlicePosLightMarker`, `ImageRowDir`, `ImageColumnDir`, and
  `ImageSliceNormDir` are diagnostic copies and must match the header values.
