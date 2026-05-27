# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation, independently choose segment geometry, 3D series marking, send order,
and postprocessing stamping, optionally add a segmentation colourmap, and compute
a maximum intensity projection. It can also send foreground-region volume metrics when
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
  `image_series_index = 101` when `segment` is true. `segmentgeometry = 3d`
  sends one scanner-safe 3D-derived series per source volume group, with one
  source-geometry MRD image per source slice. Each returned slice has
  `DataRole = Segmentation`, `SegmentSourceGeometry = 1`,
  `SegmentOutputGeometry = 3d`, `Keep_image_geometry = 1`, source physical
  geometry, unique returned slice counters, and restamped Meta / MiniHead
  storage identity. `detach3ddata = true` stamps `Detached3DData = 1` on the
  3D segment series; it no longer creates a packed MRD z-stack.
  `segmentgeometry = 2d` sends one source-geometry 2D mask image per
  source image with `Keep_image_geometry = 1`, sorted projected slice order, and
  restamped returned-stream storage counters. `segmentsendorder` controls whether
  segment outputs are batched before or after original pass-through outputs.
  `segmentpostprocessingstamp = true` forces Segment into source-geometry 2D
  masks, disables `sendoriginal`, ignores `detach3ddata`, stamps
  `SegmentPostProcessing = 1`, preserves the source image-type identity used by
  scanner compose filters, and aligns geometry Meta such as
  `SlicePosLightMarker` with the returned MRD header. All segment paths strip
  `ImageTypeValue3` because the scanner converter can reject that protocol node
  on returned images.
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
the result as binary `uint16` segmentation data. `segmentgeometry`,
`detach3ddata`, and `segmentsendorder` are independent controls for normal
segment output. `segmentpostprocessingstamp` is a constrained scanner
postprocessing mode because it cannot safely be combined with original
pass-through or normal 3D segment series output.
The upsampled output keeps the in-plane matrix unchanged and doubles the
through-plane sample count by sorting source images by physical slice position
and inserting midpoint slices between acquired slices. The final edge slice is
duplicated so the output volume contains exactly `2 * N` slices in one MRD
image message.
The MIP output projects the source magnitude stack across all source slices.
The metrics output reuses the same foreground segmentation logic as `segment`.
When `segment` and `sendmetrics` are both enabled, each segmentation output also
writes the region volume into `ImageComments` and `ImageComment`.

## Scanner Option Test Matrix

Use these empty tables for scanner testing with `segment = true` and
`sendoriginal = true`. Keep `invert`, `upsampled`, `segmentationcolormap`,
`mip`, and `sendmetrics` disabled unless a row is deliberately extended for
another output test. The matrix enumerates every combination of the independent
segment-output controls.

Current runtime behavior constrains `segmentpostprocessingstamp = true`: those
rows ignore `sendoriginal`, force `segmentgeometry = 2d`, and ignore
`detach3ddata` before sending.

### TOF

| ID | segmentgeometry | detach3ddata | segmentsendorder | segmentpostprocessingstamp | Notes |
| --- | --- | --- | --- | --- | --- |
| TOF-01 | 3d | false | after_originals | false | MIPs based on org, segment at the end |
| TOF-02 | 3d | false | after_originals | true | MIPs based on org, segment at the end |
| TOF-03 | 3d | false | before_originals | false | segment first, then org, then MIPs based on org,  |
| TOF-04 | 3d | false | before_originals | true | org first, then MIPs based on org |
| TOF-05 | 3d | true | after_originals | false | org first, then MIP based on org, then seg |
| TOF-06 | 3d | true | after_originals | true | org first, then MIP based on org, then seg  |
| TOF-07 | 3d | true | before_originals | false | seg first, then org, then MIPs based on org |
| TOF-08 | 3d | true | before_originals | true | seg first, then org, then MIPs based on org |
| TOF-09 | 2d | false | after_originals | false | org first, then MIP org+seg alternating, Radials only on org |
| TOF-10 | 2d | false | after_originals | true | MIP based on org |
| TOF-11 | 2d | false | before_originals | false | seg first, then MIP seg+org altnernating,  |
| TOF-12 | 2d | false | before_originals | true | seg first, then MIP based on seg, then org |
| TOF-13 | 2d | true | after_originals | false | org first, then MIP org+seg alternating, Radials only on org |
| TOF-14 | 2d | true | after_originals | true | MIPs based on org |
| TOF-15 | 2d | true | before_originals | false | segmentation is send first, MIP seg+org alternating |
| TOF-16 | 2d | true | before_originals | true | seg first, then MIPS based on seg, then org|

best:
| TOF-10 | 2d | false | after_originals | true | MIP based on org |
| TOF-14 | 2d | true | after_originals | true | MIP based on org |

### Wholebody multistation protocol Dixon recon F/W

| ID | segmentgeometry | detach3ddata | segmentsendorder | segmentpostprocessingstamp | Notes |
| --- | --- | --- | --- | --- | --- |
| WB-01 | 3d | false | after_originals | false | fat, water, FF, then F/W/FF seg in two series with misc, composing for F/W/FF org |
| WB-02 | 3d | false | after_originals | true | org first, seg in two series, composing org |
| WB-03 | 3d | false | before_originals | false | F/W/FF seg in two series with misc, then F/W/FF, composing for F/W/FF org  |
| WB-04 | 3d | false | before_originals | true | seg first in two series, then F/W/FF, composing of org |
| WB-05 | 3d | true | after_originals | false | FW/FF org first, seg in two series, composing of org |
| WB-06 | 3d | true | after_originals | true | org first, then seg in two series (NOT GOOD!), composing of org |
| WB-07 | 3d | true | before_originals | false | seg first in two series, then org, composing of org |
| WB-08 | 3d | true | before_originals | true | org first, seg in two series (not good), composing of org |
| WB-09 | 2d | false | after_originals | false | F/W/FF org, F/W/FF seg in one series, composing for F/W/FF org |
| WB-10 | 2d | false | after_originals | true | F/W/FF org, segments in groups (not great!), NO COMPOSE!!!! |
| WB-11 | 2d | false | before_originals | false | F/W/FF seg, F/W/FF org, composing F/W/FF org |
| WB-12 | 2d | false | before_originals | true | F/W/FF org, seg in groups (not great!), NO COMPOSE!!!! |
| WB-13 | 2d | true | after_originals | false | org first, seg next, org compose |
| WB-14 | 2d | true | after_originals | true | org first, seg in groups (NOT good!),  |
| WB-15 | 2d | true | before_originals | false | seg first, then org, org compose |
| WB-16 | 2d | true | before_originals | true | seg first in groups (not great), then org, NO COMPOSE!!!!|

best runs:
| WB-09 | 2d | false | after_originals | false | F/W/FF org, F/W/FF seg in one series, composing for F/W/FF org |
| WB-13 | 2d | true | after_originals | false | org first, seg next, org compose |

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentgeometry`,
  `detach3ddata`, `segmentsendorder`, `segmentpostprocessingstamp`,
  `segmentationcolormap`, `mip`, and `sendmetrics` are exposed in
  `OpenReconLabel.json`. Boolean outputs default to false, `segmentgeometry`
  defaults to `3d`, and `segmentsendorder` defaults to `after_originals`.
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
- `segmentpostprocessingstamp = true` is mutually exclusive with original
  pass-through at runtime. If both are enabled in the protocol, the recipe sends
  only the stamped 2D segment masks so the scanner does not receive duplicate
  source-like streams.
- Original pass-through preserves the source MRD header geometry and pixel data.
  Returned originals use a one-based per-series `image_index`. Scanner storage
  counters in Meta and `IceMiniHead` are restamped consistently from the
  returned stream: `NumberInSeries` follows `image_index`, `ChronSliceNo`
  follows returned order, and `SliceNo` / `ProtocolSliceNumber` follow the
  returned MRD slice. When `sendoriginal` is paired with `segment` and a 3D
  segment output mode, original pass-through images are sorted by projected
  physical slice position before restamping so original frame numbers align with
  the returned 3D segment series.
- Scanner partition counters such as `Actual3DImagePartNumber` and
  `AnatomicalPartitionNo` are kept at zero for original pass-through and derived
  image series, while slice counters such as `SliceNo` and `ChronSliceNo` are
  restamped for the returned stream.
- Derived outputs other than stamped postprocessing set
  `SequenceDescriptionAdditional` to `openrecon` so scanners do not append
  `_None` to the display name. Original pass-through and stamped
  postprocessing preserve the incoming value and do not synthesize an
  `openrecon` suffix.
- Returned source-geometry outputs strip scanner `ImageTypeValue3` from both MRD
  metadata and `IceMiniHead`. Some sequences reject that protocol node during
  OpenRecon conversion. Original pass-through preserves Dixon subtype identity
  through `ImageType`, `DicomImageType`, `ComplexImageComponent`, and
  `ImageTypeValue4`. Stamped segment postprocessing masks also preserve the
  source `ImageType`, `DicomImageType`, and full `ImageTypeValue4` token list so
  Dixon and orientation compose filters can classify the returned mask stream.
  The normal 3D and 2D mask paths preserve source geometry and overwrite stale
  copied geometry Meta with the actual returned MRD header orientation and
  position.
- `segmentsendorder = before_originals` sends the segment stream before the
  original stream when both are enabled. `segmentsendorder = after_originals`
  sends originals first and segment outputs second. This has no effect when
  `segmentpostprocessingstamp = true`, because original pass-through is disabled
  for that mode.
- `detach3ddata = true` stamps normal 3D segment series slices with
  `Detached3DData = 1`. The scanner-visible segment is still sent as per-slice
  source-geometry images rather than one packed MRD volume. This can be combined
  with either segment send order, but is ignored when
  `segmentpostprocessingstamp = true`.
- Runtime logs include `OPENRECONI2I_POSTPROCESSING target=...`, the independent
  segment controls in the configured-output line, and one `OPENRECONI2I_BATCH`
  line before every MRD image send.
- `Keep_image_geometry = 1` is set on original pass-through and normal 3D / 2D
  mask segmentation images. If a source group has
  more images than the advertised source slice or partition count can hold, the
  source-geometry paths fail before send rather than packing those outputs as
  a derived volume. This keeps scanner MIP/MPR postprocessing on the same 2D
  contract as the scanner received.
- Normal 3D and 2D mask segmentation outputs are emitted as one
  source-geometry 2D stream per source volume group while preserving source
  physical geometry. The stream is sorted by projected slice position before
  send, then returned header `image_index` and `slice` are restamped to
  contiguous `1..N` and `0..N-1` values. Returned storage counters in Meta and
  `IceMiniHead` are restamped from the returned segment stream, and source
  grouping header fields such as contrast, phase, repetition, set, and average
  are reset to zero to avoid composer child clashes.
- Explicit-volume outputs, such as `upsampled` and fallback packed `invert`
  outputs, use
  `Keep_image_geometry = 0`, `matrix_size[2] = N`,
  `slice_count = NumberOfSlices = N`, and `partition_count = 1`. This prevents
  the marshaller from receiving invented source SLC indices such as `416..831`
  for a 416-partition sequence.
- Packed explicit-volume outputs place the MRD header position at the center of
  the sorted output slab and stamp the full through-plane FOV.
- Explicit-volume packing requires unique projected MRD header positions. Inputs
  with repeated header positions are split by source volume fields where the
  packed output supports that. Source-geometry original and normal 3D / 2D
  segmentation outputs instead fail before send when the
  source geometry cannot safely represent the returned 2D image stream.
- The upsampled output always changes the slice count, so it removes the source
  `IceMiniHead`, sends one MRD volume image with `matrix_size[2] = 2 * N`, and
  stamps `slice_count = NumberOfSlices = 2 * N` plus `partition_count = 1`
  before send.
- The upsampled volume writes origin, direction, and full through-plane field of
  view on the MRD `ImageHeader`. Matching Meta entries such as
  `SlicePosLightMarker`, `ImageRowDir`, `ImageColumnDir`, and
  `ImageSliceNormDir` are diagnostic copies and must match the header values.
