# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends outputs according to the
enabled output options. The scanner label defaults enable original pass-through
and segmentation. It can re-emit the original scan, invert
magnitude images, upsample the slice direction, threshold each slice into a
segmentation, return segment reformats, compute a maximum intensity projection,
and send foreground-region volume metrics.

## Inputs

- Reconstructed MRD `ismrmrd.Image` messages.
- All image messages can be returned as copied original images when
  `sendoriginal` is enabled.
- Magnitude images (`IMTYPE_MAGNITUDE` or unset image type) are processed by
  `invert`, `upsampled`, `segment`, `sendreformatsagittal`,
  `sendreformatcoronal`, `sendcomputedmip`, and `sendmetrics`.

## Outputs

- No output is sent if all output options are disabled or no parameter payload is
  provided.
- `<source>-inverted`: inverted magnitude images on `image_series_index = 99`
  when `invert` is true. If the source geometry advertises fewer slice or
  partition slots than the number of received images, these are packed into one
  explicit volume image instead of returned as individual source-geometry
  frames.
- original pass-through: copied input images on `image_series_index = 100` when
  `sendoriginal` is true. Received images are split by source or source-volume
  group and returned as independent 2D streams. The source protocol, sequence,
  image-type, MiniHead, slice, partition, and pixel data are preserved as much as
  possible; only returned-series identity and required safe storage fields are
  changed.
- `<source>-segment`: thresholded segmentation outputs starting on
  `image_series_index = 101` when `segment` is true.
  `segmentheadergeometry = explicit_volume_derived_header` sends one explicit
  derived segment volume per source group with `DataRole = Image`,
  `SegmentExplicitVolume = 1`, and `Keep_image_geometry = 0`.
  `segmentheadergeometry = 3d_series_segment_header` sends one 3D
  source-geometry segment series per source volume group with one MRD image per
  source slice, `DataRole = Segmentation`, `SegmentSourceGeometry = 1`, and
  `Keep_image_geometry = 1`.
  `segmentheadergeometry = 2d_segment_header` sends one 2D
  source-geometry mask per source image with `DataRole = Segmentation`,
  `SegmentSourceGeometry = 1`, and the explicit segmentation `ImageType`.
  `segmentheadergeometry = 2d_derived_image_header` uses the same 2D
  source-geometry header path, but stamps the mask as `DataRole = Image` with a
  derived segment `ImageType`.
  `segmentheadergeometry = 2d_source_image_header` uses the same
  2D source-geometry header path, but stamps the mask as `DataRole = Image` and
  preserves the source `ImageType`, `DicomImageType`, and `ImageTypeValue4`
  identity.
  When original pass-through is also enabled, originals are sent first and
  segment outputs are sent after them in a separate MRD image message.
- `<source>-upsampled`: one volume image with twice as many slices on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one computed maximum intensity projection image on
  `image_series_index = 103` when `sendcomputedmip` is true.
- `<source>-segment-sagittal` and `<source>-segment-coronal`: explicit 3D
  segment reformat volume(s) per requested orientation starting on
  `image_series_index = 121` when `sendreformatsagittal` or
  `sendreformatcoronal` is true. Reformats are derived from the same threshold
  segment data as `<source>-segment`; if `segment` is false, segmentation is
  computed internally and only the requested reformats are sent.
- `<source>-metrics`: one derived DICOM image-table page on
  `image_series_index = 120` when `sendmetrics` is true. The table reports the
  segmented foreground region, source name, voxel count, voxel volume,
  threshold, and volume in `mm3` and `mL`.

The segment output estimates a bright-foreground threshold per source volume
group, keeps the largest connected foreground object in each slice, and stores
the result as binary `uint16` segmentation data. Metrics reuse the same
foreground segmentation logic even when `segment` is disabled.

## Scanner Option Test Matrix

### TOF

| ID | segmentheadergeometry | Notes |
| --- | --- | --- |
| TOF-01 | explicit_volume_derived_header | explicit derived segment volume |
| TOF-02 | 3d_series_segment_header | 3D source geometry, segmentation header |
| TOF-03 | 2d_segment_header | 2D source geometry, segmentation header |
| TOF-04 | 2d_derived_image_header | 2D source geometry, derived image header |
| TOF-05 | 2d_source_image_header | 2D source geometry, source image header |

### Wholebody Multistation Protocol Dixon Recon F/W

| ID | segmentheadergeometry | Notes |
| --- | --- | --- |
| WB-01 | explicit_volume_derived_header | explicit derived segment volume |
| WB-02 | 3d_series_segment_header | 3D source geometry, segmentation header |
| WB-03 | 2d_segment_header | 2D source geometry, segmentation header |
| WB-04 | 2d_derived_image_header | 2D source geometry, derived image header |
| WB-05 | 2d_source_image_header | 2D source geometry, source image header |

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentheadergeometry`,
  `segmentationcolormap`, `sendreformatsagittal`, `sendreformatcoronal`,
  `sendcomputedmip`, and `sendmetrics` are exposed in `OpenReconLabel.json`.
- Scanner protocols saved before these parameters were added may need the
  OpenRecon algorithm reselected once so the parameter schema refreshes.
- Runtime logs include an `openreconi2iexample runtime version=...` marker from
  the container environment. If this does not match the recipe version, the
  scanner is still using an older deployed image or cached protocol config.
- Runtime logs include `OPENRECONI2I_POSTPROCESSING target=...`, the configured
  output line, and one `OPENRECONI2I_BATCH` line before every MRD image send.
- Derived output names are written to `SeriesDescription`,
  `SequenceDescription`, `ProtocolName`, `ImageComments`,
  `SeriesNumberRangeNameUID`, and `SeriesInstanceUID`.
- Original pass-through preserves the source MRD header geometry and pixel data.
  Returned originals use a one-based per-series `image_index`. Scanner storage
  counters in Meta and `IceMiniHead` are restamped consistently from the
  returned stream.
- The scanner-label default segment mode is
  `2d_segment_header`. It returns source-geometry 2D masks with
  segmentation header identity after the original pass-through stream.
- `segmentheadergeometry = 2d_source_image_header` keeps original
  pass-through enabled when requested. The original stream is sent first, and
  the source-image-header mask stream is sent second.
- `segmentheadergeometry = 2d_derived_image_header` sends 2D source-geometry
  masks as `DataRole = Image` while using a derived segment `ImageType` rather
  than the source scan's image-type identity.
- When originals and segments are both enabled, originals are sent first and
  segment outputs are sent second in a separate MRD image message.
- Returned source-geometry outputs strip scanner `ImageTypeValue3` from both MRD
  metadata and `IceMiniHead`. Some sequences reject that protocol node during
  OpenRecon conversion.
- Explicit-volume outputs use `Keep_image_geometry = 0`, remove the source
  `IceMiniHead`, set `image_index = 1` and `slice = 0`, and keep
  `matrix_size[2]`, `slice_count`, `NumberOfSlices`, and
  `ImagesInAcquisition` aligned.
- Packed explicit-volume outputs place the MRD header position at the center of
  the sorted output slab and copy header geometry into Meta fields such as
  `ImageRowDir`, `ImageColumnDir`, `ImageSliceNormDir`, and
  `SlicePosLightMarker`.
- Explicit-volume packing requires unique projected MRD header positions.
  Source-geometry original and source-geometry segmentation outputs fail before
  send when the source geometry cannot safely represent the returned image
  stream.
- Segment reformats are explicit 3D volumes with centered slab geometry,
  `SegmentReformat = 1`, and `SegmentReformatOrientation = sagittal` or
  `coronal`. They are not stamped for later scanner postprocessing.
