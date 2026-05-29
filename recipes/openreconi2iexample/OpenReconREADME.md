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
  `segmentheadergeometry = 2d_segment_header_originals` uses the same 2D
  source-geometry segmentation header identity, but omits scanner
  postprocessing child-role metadata from the segment stream so scanner
  postprocessing uses originals only when `sendoriginal` is enabled.
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

Observed in `log_i2i_error.log` from the `tof_i2i_74_*` runs with runtime
version `1.0.78`.

| ID | segmentheadergeometry | Observed postprocessing target and batches | Scanner storage result | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| TOF-01 | explicit_volume_derived_header | `postprocessing_target=originals`; 40 original images with `keep_geometry=1`, then 1 `segment_explicit_3d` image with `keep_geometry=0` | Stored original, scanner MIP side products, and `tof_i2i_74_explicit-segment_openrecon` | OK. Scanner postprocessing ran on originals only; the segment was sent as an explicit derived volume. | explicit - org, mips(org), seg, geometry mismatch between org and seg
| TOF-02 | 3d_series_segment_header | `postprocessing_target=segment_3d_source_geometry`; 40 original images with `keep_geometry=1`, then 40 `segment_3d_series` images with `keep_geometry=1` | Stored original, scanner MIP side products, and `tof_i2i_74_3D-segment_openrecon` | OK. Scanner postprocessing ran on the 3D source-geometry segment stream. | 3D - org, mips(org,seg), seg
| TOF-03 | 2d_segment_header | `postprocessing_target=segment_2d_source_geometry`; 40 original images with `keep_geometry=1`, then 40 `segment_source_geometry` images with `keep_geometry=1` | Stored original, scanner MIP side products, and `tof_i2i_74_2D-segment_openrecon` | OK. Scanner postprocessing ran on the 2D source-geometry segmentation-header stream. | 2D - org, mips(org,seg), seg
| TOF-03b | 2d_segment_header_originals | Not present in the 1.0.78 log; intended target is `postprocessing_target=originals`, then a 2D source-geometry segmentation-header stream without scanner postprocessing child-role metadata. | Retest needed. | New Dixon-oriented mode. | 2D - org, mips(org), seg
| TOF-04 | 2d_derived_image_header | `postprocessing_target=originals+segment_2d_derived_image_header`; 40 original images with `keep_geometry=1`, then 40 `segment_derived_image_header` images with `keep_geometry=1` | Stored original, scanner MIP side products, and `tof_i2i_74_2D_derived-segment_openrecon` | OK. Scanner postprocessing ran on originals plus a 2D source-geometry segment stream stamped as derived image data. | 2D derived - org, mips(org,seg), seg
| TOF-05 | 2d_source_image_header | `postprocessing_target=originals+segment_2d_source_image_header`; 40 original images with `keep_geometry=1`, then 40 `segment_source_image_header` images with `keep_geometry=1` | Stored original, scanner MIP side products, and `tof_i2i_74_2D_source-segment` | OK. Scanner postprocessing ran on originals plus a 2D source-geometry segment stream stamped with source image identity. | 2D source - org, mips(org), seg -> should use that for vesselboost

### Wholebody Multistation Protocol Dixon Recon F/W

| ID | segmentheadergeometry | Notes |
| --- | --- | --- |
| WB-01 | explicit_volume_derived_header | explicit derived segment volume |
| WB-02 | 3d_series_segment_header | 3D source geometry, segmentation header |
| WB-03 | 2d_segment_header | 2D source geometry, segmentation header |
| WB-03b | 2d_segment_header_originals | 2D source geometry, segmentation header; scanner postprocessing child-role metadata omitted so originals remain the postprocessing target |
| WB-04 | 2d_derived_image_header | 2D source geometry, derived image header |
| WB-05 | 2d_source_image_header | 2D source geometry, source image header |

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentheadergeometry`,
  `segmentationcolormap`, `sendreformatsagittal`, `sendreformatcoronal`,
  `sendcomputedmip`, and `sendmetrics` are exposed in `OpenReconLabel.json`.
- Scanner protocols saved before these parameters were added may need the
  OpenRecon algorithm reselected once so the parameter schema refreshes.
  The TOF log shows this as `OpenRecon validation failed!` for a stale
  persisted protocol that still contained removed parameter ids such as
  `detach3ddata`, `segmentgeometry`, and `segmentsendorder`; conversion then
  succeeded and the later `tof_i2i_74_*` runs used the current schema.
- Runtime logs include an `openreconi2iexample runtime version=...` marker from
  the container environment. If this does not match the recipe version, the
  scanner is still using an older deployed image or cached protocol config.
- Runtime logs include `OPENRECONI2I_POSTPROCESSING target=...`, the configured
  output line, and one `OPENRECONI2I_BATCH` line before every MRD image send.
- `segmentheadergeometry` also selects which returned stream is stamped for
  scanner postprocessing. In the TOF log, `explicit_volume_derived_header`
  ran scanner postprocessing on originals only, `3d_series_segment_header` and
  `2d_segment_header` ran it on the segment stream, and the two 2D image-header
  modes ran it on originals plus the segment stream.
- `segmentheadergeometry = 2d_segment_header_originals` was added after the
  1.0.78 scanner logs for the Dixon case where original pass-through must stay
  enabled but the 2D source-geometry segmentation stream should not be selected
  for scanner postprocessing.
- Unsigned research images can produce `Used OpenRecon image has NO valid
  signature` and `ChangeContentQualification` warnings. In the TOF log these
  warnings did not prevent storage of the OpenRecon outputs.
- `Failed to read out configuration settings for AutomaticPhoenixZipSending`
  appeared after storage and fell back to scanner defaults; it did not indicate
  an OpenRecon output failure in the TOF runs.
- Derived output names are written to `SeriesDescription`,
  `SequenceDescription`, `ProtocolName`, `ImageComments`,
  `SeriesNumberRangeNameUID`, and `SeriesInstanceUID`.
- Original pass-through preserves the source MRD header geometry and pixel data.
  Returned originals use a one-based per-series `image_index`. Scanner storage
  counters in Meta and `IceMiniHead` are restamped consistently from the
  returned stream.
- For multi-partition source volumes such as wholebody Dixon, original
  pass-through and source-geometry segment outputs preserve valid source
  partition counters (`Actual3DImagePartNumber`, `Actual3DImaPartNumber`, and
  `AnatomicalPartitionNo`) instead of flattening every returned image to
  partition `0`.
- The scanner-label default segment mode is
  `2d_segment_header`. It returns source-geometry 2D masks with
  segmentation header identity after the original pass-through stream.
- `segmentheadergeometry = 2d_segment_header_originals` returns the same
  source-geometry 2D masks with segmentation header identity, but does not set
  `SegmentPostProcessingChildRole` or restamp `ExamDataRole` on the segment
  stream. With `sendoriginal` enabled, originals remain the scanner
  postprocessing target.
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
