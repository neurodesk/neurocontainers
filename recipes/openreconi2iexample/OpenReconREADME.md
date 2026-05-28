# OpenRecon Image-to-Image Example

`openreconi2iexample` is a minimal OpenRecon image-in/image-out reference. It
receives reconstructed MRD image messages and sends no outputs unless one or
more output options are enabled. It can re-emit the original scan, invert
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

- No output is sent by default.
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
  `segmentdeliverymode = exclude` sends one explicit derived segment volume per
  source group with `DataRole = Image`, `SegmentExplicitVolume = 1`, and
  `Keep_image_geometry = 0`. This is the default mode for keeping scanner
  MIP/MPR postprocessing attached to originals without letting the segment
  become a postprocessing input. `segmentdeliverymode = source_geometry_3d`
  sends one source-like 3D segment series per source volume group with one MRD
  image per source slice. `segmentdeliverymode = source_geometry_2d` sends one
  source-geometry 2D mask image per source image.
  `segmentdeliverymode = postprocess_segment` forces source-geometry 2D masks,
  disables `sendoriginal`, stamps `SegmentPostProcessing = 1`, preserves the
  source image-type identity used by scanner compose filters, and aligns
  geometry Meta such as `SlicePosLightMarker` with the returned MRD header.
  `segmentsendorder` controls whether segment outputs are batched before or
  after original pass-through outputs when both are enabled.
- `<source>-upsampled`: one volume image with twice as many slices on
  `image_series_index = 102` when `upsampled` is true.
- `<source>-mip`: one computed maximum intensity projection image on
  `image_series_index = 103` when `sendcomputedmip` is true.
- `<source>-segment-sagittal` and `<source>-segment-coronal`: one explicit 3D
  segment reformat volume per requested orientation starting on
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

Use these rows for scanner testing with `segment = true` and
`sendoriginal = true`. Keep `invert`, `upsampled`, `segmentationcolormap`,
`sendcomputedmip`, `sendmetrics`, `sendreformatsagittal`, and
`sendreformatcoronal` disabled unless a row is deliberately extended for
another output test.

### TOF

| ID | segmentdeliverymode | segmentsendorder | Notes |
| --- | --- | --- | --- |
| TOF-01 | exclude | after_originals | Expected: originals drive scanner MIPs, explicit segment is not a MIP input |
| TOF-02 | exclude | before_originals | Expected: explicit segment is sent first, originals remain scanner-postprocessing target |
| TOF-03 | source_geometry_3d | after_originals | Historical result: scanner MIPs can include org+seg |
| TOF-04 | source_geometry_3d | before_originals | Historical result: scanner MIPs can include seg+org |
| TOF-05 | source_geometry_2d | after_originals | Historical result: scanner MIPs can alternate org+seg |
| TOF-06 | source_geometry_2d | before_originals | Historical result: scanner MIPs can alternate seg+org |
| TOF-07 | postprocess_segment | after_originals | Runtime disables originals; scanner MIPs are based on segment |
| TOF-08 | postprocess_segment | before_originals | Runtime disables originals; scanner MIPs are based on segment |

### Wholebody Multistation Protocol Dixon Recon F/W

| ID | segmentdeliverymode | segmentsendorder | Notes |
| --- | --- | --- | --- |
| WB-01 | exclude | after_originals | Expected: F/W/FF originals compose, explicit segment groups are not compose inputs |
| WB-02 | exclude | before_originals | Expected: explicit segment groups are sent first, originals remain compose target |
| WB-03 | source_geometry_3d | after_originals | Historical result: F/W/FF originals compose, segment groups are visible series |
| WB-04 | source_geometry_3d | before_originals | Historical result: segment groups before originals, originals compose |
| WB-05 | source_geometry_2d | after_originals | Historical best source-like run: F/W/FF originals compose |
| WB-06 | source_geometry_2d | before_originals | Historical result: segment first, then originals compose |
| WB-07 | postprocess_segment | after_originals | Runtime disables originals |
| WB-08 | postprocess_segment | before_originals | Runtime disables originals |

## Scanner Notes

- `sendoriginal`, `invert`, `upsampled`, `segment`, `segmentdeliverymode`,
  `segmentsendorder`, `segmentationcolormap`, `sendreformatsagittal`,
  `sendreformatcoronal`, `sendcomputedmip`, and `sendmetrics` are exposed in
  `OpenReconLabel.json`.
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
- `segmentdeliverymode = exclude` is the default segment mode. It returns
  segment masks as explicit derived volumes with `Keep_image_geometry = 0` so
  scanner inline postprocessing can keep using the original image stream.
- `segmentdeliverymode = postprocess_segment` is mutually exclusive with
  original pass-through at runtime. If both are enabled in the protocol, the
  recipe sends only the stamped 2D segment masks so the scanner does not receive
  duplicate source-like streams.
- `segmentsendorder = before_originals` sends the segment stream before the
  original stream when both are enabled. `segmentsendorder = after_originals`
  sends originals first and segment outputs second.
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
