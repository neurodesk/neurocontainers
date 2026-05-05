# OpenRecon Image-to-Image Example

`openreconi2iexample` is a lightweight OpenRecon image-in/image-out example.
It expects already reconstructed MRD image messages and returns simple derived
label and downsampled image series. It is intended as a readable reference for
scanner-safe output series handling, not as a clinical segmentation algorithm.

## What It Demonstrates

1. Drain the full OpenRecon input connection before processing.
2. Group processable magnitude images by `image_series_index`.
3. Sort slices by physical position using each image position and slice
   direction.
4. Compute image matrix, field of view, voxel size, and measured slice spacing.
5. Allocate all derived output series from one connection-level allocator after
   input drain.
6. Create simple label outputs by thresholding the input volume.
7. Optionally create an interpolated downsampled image output with lower in-plane matrix
   size and fewer slices than the source series.
8. Stamp each derived output with coherent MRD header, Meta, and IceMiniHead
   identity.
9. Validate the output series contract before any image is sent back.

The hard pre-send validation is deliberate: if derived series identity is
ambiguous or collides with the input, the app fails loudly instead of returning
partial output that a scanner might hide or merge.

## Inputs

The app is designed for image input, not raw k-space reconstruction. Magnitude
images are processed. Unsupported or non-magnitude images are buffered and
returned unchanged in `multi_series` mode after validation of the derived
outputs. In the default `single_series` diagnostic mode they are suppressed so
the scanner run does not add passthrough output series.

Each processable image series should contain one 2D slice per MRD image. Slices
must have matching in-plane dimensions. The wrapper checks that the drained
slice count is compatible with the MRD header and logs the measured geometry.

## Outputs

The default `single_series` mode returns only `THRESH_MID`, with one output
image per source slice and one output series per processable input series. For
a single-series input, this is the scanner diagnostic mode for validating
whether the host DICOM pipeline accepts one input-sized derived series without
closing the concat parent early.

The `multi_series` mode returns these derived series for each processable input
series:

| Output role | Rule | Label value |
| --- | --- | --- |
| `THRESH_LOW` | voxel intensity greater than the series mean | `1` |
| `THRESH_MID` | voxel intensity greater than mean plus half a standard deviation | `2` |
| `THRESH_HIGH` | voxel intensity greater than mean plus one standard deviation | `3` |
| `DOWNSAMPLED` | linear interpolation to half the in-plane matrix and half the slice count where possible | source intensity |

The threshold outputs are sent as one MRD image per source slice. The
`DOWNSAMPLED` output is sent as its own derived series with its own smaller
matrix, thicker slice spacing, and fewer MRD image messages where the source
series has more than one slice. Source orientation and physical field of view
are preserved. Threshold output pixels are stored as unsigned integer labels;
the downsampled output keeps source image intensities.

## Parameters

| Parameter id | Type | Default | Description |
| --- | --- | --- | --- |
| `config` | choice | `openreconi2iexample` | Selects this OpenRecon app. |
| `outputmode` | choice | `single_series` | `single_series` emits only `THRESH_MID` for the scanner diagnostic build. `multi_series` emits the three threshold series and `DOWNSAMPLED`. Can be overridden with `OPENRECONI2I_OUTPUT_MODE`. |
| `sendoriginal` | boolean | `false` | Requests scanner-safe restamped copies of original magnitude images as an additional output series. Ignored unless `OPENRECONI2I_ALLOW_SENDORIGINAL=1` is set, so stale scanner protocols cannot accidentally expand the diagnostic output volume. |

## Scanner-Safe Series Identity

Every derived output role receives a fresh `image_series_index` that is distinct
from observed input series indices and reserved scanner indices. The wrapper
sets a unique `SeriesInstanceUID`, a stable `SeriesNumberRangeNameUID`,
`ProtocolName`, `SequenceDescription`, `SeriesDescription`, `ImageType`,
`DicomImageType`, and `DataRole` for each derived role. When an `IceMiniHead`
`ImageTypeValue4` array is present, the derived role is carried there and the
top-level MRD Meta `ImageTypeValue4` field is omitted to avoid scanner-side
duplicate tokens.

If source images include an `IceMiniHead`, the same identity fields and slice
numbering fields are patched inside the owning `ParamMap` blocks rather than
appended at the minihead root. Before sending, the wrapper logs an
`OPENRECONI2I_OUTPUT_SERIES_CONTRACT` summary and raises an error if derived
roles collide, reuse input identity, or disagree between Meta and IceMiniHead.

## Runtime Notes

- Runtime debug paths use `/tmp/share/debug` or `/tmp`; no runtime files are
  expected under `/home`.
- This example does not download models or external tools.
- The threshold and downsampled outputs are intentionally simple so the recipe
  stays useful as an OpenRecon integration template. Keep `outputmode` at
  `single_series` while validating scanner behavior; switch to `multi_series`
  only for integration debugging.
- The wrapper waits briefly between output series and before `MRD_MESSAGE_CLOSE`
  so the scanner-side DICOM pipeline can drain multi-series output. Set
  `OPENRECONI2I_SEND_SERIES_DRAIN_SECONDS` or
  `OPENRECONI2I_CLOSE_DRAIN_SECONDS` to tune or disable those waits. Set
  `OPENRECONI2I_CLOSE_DRAIN_SECONDS_MAX` to raise or lower the automatic
  close-drain cap.
