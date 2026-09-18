# OpenRecon ARFI Magnitude and Phase Reconstruction

`arfirecon` returns every incoming ARFI magnitude frame and converts every
matching phase frame to wrapped phase. Magnitude and phase are always returned
as separate derived series. Both schemes additionally calculate a complex
noFUS/withFUS phase difference in a third series. The workflow does not
calculate B1, Bloch-Siegert phase, B0, or a magnitude mask.

## Input Preparation

- Classify magnitude and phase images using the MRD image type and image
  metadata.
- If a scanner labels both equal-length source series as magnitude, treat the
  second series as phase for compatibility with local DICOM replay data.
- Group 2D frames by physical slice position (falling back to the MRD slice
  counter), or treat each incoming 3D image as one volume frame.
- Require the same number, dimensions, and spatial coverage of magnitude and
  phase frames for every slice.
- Replace non-finite values with zero and combine matching 2D slices into one
  volume per frame. The former fixed 11-frame and 46-frame layouts are not
  required.
- Convert scanner phase counts to radians with
  `phase = raw * 2*pi/4096 - pi`, then wrap phase to `[-pi, pi]`.

## Single Freq Processing

For `ARFI Schemes = Single Freq`:

1. Number frames from one in acquisition order.
2. Divide them into consecutive groups of `ARFI Block Length`. A final partial
   group is retained.
3. Assign every frame from one-based odd groups to noFUS and every frame from
   one-based even groups to withFUS.
4. Form each complex frame as `magnitude * exp(i * phase)`.
5. Complex-average all noFUS frames together to create the noFUS average frame.
6. Complex-average all withFUS frames together to create the withFUS average
   frame.
7. Calculate the phase difference, in radians, as
   `angle(withFUS_average * conj(noFUS_average))`.

For example, with a block length of 25, frames 1–25 are noFUS, frames 26–50 are
withFUS, frames 51–75 are noFUS, and so on. At least one odd and one even block
are required. A block length of zero is accepted by the UI but cannot be used
for Single Freq processing.

## Multiple Freq Processing

For `ARFI Schemes = Multiple Freq`, classify every one-based `frame_number`
using:

```text
myTemp = floor((sqrt(floor((frame_number - 1) / 2) * 8 + 1) + 1) / 2)
```

- If `frame_number > myTemp * myTemp`, assign the frame to withFUS.
- Otherwise, assign the frame to noFUS.

This produces noFUS frame numbers `1, 3, 4, 7, 8, 9, ...` and withFUS frame
numbers `2, 5, 6, 10, ...`. As in Single Freq, form magnitude-weighted complex
frames, average all noFUS frames together, average all withFUS frames together,
and calculate `angle(withFUS_average * conj(noFUS_average))`.

## Optional Phase Standard Deviation

When `Send Phase Standard Deviation` is enabled, the runtime also measures the
phase variation within the withFUS and noFUS sets independently. For each set:

1. Form every complex frame as `magnitude * exp(i * phase)`.
2. Calculate the complex mean of all frames in that set.
3. Calculate each frame's residual phase as
   `angle(complex_frame * conj(complex_mean))`.
4. Calculate the sample standard deviation (`N-1`) of those residual phases at
   each voxel. A set containing one frame has a standard deviation of zero.

The two standard-deviation maps are converted from radians to degrees and use
the same full-range DICOM scaling as the other phase outputs.

## Outputs

| Series description | Preferred index | Contents |
| --- | ---: | --- |
| `<source> Magnitude` | 101 | Every reconstructed magnitude frame, emitted as one 2D image per slice |
| `<source> Phase` | 102 | Every wrapped phase frame in degrees, emitted as one 2D image per slice |
| `<source> ARFI phase` | 103 | withFUS-minus-noFUS phase difference in degrees, emitted as one 2D image per slice |
| `<source> withFUS phase standard deviation` | 104 | Optional withFUS residual-phase standard deviation in degrees |
| `<source> noFUS phase standard deviation` | 105 | Optional noFUS residual-phase standard deviation in degrees |

Preferred indices move upward when an incoming series already uses them. Each
source frame keeps its order through `image_index` and `ARFIFrameIndex`.
`ARFIScheme` and `ARFIBlockLength` record the selected processing parameters.
The outputs retain the source pixel ordering and set `Keep_image_geometry = 1`
so ICE preserves the source orientation, including the Head-Feet direction in
coronal acquisitions. Each output is a single-slice 2D MRD image with its own
source slice position, slice index, and instance number. A 3D input frame is
split into 2D slices; positions are derived along the source slice direction
using DICOM spacing metadata or the source volume FOV.

## DICOM Scaling

Every nonconstant output volume is encoded across the complete scanner display
range `0..4095`. For physical bounds `minimum` and `maximum`, the output stores:

```text
display = round((physical - minimum) * 4095 / (maximum - minimum))
RescaleSlope = (maximum - minimum) / 4095
RescaleIntercept = minimum
physical = display * RescaleSlope + RescaleIntercept
```

Phase is converted from radians to degrees before this encoding. Consequently,
DICOM viewers display the correct physical phase after applying rescale slope
and intercept, while the stored pixels use the available 12-bit range. The
inverse formula, physical bounds, display bounds, and conversion are also saved
in `ImageComments` and `ARFIDisplay*` metadata. A constant volume uses zero
pixels and stores its physical value in `RescaleIntercept` because no finite
range exists to expand.

## Parameters

- `arfischemes` default `singlefreq`: select Single Freq or Multiple Freq.
- `arfiblocklength` default `25`: number of sequential frames in each ARFI
  block, from 0 through 100 frames.
- `sendphasestandarddeviation` default `false`: output separate withFUS and
  noFUS phase-standard-deviation series.

ARFI Block Length controls only Single Freq complex averaging. It must be
greater than zero and the acquisition must contain enough frames to produce at
least two blocks. Multiple Freq derives its groups from the frame-number formula
and does not use ARFI Block Length.

## Input Shapes

The runtime accepts single-channel 2D frames and single-channel 3D volume
frames. Every slice must contain the same number of matching magnitude and
phase frames so that each output volume has complete spatial coverage.
