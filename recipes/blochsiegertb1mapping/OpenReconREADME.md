# OpenRecon Bloch-Siegert B1 Mapping

`blochsiegertb1mapping` ports the MATLAB workflow in
`Read_7T_all_rev_openRecon.m` to an OpenRecon image-processing module. It consumes
reconstructed MRD image messages, separates magnitude and phase frames, and sends
derived Bloch-Siegert map volumes back to the scanner.

## Inputs

- Reconstructed MRD magnitude and phase `ismrmrd.Image` messages from the same
  Bloch-Siegert acquisition.
- The workflow requires 11 frames per anatomical slice for the 1Tx case or 46
  frames per anatomical slice for the 8Tx case. As in the MATLAB code, more
  than 25 available frames selects 8 Tx channels; otherwise it selects 1 Tx.
  The post-reference frames follow the four B/C/A/D echoes per channel, and
  the dedicated Tx-phase block occupies the final `nTx` frames.
- Phase images are expected in the same raw range used by the MATLAB code:
  `phase_radians = raw_phase * 2*pi / 4096 - pi`.

## Workflow

For each anatomical slice group, the OpenRecon path implements the MATLAB
operations:

- Replace NaNs in magnitude and phase frames with zero.
- Convert phase frames to complex unit phasors with `exp(1i * phase)`.
- Build `phaseMask` from the first `PreDummy+1` frames and every C/D
  frame. Circularly average those frames, subtract the average phase from each
  selected frame, and calculate the phase-difference standard deviation in
  degrees. `phaseMask` uses a `<30 degree` threshold.
- Build `MaskForMagnitude` from `MBSS1`, the mean of magnitude frames
  `1:(2*nTx+2)`. For values below the global `MBSS1` mean, set
  `thr = mean(low_values) + 2 * std(low_values)`, then use `MBSS1 > thr`.
  Holes are first filled independently in every 2D slice, followed by a 3D
  hole fill for multi-slice volumes.
- Compute each Tx Bloch-Siegert phase as `-angle(A * conj(B))`. The BSp and B1
  calculations do not use the pre-reference frames.
- Add `2*pi` where `BSp < -90 degrees` and retain the remaining small negative
  values in the BSp output. Clamp only a separate B1-calculation copy to zero,
  then compute `Meas_B1 = sqrt(BSp_for_B1 / KBS)` with
  `KBS = 0.044 * bspulsewidthms / 6`.
- For B0 only, exclude up to the first two pre-reference frames and circularly
  average all remaining pre-reference frames through the last one. Average the
  post-reference frames immediately after the B/C/A/D echoes and compute
  `B0 = angle(post * conj(pre)) * 1000 / (2*pi*deltatems)`.
- Use the final `nTx` frames as the dedicated transmit-phase block.
- Filter every BS phase volume and the B0 map with a 3D polynomial fit (order
  20 for 1Tx, order 10 for 8Tx)
  over voxels trusted by `phaseMask`, followed by a 3D Gaussian filter with
  `sigma=0.5` and multiplication by `MaskForMagnitude`. By default, only
  untrusted BS phase voxels are replaced by the fit. `Apply Filter` replaces
  the entire BS phase volume instead. B0 always replaces only untrusted voxels.
  B1 is calculated from the resulting filtered BS phase.

## Outputs

All derived outputs are sent as explicit-volume MRD images with fresh returned
series identities, `Keep_image_geometry = 0`, no source `IceMiniHead`, and
`SequenceDescriptionAdditional = openrecon`. Every complete, nonconstant 3D
output volume is linearly encoded into the full scanner display range
`0..4095`. DICOM `RescaleSlope` and `RescaleIntercept` restore its physical
units, and window center/width are recorded in those physical units. A constant
volume uses zero-valued pixels and stores its physical value in the intercept.

- `<source>-b1` or `<source>-b1-txNN`: measured B1 maps in uT. Preferred series
  indices start at `101`.
- `<source>-bsp` or `<source>-bsp-txNN`: Bloch-Siegert phase maps converted
  from radians to degrees. Preferred series indices start at `120`.
- `<source>-phsc` or `<source>-phsc-txNN`: dedicated transmit phase maps
  converted from radians to degrees. Preferred series indices start at `140`.
- `<source>-b0`: B0 phase-difference map in Hz on preferred series index `160`.
- `<source>-ref-amplitude`: 1Tx reference-amplitude map in V on preferred
  series index `170`, computed as `11.74 * Ref Amplitude / B1`.
- `<source>-b1-processing`: masked B1 histogram/cumulative-sum information
  images in a single derived series (`1` image for 1Tx, `8` images for 8Tx).
- `<source>-b0-processing`: masked B0 histogram/cumulative-sum information
  image in a separate derived series.

The preferred series indices are shifted only if the incoming image stream
already uses one of them.

The finite physical bounds, inverse scaling formula, and radians-to-degrees
conversion where applicable are recorded in `ImageComments`, `ImageComment`,
and `BlochSiegertDisplay*` metadata for every output.

Runtime logs report the source phase range, source rescale metadata, inferred
input domain, and the one-based frame layout. Mask-restricted quantiles cover
the pre-reference phase/coherence, every B/C/A/D phase term, and the raw and
corrected BSp phase. Per-Tx wrap counts, retained-negative counts inside and
outside the QC mask, true-zero counts, B1 clamp counts and ranges are also
reported, together with the physical bounds, rescale parameters, and window
for every output series.

## Parameters

- `sendbsp` default `true`: send Bloch-Siegert phase maps.
- `sendphsc` default `true`: send dedicated transmit phase maps.
- B1 and B0 maps are always sent. The masks are never sent as output series.
- `applymask` default `false`: apply `MaskForMagnitude` to every returned B1,
  BSp, transmit-phase, and B0 map.
- `applyfilter` default `false`: change BS phase filtering from replacing only
  untrusted voxels to replacing the entire volume with the polynomial fit.
  B0 filtering always replaces only untrusted voxels.
- Polynomial fitting uses order `20` for 1Tx and order `10` for 8Tx.
- `bspulsewidthms` default `12.0`: BS pulse width in milliseconds used in the
  KBS calculation.
- `deltatems` default `1.0`: echo-time difference in milliseconds used to
  convert the pre/post reference phase evolution to a B0 map in Hz.
- `predummy` default `2`: number of additional pre-reference dummy TRs.
- `postdummy` default `2`: number of additional post-reference dummy TRs.

The scanner UI exposes the dummy counts as integer controls from 0 through 10
with unit `TRs`.

## Scanner Notes

The OpenRecon implementation does not read DICOM directories. The DICOM file
loading in the MATLAB script is replaced by buffering the MRD image stream. The
recipe includes `dicom2mrd.py` only for local replay tests such as
`testData.tgz`.

The runtime accepts either single-slice 2D image frames or single-channel 3D
volume-frame images. This matters for the bundled replay data because the DICOM
converter writes each Bloch-Siegert acquisition frame as one 32-slice MRD
volume. If a converter marks both source series as magnitude images, the runtime
can split two equal-length source series into magnitude then phase using their
source-series ordering.

Slice groups are detected from physical position when available, falling back to
MRD slice counters or frame-count chunking. Each output volume is derived from
sorted frame order within its slice group.

Filtering uses `phaseMask` to choose trusted polynomial-fit voxels and always
multiplies the filtered BSp/B1 and B0 results by `MaskForMagnitude`, following
the MATLAB workflow. When `Apply Mask` is also enabled, `MaskForMagnitude` is
applied to every completed output map—including transmit phase—before scanner
display encoding.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/blochsiegertb1mapping

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
