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
  The dedicated Tx-phase block follows the four B/C/A/D echoes per channel and
  precedes the final three post-reference frames.
- Phase images are expected in the same raw range used by the MATLAB code:
  `phase_radians = raw_phase * 2*pi / 4096 - pi`.

## Workflow

For each anatomical slice group, the OpenRecon path implements the MATLAB
operations:

- Replace NaNs in magnitude and phase frames with zero.
- For optional visual QC output, build a magnitude mask from the mean of frames
  `1:(2*nTx+2)`, thresholded as
  `mean(low_background) + 0.5 * std(low_background)`, then fill holes. The mask
  is not applied to any calculated map.
- Convert phase frames to complex unit phasors with `exp(1i * phase)`.
- Average frames 1-3 into the pre-reference phase, then compute each Tx
  Bloch-Siegert phase from its four-echo block. Correct the scanner DICOM phase
  polarity before unwrapping as
  `-angle(A * conj(B) * pre * conj(C) * pre * conj(D))`. This BSp-only
  correction does not change the dedicated Tx phase or B0 phase difference.
- Add `2*pi` where `BSp < -25 degrees` and retain the remaining small negative
  values in the BSp output. Clamp only a separate B1-calculation copy to zero,
  then compute `Meas_B1 = sqrt(BSp_for_B1 / KBS)` with
  `KBS = 0.044 * bspulsewidthms / 6`.
- Use the dedicated contiguous transmit-phase block after the B/C/A/D echoes
  for the Tx phase output.
- Average the final three frames into the post-reference phase and compute
  `B0 = angle(post * conj(pre)) * 1000 / (2*pi)`.

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
- `<source>-mask`: optional binary mask on preferred series index `161`.

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

- `sendb1` default `true`: send measured B1 maps.
- `sendbsp` default `true`: send Bloch-Siegert phase maps.
- `sendphsc` default `true`: send dedicated transmit phase maps.
- `sendb0` default `true`: send the B0 map.
- `sendmask` default `false`: send the magnitude-derived QC mask.
- `bspulsewidthms` default `12.0`: BS pulse width in milliseconds used in the
  KBS calculation.

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

The optional magnitude mask follows the MATLAB threshold and per-slice
hole-filling steps. It is not applied during map processing and is returned
only as a separate QC series when `sendmask` is enabled.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/blochsiegertb1mapping

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
