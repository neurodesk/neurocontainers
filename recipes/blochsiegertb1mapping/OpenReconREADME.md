# OpenRecon Bloch-Siegert B1 Mapping

`blochsiegertb1mapping` ports the MATLAB workflow in
`Read_7T_all_rev_openRecon.m` to an OpenRecon image-processing module. It consumes
reconstructed MRD image messages, separates magnitude and phase frames, and sends
derived Bloch-Siegert map volumes back to the scanner.

## Inputs

- Reconstructed MRD magnitude and phase `ismrmrd.Image` messages from the same
  Bloch-Siegert acquisition.
- The workflow requires 10 frames per anatomical slice for the 1Tx case or 38
  frames per anatomical slice for the 8Tx case. As in the MATLAB code, more
  than 25 available frames selects 8 Tx channels; otherwise it selects 1 Tx.
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
  Bloch-Siegert phase from its four-echo block as
  `angle(A * conj(B) * pre * conj(C) * pre * conj(D))`.
- Add `2*pi` where `BSp < -25 degrees`, clamp remaining negative values to
  zero, and compute `Meas_B1 = sqrt(BSp / KBS)` with
  `KBS = 0.044 * bspulsewidthms / 6`.
- Use echo `A` from each Tx block for the phase output.
- Average the final three frames into the post-reference phase and compute
  `B0 = angle(post * conj(pre)) * 1000 / (2*pi)`.

## Outputs

All derived outputs are sent as explicit-volume MRD images with fresh returned
series identities, `Keep_image_geometry = 0`, no source `IceMiniHead`, and
`SequenceDescriptionAdditional = openrecon`.

- `<source>-b1` or `<source>-b1-txNN`: measured B1 maps encoded as scanner
  display `uint16` pixels in the `0..4095` range. Values are multiplied by
  `100`; divide stored pixels by `100` to recover uT. The scaling formula is
  recorded in `ImageComments` and `ImageComment`. Preferred series indices
  start at `101`.
- `<source>-bsp` or `<source>-bsp-txNN`: Bloch-Siegert phase maps converted
  from radians to degrees and multiplied by `10`. Preferred series indices
  start at `120`.
- `<source>-phsc` or `<source>-phsc-txNN`: echo-A transmit phase maps converted
  from radians to degrees and multiplied by `10`.
  Preferred series indices start at `140`.
- `<source>-b0`: B0 phase-difference map in Hz on preferred series index `160`.
- `<source>-mask`: optional binary mask on preferred series index `161`.

The preferred series indices are shifted only if the incoming image stream
already uses one of them.

Both phase outputs record `degrees = display / 10` and the radians-to-degrees
conversion in `ImageComments` and `ImageComment`.

## Parameters

- `sendb1` default `true`: send measured B1 maps.
- `sendbsp` default `true`: send Bloch-Siegert phase maps.
- `sendphsc` default `true`: send echo-A transmit phase maps.
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
