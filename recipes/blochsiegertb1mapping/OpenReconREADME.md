# OpenRecon Bloch-Siegert B1 Mapping

`blochsiegertb1mapping` ports the MATLAB workflow in
`ProcessBSS7T_OpenRecon.m` to an OpenRecon image-processing module. It consumes
reconstructed MRD image messages, separates magnitude and phase frames, and sends
derived Bloch-Siegert map volumes back to the scanner.

## Inputs

- Reconstructed MRD magnitude and phase `ismrmrd.Image` messages from the same
  Bloch-Siegert acquisition.
- The workflow expects either 5 frames per anatomical slice for the 1Tx case or
  26 frames per anatomical slice for the 8Tx case. This matches the MATLAB
  `nframe` / `nTx` detection: more than 25 frames means 26 frames and 8 Tx
  channels; otherwise 5 frames and 1 Tx channel.
- Phase images are expected in the same raw range used by the MATLAB code:
  `phase_radians = raw_phase * 2*pi / 4096 - pi`. The `phasewrap` parameter can
  be changed if the incoming phase images use a different wrap value. Setting it
  to `0` treats incoming phase values as radians.

## Workflow

For each anatomical slice group, the OpenRecon path implements the MATLAB
operations:

- Replace NaNs in magnitude and phase frames with zero.
- Build a magnitude mask from the mean of frames `1:nTx+1`, thresholded as
  `mean(low_background) + 2 * std(low_background)`, then fill holes.
- Convert phase frames to complex unit phasors with `exp(1i * phase)`.
- Compute Bloch-Siegert phase:
  `BSp = angle(frame 2,4,... * conj(frame 3,5,...))`.
- Add `2*pi` where `BSp < -pi/2`, clamp negative values to zero, then compute
  `Meas_B1 = sqrt(BSp / KBS)` with `KBS = 0.044 * bspulsewidthms / 6`.
- Compute corrected phase frames from `frame (2*nTx+3):end`.
- Compute `B0 = angle(frame (2*nTx+2) * conj(frame 1)) * 1000 / (2*pi)`.

## Outputs

All derived outputs are sent as explicit-volume MRD images with fresh returned
series identities, `Keep_image_geometry = 0`, no source `IceMiniHead`, and
`SequenceDescriptionAdditional = openrecon`.

- `<source>-b1` or `<source>-b1-txNN`: measured B1 maps in uT. Preferred series
  indices start at `101`.
- `<source>-bsp` or `<source>-bsp-txNN`: Bloch-Siegert phase maps in radians.
  Preferred series indices start at `120`.
- `<source>-phsc` or `<source>-phsc-txNN`: corrected phase maps in radians.
  Preferred series indices start at `140`.
- `<source>-b0`: B0 phase-difference map in Hz on preferred series index `160`.
- `<source>-mask`: optional binary mask on preferred series index `161`.

The preferred series indices are shifted only if the incoming image stream
already uses one of them.

## Parameters

- `sendb1` default `true`: send measured B1 maps.
- `sendbsp` default `true`: send Bloch-Siegert phase maps.
- `sendphsc` default `true`: send corrected phase maps.
- `sendb0` default `true`: send the B0 map.
- `sendmask` default `false`: send the magnitude-derived QC mask.
- `bspulsewidthms` default `10.0`: pulse width used in the KBS calculation.
- `phasewrap` default `4096.0`: raw phase wrap denominator.

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

The mask is computed because it is part of the MATLAB workflow, but it is not
applied to the B1, BSp, PHSc, or B0 maps because the MATLAB code does not apply
it before returning those arrays.
