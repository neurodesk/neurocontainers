# Cartesian 3D EPI OpenRecon

`epirecon` reconstructs a fully-sampled 3D EPI volume from ISMRMRD raw data.
Cartesian k-space needs neither a gridding kernel nor a density compensation
function, since every readout already lands on a regular grid: reconstruction
is a per-coil 3D FFT followed by root-sum-of-squares coil combination. See
`reconstruct_epi_data.py` for the offline prototype this module streams the
same algorithm from.

The reconstruction performs these steps:

1. Assemble a `(coils, partitions, lines, samples)` k-space array from the
   acquired readouts, honouring `ACQ_IS_REVERSE` for EPI's alternating
   readout direction.
2. Inverse-FFT each coil independently across the partition, line, and sample
   axes.
3. Crop the readout axis back down to the reconstruction matrix, removing
   acquisition oversampling.
4. Combine coils by root-sum-of-squares.

The output is one explicit 3D derived magnitude image named
`<protocol>_epirecon`. Its depth is the number of acquired partitions, and the
whole volume is scaled once into the scanner display range `0..4096`.

## Input requirements

The app expects ISMRMRD raw acquisitions for a fully-sampled 3D Cartesian scan
(no partial Fourier or parallel-imaging undersampling): every
`(partition, line)` position defined by the encoding limits must have been
acquired, or reconstruction raises an error rather than guessing.

For Siemens Twix `.dat` input, the container includes
`/opt/code/python-ismrmrd-server/siemens_twix2mrd.py`. It runs the bundled
`siemens_to_ismrmrd` converter in `--skipSyncData` mode and materializes the
MRD message stream as an ISMRMRD HDF5 dataset.

## GUI parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `epirecon` | Select the MRD server configuration. |
| Reverse slice axis | `orientationflipslice` | boolean | `false` | Reverse the partition (slice) axis before display-frame canonicalization. |

## Runtime notes

- The derived output is magnitude-only and is emitted as one explicit 3D MRD
  image in `[partition, line, readout]` order.
- Geometry is handled in two stages. Unlike a non-Cartesian trajectory, this
  module has no ambiguity about which k-space axis is which: partitions map
  onto the acquisition's slice axis, lines onto phase, and readout samples
  onto read, directly by construction. `orientationflipslice` is the only
  manual adjustment exposed, for the case where the partition order needs
  reversing.
- Stage 1, `_canonicalize_to_display_frame`, rotates the acquisition frame
  into the standard display view: columns toward the patient's Left, rows
  toward Posterior, and slices toward the Head. Its globally matched
  permutation and signs are derived from the acquisition's own `read_dir`,
  `phase_dir` and `slice_dir`, rather than hardcoded, and the vectors are
  transformed together with the pixels. This preserves honest geometry for
  oblique acquisitions.
- Those three targets form a right-handed frame under the DICOM rule
  `columns x rows = normal`, and `_validate_display_frame_targets` refuses to
  import the module if they ever stop doing so.
- The incoming acquisition frame is DICOM-left-handed and that is expected,
  not a defect. Siemens builds its PRS frame so that `phase x read = slice`,
  the opposite cross-product order. The log checks the acquisition against
  the Siemens rule and only the emitted frame against the DICOM one.
- That rotation is required because the FIRE Configurator sets
  `DisableNormOrientation`, so nothing downstream rotates the image into the
  standard view. Without it, this series and the native ICE reconstruction
  would appear mirrored relative to each other on screen even though both are
  internally consistent.
- Stage 2, `_compensate_ice_frame_stacking`, reverses the emitted frame order,
  because ICE stacks the frames of an emitted 3D volume against `slice_dir`.
  **This behaviour was measured against a different (non-Cartesian)
  reconstruction on this scanner/FIRE version** (see git history for this
  file) and carried over here on the assumption that it is a property of how
  ICE handles any emitted 3D volume, not of that specific sequence. It has
  not yet been re-verified against a native 3D EPI reconstruction. Confirm it
  with a test scan before relying on this module's geometry: compare the
  `Predicted frame N/M` positions the log reports against the scanner's own
  slice positions for the same frame numbers. Set
  `ICE_STACKS_FRAMES_AGAINST_SLICE_DIR` to `False` if the comparison shows the
  compensation should not be applied.
- Stage 2 is a scanner-only workaround and it has a real cost: the emitted MRD
  image is not self-consistent. Its pixels are reversed relative to the
  `slice_dir` in its own header, so anything that builds geometry from that
  header and ignores the declaration below places the volume mirrored
  through-plane.
- Every emitted image therefore carries `epireconIceFrameOrderReversed`, `1`
  when stage 2 reversed its frames. The bundled `mrd2nifti.py` honours it and
  reverses the slice axis back before building the affine, so NIfTI export is
  correct. Any other consumer of the raw MRD must do the same.
- `Keep_image_geometry` is `1` so ICE keeps the emitted description instead of
  rebuilding the geometry and applying its own flip.
- The scanner reads its parameter values from a deployed FIRE JSON config
  file separate from the container image (see the scanner's Marshal
  configuration). After changing `OpenReconLabel.json`, that file must be
  redeployed too; otherwise the app falls back to the defaults in
  `OPENRECON_DEFAULTS` and new parameters silently never arrive. The
  `Marshal - JsonConfigText` line in the scanner logfile shows exactly which
  parameters the app received.
- Each run logs the resolved configuration, the acquisition geometry with
  anatomical direction labels such as `R->L`, the intensity centroid of the
  reconstructed and packed volumes, and the display-frame permutation, signs
  and series identity of the emitted image. The container writes this to
  `/tmp/share/log/python_ismrmrd_server_<timestamp>.log` inside the chroot;
  it is **not** part of the scanner's own logfile, and it is the only place
  the geometry decisions are recorded. Retrieve it alongside any screenshot.
- To settle a geometry question, that log carries a prediction of what the
  scanner should display, so one screenshot either confirms it or refutes it:

  - `Predicted scanner display` gives the anatomical letter for each image
    edge and the boxed view-from marker. A native transversal reconstruction
    shows `R` left, `L` right, `A` top, `P` bottom, viewed from `F`.
  - `Predicted frame N/M` gives the patient-space position of five sample
    frames in the scanner's own numbering and `SP` notation. Read the `SP`
    field for the same frame number off the scanner and compare. A match
    means the emitted header was honoured, so any remaining error is this
    app's; a sign difference means the slice axis is reversed relative to the
    native series; a different magnitude means the volume centre or the
    field of view disagrees.
  - `Patient-space localisation` reports, per emitted axis, the intensity
    centroid and the extent of the signal in millimetres, plus the centroid
    as a patient-space position such as `R1.7 P1.7 F28.4`. This is what
    tells a volume that is merely stored back to front from one whose
    content is genuinely in the wrong place, without needing an image at all.
  - `Display frame handedness` must be right-handed under the DICOM rule; a
    left-handed emitted frame is logged at error level.
  - `ICE frame-stacking compensation applied` is logged as a warning whenever
    stage 2 reverses the frames, because that is the point where the emitted
    pixels stop agreeing with the emitted `slice_dir`.
- Debug arrays (k-space, per-coil images, and the combined output volume) are
  written below `/tmp/share/debug` with the `epirecon_` prefix. Runtime data
  is never stored under `/home`.

## Open source development

Source:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/epirecon

Issues: https://github.com/NeuroDesk/neurocontainers/issues
