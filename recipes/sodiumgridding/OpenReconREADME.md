# Sodium Gridding OpenRecon

`sodiumgridding` reconstructs a 3D sodium volume from ISMRMRD raw data using
Kaiser-Bessel convolutional gridding. It adapts the standalone
`TPI_gridding_N256_AC_coil_compression_kb.py` workflow to the existing
OpenRecon streaming and scanner-output contract.

The reconstruction performs these steps:

1. Read and align the raw coil data with an embedded or bundled trajectory.
2. Reject weak readouts, normalize each physical coil from the central sample
   window, and apply the optional Fermi taper.
3. Compress the physical coils into virtual coils while retaining the selected
   fraction of signal variance.
4. Normalize the physical trajectory in `1/cm` onto a two-times oversampled
   grid and iteratively estimate a Kaiser-Bessel density compensation function.
5. Grid and inverse-FFT each virtual coil, crop to the requested matrix, and
   apply Kaiser-Bessel deapodization.
6. Combine the virtual coils using adaptive sensitivity-weighted combination
   or root-sum-of-squares.
7. Optionally apply N4 bias field correction.

The output is one explicit 3D derived magnitude image named
`<protocol>_sodiumgridding`. Its depth is the reconstructed matrix size, and
the whole volume is scaled once into the scanner display range `0..4096`.

## Input requirements

The app expects ISMRMRD raw acquisitions for a 3D sodium scan. Trajectory input
is resolved in this order:

1. Embedded ISMRMRD trajectories from the incoming acquisitions.
2. The bundled HDF5 file selected by `trajectoryfile`.
3. An explicit HDF5 path supplied through runtime configuration.

The bundled trajectory choices are
`/opt/sodiumgridding/23Na_n28_trajectory.h5` and
`/opt/sodiumgridding/23Na_n50_trajectory.h5`, both using dataset `k`. Aliases
`sodiumn28`, `sodiumn50`, `23Na_n28`, and `23Na_n50` are also accepted outside
the scanner UI.

For Siemens Twix `.dat` input, the container includes
`/opt/code/python-ismrmrd-server/siemens_twix2mrd.py`. It runs the bundled
`siemens_to_ismrmrd` converter in `--skipSyncData` mode and materializes the MRD
message stream as an ISMRMRD HDF5 dataset.

## GUI parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `sodiumgridding` | Select the MRD server configuration. |
| Bundled trajectory | `trajectoryfile` | choice | `/opt/sodiumgridding/23Na_n28_trajectory.h5` | Use the bundled n28 or n50 trajectory when the MRD input has no embedded trajectory. |
| Trajectory sample offset | `trajectorysampleoffset` | integer | `0` | Skip leading trajectory samples before alignment with raw data. |
| Matrix size | `matrixsize` | integer | `128` | Final isotropic matrix size. Gridding uses a fixed oversampling factor of two. |
| FOV cm | `fovcm` | string | `22.0` | Reconstruction field of view in centimetres. |
| Apply Fermi filter | `applyfermifilter` | boolean | `true` | Apply the radial k-space Fermi taper with fixed width `0.05` and cutoff `0.98` before compression. |
| DCF iterations | `dcfiterations` | integer | `5` | Kaiser-Bessel density compensation iterations; use `0` for uniform weights. |
| Max coils | `maxcoils` | integer | `16` | Limit physical coils before compression; use `0` for all coils. |
| Max workers | `maxworkers` | integer | `8` | Parallel virtual-coil workers. At most two oversampled grids are resident concurrently. |
| Coil variance | `coilvarianceretention` | string | `0.9` | Fraction of physical-coil variance retained during compression. |
| Coil combination | `coilcombinemode` | choice | `AC` | Use adaptive combination (`AC`) or sum-of-squares (`SoS`). |
| N4 correction | `applyn4biascorrection` | boolean | `true` | Apply N4 bias field correction after coil combination. |
| Orientation | `orientation` | choice | `zyx` | Map trajectory components into the acquisition read and phase axes. Select `debug` to emit every mapping, with the slice axis both kept and reversed, as labelled series. |
| Reverse trajectory slice axis | `orientationflipslice` | boolean | `false` | Reverse trajectory component 2 before display-frame canonicalization. |

Weak-readout rejection remains enabled with the standalone defaults of three
standard deviations and a five-sample half-window. The Kaiser-Bessel kernel
width is fixed at `3.0`; Fermi width and cutoff are fixed at `0.05` and `0.98`,
matching the supplied implementation.

## Runtime notes

- The derived output is magnitude-only and is emitted as one explicit 3D MRD
  image in `[z, y, x]` order.
- Geometry is handled in three stages. First, `orientation` maps the
  trajectory components into the acquisition `(slice, phase, read)` frame and
  `orientationflipslice` optionally reverses its through-plane axis. The default
  `zyx` mapping is partly verified. The in-plane transpose is excluded by
  measurement: in `sodiumgridding_v0.1.3.PNG` the head phantom outline spans
  262 px along the rows against 187 px along the columns, a ratio of 1.40, and a
  head is elongated anterior-posterior, which is the direction the rows run for
  that transversal protocol. A transpose would have produced 0.71, so the four
  `zxy` mappings are ruled out. What remains unverified is the sign choice,
  above all left-right, which a laterally symmetric phantom cannot reveal.
  Excluding that needs one scan of an asymmetric object or a marker placed on a
  known side. Select `debug` to emit all sixteen variants as series suffixed
  `_ori_<key>_fz<0|1>`, compare them against that object, then set `orientation`
  to the matching key and `orientationflipslice` to its `fz` digit.
- The sweep covers the slice reversal as well as the eight in-plane mappings.
  The in-plane keys only transpose or reverse the two in-plane axes, so a sweep
  over them alone cannot reach a volume whose through-plane axis is wrong. That
  is exactly the error 0.1.3 shipped, and exactly the one its eight-series sweep
  could not have surfaced.
- Second, `_canonicalize_to_display_frame` rotates the chosen acquisition frame
  into the standard display view: columns toward the patient's Left, rows
  toward Posterior, and slices toward the Head. Its globally matched permutation
  and signs are derived from the acquisition's own `read_dir`, `phase_dir` and
  `slice_dir`, rather than hardcoded, and the vectors are transformed together
  with the pixels. This preserves honest geometry for oblique acquisitions.
- Those three targets form a right-handed frame under the DICOM rule
  `columns x rows = normal`, and `_validate_display_frame_targets` refuses to
  import the module if they ever stop doing so. A left-handed target set is a
  sign mistake: it reverses one axis too many, and the boxed view-from marker
  then points at the opposite side from the native series.
- The incoming acquisition frame is DICOM-left-handed and that is expected, not
  a defect. Siemens builds its PRS frame so that `phase x read = slice`, the
  opposite cross-product order, which the measured
  `read_dir=(-1,0,0)`, `phase_dir=(0,1,0)`, `slice_dir=(0,0,1)` satisfies
  exactly. The log therefore checks the acquisition against the Siemens rule and
  only the emitted frame against the DICOM one.
- The boxed view-from marker follows `columns x rows`, not `slice_dir`. That is
  why it read `H` in 0.1.3 and `F` in 0.1.4 even though the emitted `slice_dir`
  was `F->H` in both. Only the frame positions depend on `slice_dir`.
- That rotation is required because the FIRE Configurator sets
  `DisableNormOrientation`, so nothing downstream rotates the image into the
  standard view. The native ICE reconstruction is emitted in that view, so
  without it the two series appear mirrored relative to each other on screen
  even though both are internally consistent.
- The slice target is measured from the native reference series in
  `sodiumgridding_v0.1.3.PNG`. That volume is centred at isocenter, which the
  scanner logfile confirms with `dSag, dCor, Tra = 0; 0; 0` for the native
  reconstruction, and holds 128 slices over 220 mm, so frame 48 of 128 lies at
  `(47 - 63.5) * 220/128 = -28.36 mm` along the slice axis. The reference
  displays that frame at `SP F28.4`, so its slice axis points toward the Head.
- Third, `_compensate_ice_frame_stacking` reverses the emitted frame order,
  because ICE stacks the frames of a 3D volume against `slice_dir`. This is the
  only stage that moves pixels without moving the matching direction vector, and
  deliberately so: transforming both is a change of storage convention and
  cancels, since ICE derives the frame positions from that same vector. Stage 2
  therefore cannot correct a through-plane error, only stage 3 can. Set
  `ICE_STACKS_FRAMES_AGAINST_SLICE_DIR` to `False` if a future FIRE release
  stops inverting the stacking.
- The inversion is measured, twice, with the same emitted `slice_dir` of `F->H`:
  in 0.1.3 frame 48 of 128 over 220 mm had a header position of `F28.4` and the
  scanner showed `H29`; in 0.1.4 frame 41 of 128 had a header position of
  `F40.4` and the scanner showed `H41`. Same magnitude, opposite sign.
- The reconstructed pixels themselves are in the right place. The 0.1.4 log puts
  the emitted volume's intensity centroid at `F37.0`, and the native 64-slice
  reference shows a full-width cross-section at `F43.0`. Were the content
  reversed, its bulk would sit at `H37` and that reference slice would be
  nearly empty rather than the largest one in the series.
- Stage 3 is a scanner-only workaround and it has a real cost: the emitted MRD
  image is not self-consistent. Its pixels are reversed relative to the
  `slice_dir` in its own header, so anything that builds geometry from that
  header and ignores the declaration below places the volume mirrored
  through-plane. There is no way to avoid this while ICE inverts the stacking,
  because reversing the vector along with the pixels cancels the correction. The
  choice is which consumer to satisfy, and stage 3 chooses the scanner.
- Every emitted image therefore carries
  `SodiumGriddingIceFrameOrderReversed`, `1` when stage 3 reversed its frames.
  The bundled `mrd2nifti.py` honours it and reverses the slice axis back before
  building the affine, so NIfTI export is correct. Any other consumer of the raw
  MRD must do the same. `test_emitted_mrd_is_mirrored_for_a_consumer_that_
  ignores_the_flag` pins what happens when it does not.
- A second consequence is that the emitted frame order runs `H->F` while the
  native series runs `F->H`, so the two series do not scroll together by frame
  number. Making them agree would require emitting `slice_dir` negated instead
  of reversing the pixels, which only works if ICE derives the stacking from the
  emitted vector rather than inverting it unconditionally. That has not been
  measured.
- **Not yet verified on the scanner.** The inversion is measured from 0.1.3 and
  0.1.4, but the correction shipped in 0.1.5 has only been checked against
  synthetic volumes and the logged prediction. Confirming it needs a 0.1.5 run
  plus an exported DICOM whose per-frame `ImagePositionPatient` and
  `ImageOrientationPatient` are dumped and compared against the native series.
  The 0.1.4 screenshot shows `SP 0.0`, so the `TP` field alone does not
  establish that the exported geometry is valid.
- The `SP` field of the app series reads `0.0`, which is the volume centre this
  app emits, while the native series shows a per-frame slice position. The
  frame-dependent value appears in the `TP` field instead. Whether ICE writes
  per-frame `ImagePositionPatient` values for the emitted multi-frame volume at
  all is still unconfirmed; dumping that tag from an exported DICOM would settle
  it, and is worth doing before trusting the series for anything quantitative.
- Version 0.1.3 emitted the acquisition frame unrotated, which put `L` on the
  left edge and the view-from marker on `H`. Stage 2 fixed both in 0.1.4, and
  that figure confirms it: the app series now shows `R`, `A` and `F` exactly
  like the native one. Only the through-plane position remained wrong, which
  stage 3 addresses.
- Do not read the through-plane direction off the displayed area of an object.
  The 0.1.3 note in this file argued from the app image spanning 110 x 156 mm
  against the reference's 45 x 87 mm at the mirrored position, and concluded the
  phantom's widest part sat toward the head. That inference was unsound: those
  two panels were windowed at `W 3172` and `W 2187`, so the comparison measured
  the display window as much as the object, and the two figures are from
  different sessions in which the phantom need not have been placed the same
  way. Argue from the logged signal extent and centroid instead, which are
  measured on the volume rather than on the screen.
- `Keep_image_geometry` is `1` so ICE keeps the emitted description instead of
  rebuilding the geometry and applying its own flip. ICE does read `slice_dir`,
  because negating it moves every frame position, which is exactly why stage 3
  reverses the pixels alone. The in-plane vectors behave differently: with
  `UseIceFillingMiniHeader` and `IsFlipAndShiftImages` enabled, negating
  `read_dir` or `phase_dir` on its own relabels an edge without moving a pixel,
  so an in-plane correction expressed that way is silently lost.
- The scanner reads its parameter values from
  `%CustomerIceProgs%\fire\config\wip_070_fire_sodiumgridding.json`. That file
  is deployed separately from the container image, so after changing
  `OpenReconLabel.json` it must be redeployed as well; otherwise the app falls
  back to the defaults in `OPENRECON_DEFAULTS` and new parameters silently never
  arrive. The `Marshal - JsonConfigText` line in the scanner logfile shows
  exactly which parameters the app received.
- Each run logs the FIRE-visible CPU count, affinity, cgroup limits, configured
  worker cap, effective virtual-coil workers, and whether pyFFTW is available.
- Each run also logs the resolved configuration, the per-component trajectory
  k-space extent, the acquisition geometry with anatomical direction labels such
  as `R->L`, the intensity centroid of the reconstructed and packed volumes, and
  the display-frame permutation, signs and series identity of every emitted
  image. The container writes this to
  `/tmp/share/log/python_ismrmrd_server_<timestamp>.log` inside the chroot; it
  is **not** part of the scanner's own logfile, and it is the only place the
  geometry decisions are recorded. Retrieve it alongside any screenshot.
- To settle a geometry question, that log carries a prediction of what the
  scanner should display, so one screenshot either confirms it or refutes it:

  - `Predicted scanner display` gives the anatomical letter for each image edge
    and the boxed view-from marker. A native transversal reconstruction shows
    `R` left, `L` right, `A` top, `P` bottom, viewed from `F`.
  - `Predicted frame N/M` gives the patient-space position of five sample
    frames in the scanner's own numbering and `SP` notation. Read the `SP`
    field for the same frame number off the scanner and compare. A match means
    the emitted header was honoured, so any remaining error is this app's; a
    sign difference means the slice axis is reversed relative to the native
    series; a different magnitude means the volume centre or the field of view
    disagrees.
  - `Patient-space localisation` reports, per emitted axis, the intensity
    centroid and the extent of the signal in millimetres, plus the centroid as
    a patient-space position such as `R1.7 P1.7 F28.4`. This is what tells a
    volume that is merely stored back to front from one whose content is
    genuinely in the wrong place, without needing an image at all.
  - `Display frame handedness` must be right-handed under the DICOM rule; a
    left-handed emitted frame is logged at error level. The acquisition frame is
    checked separately, against the Siemens `phase x read = slice` rule, because
    measured data is DICOM-left-handed by convention.
  - `ICE frame-stacking compensation applied` is logged as a warning whenever
    stage 3 reverses the frames, because that is the point where the emitted
    pixels stop agreeing with the emitted `slice_dir`.
- Debug arrays are written below `/tmp/share/debug` with the
  `sodiumgridding_` prefix. Runtime data is never stored under `/home`.

## Open source development

Source:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/sodiumgridding

Issues: https://github.com/NeuroDesk/neurocontainers/issues
