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
| Apply Fermi filter | `applyfermifilter` | boolean | `true` | Apply the radial k-space Fermi taper before compression. |
| Fermi width | `fermiwidth` | string | `0.05` | Fermi transition width. |
| Fermi cutoff | `fermicutoff` | string | `0.98` | Normalized Fermi cutoff. |
| DCF iterations | `dcfiterations` | integer | `5` | Kaiser-Bessel density compensation iterations; use `0` for uniform weights. |
| Max coils | `maxcoils` | integer | `16` | Limit physical coils before compression; use `0` for all coils. |
| Max workers | `maxworkers` | integer | `8` | Parallel virtual-coil workers. At most two oversampled grids are resident concurrently. |
| Coil variance | `coilvarianceretention` | string | `0.9` | Fraction of physical-coil variance retained during compression. |
| Coil combination | `coilcombinemode` | choice | `AC` | Use adaptive combination (`AC`) or sum-of-squares (`SoS`). |
| N4 correction | `applyn4biascorrection` | boolean | `true` | Apply N4 bias field correction after coil combination. |

Weak-readout rejection remains enabled with the standalone defaults of three
standard deviations and a five-sample half-window. The Kaiser-Bessel kernel
width is fixed at `3.0`, matching the supplied implementation.

## Runtime notes

- The derived output is magnitude-only and is emitted as one explicit 3D MRD
  image in `[z, y, x]` order.
- Output pixels and both in-plane direction vectors are flipped to retain the
  established ICE display orientation and scanner markers.
- Each run logs the FIRE-visible CPU count, affinity, cgroup limits, configured
  worker cap, effective virtual-coil workers, and whether pyFFTW is available.
- Debug arrays are written below `/tmp/share/debug` with the
  `sodiumgridding_` prefix. Runtime data is never stored under `/home`.

## Open source development

Source:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/sodiumgridding

Issues: https://github.com/NeuroDesk/neurocontainers/issues
