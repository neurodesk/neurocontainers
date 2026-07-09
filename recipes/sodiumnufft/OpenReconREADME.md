# Sodium NUFFT OpenRecon

`sodiumnufft` reconstructs a 3D sodium volume from ISMRMRD raw data.

1. Read the non-Cartesian raw data coil by coil.
2. Optionally zero weak sample columns using the histogram-based thresholding
   from the original script.
3. Scale each coil using the central sample window.
4. Apply the configured sample offset when matching the external trajectory to
   the raw samples.
5. Build a clipped radial density compensation function from the trajectory, or
   estimate an iterative DCF if requested.
6. Optionally apply the Fermi k-space taper.
7. Run `sigpy.nufft_adjoint` for each coil.
8. Combine the reconstructed coils with root-sum-of-squares.

The output is a derived magnitude series named
`<protocol>_sodiumnufft`. If the protocol name is unavailable, the fallback
series name is `sodiumnufft`.

## Input Requirements

This reconstruction expects ISMRMRD raw acquisitions for a 3D sodium scan.

Trajectory input is resolved in this order:

1. Embedded ISMRMRD trajectories from the incoming acquisitions.
2. An external HDF5 file provided through the `trajectoryfile` parameter.
3. The bundled HDF5 file selected by `trajectorypreset`.

For Siemens Twix `.dat` files, the container also includes
`/opt/code/python-ismrmrd-server/siemens_twix2mrd.py`. That helper uses the
bundled `siemens_to_ismrmrd` converter in `--skipSyncData` mode and materializes
the resulting MRD message stream into an ISMRMRD HDF5 dataset.

The bundled trajectory presets are `23Na_n50`
(`/opt/sodiumnufft/23NA_n50_trajectory.h5`) and `23Na_n28`
(`/opt/sodiumnufft/23Na_n28_trajectory.h5`). The trajectory dataset defaults to
`k`. The code expects the trajectory values to match the standalone script
convention: k-space units in `1/cm`, multiplied by the reconstruction field of
view in cm before the adjoint NUFFT.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `sodiumnufft` | Selects the MRD server configuration. |
| Trajectory file | `trajectoryfile` | string | empty | Optional external HDF5 trajectory path used when trajectories are not embedded in the MRD data. Leave empty to use the bundled trajectory selection. |
| Bundled trajectory | `trajectorypreset` | choice | `23Na_n28` | Bundled trajectory used when no external trajectory file path is provided. Choices: `23Na_n50`, `23Na_n28`. |
| Trajectory dataset | `trajectorydataset` | string | `k` | Dataset name inside the trajectory HDF5 file. |
| Trajectory sample offset | `trajectorysampleoffset` | integer | `0` | Number of leading trajectory samples to skip before pairing the external trajectory with the raw data. |
| Matrix size | `matrixsize` | integer | `128` | Final isotropic reconstruction matrix. |
| FOV cm | `fovcm` | string | `22.0` | Reconstruction field of view in cm. |
| Reject weak samples | `rejectbadreadouts` | boolean | `true` | Zero low-signal sample columns using the histogram rule from the standalone script. |
| Reject sigma | `badreadoutsigma` | string | `3.0` | Sigma multiplier used for weak-sample rejection. |
| Center window | `centerwindow` | integer | `5` | Half-width of the central sample window used for scaling. |
| Apply Fermi filter | `applyfermifilter` | boolean | `true` | Enable the optional Fermi taper in k-space. |
| Fermi width | `fermiwidth` | string | `0.15` | Width parameter for the optional Fermi filter. |
| Fermi cutoff | `fermicutoff` | string | `0.9` | Cutoff parameter for the optional Fermi filter. |
| DCF iterations | `dcfiterations` | integer | `0` | Set above 0 to use iterative DCF estimation instead of the clipped radial DCF. |
| Max coils | `maxcoils` | integer | `0` | Limit reconstruction to the first N coils. Use `0` to reconstruct all coils. |
| Max workers | `maxworkers` | integer | `6` | Maximum number of parallel coil reconstructions. |

## Runtime Notes

- The reconstruction is implemented for raw k-space input. If image data is
  sent to this app, the images are returned unchanged.
- The derived output is magnitude-only.
- `maxcoils` is mainly useful for faster smoke tests and debugging.
- The container does not store runtime files under `/home`; use mounted paths
  such as `/tmp` or another accessible filesystem location for external
  trajectory files.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/sodiumnufft

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
