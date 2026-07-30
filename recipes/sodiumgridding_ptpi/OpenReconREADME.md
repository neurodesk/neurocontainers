# sodiumgridding_pTPI OpenRecon

`sodiumgridding_pTPI` reconstructs dual-echo palindromic TPI raw data from an
ISMRMRD stream. It adapts
`ptpi_dual_echo_lowrank_core.py` from the standalone HDF5 workflow into the
same OpenRecon container contract used by `sodiumgridding`.

The reconstruction performs these steps:

1. Read alternating TE1/TE2 raw acquisitions from the MRD stream.
2. Optionally limit and variance-compress physical coils.
3. Split interleaved echoes; reverse TE2 sample order for forward-order gridding.
4. Load the bundled pTPI trajectory and echo timing arrays.
5. Estimate separate Kaiser-Bessel DCF weights for TE1 and TE2.
6. Grid each echo, estimate a TE1/TE2 B0 field map, and optionally rerun both
   echoes with low-rank phase correction.
7. Combine coils using adaptive combination or sum-of-squares.
8. Optionally estimate an N4 bias field and apply it to the final echoes.

The output is two explicit 3D derived magnitude images named
`<protocol>_sodiumgridding_pTPI_te1` and
`<protocol>_sodiumgridding_pTPI_te2`. Both images
reuse the geometry/output helper from `sodiumgridding`, so display-frame
canonicalization, `Keep_image_geometry`, orientation debug sweeps, and scanner
prediction logging are intentionally identical.

## Input Requirements

The app expects raw acquisitions ordered as alternating TE1/TE2 readouts:

```text
TE1 readout 0, TE2 readout 0, TE1 readout 1, TE2 readout 1, ...
```

The bundled trajectory choices are:

```text
/opt/sodiumgridding_ptpi/23Na_pTPI_n28_g50_p5traj.h5
/opt/sodiumgridding_ptpi/23Na_pTPI_n28_g70_p5traj.h5
/opt/sodiumgridding_ptpi/23Na_pTPI_n50_g70_p5traj.h5
```

When available, the container uses:

- `k_echo1`
- `k_echo2_forward_order`
- `t_echo1_relative_s`
- `t_echo2_forward_relative_s`

If the dual-echo trajectory datasets are absent, it falls back to dataset `k`
for both echoes and infers sample timing from `sampling_time_us`.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `sodiumgridding_ptpi` | Select the MRD server configuration. |
| Bundled trajectory | `trajectoryfile` | choice | `/opt/sodiumgridding_ptpi/23Na_pTPI_n28_g50_p5traj.h5` | Bundled pTPI trajectory. |
| Matrix size | `matrixsize` | int | `128` | Final isotropic reconstruction matrix. |
| FOV cm | `fovcm` | string | `22.0` | Reconstruction field of view in centimetres. |
| DCF iterations | `dcfiterations` | int | `5` | Kaiser-Bessel density compensation iterations. |
| Coil compression | `coilcompression` | boolean | `true` | Use PCA variance-retention coil compression. |
| Coil variance | `coilvarianceretention` | string | `0.9` | Variance retained by the compression basis. |
| Coil combination | `coilcombinemode` | choice | `AC` | Adaptive combine or sum-of-squares. |
| Echo normalization | `echonormalizationmode` | choice | `none` | TE1/TE2 centre-sample normalization mode. |
| Delta TE s | `fieldmapdeltates` | string | `0.005` | Echo spacing used for B0 field-map estimation. |
| Phase correction | `runphasecorrection` | boolean | `true` | Run low-rank phase correction for both echoes. |
| Low-rank rank | `phaselowrankrank` | int | `10` | Rank used for the low-rank phase table. |
| N4 correction | `applyn4biascorrection` | boolean | `true` | Apply N4 bias correction to the final echoes. |
| Orientation | `orientation` | choice | `zyx` | Shared sodiumgridding trajectory/display mapping; `_fz` variants also reverse the slice axis. |

The OpenRecon packaging schema allows at most 14 label parameters, so three
settings are not exposed as parameters of their own:

| Setting | Where it lives | Default |
| --- | --- | --- |
| Reverse slice axis | `_fz` suffix on the `orientation` choice | off |
| Max coils | `SODIUMGRIDDING_PTPI_MAX_COILS` container env var | `0` (all coils) |
| Max workers | `SODIUMGRIDDING_PTPI_MAX_WORKERS` container env var | `8` |

All three are still read from `maxcoils`, `maxworkers`, and
`orientationflipslice` when supplied in a manual JSON config, so the MRD
contract is unchanged.

## Runtime Notes

- The OpenRecon wrapper disables standalone plotting and file-output flags from
  the original script.
- Debug arrays are written below `/tmp/share/debug` with the `sodiumgridding_ptpi_`
  prefix, including TE1, TE2, the B0 field map, and N4 outputs when available.
- The wrapper keeps the original low-rank correction signs and N4 defaults from
  the supplied standalone script.
- Geometry handling is delegated to `sodiumgridding.py`, so orientation changes
  should be validated the same way: use `orientation=debug` on an asymmetric
  object or a known-side marker, then set `orientation` to the matching result,
  adding the `_fz` suffix if the matching series had its slices reversed.

## Build

```bash
source env/bin/activate
python -m builder generate sodiumgridding_ptpi --recreate --architecture x86_64
sf-build sodiumgridding_ptpi --architecture x86_64
```
