# sodiumgridding_pTPI OpenRecon

`sodiumgridding_pTPI` reconstructs multi-echo palindromic TPI raw data from an
ISMRMRD stream. It adapts
`ptpi_dual_echo_lowrank_core.py` from the standalone HDF5 workflow into the
same OpenRecon container contract used by `sodiumgridding`.

The reconstruction performs these steps:

1. Read interleaved echo raw acquisitions from the MRD stream.
2. Optionally limit and variance-compress physical coils.
3. Split interleaved echoes; reverse echoes 2, 4, 6, ... for forward-order gridding.
4. Load the bundled pTPI forward/reverse trajectories and timing arrays.
5. Estimate Kaiser-Bessel DCF weights for the forward and reverse trajectories.
6. Grid each echo. For two or more echoes, estimate a TE1/TE2 B0 field map and
   optionally rerun all echoes with low-rank phase correction.
7. Combine coils using adaptive combination or sum-of-squares.
8. Optionally estimate an N4 bias field and apply it to the final echoes.
9. For two or more echoes, optionally denoise the final echo volumes with
   patch-wise temporal SVD.

The output is one explicit 3D derived magnitude image per detected echo, named
`pTPI_TE1`, `pTPI_TE2`, etc. The short names avoid scanner-side sequence name
truncation, while echo time is stored in the image metadata. The images
reuse the geometry/output helper from `sodiumgridding`, so display-frame
canonicalization, `Keep_image_geometry`, orientation debug sweeps, and scanner
prediction logging are intentionally identical.

When multiple repetitions are present, each echo/repetition pair is emitted as
its own series, for example `pTPI_TE1_rep1`, `pTPI_TE1_rep2`,
`pTPI_TE2_rep1`, and `pTPI_TE2_rep2`.

All echoes from one repetition are sent with a shared scanner display scale by
default, so relative echo brightness reflects the reconstructed magnitude
decay instead of independent per-echo windowing.

## Input Requirements

The app expects raw acquisitions ordered as interleaved echo readouts:

```text
TE1 readout 0, TE2 readout 0, TE3 readout 0, ..., TEn readout 0,
TE1 readout 1, TE2 readout 1, TE3 readout 1, ...
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

If the forward/reverse trajectory datasets are absent, it falls back to dataset
`k` for all echoes and infers sample timing from `sampling_time_us`.

The number of echoes is detected from acquisition contrast indices first and
then from the MRD `encodingLimits.contrast` range. A manual `numechoes` JSON
setting is available as a fallback, but it is intentionally not exposed in the
scanner GUI.

Single-echo acquisitions are supported. In that mode the container reconstructs
`pTPI_TE1` only and skips field-map phase correction and temporal SVD denoising
because those steps need at least two echoes. N4 bias correction can still run
from the single TE1 magnitude image.

Echo times are read from MRD `sequenceParameters.TE` when available. If the
header provides only one TE or no TE values, later echo times are inferred from
`fieldmapdeltates`.

## GUI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `sodiumgridding_ptpi` | Select the MRD server configuration. |
| Bundled trajectory | `trajectoryfile` | choice | `/opt/sodiumgridding_ptpi/23Na_pTPI_n28_g50_p5traj.h5` | Bundled pTPI trajectory. |
| Matrix size | `matrixsize` | int | `128` | Final isotropic reconstruction matrix. |
| FOV cm | `fovcm` | string | `22.0` | Reconstruction field of view in centimetres. |
| DCF iterations | `dcfiterations` | int | `5` | Kaiser-Bessel density compensation iterations. |
| SVD threshold fraction | `svdretainfraction` | string | `0.2` | Keep singular values at least this fraction of the largest singular value in each temporal SVD patch. |
| Low-rank rank | `phaselowrankrank` | int | `10` | Rank used for the low-rank phase table. |
| Coil variance | `coilvarianceretention` | string | `0.9` | Variance retained by the compression basis. |
| Coil combination | `coilcombinemode` | choice | `AC` | Adaptive combine or sum-of-squares. |
| Delta TE s | `fieldmapdeltates` | string | `0.005` | Constant echo spacing used for B0 field-map estimation and later echo timing. |
| Phase correction | `runphasecorrection` | boolean | `true` | Run low-rank phase correction for all echoes. |
| N4 correction | `applyn4biascorrection` | boolean | `true` | Apply N4 bias correction to the final echoes. |
| Orientation | `orientation` | choice | `zyx` | Shared sodiumgridding trajectory/display mapping. |
| Reverse slice axis | `orientationflipslice` | boolean | `false` | Reverse trajectory component 2 before display canonicalization. |

The scanner GUI is limited to 14 parameters. Advanced pTPI defaults remain
fixed in the wrapper: coil compression is enabled, echo normalization is
`none`, temporal SVD denoising is enabled, `maxcoils` is `0` (all coils), and
the worker cap is `8`.

Additional hidden JSON-capable SVD denoising defaults are
`denoisetemporalsvd=true`, `svdpatchsize=7`, and `svdstride=4`. Shared echo
display scaling is enabled with `sharedechodisplayscale=true`.

## Runtime Notes

- The OpenRecon wrapper disables standalone plotting and file-output flags from
  the original script.
- Debug arrays are written below `/tmp/share/debug` with the `sodiumgridding_ptpi_`
  prefix, including every echo, pre-SVD echoes when denoising is enabled, the
  B0 field map, and N4 outputs when available.
- The wrapper keeps the original low-rank correction signs and N4 defaults from
  the supplied standalone script.
- Geometry handling is delegated to `sodiumgridding.py`, so orientation changes
  should be validated the same way: use `orientation=debug` on an asymmetric
  object or a known-side marker, then set `orientation` and
  `orientationflipslice` to the matching result.

## Build

```bash
source env/bin/activate
python -m builder generate sodiumgridding_ptpi --recreate --architecture x86_64
sf-build sodiumgridding_ptpi --architecture x86_64
```
