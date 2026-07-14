# ME-ICA OpenRecon

This adapter receives reconstructed multi-echo fMRI magnitude images, separates
them by echo and repetition, writes one four-dimensional NIfTI time series per
echo, runs ME-ICA v4, and returns the selected result as a derived MRD series.

ME-ICA requires at least three echoes with matching spatial dimensions and time
points. Echo times are read from the MRD sequence header. The adapter uses
`EchoTime`, `EchoNumber`, MRD `contrast`, `repetition`, and slice fields to
separate the incoming time series. It reports an error if the sequence header
does not contain echo times. ME-ICA runs with 24 CPU workers.

The default `medn` output is ME-ICA's conservative denoised BOLD time series.
`tsoc` returns the optimally combined raw multi-echo series, `hikts` returns the
high-kappa denoised series, and `all` returns all three when present.

The input must be reconstructed magnitude multi-echo EPI. Phase images and raw
k-space acquisitions are not ME-ICA inputs. An anatomical image is optional in
ME-ICA and is intentionally not required by this inline adapter.

## UI Parameters

| GUI label | Parameter id | Default | Description |
| --- | --- | --- | --- |
| config | `config` | `meica` | Selects the ME-ICA server module. |
| Output | `output` | `medn` | Selects the returned ME-ICA time series. |
| Send original images | `sendoriginal` | `false` | Returns incoming echo images before derivatives. |

ME-ICA is a research tool and is not intended for standard clinical use.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/meica

Upstream ME-ICA documentation and source:
https://github.com/ME-ICA/me-ica
