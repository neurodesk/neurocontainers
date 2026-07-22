# ME-ICA OpenRecon

This adapter receives reconstructed multi-echo fMRI magnitude images, separates
them by echo and repetition, writes one four-dimensional NIfTI time series per
echo, runs ME-ICA v4, and returns two fixed derived MRD series.

ME-ICA requires at least three echoes with matching spatial dimensions and time
points, and each echo must contain at least 21 time points. A 20-time-point
input passes AFNI despiking but fails when ME-ICA's initial motion-correction
step selects volume and matrix indices 0 through 20 inclusive. Longer fMRI
runs are strongly recommended for meaningful ICA results. Echo times are read
from the MRD sequence header. The adapter uses `EchoTime`, `EchoNumber`, MRD
`contrast`, `repetition`, and slice fields to separate the incoming time
series. It reports an error if the sequence header does not contain echo times.
ME-ICA runs with 24 CPU workers.

If AFNI cannot skull-strip the optimally combined functional reference, the
adapter treats every nonzero voxel as phantom foreground, closes one-voxel
gaps, and fills enclosed holes so that the complete phantom interior is
segmented. Completely empty inputs still fail instead of producing an empty
mask.

OpenRecon always returns two derived series. The `dr2s_epi` series is the
denoised BOLD-like time series expressed as apparent `dR2*` in `s^-1`. The
`t2s_epi` series is the T2* map. These outputs are fixed and are not selectable
in the scanner GUI.

The input must be reconstructed magnitude multi-echo EPI. Phase images and raw
k-space acquisitions are not ME-ICA inputs. An anatomical image is optional in
ME-ICA and is intentionally not required by this inline adapter.

## UI Parameters

| GUI label | Parameter id | Default | Description |
| --- | --- | --- | --- |
| config | `config` | `meica` | Selects the ME-ICA server module. |

ME-ICA is a research tool and is not intended for standard clinical use.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/meica

Upstream ME-ICA documentation and source:
https://github.com/ME-ICA/me-ica
