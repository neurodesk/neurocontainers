# QSMxT OpenRecon

This OpenRecon adapter receives reconstructed ISMRMRD image messages, separates
the magnitude and phase series, writes a temporary BIDS MEGRE dataset, runs the
QSMxT v9 Rust binary, and sends selected derivatives back as derived MRD image
series.

The wrapper expects one magnitude series and one phase series. It classifies
phase data from MRD image type metadata, DICOM image type metadata, or source
series names such as `phase`, `pha`, or `_Pha`. If explicit metadata is absent
and exactly two series are present, the lower dynamic-range series is used as
phase.

For each magnitude/phase frame pair the wrapper writes:

```text
sub-01/anat/sub-01_acq-<source>_echo-N_part-mag_MEGRE.nii.gz
sub-01/anat/sub-01_acq-<source>_echo-N_part-phase_MEGRE.nii.gz
```

QSMxT requires `EchoTime` and `MagneticFieldStrength` in each JSON sidecar.
Echo times are read from MRD sequence metadata or image metadata when present.
If the stream does not include them, set `echotimesms` in OpenRecon. The
fallback `echotimems` plus `echospacingms` is only for test or emergency use.

Default output is the QSM map (`Chimap`). Enable `sendoutputs=all` to return all
QSMxT derivatives that exist after the run.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `qsmxt` | Selects the MRD server configuration. |
| Output maps | `sendoutputs` | choice | `qsm` | Selects which QSMxT derivatives are sent back. |
| Send original | `sendoriginal` | boolean | `false` | Sends original magnitude and phase image series before derived outputs. |
| Max echoes | `maxechoes` | int | `0` | Limits the number of magnitude/phase echo pairs; `0` uses all pairs. |
| Echo times | `echotimesms` | string | empty | Optional comma-separated echo times in ms. |
| First echo | `echotimems` | double | `20.0` | Fallback first echo time in ms. |
| Echo spacing | `echospacingms` | double | `5.0` | Fallback echo spacing in ms. |
| Field strength | `fieldstrength` | double | `3.0` | Magnetic field strength written to QSMxT sidecars. |
| B0 direction | `b0dir` | string | `0,0,1` | B0 direction vector written to QSMxT sidecars. |
| QSM algorithm | `qsmalgorithm` | choice | `default` | Optional QSMxT inversion algorithm override. |
| Unwrap | `unwrappingalgorithm` | choice | `default` | Optional QSMxT phase-unwrapping algorithm override. |
| Background | `bfalgorithm` | choice | `default` | Optional background-field removal algorithm override. |
| Mask preset | `maskpreset` | choice | `default` | Optional QSMxT masking preset override. |

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/qsmxt

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
