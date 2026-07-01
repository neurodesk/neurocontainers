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

For each derived magnitude/phase echo group the wrapper writes:

```text
sub-01/anat/sub-01_acq-<source>_echo-N_part-mag_MEGRE.nii.gz
sub-01/anat/sub-01_acq-<source>_echo-N_part-phase_MEGRE.nii.gz
```

QSMxT sidecars include `EchoTime`, `MagneticFieldStrength`, and `B0_dir`.
Echo grouping, echo times, field strength, and B0 direction are derived from the
incoming MRD image stream and generated NIfTI geometry when available. The
container still accepts `maxechoes`, `echotimesms`, `echotimems`,
`echospacingms`, `fieldstrength`, and `b0dir` as manual JSON overrides for
debugging, but they are not shown in the scanner UI.

Default output is the QSM map (`Chimap`). Enable `sendoutputs=all` to return all
QSMxT derivatives that exist after the run.

QSMxT needs unfiltered phase data. SWI sequences that already apply SWI-specific
phase processing or filtering are not suitable inputs for this OpenRecon
adapter; for example, `t2_swi_tra_wave4_2mm` does not provide the required
unfiltered phase data and should not be used for QSMxT. Start from a plain GRE
sequence instead, and enable both phase and magnitude reconstruction.

The scanner UI defaults are set for a robust inline QSM run: originals are
returned before the QSM map, QSM inversion uses RTS, unwrapping uses ROMEO,
background-field removal uses PDF, and masking uses the robust-threshold preset.

## Input Data

Use a plain GRE acquisition with unfiltered phase and magnitude outputs enabled.
Do not use filtered SWI phase images as QSMxT input.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `qsmxt` | Selects the MRD server configuration. |
| Output maps | `sendoutputs` | choice | `qsm` | Selects which QSMxT derivatives are sent back. |
| Send original | `sendoriginal` | boolean | `true` | Sends original magnitude and phase image series before derived outputs. |
| QSM algorithm | `qsmalgorithm` | choice | `rts` | QSMxT inversion algorithm. |
| Unwrap | `unwrappingalgorithm` | choice | `romeo` | QSMxT phase-unwrapping algorithm. |
| Background | `bfalgorithm` | choice | `pdf` | QSMxT background-field removal algorithm. |
| Mask preset | `maskpreset` | choice | `robust-threshold` | QSMxT masking preset. |

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/qsmxt

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
