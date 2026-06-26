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
