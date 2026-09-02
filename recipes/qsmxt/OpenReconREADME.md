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

Derived outputs are converted to unsigned 12-bit display values in the valid
`0..4095` range. Binary masks use `0` and `4095`. T2* scaling uses the 99.9th
percentile of positive finite fits so isolated extreme fits are clipped instead
of quantizing the useful map to zero. The original range, scaling range, scale,
inverse formula, and clipped-voxel count are included in the returned metadata.

Derived maps set the MRD `RescaleSlope` and `RescaleIntercept` attributes. The
OpenRecon DICOM writer maps these attributes to the standard DICOM fields.
Window center and width use the rescaled physical units.

Maps with a non-zero stored-value offset, including QSM, reserve stored code `0`.
The bridge sends `PixelPaddingValue=0`, and native map values use codes
`1..4095`. Scanner testing must confirm that the DICOM writer preserves this
attribute through distortion correction. If preserved, outside-FOV pixels are
padding rather than susceptibility measurements.

QSM images remain magnitude-type derived images. Their MRD `DataRole` contains
`Quantitative` so the scanner does not normalize the parametric pixel values.

QSMxT needs unfiltered phase data. SWI sequences that already apply SWI-specific
phase processing or filtering are not suitable inputs for this OpenRecon
adapter; for example, `t2_swi_tra_wave4_2mm` does not provide the required
unfiltered phase data and should not be used for QSMxT. Start from a plain GRE
sequence instead, and enable both phase and magnitude reconstruction.

OpenRecon selects WH-QSM explicitly:

```text
qsmxt run <bids_dir> --qsm-algorithm whqsm
```

The OpenRecon wrapper also supplies the output directory and resource settings.
Its custom-pipeline defaults are ROMEO phase unwrapping, V-SHARP background-field
removal, WH-QSM inversion, and robust-threshold masking. This differs from the
upstream QSMxT inversion default, which is RTS. Only the QSM map is returned by
default. Enable `sendoriginal` only when the original magnitude and phase series
are needed for debugging.

### Pipeline presets

The **Pipeline preset** selection box contains the ten supplied scanner-tested
algorithm combinations. A pipeline preset overrides **QSM algorithm**,
**Unwrap**, and **Background**. Select **Custom algorithm controls** to use those
three selection boxes. The custom default remains ROMEO, V-SHARP, and WH-QSM.

Treat these measurements as results from the scanner test used for this adapter,
not as universal rankings. Lower inter-scanner error and runtime are better.
Inter-scanner error measures disagreement between the tested scanner outputs.
Higher [XSIM](https://doi.org/10.1002/mrm.30271), a structural-similarity metric
tuned for QSM, is better. Runtime will vary with the acquisition and host
hardware.

| # | Pipeline preset | Preset id | Inter-scanner error | XSIM | Runtime |
| --- | --- | --- | ---: | ---: | ---: |
| 1 | ROMEO + RESHARP + RTS | `romeo-resharp-rts` | 3.7% | 0.293 | 59 s |
| 2 | ROMEO + iSMV + HD-QSM | `romeo-ismv-hdqsm` | 4.6% | 0.361 | 78 s |
| 3 | ROMEO + RESHARP + Tikhonov | `romeo-resharp-tikhonov` | 3.8% | 0.283 | 51 s |
| 4 | ROMEO + RESHARP + TV (ADMM) | `romeo-resharp-tv` | 4.2% | 0.308 | 72 s |
| 5 | ROMEO + RESHARP + HD-QSM | `romeo-resharp-hdqsm` | 6.5% | 0.360 | 84 s |
| 6 | ROMEO + iSMV + RTS | `romeo-ismv-rts` | 7.5% | 0.303 | 50 s |
| 7 | ROMEO + iSMV + WH-QSM | `romeo-ismv-whqsm` | 3.0% | 0.388 | 221 s |
| 8 | ROMEO + SHARP + WH-QSM | `romeo-sharp-whqsm` | 2.1% | 0.372 | 226 s |
| 9 | ROMEO + RESHARP + WH-QSM | `romeo-resharp-whqsm` | 3.4% | 0.384 | 224 s |
| 10 | ROMEO + SHARP + Tikhonov | `romeo-sharp-tikhonov` | 7.6% | 0.271 | 30 s |

Each preset passes the corresponding `--unwrapping-algorithm`,
`--bf-algorithm`, and `--qsm-algorithm` values to `qsmxt run`.

### Custom algorithm controls

The following tables cover every algorithm value exposed by this OpenRecon
adapter. QSMxT itself may support additional algorithms that are not present in
the scanner selection boxes. See the [QSMxT algorithm
reference](https://qsmxt.github.io/QSMxT/reference/algorithms/) for the upstream
list and method references.

`Default` is not a separate algorithm. With the custom pipeline it resolves to
the OpenRecon default named in each table.

The stages solve different parts of the reconstruction. Unwrapping removes
phase jumps, background-field removal isolates the local tissue field, and QSM
inversion estimates susceptibility from that field. The mask defines which
voxels enter those calculations.

| QSM algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, WH-QSM. |
| `whqsm` | Weak-Harmonic QSM. |
| `hdqsm` | Hybrid data-fidelity QSM. |
| `rts` | Rapid Two-Step inversion. |
| `tv` | Total Variation inversion solved with ADMM. |
| `tkd` | Thresholded K-space Division. |
| `tsvd` | Truncated Singular Value Decomposition. |
| `tgv` | Total Generalized Variation. |
| `tikhonov` | Tikhonov-regularized inversion. |
| `nltv` | Nonlinear Total Variation. |
| `medi` | Morphology Enabled Dipole Inversion. |
| `ilsqr` | iLSQR inversion. |
| `qsmart` | QSMART two-stage reconstruction. |

| Unwrap algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, ROMEO. |
| `romeo` | Rapid Opensource Minimum Spanning TreE AlgOrithm. |
| `laplacian` | Laplacian phase unwrapping. |

| Background algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, V-SHARP. |
| `vsharp` | Variable-kernel SHARP. |
| `pdf` | Projection onto Dipole Fields. |
| `lbv` | Laplacian Boundary Value. |
| `ismv` | Iterative Spherical Mean Value. |
| `sharp` | Sophisticated Harmonic Artifact Reduction for Phase data. |
| `resharp` | Regularization-enabled SHARP. |

| Mask preset | Method |
| --- | --- |
| `default` | Use the QSMxT default, robust threshold. |
| `robust-threshold` | Otsu thresholding of the phase-quality map, followed by dilation, hole filling, and erosion. |
| `bet` | Brain Extraction Tool masking of the magnitude image. |

The mask preset remains active when an algorithm pipeline preset is selected;
the pipeline preset only overrides unwrapping, background removal, and QSM
inversion. OpenRecon uses QSMxT's default parameters for the selected methods.

## Input data

Use a plain GRE acquisition with unfiltered phase and magnitude outputs enabled.
Do not use filtered SWI phase images as QSMxT input.

An [example Siemens 3 T GRE protocol
(`gre_qsm.pro`)](https://github.com/NeuroDesk/neurocontainers/blob/main/recipes/qsmxt/gre_qsm.pro)
is included as a starting point. It acquires five echoes at 5, 10, 15, 20, and
25 ms. Its saved OpenRecon settings use ROMEO, PDF, and RTS with robust-threshold
masking, return the QSM map, and also send the original magnitude and phase
series. These saved settings differ from the current OpenRecon defaults above.
Review all acquisition and safety settings on the target scanner before use.

## UI parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `qsmxt` | Selects the MRD server configuration. |
| Output maps | `sendoutputs` | choice | `qsm` | Selects which QSMxT derivatives are sent back. |
| Send original | `sendoriginal` | boolean | `false` | Sends original magnitude and phase image series before derived outputs. |
| Pipeline preset | `pipelinepreset` | choice | `custom` | Selects a three-stage algorithm preset or the custom algorithm controls. |
| QSM algorithm | `qsmalgorithm` | choice | `whqsm` | Inversion algorithm for the custom pipeline. |
| Unwrap | `unwrappingalgorithm` | choice | `romeo` | Phase-unwrapping algorithm for the custom pipeline. |
| Background | `bfalgorithm` | choice | `vsharp` | Background-field removal algorithm for the custom pipeline. |
| Mask preset | `maskpreset` | choice | `robust-threshold` | Masking method, independent of the pipeline preset. |

## Open source development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/qsmxt

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
