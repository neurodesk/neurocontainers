# QSMxT OpenRecon

This OpenRecon adapter receives reconstructed ISMRMRD image messages, separates
the magnitude and phase series, writes a temporary BIDS MEGRE dataset, runs the
QSMxT v9 Rust binary, and sends selected derivatives back as derived MRD image
series.

The **Input images** control selects distortion-corrected images, images marked
`ND` (not distortion corrected), or both. Distortion-corrected input is the
default. **Both** runs QSMxT once per magnitude/phase pair and returns distinct
`DC` and `ND` output series. It takes roughly twice as long as processing one
pair.

The wrapper classifies phase data from MRD image type metadata, DICOM image type
metadata, or source series names such as `phase`, `pha`, or `_Pha`. It recognizes
`ND` only as a separate, case-insensitive name token, so magnitude and phase are
always paired within the same distortion-correction variant. A missing or
ambiguous requested pair fails explicitly instead of selecting by arrival order.

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
debugging, but they are not shown in the scanner UI. Voxel geometry comes from
the MRD image stream.

Default output is the QSM map (`Chimap`). Enable `sendoutputs=all` to return all
QSMxT derivatives that exist after the run. The original magnitude and phase
series are sent back before the derived output by default; disable
`sendoriginal` when only the derived maps should reach the scanner database.

Derived outputs are converted to unsigned 12-bit display values in the valid
`0..4095` range. Binary masks use `0` and `4095`. QSM storage scaling preserves
the complete finite susceptibility range, while its automatic display window
uses the 1st and 99th percentiles of finite non-zero voxels. Sparse extrema
therefore remain quantitatively recoverable without flattening the visible
contrast. T2* scaling uses the 99.9th percentile of positive finite fits so
isolated extreme fits are clipped instead of quantizing the useful map to zero.
The original range, scaling range, scale, inverse formula, physical display
window, and clipped-voxel count are included in the returned metadata.

Derived maps set the MRD `RescaleSlope` and `RescaleIntercept` attributes. The
OpenRecon DICOM writer maps these attributes to the standard DICOM fields.
Siemens MRD metadata truncates sub-unit window values, so QSM DICOM values use
parts per billion. One ppm equals 1000 ppb. This unit conversion preserves the
quantitative values and lets the bridge publish integer `WindowCenter` and
`WindowWidth` values for the robust display range.

`QSMxTPhysicalWindowCenter` and `QSMxTPhysicalWindowWidth` retain the same
window in ppm. The Enhanced MR object keeps `RescaleType=US` as required.
`QSMxTWindowDomain=ppb` records the domain of the standard DICOM window fields.
`QSMxTDisplayFormula` records how to recover the source ppm value from a stored
pixel.

Maps with a non-zero stored-value offset, including QSM, reserve stored code `0`.
The bridge sends `PixelPaddingValue=0` and `PixelPaddingRangeLimit=0`, and native
map values use codes `1..4095`. Outside-FOV pixels can therefore be represented
as padding rather than susceptibility measurements.

QSM images remain magnitude-type derived images. Their MRD `DataRole` contains
`Quantitative` so the scanner does not normalize the parametric pixel values.

QSMxT needs unfiltered phase data. SWI sequences that already apply SWI-specific
phase processing or filtering are not suitable inputs for this OpenRecon
adapter; for example, `t2_swi_tra_wave4_2mm` does not provide the required
unfiltered phase data and should not be used for QSMxT. Start from a plain GRE
sequence instead, and enable both phase and magnitude reconstruction.

OpenRecon selects HD-QSM with ROMEO and iSMV explicitly:

```text
qsmxt run <bids_dir> --qsm-algorithm hdqsm --unwrapping-algorithm romeo --bf-algorithm ismv
```

The OpenRecon wrapper also supplies the output directory and resource settings.
Its custom-pipeline defaults are ROMEO phase unwrapping, iSMV background-field
removal, HD-QSM inversion, and BET magnitude masking. BET uses a 0.5 fractional
intensity threshold, closing radius 1, and automatic hole filling. This differs
from the upstream QSMxT inversion default, which is RTS. The QSM map is the only
derived output returned by default, and `sendoriginal` is on so the source
magnitude and phase series are stored alongside it.

The default prioritizes speed while retaining good similarity in the QSM-CI
in silico 2019 benchmark: ROMEO + iSMV + HD-QSM achieved xSIM 0.361 in about
1.4 minutes. The matching WH-QSM combination scored 0.388 but took about
3.6 minutes. These are benchmark measurements, not scanner runtime guarantees.
See the linked full-pipeline results below.

### Pipeline presets

The **Pipeline preset** selection box contains ten scanner-tested combinations
and six complete-method presets. A pipeline preset overrides **QSM algorithm**,
**Unwrap**, and **Background**. Select **Custom algorithm controls** to use those
three selection boxes. The custom default is ROMEO, iSMV, and HD-QSM.

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

### Complete-method presets

| Preset | Processing |
| --- | --- |
| `qsmart` | ROMEO field mapping followed by QSMART's vessel-aware background removal and inversion. |
| `tgv` | TGV reconstruction with internal background handling. Multi-echo inputs use ROMEO field mapping; single-echo inputs go directly to TGV. |
| `autoqsm` | ROMEO field mapping followed by AutoQSM from the total field. |
| `nextqsm` | ROMEO field mapping followed by NeXtQSM from the total field. |
| `iqsm` | iQSM from wrapped phase; no separate unwrapping or background removal. |
| `iqsm-plus` | iQSM+ from wrapped phase; no separate unwrapping or background removal. |

These presets override the three custom stage controls. Mask settings still
apply. QSMxT dispatches the complete method and bypasses the stages it replaces;
selecting QSMART does not run V-SHARP before QSMART. These six presets have not
been measured in the scanner experiment above. Their QSM-CI results appear below.

### Custom algorithm controls

The selection boxes cover all inversion, phase-unwrapping, and background-field
removal algorithms in the packaged QSMxT release. The release smoke test compares
them with `qsmxt run --help`. See the [QSMxT algorithm
reference](https://qsmxt.github.io/QSMxT/reference/algorithms/) for method references.

All 16 ONNX weight files in QSMxT's pinned model registry are downloaded through
the builder cache and verified with SHA-256 during the build. This includes both
NeXtQSM networks and the source-separation networks available on the command line.
The uncompressed weights add about 1.46 GB to the image.

The container sets `QSM_MODEL_DIR=/opt/qsmxt/models`, so packaged neural-network
methods work offline and do not require a home directory. `models.sha256` records
the upstream hashes; the download URLs pin an immutable model-repository revision.
`QSM_MODEL_CACHE=/tmp/share/qsmxt_models` remains a writable fallback. To supply
other compatible weights, mount a directory and override `QSM_MODEL_DIR`.
These methods use CPU inference and can require more memory than classical
methods. Their inclusion does not imply validation on the scanner dataset used
for the preset table.

`Default` is not a separate algorithm. With the custom pipeline it resolves to
the OpenRecon default named in each table.

The stages solve different parts of the reconstruction. Unwrapping removes
phase jumps, background-field removal isolates the local tissue field, and QSM
inversion estimates susceptibility from that field. The mask defines which
voxels enter those calculations.

| QSM algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, HD-QSM. |
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
| `tfi` | Total Field Inversion. |
| `ndi` | Nonlinear Dipole Inversion. |
| `fansi` | FANSI nonlinear TV inversion. |
| `fansi-tgv` | FANSI nonlinear TGV inversion. |
| `l1qsm` | L1-QSM inversion. |
| `amp-pe` | AMP-PE inversion. |
| `xqsm` | xQSM deep-learning inversion. |
| `qsmnet` | QSMnet deep-learning inversion. |
| `qsmnet-plus` | QSMnet+ deep-learning inversion. |
| `autoqsm` | AutoQSM reconstruction from total field. |
| `qsmgan` | QSMGAN deep-learning inversion. |
| `ir2qsm` | IR2QSM deep-learning inversion. |
| `lpcnn` | LPCNN deep-learning inversion. |
| `modl-qsm` | MoDL-QSM inversion, producing the susceptibility tensor's chi33 component. |
| `nextqsm` | NeXtQSM reconstruction from total field. |
| `iqsm` | iQSM reconstruction from phase. |
| `iqsm-plus` | iQSM+ reconstruction from phase. |

| Unwrap algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, ROMEO. |
| `romeo` | Rapid Opensource Minimum Spanning TreE AlgOrithm. |
| `laplacian` | Laplacian phase unwrapping. |

| Background algorithm | Method |
| --- | --- |
| `default` | Use the OpenRecon default, iSMV. |
| `vsharp` | Variable-kernel SHARP. |
| `pdf` | Projection onto Dipole Fields. |
| `lbv` | Laplacian Boundary Value. |
| `ismv` | Iterative Spherical Mean Value. |
| `sharp` | Sophisticated Harmonic Artifact Reduction for Phase data. |
| `resharp` | Regularization-enabled SHARP. |
| `harperella` | HARPERELLA background removal. |
| `iharperella` | Iterative HARPERELLA background removal. |
| `bfrnet` | BFRnet deep-learning background removal. |
| `iqfm` | iQFM joint phase unwrapping and background removal. |

| Mask preset | Method |
| --- | --- |
| `bet` | Brain Extraction Tool masking of magnitude. This is the OpenRecon default. |
| `robust-threshold` | Otsu or percentile thresholding of the selected magnitude or phase-quality input. |
| `combined` | Union of independently generated BET and threshold masks. |

The mask preset remains active when an algorithm pipeline preset is selected;
the pipeline preset only overrides unwrapping, background removal, and QSM
inversion. Mask controls expose the threshold input and method, BET fractional
intensity, and a cleanup preset.
Magnitude thresholding is the safer choice for single-echo data because a
single echo provides no inter-echo phase-coherence information. Closing and
hole filling run before erosion; erosion defaults to zero so repaired gaps are
not reopened.

<!-- BEGIN QSM-CI benchmarks -->

### QSM-CI in silico 2019 benchmarks

Snapshot retrieved 2026-09-07 from the [QSM-CI results page](https://qsmxt.github.io/QSM-CI/results.html).
The tables use the default isolated run for each packaged implementation;
tuned variants and retired implementations are excluded. The pipeline table
uses the matching composed run starting from phase. Higher xSIM and lower
NRMSE are better. Compare scores within the same input/stage, not across
different reconstruction tasks.

Times are rounded QSM-CI wall-clock measurements, not OpenRecon scanner
estimates. Hardware, threading, acquisition size, masking, echo handling,
and algorithm versions can differ. These are results on one simulated
head phantom, not evidence of clinical accuracy. Standalone stage timings
exclude the rest of the reconstruction and must not be added to predict
scanner turnaround. Read each linked run for its benchmark details.

#### QSM algorithms

| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | --- | ---: | ---: | ---: | --- |
| `whqsm` | Inversion from true local field | 0.622 | 42.5% | ~2.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=whqsm-qsmrs-iso) |
| `hdqsm` | Inversion from true local field | 0.672 | 33.2% | ~34 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=hdqsm-qsmrs-iso) |
| `rts` | Inversion from true local field | 0.694 | 38.4% | ~12 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=rts-qsmrs-iso) |
| `tv` | Inversion from true local field | 0.765 | 28.7% | ~10 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tv-qsmrs-iso) |
| `tkd` | Inversion from true local field | 0.730 | 36.3% | ~3 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tkd-qsmrs-iso) |
| `tsvd` | Inversion from true local field | 0.599 | 46.4% | ~3 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tsvd-qsmrs-iso) |
| `tgv` | QSM from true total field | 0.467 | 56.1% | ~3.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tgv-qsmrs-iso) |
| `tikhonov` | Inversion from true local field | 0.702 | 38.7% | ~3 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tikhonov-qsmrs-iso) |
| `nltv` | Inversion from true local field | 0.782 | 35.0% | ~1.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=nltv-qsmrs-iso) |
| `medi` | Inversion from true local field | 0.622 | 53.1% | ~41 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=medi-qsmrs-iso) |
| `ilsqr` | Inversion from true local field | 0.581 | 53.3% | ~1.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ilsqr-qsmrs-iso) |
| `qsmart` | QSM from true total field | 0.245 | 83.8% | ~11.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmart-qsmrs-iso) |
| `tfi` | QSM from true total field | 0.441 | 81.1% | ~4.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tfi-qsmrs-iso) |
| `ndi` | Inversion from true local field | 0.616 | 48.3% | ~2.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ndi-qsmrs-iso) |
| `fansi` | Inversion from true local field | 0.667 | 31.9% | ~3.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=fansi-nltv-qsmrs-iso) |
| `fansi-tgv` | Inversion from true local field | 0.677 | 33.0% | ~11.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=fansi-nltgv-qsmrs-iso) |
| `l1qsm` | Inversion from true local field | 0.710 | 47.7% | ~2.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=l1qsm-qsmrs-iso) |
| `amp-pe` | Inversion from true local field | 0.653 | 52.5% | ~6.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=amp-pe-qsmrs-iso) |
| `xqsm` | Inversion from true local field | 0.620 | 49.8% | ~1.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=xqsm-iso) |
| `qsmnet` | Inversion from true local field | 0.622 | 49.0% | ~3.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmnet-iso) |
| `qsmnet-plus` | Inversion from true local field | 0.511 | 52.8% | ~3.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmnet-plus-iso) |
| `autoqsm` | QSM from true total field | 0.247 | 85.7% | ~4.5 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=autoqsm-iso) |
| `qsmgan` | Inversion from true local field | 0.245 | 77.5% | ~7.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmgan-iso) |
| `ir2qsm` | Inversion from true local field | 0.668 | 41.4% | ~2.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ir2qsm-iso) |
| `lpcnn` | Inversion from true local field | 0.620 | 45.7% | ~8.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=lpcnn-iso) |
| `modl-qsm` | Inversion from true local field | 0.255 | 73.8% | ~5.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=modl-qsm-iso) |
| `nextqsm` | QSM from true total field | 0.428 | 70.3% | ~2.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=nextqsm-iso) |
| `iqsm` | QSM from phase | 0.312 | 74.6% | ~1.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-iso) |
| `iqsm-plus` | QSM from phase | 0.320 | 77.0% | ~1.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-plus-iso) |

#### Phase unwrapping and field mapping

| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | --- | ---: | ---: | ---: | --- |
| `romeo` | Field mapping from phase | 0.190 | 73.2% | ~32 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs-iso) |
| `laplacian` | Field mapping from phase | 0.183 | 78.8% | ~25 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=laplacian-qsmci-iso) |

#### Background removal

| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | --- | ---: | ---: | ---: | --- |
| `vsharp` | Background removal from true total field | 0.866 | 42.6% | ~8 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=vsharp-qsmrs-iso) |
| `pdf` | Background removal from true total field | 0.843 | 44.5% | ~4.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=pdf-qsmrs-iso) |
| `lbv` | Background removal from true total field | 0.878 | 42.4% | ~17 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=lbv-qsmrs-iso) |
| `ismv` | Background removal from true total field | 0.925 | 28.1% | ~22 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ismv-qsmrs-iso) |
| `sharp` | Background removal from true total field | 0.891 | 36.3% | ~2 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=sharp-qsmrs-iso) |
| `resharp` | Background removal from true total field | 0.903 | 33.0% | ~18 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=resharp-qsmrs-iso) |
| `harperella` | Local field from phase | 0.251 | 105.3% | ~33 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=harperella-qsmrs-iso) |
| `iharperella` | Local field from phase | 0.535 | 75.9% | ~49 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iharperella-qsmrs-iso) |
| `bfrnet` | Background removal from true total field | 0.813 | 58.7% | ~6.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=bfrnet-iso) |
| `iqfm` | Local field from phase | 0.571 | 70.4% | ~1.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqfm-iso) |

#### Full pipeline presets

These measurements include the upstream reconstruction stages. They are
separate from the scanner measurements in the earlier preset table.

| Preset | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | ---: | ---: | ---: | --- |
| `romeo-resharp-rts` | 0.293 | 84.6% | ~1.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~rts-qsmrs-cmp) |
| `romeo-ismv-hdqsm` | 0.361 | 82.5% | ~1.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~hdqsm-qsmrs-cmp) |
| `romeo-resharp-tikhonov` | 0.283 | 85.8% | ~51 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~tikhonov-qsmrs-cmp) |
| `romeo-resharp-tv` | 0.308 | 82.5% | ~1.2 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~tv-qsmrs-cmp) |
| `romeo-resharp-hdqsm` | 0.360 | 81.4% | ~1.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~hdqsm-qsmrs-cmp) |
| `romeo-ismv-rts` | 0.303 | 84.9% | ~49 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~rts-qsmrs-cmp) |
| `romeo-ismv-whqsm` | 0.388 | 77.8% | ~3.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-sharp-whqsm` | 0.372 | 78.6% | ~3.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~sharp-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-resharp-whqsm` | 0.384 | 77.7% | ~3.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-sharp-tikhonov` | 0.271 | 88.7% | ~30 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~sharp-qsmrs~tikhonov-qsmrs-cmp) |
| `qsmart` | 0.138 | 97.4% | ~9.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~qsmart-qsmrs-cmp) |
| `tgv` | 0.294 | 93.0% | ~2.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~tgv-qsmrs-cmp) |
| `autoqsm` | 0.200 | 90.3% | ~3.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~autoqsm-cmp) |
| `nextqsm` | 0.234 | 100.8% | ~2.5 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~nextqsm-cmp) |
| `iqsm` | 0.312 | 74.6% | ~47 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-cmp) |
| `iqsm-plus` | 0.320 | 77.0% | ~1.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-plus-cmp) |

ROMEO and Laplacian rows measure field mapping, including echo handling,
rather than unwrapping alone. HARPERELLA, iHARPERELLA, and iQFM benchmark
joint unwrapping/background removal. QSMART refers to QSM.rs, not MATLAB.

TFI is benchmarked from the true total field. The packaged QSMxT `run` dispatcher
still applies the selected background-removal stage before TFI; its GUI
selection therefore does not reproduce that isolated TFI benchmark.

Regenerate these tables with `python3 recipes/qsmxt/update_benchmarks.py`.
Use `--refresh` to fetch new results or `--check` to check the committed
tables against `qsmci-in-silico-2019.json`. The snapshot records source hashes.

<!-- END QSM-CI benchmarks -->

## Input data

Use a plain GRE acquisition with unfiltered phase and magnitude outputs enabled.
Do not use filtered SWI phase images as QSMxT input.

An [example Siemens 3 T GRE protocol
(`gre_qsm.pro`)](https://github.com/NeuroDesk/neurocontainers/blob/main/recipes/qsmxt/gre_qsm.pro)
is included as a starting point. It acquires five echoes at 5, 10, 15, 20, and
25 ms. Its saved OpenRecon settings use ROMEO, PDF, and RTS with robust-threshold
masking, return the QSM map, and also send the original magnitude and phase
series. Their algorithm choices differ from the current OpenRecon defaults above.
Review all acquisition and safety settings on the target scanner before use.

## UI parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `qsmxt` | Selects the MRD server configuration. |
| Input images | `inputseries` | choice | `distortion-corrected` | Processes corrected, ND, or both magnitude/phase pairs. |
| Output maps | `sendoutputs` | choice | `qsm` | Selects which QSMxT derivatives are sent back. |
| Send original | `sendoriginal` | boolean | `true` | Sends original magnitude and phase image series before derived outputs. |
| Pipeline preset | `pipelinepreset` | choice | `custom` | Selects a three-stage combination, a complete-method preset, or custom controls. |
| QSM algorithm | `qsmalgorithm` | choice | `hdqsm` | Inversion algorithm for the custom pipeline. |
| Unwrap | `unwrappingalgorithm` | choice | `romeo` | Phase-unwrapping algorithm for the custom pipeline. |
| Background | `bfalgorithm` | choice | `ismv` | Background-field removal algorithm for the custom pipeline. |
| Mask preset | `maskpreset` | choice | `bet` | BET, threshold, or their union; independent of the pipeline preset. |
| Threshold input | `maskinginput` | choice | `magnitude` | Input for threshold-based masking. |
| BET threshold | `betfractionalintensity` | double | `0.5` | BET fractional intensity threshold. |
| Threshold method | `maskthresholdmethod` | choice | `otsu` | Otsu or percentile threshold generation. |
| Mask percentile | `maskthresholdpercentile` | double | `65` | Cutoff used by percentile thresholding. |
| Mask cleanup | `maskcleanup` | choice | `close-fill` | None, fill holes, close and fill, or robust dilate/fill/erode cleanup. |

## Open source development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/qsmxt

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
