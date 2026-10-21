# QSMxT OpenRecon technical reference

Implementation details and benchmark results for developers and advanced users.
For scanner setup and usage, see the [user guide](OpenReconREADME.md).

## Image processing and storage

This OpenRecon adapter receives reconstructed ISMRMRD image messages, separates
the magnitude and phase series, writes a temporary BIDS MEGRE dataset, runs the
QSMxT v10.0.0 Rust binary, and sends selected derivatives back as derived MRD image
series.

The **Input images** control defaults to **Prefer distortion corrected**.
It selects the preferred magnitude and phase pair, or a complete `ND` pair when
a preferred component is missing. Explicit ND selection remains strict.
**Both** runs QSMxT once per magnitude/phase pair and returns distinct
`DC` and `ND` output series. It takes roughly twice as long as processing one
pair.

The wrapper classifies phase data from MRD image type metadata, DICOM image type
metadata, or source series names such as `phase`, `pha`, or `_Pha`. It recognizes
`ND` only as a separate, case-insensitive name token, so magnitude and phase are
always paired within the same distortion-correction variant. Unmarked series use
the preferred slot; names do not verify physical correction. Only missing
components permit default selection to fall back, after processed-product
filtering and phase inference. Ambiguity, echo alignment, geometry, and native
reconstruction errors propagate without retry. Both requires both complete
pairs. A successful fallback logs the missing component and selected ND source
names, and returns ND-labelled results without changing the requested setting.

Within each correction variant, source selection excludes the known processed
series suffixes `Romeo_Mask`, `Romeo_P`, `Romeo_B0`,
`sensitivity_corrected_mag`, `CLEAR-SWI`, and `CLEAR-SWI_mIP`. Matching is
case-insensitive and uses complete suffix tokens. A terminal `ND` or `DC` token
does not prevent product detection. The DICOM `DERIVED` flag alone does not
exclude an input, because coil-combined source images can carry that flag.

A single eligible magnitude and phase pair remains valid regardless of its
series names. When several candidates remain, the wrapper selects a pair only
if their normalized source family identifies exactly one magnitude and phase
match. For example, `Aspire_M` and `Aspire_P` share the `Aspire` family.
Multiple complete families or duplicate candidates fail with an ambiguity.
Original passthrough applies the same processed-series exclusions and retains
all genuine sources, including correction variants not selected for QSM.

Before applying `maxechoes`, the BIDS writer requires equal echo counts and
checks source echo identities when both components provide them. It aligns
explicit echo identities and validates the converted magnitude and phase
volume shapes and affines before saving each echo. Incompatible input fails
before QSMxT starts.

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

Full-volume diagnostic statistics and voxel hashes are opt-in. Set
`QSMXT_OPENRECON_DIAGNOSTICS=full` in the container environment to enable the
source, BIDS-volume and generated-NIfTI scans. Source selection, geometry,
command results, output inventory and errors remain logged by default. This
setting changes logging only; reconstruction and output validation are unchanged.
The EPI driver writes uncompressed averaged NIfTIs for its internal handoff;
the transient BIDS inputs above remain compressed.

Bulk scanner phase conversion uses a bounded lookup for finite nonnegative
integer counts within the uint16 range. The lookup evaluates the same float32
complex exponential and angle expression as the full-array conversion. Other
inputs retain the existing behavior, including fractional-count precision,
already-radian detection and rejection of spans wider than one configured cycle.

The container builds QSMxT 10.0.0 from pinned source with a runner patch and a
patch to the pinned QSM.rs numerical library.
For a single echo, masking can consume the owned, full-precision corrected
magnitude from the preceding RSS stage. Reuse requires that the raw working
magnitude and its RSS result match bit for bit. Cached magnitude stages,
original-source fallbacks, custom masks, multiple echoes and nonidentical RSS
inputs retain the existing masking path. The saved combined image remains
float32; the handoff avoids its precision loss by keeping the float64 result.

Native bias correction schedules independent lines along all three axes on
the existing Rayon pool. Data and weights share the same disjoint partitions,
and each line retains its arithmetic order. Strided Z-axis work groups mutable
row references without copying another voxel volume. V-SHARP reuses
FFT plans and a complex work buffer across kernel radii, and transforms its
unchanged mask once. Kernel radii, erosion thresholds, deconvolution and working
resolution retain their configured values.

`/opt/qsmxt/native-build.json` records the source, patch, build script, compiler,
Cargo lockfile, QSM.rs source, numerical patch and installed binary identities.
The build checks the pinned patches, script and QSM.rs commit, runs the core and
handoff tests, and removes the temporary Rust toolchain, Cargo cache and build
tree. The core patch applies inside that build's private Cargo checkout while
the native executable retains the upstream dependency lockfile. Runtime
verification checks the expected provenance and installed binary digest.

Default output is the QSM map (`Chimap`). Enable `sendoutputs=all` to return all
compatible QSMxT derivatives that exist after the run. Below three selected
echoes, All omits implicit T2*/R2* requests and logs the count. GRE uses aligned
echo groups after `maxechoes`. EPI repetitions yield one averaged echo. Each
DC or ND pair resolves independently. Explicit `t2star` or `r2star` selections,
or `dot2starmap=true` or `dor2starmap=true`, fail below three echoes before
reconstruction or EPI preprocessing, including when combined with All. False
advanced flags do not cancel an explicit map selection.

The original magnitude and phase
source series are sent back before the derived output, including unselected DC
and ND variants. The same source-name filter excludes ROMEO phase, mask, and B0,
sensitivity-corrected magnitude, CLEAR-SWI, and CLEAR-SWI mIP companions from
both reconstruction input and original image return. Original pixels, physical
geometry, and echo identities are preserved while return series and instance
identities are reassigned. All incoming series identities remain reserved,
including excluded companions. Original metadata validation remains strict for
eligible sources. There is no `sendoriginal` control; an older configuration
containing that key cannot disable original image return.

Derived outputs are converted to unsigned 12-bit display values in the valid
`0..4095` range. Binary masks use `0` and `4095`. QSM storage scaling preserves
the complete finite susceptibility range, while its automatic display window
uses the 1st and 99th percentiles of finite non-zero voxels. Sparse extrema
therefore remain quantitatively recoverable without flattening the visible
contrast. T2* scaling uses the 99.9th percentile of positive finite fits, capped
at 4.095 seconds to retain storage steps of 1 ms or finer. Larger values saturate
at stored code `4095`. The default T2* window spans zero to the 95th percentile
of positive finite fits, bounded by the scaling range and shared across slices.
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

T2* DICOM values and standard window fields use milliseconds to avoid the same
integer-window limitation. `QSMxTWindowDomain=ms` identifies this conversion.
`QSMxTUnits`, the inverse display formula, and the physical window metadata
remain in seconds, matching the source NIfTI. Scanner export clipping does not
modify that NIfTI, and saturated DICOM pixels cannot recover the original fits.

SWI DICOM values use scaled arbitrary units with a rescale slope of one.
`QSMxTWindowDomain=scaled-a.u.` identifies this domain. Its explicit integer
window spans zero to the 99th percentile of positive finite voxels across the
volume, so every slice uses the same contrast, including empty edge slices.
This avoids fractional scanner-generated windows being rounded to zero or one.
Storage scaling still preserves the full finite range; the display window does
not clip stored pixels. `QSMxTDisplayFormula` and the physical window metadata
retain the conversion to the source arbitrary units. Source NIfTI values do not
change.

QSMxT computes T2* from multi-echo magnitude data inside its reconstruction mask.
The [v10.0.0 pipeline](https://github.com/QSMxT/QSMxT/blob/v10.0.0/src/pipeline/runner.rs)
requires at least three echoes and calls the
[QSM.rs v0.39.0 ARLO estimator](https://github.com/astewartau/QSM.rs/blob/v0.39.0/src/utils/r2star.rs),
with a log-linear fallback for rejected estimates. The ARLO calculation assumes
equally spaced echo times. The pipeline writes R2* in inverse seconds and takes
`T2* = 1 / R2*` for positive rates inside the mask, otherwise zero. Small positive
rates therefore produce arbitrarily large finite T2* values. The scanner display
limits are presentation choices, not fit-quality thresholds or changes to this
estimator. Assessing an extreme fit requires the source magnitude echoes.

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

OpenRecon selects RTS with ROMEO and V-SHARP explicitly:

```text
qsmxt run <bids_dir> --qsm-algorithm rts --unwrapping-algorithm romeo --bf-algorithm vsharp --mask magnitude,hd-bet,close:1,fill-holes:0
```

The OpenRecon wrapper also supplies the output directory and resource settings.
Its custom-pipeline defaults are ROMEO phase unwrapping, V-SHARP background-field
removal, RTS inversion, and HD-BET magnitude masking using the packaged model,
closing radius 1, and automatic hole filling. The adapter adds no signal erosion. The QSM map is the only
derived output returned by default. The source magnitude and phase series
are always stored alongside it.

The current [QSM-CI ROMEO + HARPERELLA + WH-QSM phantom run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~harperella-qsmrs~whqsm-qsmrs-cmp)
reports xSIM 0.3843, NRMSE 73.73%, and runtime 344.3 seconds, about 5.7 minutes.
This is phantom evidence, not scanner or clinical validation. In that CI run,
HARPERELLA jointly unwraps and removes the background field from wrapped phase.
The OpenRecon adapter calls the packaged `qsmxt run` command with the flags above.
These benchmark results do not establish equivalent processing or runtime for
the adapter.

### GRE source separation

`sourceseparation` accepts `off` (default), `r2star-qsm`, or `decompose`.
The enabled methods add `--do-chisep --chisep <method>` to `qsmxt run`.
QSMxT enables the R2* fit for R2*-QSM automatically. DECOMPOSE fits the
multi-echo magnitude using the reconstructed QSM. Neither method needs R2'.
The bridge rejects fewer than three selected echoes, nonpositive or unordered
echo times, unequal spacing for R2*-QSM, and the incompatible `noqsm=true` override.

Enabling separation adds `paramagnetic`, `diamagnetic`, and `separated-total`
to the returned outputs regardless of `sendoutputs`. Their NIfTI names end in
`desc-paramagnetic_Chimap`, `desc-diamagnetic_Chimap`, and `desc-total_Chimap`.
Ordinary QSM matching excludes those descriptors. Missing requested source
maps fail explicitly. With separation off, `sendoutputs=all` retains its
existing behavior and does not enable separation.

All susceptibility outputs share the QSM display range, robust window,
ppm metadata, and ppb DICOM rescaling. QSMxT writes the diamagnetic map as
positive magnitudes, so separated total equals paramagnetic minus diamagnetic.

These expose the implementations in the pinned QSMxT release. QSM-core's
[R2*-QSM](https://github.com/astewartau/QSM.rs/blob/v0.38.0/src/separation/r2star_qsm.rs)
uses the voxelwise closed-form solve without the paper's spatial
regularization. Its
[DECOMPOSE implementation](https://github.com/astewartau/QSM.rs/blob/v0.38.0/src/separation/decompose.rs)
synthesizes per-echo phase from
the reconstructed QSM rather than fitting the original complex GRE signal.

### SMWI

`sendoutputs=smwi` adds `--do-smwi` to `qsmxt run` and returns two series,
`smwi-paramagnetic` and `smwi-diamagnetic`, read from the NIfTI files ending in
`desc-paramagnetic_smwi` and `desc-diamagnetic_smwi`. `sendoutputs=all` includes
both. QSMxT enables QSM for SMWI even when `noqsm=true`. The per-contrast minIP
volumes QSMxT also writes stay in the NIfTI output, as the SWI minIP does.

The [QSM.rs v0.38.0 implementation](https://github.com/astewartau/QSM.rs/blob/v0.38.0/src/swi.rs)
multiplies the combined magnitude inside the mask by `w(χ)^4`, with
`w(χ) = clamp(1 - χ/1 ppm, 0, 1)` for the paramagnetic image and
`clamp(1 + χ/1 ppm, 0, 1)` for the diamagnetic image. These are QSMxT's defaults,
following SEPIA; the bridge does not override `--smwi-threshold` or
`--smwi-power`. Both series share the SWI display handling described above:
scaled arbitrary units, `QSMxTWindowDomain=scaled-a.u.`, and one window across
the volume. Their image-type tokens are `QSMXT_SMWI_PARA` and `QSMXT_SMWI_DIA`,
within the 16-character DICOM limit.

## Pipeline presets

The scanner **Pipeline preset** control exposes Custom, ten historically measured
combinations, and six complete-method presets. Advanced `pipelinepreset` uses the
same choices. Named presets override **Unwrap**, **Background**, and **Dipole
inversion**. Custom uses those stage controls independently, with defaults ROMEO,
V-SHARP, and RTS.

These historical measurements are not validation of QSMxT 10 or the current
scanner package. Lower inter-scanner error and runtime are better.
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

All 19 ONNX weight files in QSMxT's pinned model registry are downloaded through
the builder cache and verified with SHA-256 during the build. This includes both
NeXtQSM networks, the source-separation and R2PRIMEnet networks, HD-BET, and
SynthSeg, which are available on the command line.
The uncompressed weights add about 1.73 GB to the image.

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

| Dipole inversion | Method |
| --- | --- |
| `default` | Use the OpenRecon default, RTS. |
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
| `lsqr` | Minimally regularized LSQR inversion. |
| `heidi` | HEIDI inversion, seeded with an LSQR solve. |
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
| `default` | Use the OpenRecon default, V-SHARP. |
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
| `hd-bet` | HD-BET extraction from magnitude using the packaged model. This is the OpenRecon default. |
| `bet` | Brain Extraction Tool masking of magnitude. |
| `robust-threshold` | Otsu or percentile thresholding of the selected magnitude or phase-quality input. |
| `combined` | Union of independently generated BET and threshold masks. |

The mask preset remains active when an algorithm pipeline preset is selected;
the pipeline preset only overrides unwrapping, background removal, and QSM
inversion. Advanced `maskinginput` defaults to `magnitude`, `maskthresholdmethod`
to `otsu`, and `betfractionalintensity` to 0.5. `maskthresholdpercentile` defaults
to 65% and applies when `maskthresholdmethod` is `percentile`.
HD-BET always uses magnitude, including for single-echo input. For the optional
threshold preset, a single echo provides no inter-echo phase-coherence information,
so magnitude is the default threshold input. Each mask section closes gaps with
radius 1 and fills holes using the native automatic cap of 5% of the image volume
(`close:1,fill-holes:0`). A zero size selects this automatic cap, not unlimited filling.
Dilation and erosion default to zero. Mask cleanup is no longer a scanner control.
Historical cleanup presets and numeric refinement overrides remain supported.
As before, `maskcleanup: none` selects the default close-and-fill behavior.
Numeric overrides can disable individual refinements.

### Homogeneity correction

The scanner control **Homogeneity correction** (`inhomogeneitycorrection`) selects
one profile for magnitude intensity correction.

| Profile | Smoothing sigma | Tissue-estimation boxes per axis |
| --- | --- | --- |
| `standard` (default) | 7 mm | 3 |
| `fine` | 7 mm | 15 |
| `off` | Correction disabled | None |

Enabled profiles pass `--inhomogeneity-correction`, `--homogeneity-sigma-mm 7`, and
`--homogeneity-nbox 3` or `--homogeneity-nbox 15` to QSMxT.
Off passes only `--no-inhomogeneity-correction`, without numeric homogeneity flags.
These settings affect masking, combined magnitude, and SMWI. Version 10 CLEAR-SWI
reads individual echo magnitudes and applies its own receive-sensitivity correction
independently of this profile, including when the profile is Off.
The box count controls tissue estimation, not smoothing passes. Fine provides a
comparison setting. Fine has not been shown to improve 7 T reconstruction quality.
Its effect on a particular acquisition requires image review.
Historical Boolean values and Boolean strings map true to Standard and false to
Off. Numeric Boolean inputs keep their previous true/false interpretation.
Missing, null, and blank values select Standard. Profile IDs ignore case and
surrounding whitespace. Unknown nonblank values raise a configuration error.

<!-- BEGIN QSM-CI benchmarks -->

### QSM-CI in silico 2019 benchmarks

Snapshot retrieved 2026-09-23 from the [QSM-CI results page](https://qsmxt.github.io/QSM-CI/results.html).
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
| `whqsm` | Inversion from true local field | 0.645 | 42.5% | ~3.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=whqsm-qsmrs-iso) |
| `hdqsm` | Inversion from true local field | 0.707 | 33.2% | ~37 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=hdqsm-qsmrs-iso) |
| `rts` | Inversion from true local field | 0.740 | 38.4% | ~15 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=rts-qsmrs-iso) |
| `tv` | Inversion from true local field | 0.822 | 28.7% | ~13 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tv-qsmrs-iso) |
| `tkd` | Inversion from true local field | 0.780 | 36.3% | ~2 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tkd-qsmrs-iso) |
| `tsvd` | Inversion from true local field | 0.645 | 46.4% | ~4 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tsvd-qsmrs-iso) |
| `tgv` | QSM from true total field | 0.398 | 65.1% | ~3.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tgv-qsmrs-iso) |
| `tikhonov` | Inversion from true local field | 0.752 | 38.7% | ~4 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tikhonov-qsmrs-iso) |
| `nltv` | Inversion from true local field | 0.835 | 35.0% | ~1.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=nltv-qsmrs-iso) |
| `medi` | Inversion from true local field | 0.659 | 53.1% | ~48 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=medi-qsmrs-iso) |
| `ilsqr` | Inversion from true local field | 0.616 | 53.3% | ~44 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ilsqr-qsmrs-iso) |
| `lsqr` | Not reported | Not reported | Not reported | Not reported | No matching run |
| `heidi` | Not reported | Not reported | Not reported | Not reported | No matching run |
| `qsmart` | QSM from true total field | 0.251 | 83.8% | ~10.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmart-qsmrs-iso) |
| `tfi` | QSM from true total field | 0.459 | 81.1% | ~4.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=tfi-qsmrs-iso) |
| `ndi` | Inversion from true local field | 0.651 | 48.3% | ~1.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ndi-qsmrs-iso) |
| `fansi` | Inversion from true local field | 0.705 | 31.9% | ~3.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=fansi-nltv-qsmrs-iso) |
| `fansi-tgv` | Inversion from true local field | 0.719 | 33.0% | ~13.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=fansi-nltgv-qsmrs-iso) |
| `l1qsm` | Inversion from true local field | 0.755 | 47.7% | ~2.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=l1qsm-qsmrs-iso) |
| `amp-pe` | Inversion from true local field | 0.691 | 52.5% | ~11.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=amp-pe-qsmrs-iso) |
| `xqsm` | Inversion from true local field | 0.663 | 49.8% | ~1.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=xqsm-iso) |
| `qsmnet` | Inversion from true local field | 0.668 | 49.0% | ~3.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmnet-iso) |
| `qsmnet-plus` | Inversion from true local field | 0.546 | 52.8% | ~3.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmnet-plus-iso) |
| `autoqsm` | QSM from true total field | 0.253 | 85.7% | ~4.8 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=autoqsm-iso) |
| `qsmgan` | Inversion from true local field | 0.262 | 77.5% | ~5.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=qsmgan-iso) |
| `ir2qsm` | Inversion from true local field | 0.720 | 41.2% | ~2.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ir2qsm-iso) |
| `lpcnn` | Inversion from true local field | 0.666 | 45.7% | ~5.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=lpcnn-iso) |
| `modl-qsm` | Inversion from true local field | 0.272 | 74.4% | ~8.2 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=modl-qsm-iso) |
| `nextqsm` | QSM from true total field | 0.452 | 70.3% | ~2.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=nextqsm-iso) |
| `iqsm` | QSM from phase | 0.271 | 76.6% | ~1.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-iso) |
| `iqsm-plus` | QSM from phase | 0.279 | 79.5% | ~1.9 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-plus-iso) |

#### Phase unwrapping and field mapping

| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | --- | ---: | ---: | ---: | --- |
| `romeo` | Field mapping from phase | 0.259 | 72.1% | ~18 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs-iso) |
| `laplacian` | Field mapping from phase | 0.249 | 78.8% | ~26 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=laplacian-qsmci-iso) |

#### Background removal

| GUI algorithm | Benchmark input/stage | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | --- | ---: | ---: | ---: | --- |
| `vsharp` | Background removal from true total field | 0.820 | 51.2% | ~14 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=vsharp-qsmrs-iso) |
| `pdf` | Background removal from true total field | 0.846 | 44.5% | ~3.5 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=pdf-qsmrs-iso) |
| `lbv` | Background removal from true total field | 0.862 | 46.5% | ~21 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=lbv-qsmrs-iso) |
| `ismv` | Background removal from true total field | 0.746 | 51.4% | ~23 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=ismv-qsmrs-iso) |
| `sharp` | Background removal from true total field | 0.682 | 58.1% | ~3 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=sharp-qsmrs-iso) |
| `resharp` | Background removal from true total field | 0.690 | 56.5% | ~24 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=resharp-qsmrs-iso) |
| `harperella` | Local field from phase | 0.254 | 105.3% | ~35 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=harperella-qsmrs-iso) |
| `iharperella` | Local field from phase | 0.536 | 75.9% | ~49 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iharperella-qsmrs-iso) |
| `bfrnet` | Background removal from true total field | 0.817 | 58.7% | ~6.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=bfrnet-iso) |
| `iqfm` | Local field from phase | 0.507 | 75.5% | ~2.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqfm-iso) |

#### Full pipeline presets

These measurements include the upstream reconstruction stages. They are
separate from the scanner measurements in the earlier preset table.

| Preset | xSIM ↑ | NRMSE ↓ | Runtime | Source |
| --- | ---: | ---: | ---: | --- |
| `romeo-resharp-rts` | 0.225 | 85.1% | ~43 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~rts-qsmrs-cmp) |
| `romeo-ismv-hdqsm` | 0.291 | 82.0% | ~1.3 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~hdqsm-qsmrs-cmp) |
| `romeo-resharp-tikhonov` | 0.219 | 85.4% | ~32 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~tikhonov-qsmrs-cmp) |
| `romeo-resharp-tv` | 0.229 | 83.7% | ~54 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~tv-qsmrs-cmp) |
| `romeo-resharp-hdqsm` | 0.272 | 82.8% | ~1.0 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~hdqsm-qsmrs-cmp) |
| `romeo-ismv-rts` | 0.250 | 83.6% | ~45 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~rts-qsmrs-cmp) |
| `romeo-ismv-whqsm` | 0.292 | 80.4% | ~3.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~ismv-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-sharp-whqsm` | 0.278 | 81.0% | ~2.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~sharp-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-resharp-whqsm` | 0.275 | 81.3% | ~3.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~resharp-qsmrs~whqsm-qsmrs-cmp) |
| `romeo-sharp-tikhonov` | 0.212 | 86.3% | ~15 s | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~sharp-qsmrs~tikhonov-qsmrs-cmp) |
| `qsmart` | 0.147 | 92.9% | ~11.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~qsmart-qsmrs-cmp) |
| `tgv` | 0.257 | 82.7% | ~2.6 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~tgv-qsmrs-cmp) |
| `autoqsm` | 0.218 | 88.8% | ~3.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~autoqsm-cmp) |
| `nextqsm` | 0.256 | 91.1% | ~2.1 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=romeo-qsmrs~nextqsm-cmp) |
| `iqsm` | 0.271 | 76.6% | ~1.4 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-cmp) |
| `iqsm-plus` | 0.279 | 79.5% | ~1.7 min | [run](https://qsmxt.github.io/QSM-CI/submission.html?run=iqsm-plus-cmp) |

ROMEO and Laplacian rows measure field mapping, including echo handling,
rather than unwrapping alone. HARPERELLA, iHARPERELLA, and iQFM benchmark
joint unwrapping/background removal. QSMART refers to QSM.rs, not MATLAB.

TFI is benchmarked from the true total field. QSMxT 10 also supplies the total
field to TFI. These tables are a historical benchmark snapshot; they do not
measure the packaged QSMxT 10 pipeline or its new defaults.

Regenerate these tables with `python3 recipes/qsmxt/update_benchmarks.py`.
Use `--refresh` to fetch new results or `--check` to check the committed
tables against `qsmci-in-silico-2019.json`. The snapshot records source hashes.

<!-- END QSM-CI benchmarks -->

## 3D EPI preprocessing

The EPI configurations and the `ep3d` app share `macros/ep3d/helpers.yaml`.
`ep3d_mrd.preprocess_images` pairs repetitions and runs `ep3d_preproc.py` before
QSMxT. niimath estimates rigid
motion from the 4D magnitude series with `-moco -ref 0 -estimate`. Its
`-resample` operation applies the same `-hmc` parameters to both Cartesian components on the
first repetition's grid, using cubic B-spline interpolation and zero outside the
field of view.

The driver removes smooth phase differences relative to the first repetition
with Gaussian sigma 2 voxels, then averages the corrected complex values across
all repetitions. The denoising mode applies ANTs NLM to the complex average.
Both modes write one magnitude and phase pair for single-echo QSM reconstruction.
The driver calls niimath directly and uses NumPy, SciPy, and NiBabel for the
remaining operations.

Temporary Cartesian volumes use uncompressed NIfTI files. Real and imaginary
preparation overlaps magnitude-based motion estimation when at least two CPUs
are available. Preparation reserves up to two workers and niimath receives the
remaining CPU budget. Both tasks finish before Cartesian resampling starts.
With one CPU, preparation and motion estimation run sequentially. Real and imaginary
resampling run concurrently, splitting the available thread budget between them.
The two Gaussian phase filters and the final magnitude/phase writes also run
concurrently. These scheduling and I/O changes preserve the niimath motion model,
interpolation, phase matching, and final output format.

The adapter shares a CPU budget between EPI preprocessing and native QSMxT,
bounded by CPU affinity, the cgroup v2 quota, and 24 workers. Each native call
contains one acquisition, including all its echoes. It uses `--no-mem-limit`
because QSMxT 10 otherwise uses its memory-based concurrent-acquisition limit as
the intra-acquisition Rayon worker count. This flag changes scheduling; it does
not reduce the memory required by one acquisition. Container or job memory limits
still apply.

## Native version 10 controls

The scanner label retains 14 controls. **Pipeline preset** defaults to Custom.
**Config** offers standard GRE, standard GRE + echo motion alignment, and the two
EPI modes. The GRE alignment module copies parsed parameters and forces
`motioncorrection: true`, including when the caller supplies false. It preserves
`motionreference` and unrelated advanced options without changing caller data.
Standard GRE still accepts advanced `motioncorrection`, which defaults to false.
`twopass` is a scanner boolean with default false. `qsmreference` is a string with default `mean`. `vsharpgrid` accepts `native`, `finest`, or `isotropic`, with default `native`. Advanced `swiunwrapping` accepts `laplacian`, `laplacian-slice`, or `romeo`, with default `laplacian`. Its parser still honors saved configurations. The default-off boolean `regionalqsmtable` replaces this GUI choice to retain the schema's maximum of 14 controls.

Advanced `motionreference` accepts `first`, `last`, or a positive 1-based echo index. Advanced `vsharpisotropicspacing` is a positive spacing in millimeters passed as `--vsharp-grid-spacing`. The native reliable-pass mask remains unfilled. Ordinary close/fill mask refinements do not apply to that mask. Main QSM output discovery excludes `desc-singlepass` alongside source-separation maps.

The adapter passes explicit ROMEO, V-SHARP, and RTS choices. Native version 10 uses Laplacian SWI unwrapping and the native V-SHARP grid by default. Slice-wise Laplacian is a SWI option, not a general QSM unwrapping option. Native echo motion correction registers magnitude and resamples complex magnitude/phase pairs. It intersects echo coverage before reconstruction.

### Anatomical reference regions

Every non-background SynthSeg v1 label is available by id or slug. Bilateral slugs select both sides. Prefix a bilateral slug with `left-` or `right-` to select one side.

| Bilateral or unsided slug | Left or unsided id | Right id |
| --- | --- | --- |
| `cerebral-white-matter` | 2 | 41 |
| `cerebral-cortex` | 3 | 42 |
| `lateral-ventricle` | 4 | 43 |
| `inferior-lateral-ventricle` | 5 | 44 |
| `cerebellum-white-matter` | 7 | 46 |
| `cerebellum-cortex` | 8 | 47 |
| `thalamus` | 10 | 49 |
| `caudate` | 11 | 50 |
| `putamen` | 12 | 51 |
| `pallidum` | 13 | 52 |
| `3rd-ventricle` | 14 | |
| `4th-ventricle` | 15 | |
| `brain-stem` | 16 | |
| `hippocampus` | 17 | 53 |
| `amygdala` | 18 | 54 |
| `accumbens-area` | 26 | 58 |
| `ventral-dc` | 28 | 60 |

`ventricles` is the union `4,5,14,15,43,44`. Other unions use comma-separated terms, such as `thalamus,caudate`, `left-thalamus,right-caudate`, or `4,43`. Duplicate terms are deduplicated by the native resolver. Background id 0 and unknown ids are invalid. `mean` and `none` cannot be mixed with region terms.

Region selection enables SynthSeg on combined GRE magnitude anatomy. The offset is the ROI mean within defined reconstruction support. QSMxT subtracts this offset over the whole support and keeps unsupported voxels at zero. Empty intersections fail. Fewer than 50 supported ROI voxels produce a warning. Native reference summaries record the offset and voxel count. `csf` is label 24 in SynthSeg v2, distinct from `ventricles`. Public v2 weights are unavailable. This recipe packages the pinned v1 model.

### Regional QSM report

`regionalqsmtable=true` adds native `--do-analysis`, which implies SynthSeg
segmentation on combined GRE magnitude anatomy in QSMxT 10.0.0. It requires QSM
and rejects advanced `noqsm=true` before reconstruction. Selecting a non-QSM
`sendoutputs` value still permits QSM reconstruction and analysis, while map
delivery follows the selected outputs.

`qsmxt_regional_report.build_pages(output_dir, *, input_variant,
requested_reference)` discovers exactly one per-run
`derivatives/qsmxt/sub-*/**/anat/*_desc-segmentation_stats.tsv`. It excludes the
dataset aggregate and rejects missing or ambiguous run files. The complete UTF-8
TSV text is logged with the input variant and requested reference before parsing
or renderer imports. The log retains native formatting and rows for other maps.

The report accepts only exact `map=Chimap`, `unit=ppm` rows with unique positive
labels, names, finite mean/SD/median, nonnegative SD, and positive valid counts
no larger than total counts. Recorded reference offsets must be finite. Native
reference and offset fields may be blank and appear as `not recorded`, separately
from the requested reference. All QSM rows must agree on native provenance.
Missing, malformed, or inconsistent QSM data fails the optional report rather
than displaying partial results. Other maps, including source separation maps,
are ignored. No atlas label list or statistics are computed by the adapter.
QSMxT excludes zero and nonfinite voxels and labels without valid values.

Each page repeats the input variant, requested reference, native reference, and
offset, and displays region labels, mean/SD/median in ppm, and valid/total voxel
counts. Long names and references wrap; all rows paginate into equal-size
1600 by 1800 grayscale pages. Rendering uses matplotlib's packaged DejaVu font
and Pillow, supplied by the existing OpenRecon macro.

The adapter validates and sends quantitative maps first. Inside the temporary
run directory, it prepares and separately validates one report MRD message
containing all pages, rotated 180 degrees for the scanner viewer. The report uses
one channel, uint16 `0..4095` pixels, explicit matrix and FOV
`(width, height, pages)`, zero origin, and identity directions. Metadata includes
`DataRole=Segmentation`, `Keep_image_geometry=0`, `partition_count=1`, and
`slice_count=NumberOfSlices=ImagesInAcquisition=pages`. Header slice is zero and
image index is one. Integer display window center/width are `2048`/`4096`.
Source minihead, parent references, scaling, and physical map attributes are
removed. Patient and study identity remain. Report pixels contain text with no
physical ppm rescaling.

Reports share series allocation with maps and originals. Each selected DC or ND
variant receives a distinct series, SOP, and grouping identity. Report preparation
errors are logged locally and to the scanner error channel, preserve sent maps,
and allow the next variant to run. A successful report is sent outside that
error handler, so connection failures follow normal process error handling.
The temporary output directory is removed at run completion. This feature adds
no persistent patient file lifecycle and does not change quantitative map
validation or the pinned native version.

### Acquired slice geometry

Image metadata or MRD `userParameterDouble` and `userParameterString` entries named `SliceThickness` and `MRAcquisitionType` supply independent acquired geometry. Thickness must be finite and positive, and metadata must agree across the acquisition. Acquisition type accepts `2D` or `3D`. The NIfTI stack axis is declared as `SliceEncodingDirection: k`.

Advanced `slicethicknessmm` and `acquisitiontype` override those metadata values explicitly. Unknown thickness stays absent and produces an actionable warning. The adapter never derives acquired thickness from affine pitch, volume field of view, or packed 3D partition spacing. A 2D stack with nonuniform scanner positions fails because one NIfTI affine cannot represent it. Actual stack spacing remains in the affine.

QSMxT compares thickness with slice pitch and refuses gapped QSM acquisitions by default. Advanced `ignoreslicegap: true` passes the explicit native override. The result then carries the native unreliability warning. Synthetic contiguous/gapped stack tests prove the integration contract, not scanner-specific 2D quality.

### Runtime verification scope

`verify_v10_runtime.py` runs actual native iLSQR, two-pass, displaced echo registration, anisotropic V-SHARP, slice-wise SWI, gap refusal, and ROI arithmetic on deterministic synthetic fixtures. The two-pass check verifies ordinary-pass fallback outside the reliable mask. It does not assess scientific accuracy of the second pass. Named ventricular, numeric-union, and sided reference arithmetic use a supplied dseg to isolate that operation.

A separate smoke test executes the packaged SynthSeg model on MNI magnitude anatomy with a 128-voxel crop and flip averaging disabled. It checks all 32 v1 labels, ventricular voxels, lookup names, and acquired geometry while Internet sockets are denied. This proves offline model compatibility. It does not establish full-volume segmentation quality or scanner validation.
