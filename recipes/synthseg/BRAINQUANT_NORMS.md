# Brain volume report: population model

This document describes how the reference curves were built, how the report
evaluates a patient against them, how the models were validated, and what they
cannot do. There are two references:

- `brainquant_norms.json`: the IXI cohort, scanned at 1.5 T and 3 T. Most of
  this document describes it.
- `brainquant_norms_7t.json`: the AHEAD cohort, scanned at 7 T. See
  [7 T reference](#7-t-reference).

The report is for research use only. It is not for diagnostic use.

## Reference cohort

The reference is the [IXI dataset](https://brain-development.org/ixi-dataset/)
of healthy adults, licensed CC BY-SA 3.0. All 581 IXI T1 images were segmented.
20 subjects were excluded because `IXI.xls` has no age and sex for them, or has
conflicting rows for the same ID. No subject was excluded for segmentation
quality. No subject's intracranial volume was more than 4 robust standard
deviations from the median.

| | Value |
| --- | --- |
| Subjects | 561 |
| Female / male | 311 / 250 |
| Age | 20.0 to 86.3 years, median 48.6 |
| Sites | Guys (Philips 1.5 T) 313, HH (Philips 3 T) 180, IOP (GE 1.5 T) 68 |
| Intracranial volume | 1128 to 1921 mL, median 1493 mL |

`brainquant_ixi_volumes.csv` holds each subject's site, sex, age, exclusion
reason and SynthSeg volumes. It is enough to refit the model without
re-running SynthSeg.

## Segmentation

The norms must come from the same measurement as the patient. Each IXI image
was segmented with FreeSurfer 8.2.0 `mri_synthseg` using the SynthSeg 2.0 model
in fast mode on CPU (`--fast --vol`), which is the scanner default.

Volumes come from the `--vol` CSV. SynthSeg computes them by summing the soft
posterior probabilities, not by counting labelled voxels. The report reads the
same CSV, so the patient's volume and the population's volume use the same
definition. The total intracranial volume (ICV) column is the sum of all
non-background posteriors.

Each image was first trimmed to its SynthStrip brain mask bounding box plus a
15 mm margin. The trim only removes voxels. It keeps the original intensities
and voxel grid. Two problems made the trim necessary:

- CPU SynthSeg on the full IXI field of view needs more than 9 GB of RAM and
  was killed on the machine used for the fit.
- SynthSeg's `--crop` box is centred on the field of view. IXI heads sit off
  centre, so a 192 mm crop placed tissue on the crop face in 18 of the first 100
  subjects, mostly at the occipital pole.

After trimming, every image was segmented whole, with no `--crop`. On the
subject with the most clipping, trimming and not cropping changed ICV by 0.08%
and cortical volume by under 0.3% compared with the 192 mm crop.

## Structures

| Report row | `--vol` columns summed |
| --- | --- |
| Left / right hippocampus | `left hippocampus` / `right hippocampus` |
| Left / right amygdala | `left amygdala` / `right amygdala` |
| Left / right thalamus | `left thalamus` / `right thalamus` |
| Lateral ventricles | left and right `lateral ventricle` and `inferior lateral ventricle` |
| Cortical grey matter | `left cerebral cortex`, `right cerebral cortex` |
| Cerebral white matter | `left cerebral white matter`, `right cerebral white matter` |
| Whole brain | both sides of cerebral white matter, cerebral cortex, cerebellum white matter, cerebellum cortex, thalamus, caudate, putamen, pallidum, hippocampus, amygdala, accumbens area and ventral DC, plus `brain-stem`. Ventricles and CSF are excluded. |

The column lists are stored in `brainquant_norms.json`. The report sums the
columns that the file names, so a structure can be added by refitting with a
new entry in `STRUCTURES` in `fit_brainquant_norms.py`.

## Model

The model is fitted separately for each structure and for three groups: female,
male, and all subjects pooled. The report uses the pooled model when the
patient's sex is unknown.

Let `a = (age - 50) / 10`, `V` be the structure volume, and `ICV` be the total
intracranial volume. For subject `i` at site `s`:

```text
ln V_i = b0 + b1 a + b2 a^2 + c ln(ICV_i / 1500 mL) + d_s + e_i
e_i ~ Normal(0, sigma(a)^2),   ln sigma(a) = g0 + g1 a
```

- The quadratic in age allows the rise and fall seen in the hippocampus and white
  matter.
- `c` is the ICV slope. Fitting it in log space handles the fact that brain
  structures do not scale proportionally with head size.
- `d_s` are site effects with sum-to-zero coding. This makes `b0` the unweighted
  mean over the three IXI scanners, so the shipped curve is not tied to one
  site. The site terms are not shipped.
- `sigma(a)` lets the spread change with age. Ventricles in particular vary
  much more in older adults.

All parameters are estimated together by maximum likelihood with BFGS and the
analytic gradient. The fit is accepted only when the largest gradient
component is below 0.01. BFGS often reports precision loss at the optimum, so
its status flag is not used.

`brainquant_norms.json` stores `mu = [b0, b1, b2]`, `icv_slope = c` and
`log_sigma = [g0, g1]` for each structure and group.

## Evaluating a patient

The report computes the ICV-adjusted log volume and its z score:

```text
y = ln V - c ln(ICV / 1500 mL)
z = (y - (b0 + b1 a + b2 a^2)) / sigma(a)
percentile = 100 * Phi(z)
```

The charts plot the adjusted volume `exp(y)` against age. The bands are
`exp(mu(a) + Phi^-1(p) sigma(a))` for p = 0.05, 0.25, 0.5, 0.75 and 0.95, all
at the reference ICV of 1500 mL. Values below the 5th or above the 95th
percentile are flagged.

Age is the number of whole years from the MRD header birth date to the study
date. If either date is missing, the age is unknown unless the `ssquantage` GUI
field sets it. With an unknown age the charts draw the patient value as a line
and the percentiles read n/a.

## Validation

**In-sample coverage.** On the fitted data, 88% to 92% of subjects fall inside
each structure's 5th to 95th percentile band. The nominal value is 90%. By sex
the range is 88% to 92%, and by age tertile it is 83% to 94%.

**Held-out coverage.** In five-fold cross-validation the models were refitted
without each fold, and the held-out subjects were scored against the site-free
curve. A new scanner sees that curve too.

| Structure | Held-out subjects inside the 5th to 95th band |
| --- | --- |
| Left hippocampus | 89.5% |
| Right hippocampus | 89.7% |
| Left amygdala | 87.5% |
| Right amygdala | 88.9% |
| Left thalamus | 89.5% |
| Right thalamus | 89.3% |
| Lateral ventricles | 89.1% |
| Cortical grey matter | 89.5% |
| Cerebral white matter | 88.8% |
| Whole brain | 90.0% |

**Site offsets.** Mean held-out z score by IXI site:

| Structure | Guys | HH | IOP |
| --- | --- | --- | --- |
| Left hippocampus | +0.06 | +0.06 | -0.19 |
| Right hippocampus | -0.02 | -0.09 | +0.08 |
| Left amygdala | +0.17 | +0.29 | -0.47 |
| Right amygdala | +0.18 | +0.13 | -0.32 |
| Left thalamus | +0.06 | -0.09 | +0.01 |
| Right thalamus | -0.03 | +0.29 | -0.24 |
| Lateral ventricles | +0.03 | -0.11 | +0.12 |
| Cortical grey matter | -0.20 | -0.19 | +0.35 |
| Cerebral white matter | -0.09 | -0.14 | +0.20 |
| Whole brain | -0.17 | -0.21 | +0.34 |

**Head-size adjustment.** With site effects included, the mean z score in each
ICV band (below the 10th percentile, 10th to 30th, 30th to 70th, 70th to 90th,
above the 90th) is within 0.23 of zero for every structure. Adding a quadratic
ICV term did not reduce these residuals, so the shipped model keeps the linear
term. Without site terms, held-out scoring shows a slight negative shift in the
largest-ICV decile, down to -0.35 for whole brain and -0.26 for left thalamus.

**End to end.** The built container ran real SynthSeg inference on IXI002
through `process_image` with an MRD header and returned the report. It read
age 35 and sex female from the header, which match the IXI record.

## Limitations

- **Age range.** The reference covers ages 20 to 86. Patients outside this range
  get a percentile, but each chart is marked "outside reference age range" and
  the curve is an extrapolation.
- **Scanner effects.** The curves average three scanners from the 2000s, at 1.5 T
  and 3 T. A different scanner, field strength or protocol can shift volumes,
  and the report cannot correct for it. Across the three IXI sites the mean
  held-out z score shifts by up to 0.47, largest for the amygdala and the
  tissue totals. Expect offsets of this size on a new scanner.
- **Population.** IXI is a London cohort of healthy volunteers. Its ethnic and
  socioeconomic mix may not match the patients being scanned.
- **Measurement.** The model applies to SynthSeg 2.0 in fast mode. Other models,
  or fast mode off, add a footnote because volumes may shift. SynthSeg 1.0 skips
  the report because its CSV has no ICV column.
- **Distribution.** The model assumes log-normal residuals. Ventricle volume is
  strongly skewed, and its upper tail is the least certain part of the model.
- **Single time point.** Percentiles describe one scan against a
  cross-sectional cohort. They are not a measure of atrophy rate.

## 7 T reference

Scored against the IXI curves, healthy 7 T adults look abnormal. The table
shows the AHEAD subjects' mean z score against IXI, and the share of them that
fall inside the IXI 5th to 95th band, against the nominal 90%:

| Structure | Mean z against IXI | Inside the IXI 5th to 95th band |
| --- | --- | --- |
| Left / right hippocampus | +0.78 / +0.76 | 79% / 81% |
| Left / right amygdala | -0.09 / +0.34 | 86% / 81% |
| Left / right thalamus | +0.38 / -0.12 | 92% / 90% |
| Lateral ventricles | +0.25 | 86% |
| Cortical grey matter | +3.49 | 12% |
| Cerebral white matter | +0.40 | 73% |
| Whole brain | +1.49 | 54% |

SynthSeg measures the cortex about 3.5 standard deviations larger on 7 T
MP2RAGE images than on the IXI images. Against IXI, almost every healthy 7 T
scan would be flagged above the 95th percentile for cortical grey matter. Scans
from scanners at 5 T and above are therefore compared with a separate 7 T
reference.

### Choosing the reference

The OpenRecon wrapper reads `systemFieldStrength_T` from the MRD header. At 5 T
and above it uses `brainquant_norms_7t.json`, and below it uses
`brainquant_norms.json`. If the header has no field strength, it uses IXI. The
report footnote and the reconstruction log state which reference was used and
why. The OpenRecon GUI has no reference selector, because the scanner UI allows
at most 14 parameters and all 14 are taken.

### Cohort

The reference is the
[AHEAD database](https://doi.org/10.21942/uva.10007840) (Alkemade et al.
2020), licensed CC BY 4.0. It has 105 healthy adults, 60 female and 45 male,
scanned on one Philips 7 T system with a multi-echo MP2RAGE (MP2RAGEME) sequence
at 0.64 x 0.64 x 0.7 mm. All 105 subjects are used.

AHEAD publishes age only in bins, so each subject takes the bin midpoint:

| Age bin | 18-30 | 31-40 | 41-50 | 51-60 | 61-70 | 71-80 |
| --- | --- | --- | --- | --- | --- | --- |
| Female / male | 27 / 15 | 6 / 6 | 6 / 7 | 7 / 5 | 7 / 6 | 7 / 6 |
| Age used | 24.5 | 36 | 46 | 56 | 66 | 76 |

The reference age range is the full span of the bins, 18 to 80.

### Skull-stripping correction

No open 7 T dataset with full-head images covers the adult lifespan. Every AHEAD
image, including the source images, is skull-stripped. The 7 T T1-weighted image
`*_mod-t1w_orient-std_brain.nii.gz` was segmented as published, with the same
SynthSeg 2.0 fast settings and the `--vol` CSV.

Skull-stripping changes SynthSeg's volumes, most of all ICV and the CSF next to
the skull. To measure the effect, 20 IXI subjects were segmented twice: whole
head, and with their SynthStrip brain mask applied. For each `--vol` column,
the mean log ratio of whole-head to stripped volume was taken from these pairs.
The AHEAD volumes are multiplied by the matching factor before the fit. The
factors are in `brainquant_strip_correction.json`.

| Column | Whole-head / stripped | Paired SD |
| --- | --- | --- |
| Total intracranial | 1.012 | 0.007 |
| Left / right cerebral cortex | 0.988 / 0.996 | 0.006 / 0.005 |
| Left / right cerebellum cortex | 0.977 / 0.972 | 0.011 / 0.011 |
| Left / right hippocampus | 1.003 / 1.011 | 0.007 / 0.009 |
| Left / right thalamus | 1.015 / 1.016 | 0.010 / 0.016 |
| CSF | 1.075 | 0.035 |

Most other structures change by under 1%. The small, noisier pallidum and
accumbens shift by up to 3.1%. The correction assumes that AHEAD's
mask behaves like SynthStrip's. AHEAD was skull-stripped with its own pipeline,
and any difference between the two masks stays in the reference as a bias.

### Model

With 105 subjects and binned ages, fitting each sex separately leaves too few
subjects per curve. Instead, one model is fitted to all subjects with a centred
sex term:

```text
ln V_i = b0 + b1 a + b2 a^2 + c ln(ICV_i / 1500 mL) + d (female_i - 1/2) + e_i
e_i ~ Normal(0, sigma(a)^2),   ln sigma(a) = g0 + g1 a
```

The female and male models in `brainquant_norms_7t.json` use intercepts
`b0 + d/2` and `b0 - d/2`. The pooled model for unknown sex uses `b0`. All three
share the age terms, the ICV slope and the spread. The file has the same schema
as the IXI file, so the report and the web app read either one.

### Validation

Four variants were compared by 10 repeats of five-fold cross-validation on
AHEAD, scoring held-out subjects against the 5th to 95th band:

| Variant | Mean held-out coverage |
| --- | --- |
| Separate per-sex fits, age-dependent spread | 84.6% |
| Separate per-sex fits, constant spread | 85.6% |
| One fit with a sex term, age-dependent spread (shipped) | 87.0% |
| One fit with a sex term, constant spread | 87.1% |

The two pooled variants tie. The shipped one keeps an age-dependent spread,
which matches the IXI model and covers the lateral ventricles better, 88.5%
against 87.5%. Held-out coverage per structure for the shipped model:

| Structure | Held-out coverage |
| --- | --- |
| Left / right hippocampus | 84.6% / 88.2% |
| Left / right amygdala | 87.2% / 87.3% |
| Left / right thalamus | 88.0% / 85.9% |
| Lateral ventricles | 88.5% |
| Cortical grey matter | 86.7% |
| Cerebral white matter | 86.8% |
| Whole brain | 86.5% |

The bands are slightly narrow. About 87% of held-out healthy subjects fall
inside the nominal 90% band, so about 13% of healthy 7 T scans fall outside it
instead of 10%. The binned ages and the small sample both add spread that the
model does not fully capture.

### 7 T limitations

- **Single scanner and site.** The curves come from one Philips 7 T system in
  Amsterdam. A Siemens MP2RAGE UNI or UNI-DEN image, or other inversion times,
  can shift volumes by an unknown amount.
- **Skull-stripping.** The correction comes from SynthStrip masks on IXI. It is
  approximate for AHEAD's own masks.
- **Binned ages.** Ages within a bin are treated as the midpoint. The 18 to 30 bin
  holds 42 of the 105 subjects, so the curves are best determined for young
  adults and least determined between 31 and 80, with about 12 subjects per
  decade.
- **Sex differences.** One sex offset is shared across all ages.

## Reproducing the norms

`fit_brainquant_norms.py` runs every step. Its docstring lists the commands:

1. Run `mri_synthstrip` on each IXI T1 image to get a brain mask.
2. Run `fit_brainquant_norms.py trim` to cut each image to the mask box plus 15 mm.
3. Run `mri_synthseg --fast --cpu --vol` on each trimmed image.
4. Concatenate the per-subject CSVs under one header.
5. Run `fit_brainquant_norms.py assemble` to join the volumes with `IXI.xls`
   and record exclusions. This writes `brainquant_ixi_volumes.csv`.
6. Run `fit_brainquant_norms.py fit` to write `brainquant_norms.json`. It also
   prints the coverage of each model by age tertile.

Step 6 alone reproduces the shipped JSON exactly from the committed volume
table.

For the 7 T reference:

1. Download `part1.tar.gz`, `part2.tar.gz` and `participants.csv` from the AHEAD
   figshare record.
2. Run `mri_synthseg --fast --cpu --vol` on each
   `*_mod-t1w_orient-std_brain.nii.gz`. Concatenate the CSVs under one header.
3. Run `fit_brainquant_norms.py assemble-ahead` to join the volumes with
   `participants.csv`. This writes `brainquant_ahead_volumes.csv`, with the raw
   skull-stripped volumes.
4. Run `fit_brainquant_norms.py strip-correction` on the paired whole-head and
   stripped IXI CSVs. This writes `brainquant_strip_correction.json`.
5. Run `fit_brainquant_norms.py fit brainquant_ahead_volumes.csv
   brainquant_norms_7t.json --cohort ahead --strip-correction
   brainquant_strip_correction.json`.

Step 5 alone reproduces the shipped 7 T JSON from the committed table and
correction.
