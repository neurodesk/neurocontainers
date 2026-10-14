# Brain volume report: population model

This document describes how the reference curves in `brainquant_norms.json`
were built, how the report evaluates a patient against them, how the model was
validated, and what it cannot do. The report is for research use only. It is not
for diagnostic use.

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
