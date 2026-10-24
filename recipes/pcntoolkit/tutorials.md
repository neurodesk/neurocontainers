# PCNtoolkit tutorials

These notebooks come unchanged from the installed PCNtoolkit release, by the
[Predictive Clinical Neuroscience team](https://github.com/predictive-clinical-neuroscience/PCNtoolkit).
The upstream GPL-3.0-only license is included as `LICENSE`.
The container recipe at `/build.yaml` records the source archive URL and checksum.
For the initial 1.3.0 bundle, the source commit is
`5b6073a1e70d2d455f615f39077a609a117eab47`.
The container checks that the archive's package version matches the installed
PCNtoolkit version. The update policy selects a release tag and updates the archive URL, checksum,
and installed package version together. Review this index when release examples
change. The build also checks the notebooks' PCNtoolkit imports.

Copy the tutorials to a writable project directory before editing them.

```bash
mkdir -p ./pcntoolkit-tutorials
cp -r /opt/pcntoolkit/tutorials/. ./pcntoolkit-tutorials/
cd ./pcntoolkit-tutorials
python offline_blr.py ./example-output
python offline_deviations.py ./deviation-output
```

`offline_blr.py` generates deterministic synthetic data, fits Bayesian linear
regression, and checks finite predictions and deviation scores. It needs no
network access. The generated data demonstrate the API, not scientific validity.

`offline_deviations.py` fits BLR models to an independent reference of 240
synthetic controls and computes deviation scores for 240 new synthetic subjects.
Two of three response variables have planted case effects. It demonstrates:

- Post-hoc Welch group comparisons, Benjamini-Hochberg adjustment across the
  three regions, and the fraction of subjects with absolute deviation above 2.
  Results are saved in `posthoc.csv`.
- Logistic-regression prediction using all three deviation scores. It reserves
  a stratified 25% test set before classifier fitting and scaling. Five-fold
  cross-validation fits a separate scaling/classifier pipeline within each
  training fold. It reports cross-validation and held-out ROC AUC in
  `prediction.json`, and saves test indices, labels and probabilities in
  `prediction.npz`.
- A deviation histogram and held-out ROC curve in `deviations.png`. The input
  scores, ages, responses and labels are saved in `deviations.npz`.

The normative reference has no target subjects or target labels. The exploratory
group tests use the entire target sample; their results never select features or
tune the classifier. This separation avoids leaking held-out target information
into prediction. For real data, define the split by subject, family and site as
appropriate, fit the normative model and every learned preprocessing step
within the permitted training/reference population, and choose any features or
hyperparameters inside training folds. The deliberately strong synthetic signal
checks the implementation; its AUC does not estimate clinical performance.

Open notebooks with a Jupyter frontend using this container's Python kernel.
A notebook frontend is not included by this recipe. Most upstream notebooks
fetch the FCON1000 dataset and need network access on their first run. HBR
notebooks also run Bayesian sampling and take longer than the offline example.
`13_evaluation_metrics.ipynb` is a reference notebook that assumes an existing
fitted model and test data.

## Requested workflows

The original request used the incompatible PCNtoolkit 0.x API. The following
mapping describes the available 1.3.0 material. A related notebook is not evidence
that an old workflow has a scientific equivalent.

| Requested topic | Bundled 1.3.0 material | Coverage |
| --- | --- | --- |
| Gaussian process regression | Removed by upstream in PCNtoolkit 1.0.0 | Unsupported by the installed 1.3.0 API. A separately pinned legacy environment or an explicit scope decision is required. BLR and HBR are different regression models. |
| Hierarchical Bayesian regression | `03_HBR_Normal.ipynb`, `04_HBR_SHASH.ipynb`, `05_HBR_Beta.ipynb` | HBR with normal, SHASH, and beta likelihoods. |
| Braincharts transfer | `06_transfer_extend.ipynb` | Demonstrates transfer and extension. Applying a specific pretrained Braincharts model still requires that model and compatible data. |
| Bayesian linear regression | `02_BLR.ipynb`, `offline_blr.py` | BLR tutorial plus an offline fit/predict example. |
| Visualization of normative model outputs | `00_getting_started.ipynb`, `02_BLR.ipynb`, `03_HBR_Normal.ipynb` | Centile, QQ, and ridge plots within the modelling tutorials. |
| Post-hoc analysis of normative model outputs | `offline_deviations.py`; `07_model_comparison.ipynb`, `13_evaluation_metrics.ipynb` are related references | Offline group comparisons, multiplicity adjustment and extreme-deviation summaries. |
| Predictive modelling using deviation scores | `offline_deviations.py` | Offline classifier with fold-local scaling, cross-validation and held-out evaluation. |

The remaining release examples cover data loading, cluster execution, the CLI,
model merging, composite basis functions, and federated learning. Runtime smoke
tests check notebook JSON and PCNtoolkit imports, and run both offline scripts.
They do not execute every scientific notebook. Keep issue
[#1177](https://github.com/neurodesk/neurocontainers/issues/1177) open until the
missing workflows are supplied or maintainers explicitly narrow its scope.

The [release changelog](https://github.com/predictive-clinical-neuroscience/PCNtoolkit/blob/5b6073a1e70d2d455f615f39077a609a117eab47/CHANGES#L91-L104)
records Gaussian-process removal and the incompatibility of legacy models.
