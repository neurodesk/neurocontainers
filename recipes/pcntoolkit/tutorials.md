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
```

`offline_blr.py` generates deterministic synthetic data, fits Bayesian linear
regression, and checks finite predictions and deviation scores. It needs no
network access. The generated data demonstrate the API, not scientific validity.

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
| Gaussian process regression | No matching notebook in this release | Missing. BLR and HBR are different regression models. |
| Hierarchical Bayesian regression | `03_HBR_Normal.ipynb`, `04_HBR_SHASH.ipynb`, `05_HBR_Beta.ipynb` | HBR with normal, SHASH, and beta likelihoods. |
| Braincharts transfer | `06_transfer_extend.ipynb` | Demonstrates transfer and extension. Applying a specific pretrained Braincharts model still requires that model and compatible data. |
| Bayesian linear regression | `02_BLR.ipynb`, `offline_blr.py` | BLR tutorial plus an offline fit/predict example. |
| Visualization of normative model outputs | `00_getting_started.ipynb`, `02_BLR.ipynb`, `03_HBR_Normal.ipynb` | Centile, QQ, and ridge plots within the modelling tutorials. |
| Post-hoc analysis of normative model outputs | `07_model_comparison.ipynb`, `13_evaluation_metrics.ipynb` are related references | Missing dedicated post-hoc workflow. Model comparison and metrics do not replace it. |
| Predictive modelling using deviation scores | `offline_blr.py` computes deviation scores | Missing downstream predictive modelling workflow using those scores. |

The remaining release examples cover data loading, cluster execution, the CLI,
model merging, composite basis functions, and federated learning. Runtime smoke
tests check notebook JSON and PCNtoolkit imports, and run the offline BLR script.
They do not execute every scientific notebook. Keep issue
[#1177](https://github.com/neurodesk/neurocontainers/issues/1177) open until the
missing workflows are supplied or maintainers explicitly narrow its scope.
