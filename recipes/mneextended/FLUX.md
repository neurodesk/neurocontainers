# Run the FLUX first-look notebook

The maintained MNE implementation is in `/opt/flux/MNEPython`.
`SOURCE_COMMIT` records the exact upstream source revision. Copy the notebooks
and their images into a writable project directory before you edit them:

```sh
cp -r /opt/flux/MNEPython ./flux-notebooks
code ./flux-notebooks
```

In VS Code, choose `/opt/miniconda/envs/mne-extended/bin/python` as the notebook
kernel. For Jupyter, run that environment's `jupyter lab ./flux-notebooks`.
Open `Afirstlook.ipynb` and replace `bids_root` with your dataset directory.
The notebook includes an automatic download of the whole dataset. Skip that
cell when you already have the data.

The reference dataset is [OpenNeuro ds004346, version 1.0.2](https://openneuro.org/datasets/ds004346/versions/1.0.2).
To retrieve the exact snapshot with DataLad on the host:

```sh
datalad install https://github.com/OpenNeuroDatasets/ds004346.git
git -C ds004346 checkout 1.0.2
datalad -C ds004346 get sub-01/ses-01/meg/sub-01_ses-01_task-SpAtt_run-01_meg.fif
```

The first recording is about 2 GB. The smoke test reads its first 10 seconds,
executes the notebook's actual import, BIDS-path, data-loading, and Welch PSD
cells, and checks the spectrum. It verifies the version 1.0.2 recording hash:

```sh
/opt/miniconda/envs/mne-extended/bin/python /opt/flux/flux-smoke-test.py --dataset ./ds004346
```

Without `--dataset`, the same notebook cells run on a synthetic BIDS MEG
recording. That check needs no network or reference recording.
Interactive Qt plots need a graphical desktop. The headless smoke test skips
GUI cells. MaxFilter stages need a separate licensed MaxFilter installation.
Surface reconstruction and source modelling stages need FreeSurfer and the
appropriate subject reconstructions. These external tools are not part of
this notebook addition.

[Upstream FLUX documentation](https://www.neuosc.com/flux)
