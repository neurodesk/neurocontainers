# Run the FLUX first-look notebook

The maintained MNE implementation is in `/opt/flux/MNEPython`.
`SOURCE_COMMIT` records the exact upstream source revision. Copy the notebooks
and their images into a writable project directory before you edit them:

```sh
cp -r /opt/flux/MNEPython ./flux-notebooks
code ./flux-notebooks
```

In VS Code, choose `/opt/miniconda/envs/mne-extended/bin/python` as the notebook
kernel. To start Jupyter, run:

```sh
/usr/local/bin/mneextended-run /opt/miniconda/envs/mne-extended/bin/jupyter lab ./flux-notebooks
```

The launcher supplies a private writable configuration directory outside HOME
when `XDG_CONFIG_HOME` is unset. The VS Code launcher also passes this directory
to notebook kernels. Set `XDG_CONFIG_HOME` to a writable directory before launch
if you want to retain configuration between sessions. Use `mneextended-run` for
direct Jupyter or Python commands, including `apptainer exec` commands that
bypass the container entrypoint.
Jupyter also stores its data and runtime files in this directory unless you set
`JUPYTER_CONFIG_DIR`, `JUPYTER_DATA_DIR`, or `JUPYTER_RUNTIME_DIR` before launch.

Open `Afirstlook.ipynb` and set `bids_root` to your dataset directory in the
download cell. When you already have the data, comment out only the
`openneuro.download(...)` call and run the cell so that `bids_root` is defined.

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
/usr/local/bin/mneextended-run /opt/miniconda/envs/mne-extended/bin/python /opt/flux/flux-smoke-test.py --dataset ./ds004346
```

Without `--dataset`, the same notebook cells run on a synthetic BIDS MEG
recording. That check needs no network or reference recording.
Interactive Qt plots need a graphical desktop. The headless smoke test skips
GUI cells. MaxFilter stages need a separate licensed MaxFilter installation.
Surface reconstruction and source modelling stages need FreeSurfer and the
appropriate subject reconstructions. These external tools are not part of
this notebook addition.

[Upstream FLUX documentation](https://www.neuosc.com/flux)
