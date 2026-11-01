# BrainQCNet OpenRecon

This research container detects artifacts in reconstructed brain T1-weighted
magnitude images with BrainQCNet, a ProtoPNet with a ResNet152 backbone. The
adapter writes each volume as a one-subject BIDS dataset and runs the upstream
CPU BIDS app on it with the published checkpoint
`saved_models/resnet152/19112020/10push0.8167.pth`.

Run `openrecon-brainqcnet --help` for server options and select the
`brainqcnet` configuration. JSON settings may appear at the top level or inside
`parameters`:

| Parameter | Values | Default | Effect |
| --- | --- | --- | --- |
| `predmethod` | `percentage`, `mean`, `median` | `percentage` | How slice predictions combine into the scan probability. |
| `sendoriginal` | boolean | `true` | Return the originals in a new series labelled with the verdict. |

A scan fails when its artifact probability is above 0.5, as in upstream
`transform_log_to_csv.py`. For each volume the adapter sends:

- the originals in a new series, when `sendoriginal` is on. They keep the
  source `SeriesDescription`, as scanner postprocessing expects, and carry
  `SequenceDescriptionAdditional` `BrainQCNet_PASS` or `BrainQCNet_FAIL`, the
  probabilities in `ImageComments`, and `BrainQCNetVerdict` and
  `BrainQCNetProbability` metadata.
- `<series>-brainqcnet-<verdict>-<probability>-<method>-report`, one rendered
  page with the verdict and the probability overall and per axis. It uses the same explicit standalone output
  path as the OpenRecon i2i example's metrics table.

The volume is reoriented to the closest canonical (RAS) axes, as dcm2niix does
for 3D acquisitions, so a sagittal, coronal or axial acquisition gives the same
NIfTI and the x, y and z probabilities always refer to sagittal, coronal and
axial slices. Output series names include the verdict, probability and method,
and the shared helpers derive series and instance UIDs from them, so a rerun
with a different result never reuses UIDs. The adapter partitions header series and repeated or contrast volumes
before physical slice ordering. It accepts finite single-channel, single-plane
magnitude images with regular right-handed orthonormal geometry. It rejects raw
acquisitions, complex images, duplicate slice positions, gaps and inconsistent
geometry. Output series never reuse input series indices. Failures are logged
through the MRD server and the connection always closes.

BrainQCNet runs on CPU. A 1 mm whole-head T1w volume has about 500 slices,
which takes a few minutes on four cores. Physical scanner integration has not
been tested.

`python3 /opt/brainqcnet/verify_runtime.py adapter` checks MRD geometry,
grouping, outputs and error handling with a mocked BIDS app.
`python3 /opt/brainqcnet/verify_runtime.py model` runs the checkpoint on a small
synthetic volume through the `brainqcnet` CLI at participant and group level.
