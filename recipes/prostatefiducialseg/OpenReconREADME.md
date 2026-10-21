# Prostate Fiducial Segmentation OpenRecon

`prostatefiducialseg` is an OpenRecon image-in/image-out package that detects
implanted gold fiducial markers in T1-weighted prostate images. It receives the
reconstructed magnitude images of a series and returns them with the outlines of
the detected markers drawn in.

## Recommended Sequence

The model was trained on 2D T1-weighted FLASH images of the prostate (2 mm slices,
0.69 x 0.69 mm in plane). Images with other voxel sizes are resampled to that grid.

## Processing

SeedSeg v2: N4 bias field correction, then an nnU-Net (residual encoder) 5-fold
ensemble run with ONNX Runtime, with nnU-Net's
sliding-window inference and optional test-time mirroring. Marker candidates are the
connected regions of marker probability above 0.1, ranked by mean probability; the
K most probable are outlined.

Device: before each run the free GPU memory is checked. With at least 13 GB free all selected
networks are kept on the GPU; with at least 3.5 GB they are loaded one at a time (same result,
about 2 s slower); otherwise, or without a usable GPU, the CPU is used, with only the first
selected network by default (`cpuonenetwork`). The device and the reason are logged and
written to `SeedSegMarkers`.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `prostatefiducialseg` | Selects the MRD server configuration. |
| Implanted markers | `markers` | int | 3 | Number of implanted markers (K). |
| Network 1 of 5 ... Network 5 of 5 | `network1` ... `network5` | boolean | true | Networks to use; their probability maps are averaged. All 5 is the configuration evaluated on independent patients; fewer networks are faster. |
| Test-time mirroring | `mirroring` | boolean | false | Averages over mirrored copies (8 times the compute). |
| One network without GPU | `cpuonenetwork` | boolean | true | Without a usable GPU, runs only the first selected network so that CPU processing stays short. |

## Outputs

- Images: the input images with the marker outlines at maximum intensity.
- `SequenceDescriptionAdditional`: `SeedSeg <n>of<K>`, with `_CHECK` appended when
  fewer than K candidates were found (review the series).
- `ImageComments`: the mean probability of each marker.
- `SeedSegMarkers` (base64 JSON): rank, mean probability, image index, row, column
  and volume of each marker, the number of candidates, the count flag, and the device,
  networks and mirroring setting used.

## Command Line

`predict3.py -i t1.nii -o out/ [--n-markers 3] [--folds 0 1 2 3 4] [--mirror]`
writes `marker_prob.nii.gz`, `top3_markers.nii.gz` and `detections.json`.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/prostatefiducialseg

The SeedSeg pipeline itself is `scripts/deploy/seedseg_onnx.py` in
https://github.com/astewartau/seedseg.

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
