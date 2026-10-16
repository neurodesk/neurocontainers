# White matter lesions OpenRecon

This OpenRecon workflow segments white matter lesions on a reconstructed FLAIR
series during the scanner session. It runs FLAMeS exactly as the Neurodesk
White matter lesions web app does. The container holds the web app's standalone
`flames` command, unchanged. That command includes SynthStrip and all five
FLAMeS folds. It runs on the CPU and needs no GPU or network connection.

FLAMeS is research software. An automated lesion mask can miss lesions or
include artefacts. Review it before use.

## Input

The input is one reconstructed magnitude FLAIR series with at least two slices.
3D FLAIR and 2D multi-slice FLAIR both work. The FLAMeS pipeline resamples
anisotropic data internally. Slices must be evenly spaced, parallel and share
one matrix and field of view. A gap between slices is allowed. A series whose
slices do not lie on one regular grid is rejected with an error in the scanner
log. The workflow treats each received series as a separate FLAIR.

The adapter sorts slices by physical position and converts the center-based
MRD/LPS geometry to a NIfTI RAS volume. It then runs `flames` on that volume.
The outputs come back on the source slices, with the source geometry.

`flames` downsamples a FLAIR finer than 1 × 0.9 × 0.9 mm, such as a 0.4 mm 3D
acquisition, to that grid and segments it there. The adapter resamples the mask
(nearest neighbour) and the probability (linear) back onto the source slices. The
lesion count and volume are then recomputed from the returned mask.

## Outputs

For a source series named `t2_flair_tra`, the scanner receives:

- `t2_flair_tra_original`, a restamped copy of the FLAIR, when **Keep original
  images** is enabled.
- `t2_flair_tra_lesion_overlay`, the FLAIR with each lesion outlined in-plane at
  the brightest source value. Only the outline is drawn, so the FLAIR signal
  inside each lesion stays visible.
- `t2_flair_tra_lesions`, the lesion mask. Lesion voxels are 1 and all other
  voxels are 0. A voxel is a lesion when its FLAMeS probability is above 0.5.
  The series is windowed from 0 (black) to 1 (white), without the FLAIR's
  rescale values.
- `t2_flair_tra_lesion_probability`, the lesion probability scaled from 0 to
  1000, when **Send lesion probability** is enabled.
- `t2_flair_tra_lesion_metrics`, a DICOM image table with the lesion count,
  total volume in ml and one row per 26-connected lesion. Each row gives the
  voxel count, volume in ml and centroid in scanner RAS coordinates in mm.
  Large tables span multiple pages in the same series. An empty mask returns
  a page stating that no lesions were detected.

The metrics table measures the mask returned to the scanner, after any
resampling. Its pages use their own geometry, as the MuscleMap and OpenMSK
tables do. Each derived image's comment gives the lesion count and total
lesion volume in ml. The scanner log shows the same summary.

Each run also writes the web app's files below
`/tmp/share/whitematterlesions/series-<index>-<id>/flames`. The lesion table
(`flair_lesions.tsv`) has one row per 26-connected lesion: voxels, volume in ml
and centroid in scanner coordinates. This file describes the FLAMeS working
grid, so its measurements can differ from the DICOM table when the mask is
resampled onto the scanner grid.

## Parameters

- **Model.** FLAMeS fold 0 is the default. The five-fold ensemble averages all
  published folds and takes about five times as long. On held-out scans the
  ensemble raised Dice from 0.61 to 0.65 for multiple sclerosis lesions and from
  0.726 to 0.733 for vascular lesions.
- **Image is already skull-stripped.** By default SynthStrip finds the brain.
  Enable this only for brain-only input, so that nonzero voxels are used as the
  brain.
- **Keep original images.** Return the FLAIR before FLAMeS runs, so the scanner
  keeps it even when segmentation fails.
- **Send lesion probability.** Also return the probability map.

## Run time and resources

With fold 0, the web app's 240 × 240 × 81 example FLAIR took 86 s on 8 CPU
threads, with a peak memory of 4.8 GB. The workflow uses every available core.

## Accuracy

The web app's validation compares FLAMeS with other FLAIR-only methods on 25
held-out scans from the MICCAI 2017 WMH challenge and on the 22-patient
MSLesSeg test split. With one fold, the pipeline scores Dice 0.726 on vascular
lesions and 0.608 on multiple sclerosis lesions. With the ensemble it scores
0.733 and 0.647. These results come from research datasets. They are not a
clinical validation.

## Citation

Dereskewicz E, La Rosa F, dos Santos Silva J, et al. FLAMeS: a robust deep
learning model for automated multiple sclerosis lesion segmentation. medRxiv.
2025. https://doi.org/10.1101/2025.05.19.25327707

Hoopes A, Mora JS, Dalca AV, Fischl B, Hoffmann M. SynthStrip: skull-stripping
for any brain image. NeuroImage 260 (2022): 119474.
