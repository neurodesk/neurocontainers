# mobstr3D Single-Slice DENSE OpenRecon

`mobstr3d` is an OpenRecon image-in/image-out package that runs the segmentation and
phase unwrapping steps of [mobstr3D](https://github.com/UOA-Heart-Mechanics-Research/mobstr3D)
on cine DENSE images with 3D displacement encoding. Each slice is processed on its own
as soon as all of its frames have arrived, and the unwrapped displacements are returned
while the remaining slices are still being acquired.

## Recommended Sequence

Cine balanced 4-point DENSE with x, y and z displacement encoding, reconstructed into
an averaged magnitude series and x, y and z encoded phase series per slice (for example
`*_AveMag`, `*_x-encPha`, `*_y-encPha`, `*_z-encPha`). The segmentation model was
trained on short-axis mid-ventricular slices.

The DENSE reconstruction must write its ImageComments, which identify the images and
carry the encoding, for example
`DENSE x-enc pha - Scale:2.309401 EncFreq:0.06 Rep:0/1 Slc:0/1 Par:0/1 Phs:3/29 RCswap:1 RCSflip:0/0/0`.
Phase images are expected as stored values 0..4095 spanning -pi..pi.

## Processing

Per slice (mobstr3D's DICOM preprocessing without the steps that need the whole stack):

1. nnU-Net modelESv2 (2D, fold 0, test-time mirroring) segments every magnitude frame
   from the seed frame onwards: background, myocardium, blood pool.
2. Endocardial and epicardial contours from the largest myocardium and blood pool regions.
3. The seed is the highest combined x/y/z phase quality pixel in the eroded myocardium of
   the seed frame.
4. Spatiotemporal quality-guided flood-fill unwrapping of each encoding direction across
   frames, followed by a 2D flood fill of every frame.
5. Displacement (mm) = unwrapped phase / (2 pi EncFreq Scale), with the RCswap/RCSflip
   axis mapping and the slice-location sign used by mobstr3D.

Slice correction, the pseudo-cardiac coordinate transform, model fitting and strain
analysis combine slices and are not run here.

A slice is processed when every encoding has every frame announced by `Phs:k/N`; a slice
still incomplete when the connection ends is processed with the frames received.
Everything runs on the CPU; a 70 x 70, 24-frame slice takes a few seconds.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| config | `config` | choice | `mobstr3d` | Selects the MRD server configuration. |
| Seed frame | `frameofseed` | int | 5 | First frame processed and the frame the seed is selected on (zero-based). |
| End frame | `endframe` | int | -1 | Last frame processed; -1 for the last acquired frame. |
| Phase quality | `phasequality` | choice | `cropped` | `cropped` (DENSEanalysis) or `masked` quality map ordering the unwrap. |
| Contour method | `contourmethod` | choice | `marching_squares` | `marching_squares` or `cv2_find_contours`. |
| Send original images | `sendoriginal` | boolean | true | Also return the received magnitude and phase images. |

## Outputs

Per slice, each a cine series in the geometry of the magnitude frames processed:

- `*_original`: the received magnitude and x/y/z phase series (with `sendoriginal`).
- `*_mobstr3d-labels`: nnU-Net labels, 0 background, 1 myocardium, 2 blood pool.
  `Mobstr3DContours` (base64 JSON) holds the frame's `endo` and `epi` contours as pixel
  x (column) / y (row) coordinates, the format of mobstr3D's `contours_ics.json`.
- `*_mobstr3d-dx-mm`, `*_mobstr3d-dy-mm`, `*_mobstr3d-dz-mm`: displacement in mm in
  mobstr3D's image coordinate system, stored as `mm * 100 + 2048` (RescaleSlope 0.01,
  RescaleIntercept -20.48, range +/-20.47 mm) and 0 where the myocardium was not
  unwrapped. `Mobstr3DSlice` (base64 JSON) holds the slice location, the DENSE encoding,
  the frames processed and the seed or anchor points of that frame and direction.

## Command Line

`mobstr3d-slice --dicom-dir dicoms --output-dir out` processes every DENSE slice in a
DICOM folder independently, writing `segmentation.nii.gz`, `displacements_ics.json`,
`contours_ics.json`, `seed_points.json` and `imaging_parameters.json` to `out/Slice_n`.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/mobstr3d

mobstr3D: https://github.com/UOA-Heart-Mechanics-Research/mobstr3D. Please cite
G. E. Harpur et al., JCMR 28:102336, 2026, https://doi.org/10.1016/j.jocmr.2025.102336.
