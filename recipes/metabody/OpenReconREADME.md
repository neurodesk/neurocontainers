# Metabody OpenRecon

`metabody` is an OpenRecon image-in/image-out package for body-localizer fMRI
processing. It receives reconstructed image messages, stacks slices and
repetitions into a 4D NIfTI image, runs the bundled AFNI processing workflow,
and returns statistical maps. It can also return copies of the input images as
a separate series.

## Recommended Sequence

Use a body-localizer fMRI sequence with consistent slice and repetition counters
in the incoming MRD image stream. The bundled AFNI workflow uses fixed stimulus
timing files from the BodyLocaliser repository for left arm, right arm, left
foot, right foot, left hand, right hand, and tongue contrasts.

## UI Parameters

| GUI label | Parameter id | Type | Default | Description |
| --- | --- | --- | --- | --- |
| Configuration | `config` | choice | `metabody` | Run the Metabody MRD server configuration. |
| Send original images | `sendOriginal` | boolean | `true` | Return copies of the input images as series 99 before the statistical maps. |
| Output appearance | `colormap` | choice | `scanner_lut` | Use the scanner hot-metal palette, embed a selected color map as RGB, or return grayscale output. |

The scanner hot-metal option returns one scalar channel and asks the scanner to
apply `MicroDeltaHotMetal.pal`. This is the recommended and most compatible
color output. The grayscale option also returns one scalar channel but does not
request a palette. Named Matplotlib color maps are embedded as three-channel RGB
data and remain experimental until scanner round-trip behavior is confirmed.

## Runtime Notes

- AFNI model settings are fixed in the bundled `afni_processing.sh` workflow.
- Returned statistical maps carry `ImageComments` labels from the AFNI output.
- Palette and embedded RGB modes are mutually exclusive. Embedded RGB images do
  not carry scanner palette or windowing metadata.
- The processor uses the incoming slice and repetition counters to stack images,
  so their arrival order does not affect the 4D NIfTI layout.
- Runtime work is written under temporary directories such as `/tmp/afni`, not
  under `/home`.

## Open Source Development

The source for this OpenRecon package is in the NeuroContainers repository:
https://github.com/NeuroDesk/neurocontainers/tree/main/recipes/metabody

For bugs and feature requests, opening an issue in the NeuroContainers
repository is preferred: https://github.com/NeuroDesk/neurocontainers/issues.
Questions can also be posted in the Neurodesk discussion forum at
https://github.com/orgs/neurodesk/discussions or sent via
https://neurodesk.org/contact/.
