----------------------------------
## samri/0.5 ##

SAMRI (Small Animal Magnetic Resonance Imaging) provides fMRI
preprocessing, Bruker ParaVision metadata parsing, BIDS conversion, and
analysis workflows for small rodent MRI data.

This container includes SAMRI, FSL, ANTs, Bru2Nii, Blender, and the
Python packages listed by upstream SAMRI for the 0.5
workflow generation. Example datasets and atlas releases are not bundled.

Example:
```
SAMRI --help
SAMRI bru2bids -o . -f '{"acquisition":["EPI"]}' -s '{"acquisition":["TurboRARE"]}' samri_bindata
SAMRI diagnose bids
SAMRI generic-prep -m /usr/share/mouse-brain-atlases/dsurqec_200micron_mask.nii \
    -f '{"acquisition":["EPIlowcov"]}' \
    -s '{"acquisition":["TurboRARElowcov"]}' \
    bids /usr/share/mouse-brain-atlases/dsurqec_200micron.nii
```

More documentation can be found here: https://github.com/IBT-FMI/SAMRI

To run container outside of this environment: ml samri/0.5

License: GPL-3.0-or-later
----------------------------------
