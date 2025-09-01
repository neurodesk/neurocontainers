
----------------------------------
## segmentator/toolVersion ##
Segmentator is a free and open-source package for multi-dimensional data exploration and segmentation for 3D images. This application is mainly developed and tested using ultra-high field magnetic resonance imaging (MRI) brain data.

The goal is to provide a complementary tool to the already available brain tissue segmentation methods (to the best of our knowledge) in other software packages (FSL, CBS-Tools, ITK-SNAP, Freesurfer, SPM, Brainvoyager, etc.).

Example:
```
segmentator /path/to/file.nii.gz
```

More documentation can be found here: https://github.com/ofgulban/segmentator/wiki

To make the executables and scripts inside this container transparently available in the command line of environments where Neurocommand is installed: ml segmentator/toolVersion

Citation:
```
Kniss, J., Kindlmann, G., & Hansen, C. D. (2005). Multidimensional transfer functions for volume rendering. Visualization Handbook, 189–209. http://doi.org/10.1016/B978-012387582-2/50011-3
```

License: BSD-3-Clause https://github.com/ofgulban/segmentator?tab=BSD-3-Clause-1-ov-file

----------------------------------
