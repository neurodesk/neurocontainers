----------------------------------
## segmentator/{{ context.version }} ##

Segmentator is a free and open-source package for multi-dimensional data exploration and segmentation of 3D images, mainly developed and tested with ultra-high-field MRI brain data.

Example:
```
segmentator --help
segmentator_filters --help
segmentator /path/to/file.nii.gz
```

More documentation can be found here: https://github.com/ofgulban/segmentator/wiki

Citation:
```
Gulban, O. F., Schneider, M., Marquardt, I., Haast, R., & De Martino, F. (2023). A scalable method to improve gray matter segmentation at ultra high field MRI. In (Version v{{ context.version }}) Zenodo.
Gulban, O. F., Schneider, M., Marquardt, I., Haast, R. A. M., & De Martino, F. (2018). A scalable method to improve gray matter segmentation at ultra high field MRI. PLoS One, 13(6), e0198335. https://doi.org/10.1371/journal.pone.0198335
```

To run container outside of this environment: ml segmentator/{{ context.version }}

----------------------------------
