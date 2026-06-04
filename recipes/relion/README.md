----------------------------------
## relion/4.0.1.sm75 ##
RELION (REgularised LIkelihood OptimisatioN) is a stand-alone program for Maximum A Posteriori refinement of 3D reconstructions and 2D class averages in cryo-electron microscopy.

This container builds RELION 4.0.1 from source for CUDA compute capability 75 and includes helper integrations for CTFFIND, MotionCor2, and Topaz.

Example:
```
relion
relion_refine --help
relion_motion_refine --help
```

Installed tools and paths:
- relion - 4.0.1.sm75 - /opt/relion-4.0.1.sm75
- ctffind - 4.1.14 - /opt/ctffind-4.1.14
- motioncor2 - 1.6.4 - /opt/motioncor2-1.6.4
- cudatoolkit - 11.8 - /usr/local/cuda-11.8

Environment variables configured in the container:
- RELION_CTFFIND_EXECUTABLE=/opt/ctffind-4.1.14/bin/ctffind
- RELION_MOTIONCOR2_EXECUTABLE=/opt/motioncor2-1.6.4/bin/MotionCor2_1.6.4_Cuda118_Mar312023
- RELION_TOPAZ_EXECUTABLE=/opt/relion-4.0.1.sm75/load_topaz.sh

More documentation:
- RELION documentation: https://relion.readthedocs.io/en/release-4.0/
- RELION source: https://github.com/3dem/relion/tree/4.0.1

To make the executables and scripts inside this container transparently available in Neurodesk environments, run:
```
ml relion/4.0.1.sm75
```

Citation:
```
Sjors H W Scheres. RELION: Implementation of a Bayesian approach to cryo-EM structure determination. Journal of Structural Biology, 180(3):519-530, December 2012. doi:10.1016/j.jsb.2012.09.006.
```

License: GPL-2.0

----------------------------------
