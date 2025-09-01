
----------------------------------
## relion/toolVersion ##
RELION (for REgularised LIkelihood OptimisatioN) is a stand-alone computer program for Maximum A Posteriori (MAP) refinement of (multiple) 3D reconstructions or 2D class averages in cryo electron microscopy (cryo-EM).

Example:
```
relion
```

More documentation can be found here: https://github.com/3dem/relion/tree/toolVersion

To make the executables and scripts inside this container transparently available in the command line of environments where Neurocommand is installed (without the need to use 'Apptainer exec'): ml relion/toolVersion

The packages included in this container, their version, and the base folder of the installation within the container:

relion - toolVersion - /opt/relion-toolVersion

ctffind - 4.1.14 - /opt/ctffind-4.1.14

motioncor2 - 1.6.4 - /opt/motioncor2-1.6.4

cudatoolkit - 11.8 - /usr/local/cuda-11.8

Citation:
```
Sjors H W Scheres. RELION: Implementation of a Bayesian approach to cryo-EM structure determination. Journal of Structural Biology, 180(3):519–530, December 2012. doi:10.1016/j.jsb.2012.09.006.
```

License: GPLv2 license

----------------------------------
