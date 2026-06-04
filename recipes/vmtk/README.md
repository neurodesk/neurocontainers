----------------------------------
## vmtk/1.5.0 ##

The Vascular Modeling Toolkit is a collection of libraries and tools for 3D reconstruction, geometric analysis, mesh generation, and surface data analysis for image-based modeling of blood vessels.

VMTK can be used through its command line tools, as Python/C++ libraries, or as part of 3D Slicer workflows. This container installs VMTK 1.5.0 from conda-forge with VTK, ITK, and Python 3.10.

Homepage: http://www.vmtk.org/
Documentation: http://www.vmtk.org/documentation/

Example:
```sh
vmtkcenterlines -ifile foo.vtp -ofile foo_centerlines.vtp
vmtksurfacereader -ifile foo.vtp --pipe vmtkcenterlines --pipe vmtkrenderer --pipe vmtksurfaceviewer -opacity 0.25 --pipe vmtksurfaceviewer -i @vmtkcenterlines.o -array MaximumInscribedSphereRadius
```

To make VMTK commands available in Neurodesk environments, run:
```sh
ml vmtk/1.5.0
```

License: BSD-3-Clause

----------------------------------
