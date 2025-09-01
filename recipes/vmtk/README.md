
----------------------------------
## vmtk/toolVersion##

The Vascular Modeling Toolkit is a collection of libraries and tools for 3D reconstruction, geometric analysis, mesh generation and surface data analysis for image-based modeling of blood vessels. VMTK can be used via its standalone interface, included as a Python or C++ library, or as an extension to the medical image processing platform 3D Slicer. By providing multiple user interfaces with various requirements of technical ability, VMTK aims to be usable by anyone with an interest in medical image processing; be they clinicians, researchers, industries, or educational institutions.

**Homepage:** http://www.vmtk.org/

**Example:**

```sh
# Computing centerlines
vmtkcenterlines -ifile foo.vtp -ofile foo_centerlines.vtp
# Look the resulting centerlines
vmtksurfacereader -ifile foo.vtp --pipe vmtkcenterlines --pipe vmtkrenderer --pipe vmtksurfaceviewer -opacity 0.25 --pipe vmtksurfaceviewer -i @vmtkcenterlines.o -array MaximumInscribedSphereRadius
# Inspect the voronoi diagram
vmtksurfacereader -ifile foo.vtp --pipe vmtkcenterlines --pipe vmtkrenderer --pipe vmtksurfaceviewer -opacity 0.25 --pipe vmtksurfaceviewer -i @vmtkcenterlines.voronoidiagram -array MaximumInscribedSphereRadius --pipe vmtksurfaceviewer -i @vmtkcenterlines.o
```

License: BSD license

----------------------------------
