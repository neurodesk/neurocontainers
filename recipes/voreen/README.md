
----------------------------------
## Voreen/5.3.0 ##

Voreen is an open source rapid application development framework for the interactive visualization and analysis of multi-modal volumetric data sets. It provides GPU-based volume rendering and data analysis techniques and offers high flexibility when developing new analysis workflows in collaboration with domain experts. The Voreen framework consists of a multi-platform C++ library, which can be easily integrated into existing applications, and a Qt-based stand-alone application. It is licensed under the terms of the GNU General Public License.

Example:
```
./voreentool -platform minimal -w someworkspace.vws --script somescript.py
```

More documentation can be found here: [Voreen Documentation](https://www.uni-muenster.de/Voreen/documentation/index.html)

To make the executables and scripts inside this container transparently available in the command line of environments where Neurocommand is installed: ml voreen/5.3.0

Citation: "Images produced using Voreen (voreen.uni-muenster.de)."
```
Jennis Meyer-Spradow, Timo Ropinski, Jörg Mensmann, Klaus H. Hinrichs:
Voreen: A Rapid-Prototyping Environment for Ray-Casting-Based Volume Visualizations. IEEE Computer Graphics and Applications 29(6): 6-13 (2009)
```

License: GNU General Public License

----------------------------------
