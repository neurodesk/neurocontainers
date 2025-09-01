#!/usr/bin/env bash
set -e

export toolName='vmtk'
export toolVersion='1.5.0'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Use Singlrity/Apptainer to run TinyRange to generate the conda environment.
singularity run -B /storage:/storage docker://ghcr.io/tinyrange/tinyrange:stable login -c vmtk.yml --storage 8192 --ram 2048

neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu:24.04 \
    --pkg-manager apt \
    --add vmtk.tar.gz . \
    --install libgl1 libglu1-mesa \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --env DEPLOY_BINS=vmtk:vmtkimagecompare:vmtkmeshpolyballevaluation:vmtksurfacecapper:vmtkactivetubes:vmtkimagecompose:vmtkmeshprojection:vmtksurfacecelldatatopointdata:vmtkbifurcationprofiles:vmtkimagecurvedmpr:vmtkmeshreader:vmtksurfacecenterlineprojection:vmtkbifurcationreferencesystems:vmtkimagefeaturecorrection:vmtkmeshscaling:vmtksurfacecliploop:vmtkbifurcationsections:vmtkimagefeatures:vmtkmeshtetrahedralize:vmtksurfaceclipper:vmtkbifurcationvectors:vmtkimageinitialization:vmtkmeshtetrahedralize2:vmtksurfacecompare:vmtkboundarylayer:vmtkimagemipviewer:vmtkmeshtonumpy:vmtksurfaceconnectivity:vmtkboundarylayer2:vmtkimagemorphology:vmtkmeshtosurface:vmtksurfaceconnectivityselector:vmtkboundaryreferencesystems:vmtkimagenormalize:vmtkmeshtransform:vmtksurfacecurvature:vmtkbranchclipper:vmtkimageobjectenhancement:vmtkmeshtransformtoras:vmtksurfacedecimation:vmtkbranchextractor:vmtkimageotsuthresholds:vmtkmeshvectorfromcomponents:vmtksurfacedistance:vmtkbranchgeometry:vmtkimagereader:vmtkmeshviewer:vmtksurfaceendclipper:vmtkbranchmapping:vmtkimagereslice:vmtkmeshviewer2:vmtksurfaceextractannularwalls:vmtkbranchmetrics:vmtkimageseeder:vmtkmeshvolume:vmtksurfaceextractinnercylinder:vmtkbranchpatching:vmtkimageshiftscale:vmtkmeshvorticityhelicity:vmtksurfacekiteremoval:vmtkbranchsections:vmtkimagesmoothing:vmtkmeshwallshearrate:vmtksurfaceloopextraction:vmtkcenterlineattributes:vmtkimagetonumpy:vmtkmeshwriter:vmtksurfacemassproperties:vmtkcenterlinegeometry:vmtkimagevesselenhancement:vmtkmeshwriter2:vmtksurfacemodeller:vmtkcenterlineimage:vmtkimageviewer:vmtknetworkeditor:vmtksurfacenormals:vmtkcenterlineinterpolation:vmtkimagevoipainter:vmtknetworkextraction:vmtksurfacepointdatatocelldata:vmtkcenterlinelabeler:vmtkimagevoiselector:vmtknetworkwriter:vmtksurfacepolyballevaluation:vmtkcenterlinemerge:vmtkimagevolumeviewer:vmtknumpyreader:vmtksurfaceprojection:vmtkcenterlinemeshsections:vmtkimagewriter:vmtknumpytocenterlines:vmtksurfacereader:vmtkcenterlinemodeller:vmtklevelsetsegmentation:vmtknumpytoimage:vmtksurfacereferencesystemtransform:vmtkcenterlineoffsetattributes:vmtklineartoquadratic:vmtknumpytomesh:vmtksurfaceregiondrawing:vmtkcenterlineresampling:vmtklineresampling:vmtknumpytosurface:vmtksurfaceremeshing:vmtkcenterlines:vmtklocalgeometry:vmtknumpywriter:vmtksurfaceresolution:vmtkcenterlinesections:vmtkmarchingcubes:vmtkparticletracer:vmtksurfacescaling:vmtkcenterlinesmoothing:vmtkmeshaddexternallayer:vmtkpathlineanimator:vmtksurfacesmoothing:vmtkcenterlinesnetwork:vmtkmesharrayoperation:vmtkpetergeneratesurface:vmtksurfacesubdivision:vmtkcenterlinestonumpy:vmtkmeshboundaryinspector:vmtkpeterresurface:vmtksurfacetobinaryimage:vmtkcenterlineviewer:vmtkmeshbranchclipper:vmtkpetersurfaceclipper:vmtksurfacetomesh:vmtkdelaunayvoronoi:vmtkmeshclipcenterlines:vmtkpointsplitextractor:vmtksurfacetonumpy:vmtkdijkstradistancetopoints:vmtkmeshclipper:vmtkpointtransform:vmtksurfacetransform:vmtkdistancetocenterlines:vmtkmeshcompare:vmtkpolyballmodeller:vmtksurfacetransforminteractive:vmtkdistancetospheres:vmtkmeshconnectivity:vmtkpotentialfit:vmtksurfacetransformtoras:vmtkendpointextractor:vmtkmeshcutter:vmtkpythonscript:vmtksurfacetriangle:vmtkendpointsections:vmtkmeshdatareader:vmtkrbfinterpolation:vmtksurfaceviewer:vmtkentityrenumber:vmtkmeshextractpointdata:vmtkrenderer:vmtksurfacewriter:vmtkflowextensions:vmtkmeshgenerator:vmtkrendertoimage:vmtksurfacewriter2:vmtkgeodesicsurfaceresolution:vmtkmeshlambda2:vmtksurfaceappend:vmtksurfmesh:vmtkicpregistration:vmtkmeshlinearize:vmtksurfacearrayoperation:vmtktetgen:vmtkimagebinarize:vmtkmeshmerge:vmtksurfacearraysmoothing:vmtktetringenerator:vmtkimagecast:vmtkmeshmergetimesteps:vmtksurfacebooleanoperation:vmtkthresholdf \
    --env PATH=/opt/miniforge3/bin:${PATH} \
    --env CONDA_PATH=/opt/miniforge3 \
    --copy README.md /README.md \
 > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi