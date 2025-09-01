#!/usr/bin/env bash
set -e

export toolName='mrtrix3tissue'
export toolVersion='5.2.8'
# Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

if [ "$debug" = "true" ]; then
   pip install --no-cache-dir https://github.com/NeuroDesk/neurodocker/tarball/add_MRtrix3tissue --upgrade
fi

neurodocker generate ${neurodocker_buildMode} \
   --base-image ghcr.io/neurodesk/caid/fsl_6.0.3:20200905 \
   --pkg-manager apt \
   --run="mkdir -p ${mountPointList}" \
   --${toolName} version=master method="source" \
   --ants version="2.3.4" \
   --env DEPLOY_PATH=/opt/${toolName}-master/bin/ \
   --copy README.md /README.md \
  > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi