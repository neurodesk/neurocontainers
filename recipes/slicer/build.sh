#!/usr/bin/env bash
set -e

# https://slicer.kitware.com/midas3/folder/274
# export downloadLink='https://slicer.kitware.com/midas3/download/item/549121/Slicer-4.11.20200930-linux-amd64.tar.gz'
export downloadLink='https://download.slicer.org/bitstream/62cc52d2aa08d161a31c1af0'
export toolName='slicer'
export toolVersion='5.0.3' #When updating this version you also need to update the MONAILabel plugin version (line 31)!
# Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:22.04 \
   --pkg-manager apt \
   --env DEBIAN_FRONTEND=noninteractive \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install python3-dev gcc libopenslide0 curl ca-certificates libxdamage1 libpulse-dev libnss3 libglu1-mesa libsm6 libxrender1 libxt6 libxcomposite1 libfreetype6 libasound2 libfontconfig1 libxkbcommon0 libxcursor1 libxi6 libxrandr2 libxtst6 libqt5svg5-dev wget libqt5opengl5-dev libqt5opengl5 libqt5gui5 libqt5core5a \
   --miniconda version=latest \
         conda_install='pytorch torchvision torchaudio pytorch-cuda=11.6 -c pytorch -c nvidia' \
   --run="pip install h11==0.11 monailabel" \
   --run="curl -fsSL --retry 5 ${downloadLink} | tar -xz -C /opt/ " \
   --workdir /opt/Slicer-${toolVersion}-linux-amd64/NA-MIC/ \
   --run="curl -fsSL --retry 5 https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/build/MONAILabel30893.tar.gz \
      | tar -xz -C /opt/Slicer-${toolVersion}-linux-amd64/NA-MIC/ --strip-components 1" \
   --install nvidia-cuda-toolkit \
   --env DEPLOY_PATH=/opt/Slicer-${toolVersion}-linux-amd64/bin \
   --env DEPLOY_BINS=Slicer \
   --env PATH=/miniconda3/bin:/usr/bin:/opt/Slicer-${toolVersion}-linux-amd64/bin:/opt/Slicer-${toolVersion}-linux-amd64 \
   --copy README.md /README.md \
  > ${toolName}_${toolVersion}.Dockerfile

if [ "$1" != "" ]; then
   ./../main_build.sh
fi


# /opt/Slicer-5.0.3-linux-amd64/NA-MIC/Extensions-30893

# This would be more elegant or an overlay fs
# singularity shell --bind /neurodesktop-storage/slicerplugins:/opt/Slicer-5.0.3-linux-amd64/NA-MIC/ /cvmfs/neurodesk.ardc.edu.au/containers/slicer_5.0.3_20220809/slicer_5.0.3_20220809.simg


# dependencies for 18.04
   # --install python3-dev gcc libopenslide0 curl ca-certificates libxdamage1 libpulse-dev libnss3 libglu1-mesa libsm6 libxrender1 libxt6 libxcomposite1 libfreetype6 libasound2 libfontconfig1 libxkbcommon0 libxcursor1 libxi6 libxrandr2 libxtst6 qt5-default libqt5svg5-dev wget libqt5opengl5-dev libqt5opengl5 libqt5gui5 libqt5core5a \
