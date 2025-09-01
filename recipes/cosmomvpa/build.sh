#!/usr/bin/env bash
set -e

# this template file builds itksnap and is then used as a docker base image for layer caching
export toolName='cosmomvpa'
export toolVersion='1.1.0'


if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:23.10 \
   --env DEBIAN_FRONTEND=noninteractive \
   --pkg-manager apt \
   --install octave curl ca-certificates wget git unzip liboctave-dev patch make fonts-freefont-otf \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --workdir /opt/${toolName}-${toolVersion}/ \
   --run="git clone https://github.com/CoSMoMVPA/CoSMoMVPA.git" \
   --run="make -C CoSMoMVPA install" \
   --copy setup_cosmomvpa.m /opt/${toolName}-${toolVersion}/setup_cosmomvpa.m \
   --run="octave setup_cosmomvpa.m" \
   --env DEPLOY_BINS=octave \
   --copy README.md /README.md \
  > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
