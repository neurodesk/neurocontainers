#!/usr/bin/env bash
set -e

# this template file builds rabies and is then used as a docker base image for layer caching
export toolName='rabies'
export toolVersion='0.5.3'
# Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image gabdesgreg/${toolName}:$toolVersion \
   --pkg-manager apt \
   --user root \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --env DEPLOY_BINS=rabies \
   --copy README.md /README.md \
   --user rabies \
  > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi