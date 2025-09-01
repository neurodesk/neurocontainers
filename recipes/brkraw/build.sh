#!/usr/bin/env bash
set -ex

export toolName='brkraw'
export toolVersion='0.3.11'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

echo "EXPERIMENTAL: using tinyrange"

$TINYRANGE login \
    --size lg \
    --package-path brkraw.pkg \
    --pkg ubuntu \
    --pkg brkraw \
    --exec "whoami" \
    --pull-snapshot brkraw.tar.gz

neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu:22.04 \
    --pkg-manager apt \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --add ./brkraw.tar.gz / \
    --env PATH='$PATH':/usr/local/bin/ \
    --env DEPLOY_BINS=brkraw \
    --copy README.md /README.md \
> ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi