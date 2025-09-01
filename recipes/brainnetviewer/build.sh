#!/usr/bin/env bash
set -e

export toolName='brainnetviewer'
export toolVersion='1.7.20191031'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Use Singlrity/Apptainer to run TinyRange to generate the rootfs.
singularity run -B /storage:/storage docker://ghcr.io/tinyrange/tinyrange:stable build bnv.star:bnv_root -o bnv.tar

# Make sure the automaticcly included template doesn't add to the final layer.
neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu \
    --pkg-manager apt \
    --base-image scratch \
    --add bnv.tar . \
    --run "/init -run-scripts /.pkg/scripts.json" \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --copy brainnetviewer /usr/bin/brainnetviewer \
    --entrypoint "/usr/bin/brainnetviewer" \
    --env DEPLOY_BINS=brainnetviewer \
    --copy README.md /README.md \
 > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
