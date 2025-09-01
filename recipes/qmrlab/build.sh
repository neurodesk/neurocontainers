#!/usr/bin/env bash
set -e

export toolName='qmrlab'
export toolVersion='2.4.2'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Use Singlrity/Apptainer to run TinyRange to generate the rootfs.
singularity run docker://ghcr.io/tinyrange/tinyrange:stable build qMRLab.star:qmr_lab_root -o qmr_lab.tar

# Make sure the automaticcly included template doesn't add to the final layer.
neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu \
    --pkg-manager apt \
    --base-image scratch \
    --add qmr_lab.tar . \
    --run "/init -run-scripts /.pkg/scripts.json" \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --run "cd /qMRLab-2.4.2;octave --exec \"qMRLabVer\"" \
    --copy qMRLab.sh /usr/bin/qMRLab \
    --entrypoint "/usr/bin/qMRLab" \
    --env DEPLOY_BINS=qMRLab \
    --copy README.md /README.md \
 > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
