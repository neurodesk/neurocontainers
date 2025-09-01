#!/usr/bin/env bash
set -e

export toolName='radtract'
export toolVersion='0.2.3'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Use Singlrity/Apptainer to run TinyRange to generate the rootfs.
singularity run -B /storage:/storage docker://ghcr.io/tinyrange/tinyrange:stable build radtract.star:radtract_root -o radtract.tar

# Make sure the automaticcly included template doesn't add to the final layer.
neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu \
    --pkg-manager apt \
    --base-image scratch \
    --add radtract.tar . \
    --run "/init -run-scripts /.pkg/scripts.json" \
    --run "/init -run-scripts /wheels/scripts.json" \
    --run "pip install --no-deps /wheels/numpy-1.25.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --entrypoint "/bin/bash" \
    --env DEPLOY_BINS=radtract_estimate_num_parcels:radtract_filter_endpoints:radtract_parcellate:radtract_features:radtract_filter_length:radtract_tdi:radtract_filter_curvature:radtract_filter_maskoverlap:radtract_filter_density:radtract_filter_visitationcount \
    --copy README.md /README.md \
 > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
