#!/usr/bin/env bash
set -e

export toolName='synthstrip'
export toolVersion='7.4.1'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Use Singlrity/Apptainer to run TinyRange to generate the rootfs.
singularity run -B /storage:/storage docker://ghcr.io/tinyrange/tinyrange:stable build synthstrip.star:synthstrip_root -o synthstrip.tar

# Make sure the automaticcly included template doesn't add to the final layer.
neurodocker generate ${neurodocker_buildMode} \
    --base-image ubuntu \
    --pkg-manager apt \
    --base-image scratch \
    --add synthstrip.tar . \
    --run "/init -run-scripts /.pkg/scripts.json" \
    --run "/init -run-scripts /wheels/scripts.json" \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    --env FREESURFER_HOME=/freesurfer \
    --run="ln -s /usr/bin/python3 /usr/bin/python" \
    --entrypoint "/bin/bash" \
    --env DEPLOY_BINS=mri_synthstrip \
    --copy README.md /README.md \
 > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
