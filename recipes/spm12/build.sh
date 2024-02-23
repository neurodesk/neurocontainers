#!/usr/bin/env bash
set -e

export toolName='spm12'
export toolVersion='r7771'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# try slimdown version next: https://github.com/spm/spm-docker/pull/2

export MATLAB_VERSION=R2019b
export MCR_VERSION=v97
export MCR_UPDATE=9
export SPM_VERSION=12
export SPM_REVISION=$toolVersion

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:16.04 \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install wget unzip ca-certificates openjdk-8-jre dbus-x11 \
   --env MATLAB_VERSION=$MATLAB_VERSION \
   --env MCR_VERSION=$MCR_VERSION \
   --env MCR_UPDATE=$MCR_UPDATE \
   --matlabmcr version=2019b install_path=/opt/mcr  \
   --env SPM_VERSION=$SPM_VERSION \
   --env SPM_REVISION=$toolVersion \
   --env MCR_INHIBIT_CTF_LOCK=1 \
   --env SPM_HTML_BROWSER=0 \
   --run="wget --no-check-certificate --progress=bar:force -P /opt https://www.fil.ion.ucl.ac.uk/spm/download/restricted/bids/spm${SPM_VERSION}_${SPM_REVISION}_Linux_${MATLAB_VERSION}.zip \
      && unzip -q /opt/spm${SPM_VERSION}_${SPM_REVISION}_Linux_${MATLAB_VERSION}.zip -d /opt \
      && rm -f /opt/spm${SPM_VERSION}_${SPM_REVISION}_Linux_${MATLAB_VERSION}.zip" \
   --env LD_LIBRARY_PATH=/opt/mcr/${MCR_VERSION}/runtime/glnxa64:/opt/mcr/${MCR_VERSION}/bin/glnxa64:/opt/mcr/${MCR_VERSION}/sys/os/glnxa64:/opt/mcr/${MCR_VERSION}/sys/opengl/lib/glnxa64:/opt/mcr/${MCR_VERSION}/extern/bin/glnxa64 \
   --run="/opt/spm${SPM_VERSION}/spm${SPM_VERSION} function exit \
      && chmod +x /opt/spm${SPM_VERSION}/*" \
   --miniconda \
         version=4.7.12.1 \
         conda_install='python=3.6 traits nipype numpy scipy h5py scikit-image' \
   --env XAPPLRESDIR=/opt/mcr/${MCR_VERSION}/x11/app-defaults \
   --env DEPLOY_BINS=run_spm12.sh:spm12 \
   --env PATH='$PATH':/opt/spm12:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
   --env DEPLOY_ENV_FORCE_SPMMCR="1" \
   --env DEPLOY_ENV_SPMMCRCMD="run_spm12.sh /opt/mcr/${MCR_VERSION}/ script" \
   --copy README.md /README.md \
  > ${toolName}_${toolVersion}.${neurodocker_buildExt}



if [ "$1" != "" ]; then
   ./../main_build.sh
fi
