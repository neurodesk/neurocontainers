#!/usr/bin/env bash
set -e

export toolName='rstudio'
export toolVersion='2023.12.1'
export additionalVersion='402'
# https://posit.co/download/rstudio-desktop/
# based on this, but no CUDA (yet): https://github.com/Characterisation-Virtual-Laboratory/CharacterisationVL-Software/blob/master/R/Singularity.R_4.0.5


if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:22.04 \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --workdir /opt \
   --install locales software-properties-common \
   --env LC_ALL=en_AU.UTF-8 \
   --env LANGUAGE=en_AU.UTF-8 \
   --env DEBIAN_FRONTEND=noninteractive \
   --run="locale-gen en_AU.UTF-8" \
   --install wget ubuntu-desktop vim software-properties-common git cmake mesa-utils sudo build-essential \
      python3-pip python3-pyqt5 pyqt5-dev python3-tk python3-pandas python3-fire \
      dirmngr gnupg apt-transport-https ca-certificates software-properties-common \
      r-base gdebi-core libssl-dev curl libxml2-dev libcurl4-openssl-dev libharfbuzz-dev libfribidi-dev \
      libclang-dev libpq5 libfftw3-dev gpg-agent \
      libgfortran-9-dev libblas-dev libblas64-dev liblapack-dev gfortran libudunits2-dev r-cran-ncdf4 \
      libgdal-dev libproj-dev libgeos-dev libudunits2-dev libnode-dev libcairo2-dev libnetcdf-dev \
   --workdir /opt \
   --run="wget https://download1.rstudio.org/electron/jammy/amd64/rstudio-${toolVersion}-${additionalVersion}-amd64.deb" \
   --run="gdebi -q -n /opt/rstudio-${toolVersion}-${additionalVersion}-amd64.deb" \
   --run="rm -rf rstudio-${toolVersion}-${additionalVersion}-amd64.deb" \
   --copy dependencies.R /opt \
   --run="Rscript /opt/dependencies.R" \
   --env DEPLOY_BINS=Rscript:rstudio \
   --env PATH=${PATH}:/usr/local/cuda/bin \
   --env LD_LIBRARY_PATH=/usr/local/cuda/lib64/stubs:/usr/local/cuda/lib64/:/usr/local/cuda/lib:${LD_LIBRARY_PATH} \
   --copy README.md /README.md \
  > ${imageName}.${neurodocker_buildExt}
  


if [ "$1" != "" ]; then
   ./../main_build.sh
fi


   