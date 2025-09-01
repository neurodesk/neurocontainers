#!/usr/bin/env bash
set -e

export toolName='afni'
export toolVersion=`wget -O- https://afni.nimh.nih.gov/pub/dist/AFNI.version | head -n 1 | cut -d '_' -f 2`
#this is currently: 24.3.00
# https://hub.docker.com/r/afni/afni_make_build/tags

if [ "$1" != "" ]; then
   echo "Entering Debug mode"
   export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:24.04 \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install software-properties-common \
   --run="add-apt-repository universe -y" \
   --install libgdal-dev libopenblas-dev libnode-dev libudunits2-dev r-base r-base-dev tcsh xfonts-base libssl-dev python-is-python3 python3-matplotlib python3-numpy python3-flask python3-flask-cors python3-pil gsl-bin netpbm gnome-tweaks libjpeg62 xvfb xterm vim curl gedit evince eog libglu1-mesa-dev libglw1-mesa-dev libxm4 build-essential libcurl4-openssl-dev libxml2-dev libgfortran-14-dev libgomp1 gnome-terminal nautilus firefox xfonts-100dpi r-base-dev cmake bc libxext-dev libxmu-dev libxpm-dev libgsl-dev libglut-dev libxi-dev libglib2.0-dev \
   --workdir /opt \
   --run="curl -O https://afni.nimh.nih.gov/pub/dist/tgz/linux_ubuntu_24_64.tgz \
         && tar -xf linux_ubuntu_24_64.tgz \
         && mv linux_ubuntu_24_64  /usr/local/abin \
         && rm -f linux_ubuntu_24_64.tgz" \
   --env PATH=/usr/local/abin:${PATH} \
   --env R_LIBS=/usr/local/share/R-4.3 \
   --run="curl -O         https://afni.nimh.nih.gov/pub/dist/tgz/package_libs/linux_ubuntu_24_R-4.3_libs.tgz \
      && tar -xf         linux_ubuntu_24_R-4.3_libs.tgz \
      && mv              linux_ubuntu_24_R-4.3_libs  /usr/local/share/R-4.3 \
      && rm -f           linux_ubuntu_24_R-4.3_libs.tgz" \
   --run="@afni_R_package_install ALL" \
   --freesurfer version=7.4.1 \
   --env SUBJECTS_DIR="~/freesurfer-subjects-dir" \
   --env DEPLOY_PATH=/usr/local/abin/ \
   --copy dependencies.R /opt \
   --copy README.md /README.md \
   --copy test.sh /test.sh \
   --copy test.tgz /opt/test.tgz \
   --run="Rscript /opt/dependencies.R" \
   --copy license.txt /opt/freesurfer-7.4.1/license.txt \
   --workdir /opt \
> ${imageName}.${neurodocker_buildExt}

# This hack is needed for images which set the user to non-root, because neurodocker needs startup scripts that run as root.
# sed -i '/^FROM/a USER root' ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
