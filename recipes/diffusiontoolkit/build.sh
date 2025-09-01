#!/usr/bin/env bash
set -e

# this template file builds datalad and is then used as a docker base image for layer caching
export toolName='diffusiontoolkit'
export toolVersion='0.6.4.1' 
# Don't forget to update version change in README.md!!!!!
# toolName or toolVersion CANNOT contain capital letters or dashes or underscores (Docker registry does not accept this!)

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

###########################################################################################################################################
# IF POSSIBLE, PLEASE DOCUMENT EACH ARGUMENT PROVIDED TO NEURODOCKER. USE THE `# your comment` NOTATION THAT ALLOWS MID-COMMAND COMMENTS
# NOTE 1: THE QUOTES THAT ENCLOSE EACH COMMENT MUST BE BACKQUOTES (`). OTHER QUOTES WON'T WORK!
# NOTE 2: THE BACKSLASH (\) AT THE END OF EACH LINE MUST FOLLOW THE COMMENT. A BACKSLASH BEFORE THE COMMENT WON'T WORK!
##########################################################################################################################################
neurodocker generate ${neurodocker_buildMode} \
   --base-image centos:7                                `# centos required due to libXt dependency` \
   --pkg-manager yum                                    `# desired package manager, has to match the base image (e.g. debian needs apt; centos needs yum)` \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll"   `# define the ll command to show detailed list including hidden files`  \
   --run="chmod +x /usr/bin/ll"                         `# make ll command executable`  \
   --run="mkdir -p ${mountPointList}"                      `# create folders for singularity bind points` \
   --install curl ca-certificates libXt libjpeg-turbo libpng12 libXrender fontconfig libXext`# install curl and ca-certificates for ssl + gui dependencies` \
   --workdir /opt/${toolName}-${toolVersion}/           `# create install directory` \
   --run="curl -fsSL --retry 5 https://object-store.rc.nectar.org.au/v1/AUTH_dead991e1fa847e3afcca2d3a7041f5d/build/Diffusion_Toolkit_v${toolVersion}_x86_64.tar.gz \
         | tar -xz -C /opt/${toolName}-${toolVersion}/ --strip-components 1" `# install from object storage - upload there beforehand; strip-components removes remove top directory in archive` \
   --env PATH='$PATH':/opt/${toolName}-${toolVersion}   `# set PATH` \
   --env DEPLOY_PATH=/opt/${toolName}-${toolVersion}/   `# specify a path where ALL binary files will be exposed outside the container for the module system` \
   --copy README.md /README.md                          `# include readme file in container` \
  > ${imageName}.${neurodocker_buildExt}                `# LAST COMMENT; NOT FOLLOWED BY BACKSLASH!`

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
