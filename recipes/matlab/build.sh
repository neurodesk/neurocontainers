#!/usr/bin/env bash
set -e

export toolName='matlab'
export toolVersion='2022b' 
# https://hub.docker.com/r/mathworks/matlab-deep-learning
# update version in matlab wrapper script:
grep -q $toolVersion matlab && echo "Found" || exit 2

# !!!!
# You can test the container build locally by running `bash build.sh -ds`
# !!!!

export GO_VERSION="1.17.2" 
export SINGULARITY_VERSION="3.9.3" 
export OS=linux 
export ARCH=amd64

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
   --base-image mathworks/matlab-deep-learning:r${toolVersion}                 `# use Matlab deep learning 2022a docker container provided by Mathworks` \
   --env DEBIAN_FRONTEND=noninteractive                 `# The matlab image uses Ubuntu, so it's Debian` \
   --pkg-manager apt                                    `# desired package manager, has to match the base image (e.g. debian needs apt; centos needs yum)` \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll"   `# define the ll command to show detailed list including hidden files`  \
   --run="chmod +x /usr/bin/ll"                         `# make ll command executable`  \
   --run="mkdir -p ${mountPointList}"                      `# create folders for singularity bind points` \
   --install csh wget git curl ca-certificates datalad unzip libfftw3-3 apt-transport-https coreutils\
   cryptsetup squashfs-tools lua-bit32 lua-filesystem lua-json lua-lpeg lua-posix lua-term lua5.2 lmod imagemagick less nano tree \
   gcc libzstd1 zlib1g-dev zip build-essential uuid-dev libgpgme-dev libseccomp-dev pkg-config \
   --miniconda version=latest \
   --env PATH='${PATH}'":/opt/matlab/R${toolVersion}/bin/"   	 `# set PATH; not required to run matlab, but required for other Matlab tools like mex` \
   --env DEPLOY_BINS=datalad:matlab:mex                 `# specify individual binaries (separated by :) on the PATH that should be exposed outside the container for the module system` \
   --env MLM_LICENSE_FILE='~/Downloads'		 `# tell Matlab to look for the license file in Downloads under the home directory. There is the default download folder in Neurodesktop` \
   --env GOPATH='$HOME'/go \
   --env PATH='$PATH':/usr/local/go/bin:'$PATH':${GOPATH}/bin \
   --run="wget https://dl.google.com/go/go$GO_VERSION.$OS-$ARCH.tar.gz \
    && tar -C /usr/local -xzvf go$GO_VERSION.$OS-$ARCH.tar.gz \
    && rm go$GO_VERSION.$OS-$ARCH.tar.gz \
    && mkdir -p $GOPATH/src/github.com/sylabs \
    && cd $GOPATH/src/github.com/sylabs \
    && wget https://github.com/sylabs/singularity/releases/download/v${SINGULARITY_VERSION}/singularity-ce-${SINGULARITY_VERSION}.tar.gz \
    && tar -xzvf singularity-ce-${SINGULARITY_VERSION}.tar.gz \
    && cd singularity-ce-${SINGULARITY_VERSION} \
    && ./mconfig --without-suid --prefix=/usr/local/singularity \
    && make -C builddir \
    && make -C builddir install \
    && cd .. \
    && rm -rf singularity-ce-${SINGULARITY_VERSION} \
    && rm -rf /usr/local/go $GOPATH \
    && ln -s /usr/local/singularity/bin/singularity /bin/" \
   --copy README.md /README.md                          `# include readme file in container` \
   --run="rm /usr/local/bin/matlab"			`# rm original matlab symbolic link` \
   --copy matlab /usr/local/bin/matlab `# replace original matlab with a script that sets MLM_LICENSE_FILE and then call matlab; license dir is set to ~/Downloads because there is where Firefox download the license to` \
   --run="chmod a+x /usr/local/bin/matlab"     		`# make matlab executables` \
   --run="mkdir /opt/matlab/R${toolVersion}/licenses"     		`# create license directory - this will later be bind-mounted to the homedirectory download folder` \
   --run="chmod a+rwx /opt/matlab/R${toolVersion}/licenses"     		`# make licenses folder writable - this will be used for an overlay test` \
   --copy module.sh /usr/share/ \
  > ${imageName}.${neurodocker_buildExt}                `# LAST COMMENT; NOT FOLLOWED BY BACKSLASH!`


# this is a workaround for a neurodocker bug assuming root user before it can be set
sed -i '/^FROM/a USER root' ${imageName}.${neurodocker_buildExt} # add USER root after FROM line

# improvements: combine copy and chmod in one line

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
