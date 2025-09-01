# this template file builds datalad and is then used as a docker base image for layer caching + it contains examples for various things like github install, curl, ...
export toolName='relion'
# toolName or toolVersion CANNOT contain capital letters or dashes or underscores (Docker registry does not accept this!)

# export COMPUTE_CAPABILITY=61
# export COMPUTE_CAPABILITY=89
export COMPUTE_CAPABILITY=75
# set the Compute Capability of the GPU to compile relion

export relionVersion='4.0.1'
export toolVersion=${relionVersion}'.sm'${COMPUTE_CAPABILITY} 
# the version number cannot contain a "-" - try to use x.x.x notation always
# toolVersion will automatically be written into README.md - for this to work leave "toolVersion" in the README unaltered.

export CTFFIND_VERSION='4.1.14'
export CTFFIND_LINK='https://grigoriefflab.umassmed.edu/system/tdf?path=ctffind-4.1.14.tar.gz&file=1&type=node&id=26'
# ctffind version and download link

export MOTIONCOR2_VERSION='1.6.4'
export MOTIONCOR2_LINK='https://drive.google.com/uc?export=download&id=1hskY_AbXVgrl_BUIjWokDNLZK0c1FLxF'
# motioncor2 version and download link (the link may not work forever, and may need to be updated or changed to a local install)

export GO_VERSION='1.17.2'
export SINGULARITY_VERSION='3.9.3'
export OS=linux 
export ARCH=amd64
# GO and singularity version to run modules with lmod

# Working directory
export WORKDIR=/tmp

# !!!!
# You can test the container build locally by running `bash build.sh -ds`
# !!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi
source ../main_setup.sh
###########################################################################################################################################
# IF POSSIBLE, PLEASE DOCUMENT EACH ARGUMENT PROVIDED TO NEURODOCKER. USE THE `# your comment` NOTATION THAT ALLOWS MID-COMMAND COMMENTS
# NOTE 1: THE QUOTES THAT ENCLOSE EACH COMMENT MUST BE BACKQUOTES (`). OTHER QUOTES WON'T WORK!
# NOTE 2: THE BACKSLASH (\) AT THE END OF EACH LINE MUST FOLLOW THE COMMENT. A BACKSLASH BEFORE THE COMMENT WON'T WORK!
# NOTE 3: COMMENT LINES, I.E. LINES THAT START WITH #, CANNOT BE INCLUDED IN THE MIDDLE OF THE neurodocker generate COMMAND. INSTEAD,
#         USE AN EMPTY LINE AND PUT YOUR COMMENT AT THE END USING THIS FORMAT: `# your comment goes here` \ 
##########################################################################################################################################
neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:22.04                		`# RELION: Using Ubuntu 22.04 because not all required packages were available on neurodebian` \
   --env DEBIAN_FRONTEND=noninteractive                 `# RECOMMENDED TO KEEP AS IS: this disables interactive questions during package installs` \
   --pkg-manager apt                                    `# RECOMMENDED TO KEEP AS IS: desired package manager, has to match the base image (e.g. debian needs apt; centos needs yum)` \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll"   `# RECOMMENDED TO KEEP AS IS: define the ll command to show detailed list including hidden files`  \
   --run="chmod +x /usr/bin/ll"                         `# RECOMMENDED TO KEEP AS IS: make ll command executable`  \
   --run="mkdir -p ${mountPointList}"                   `# MANDATORY: create folders for singularity bind points` \
   --install wget git curl ca-certificates unzip        `# RECOMMENDED: install system packages` \
             cmake git build-essential mpi-default-bin mpi-default-dev libfftw3-dev libtiff-dev libpng-dev ghostscript libxft-dev	`# RELION: install relion dependencies` \
             libwxgtk3.0-gtk3-dev			`# CTFFIND: install wx-config-3.0 for ctffind-4.1.14` \
             lmod 					`# TOPAZ: install lmod to run modules, like topaz` \
   --workdir=$WORKDIR \
   --run="wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-ubuntu2204.pin	`# RELION: steps to install CUDA 11.8 from https://developer.nvidia.com/cuda-11-8-0-download-archive?target_os=Linux&target_arch=x86_64&Distribution=Ubuntu&target_version=22.04&target_type=deb_local` \
       && mv cuda-ubuntu2204.pin /etc/apt/preferences.d/cuda-repository-pin-600 \
       && wget https://developer.download.nvidia.com/compute/cuda/11.8.0/local_installers/cuda-repo-ubuntu2204-11-8-local_11.8.0-520.61.05-1_amd64.deb \
       && dpkg -i cuda-repo-ubuntu2204-11-8-local_11.8.0-520.61.05-1_amd64.deb \
       && cp /var/cuda-repo-ubuntu2204-11-8-local/cuda-*-keyring.gpg /usr/share/keyrings/" \
   --install cuda-toolkit-11-8				`# RELION: install CUDA Toolkit 11.8` \
   --run="git clone 'https://github.com/3dem/relion.git' --branch=${relionVersion}	`# RELION: clone relion git repository` \
       && cd relion && mkdir build && cd build		`# RELION: create and move into build directory` \
       && cmake -DCUDA_ARCH=${COMPUTE_CAPABILITY} -DCUDA_TOOLKIT_ROOT_DIR=/usr/local/cuda-11.8 -DCMAKE_INSTALL_PREFIX=/opt/${toolName}-${toolVersion}/ -DFORCE_OWN_FLTK=ON .. `# RELION: Compile with GPU architecture and CUDA version` \
       && make && make install" \
   --run="wget -O ctffind-${CTFFIND_VERSION}.tar.gz '${CTFFIND_LINK}'	`# CTFFIND: download and install ctffind` \
       && tar -xf ctffind-${CTFFIND_VERSION}.tar.gz" \
   --copy ctffind.cpp $WORKDIR/ctffind-${CTFFIND_VERSION}/src/programs/ctffind \
   --run="cd ctffind-${CTFFIND_VERSION} \
       && ./configure --disable-debugmode --enable-mkl --prefix=/opt/ctffind-${CTFFIND_VERSION}/ \
       && make && make install" \
   --run="wget -O MotionCor2_${MOTIONCOR2_VERSION}.zip '${MOTIONCOR2_LINK}'	`# MOTIONCOR2: download and install motioncor2` \
       && mkdir -p /opt/motioncor2-${MOTIONCOR2_VERSION}/bin/ \
       && unzip MotionCor2_${MOTIONCOR2_VERSION}.zip -d /opt/motioncor2-${MOTIONCOR2_VERSION}/bin/" \
   --copy load_topaz.sh /opt/${toolName}-${toolVersion}/        `# TOPAZ: copy script to launch topaz` \
   --run="chmod +x /opt/${toolName}-${toolVersion}/load_topaz.sh" \
   --env RELION_CTFFIND_EXECUTABLE=/opt/ctffind-${CTFFIND_VERSION}/bin/ctffind    `# CTFFIND: relion environment variable for ctffind` \
   --env RELION_MOTIONCOR2_EXECUTABLE=/opt/motioncor2-${MOTIONCOR2_VERSION}/bin/MotionCor2_${MOTIONCOR2_VERSION}_Cuda118_Mar312023	`# MOTIONCOR2: relion environment variable for motioncor2` \
   --env RELION_TOPAZ_EXECUTABLE=/opt/${toolName}-${toolVersion}/load_topaz.sh	`# TOPAZ: relion environment variable for Topaz` \
   --env PATH='$PATH':/opt/${toolName}-${toolVersion}/bin `# MANDATORY: add your tool executables to PATH` \
   --env DEPLOY_PATH=/opt/${toolName}-${toolVersion}/bin/ `# MANDATORY: define which directory's binaries should be exposed to module system (alternative: DEPLOY_BINS -> only exposes binaries in the list)` \
   --env GOPATH='$HOME'/go 				`# TOPAZ: install GO to compile singularity` \
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
   --copy module.sh /usr/share/ 			`# TOPAZ: copy module file for lmod` \
   --copy README.md /README.md                          `# MANDATORY: include readme file in container` \
   --copy license.txt /license.txt                      `# MANDATORY: include license file in container` \
   --copy * /neurodesk/                              	`# MANDATORY: copy test scripts to /neurodesk folder - build.sh will be included as well, which is a good idea` \
   --run="chmod +x /neurodesk/*.sh"                     `# MANDATORY: allow execution of all shell scripts in /neurodesk inside the container` \
  > ${imageName}.${neurodocker_buildExt}                `# THIS IS THE LAST COMMENT; NOT FOLLOWED BY BACKSLASH!`
  
if [ "$1" != "" ]; then
   ./../main_build.sh
fi
