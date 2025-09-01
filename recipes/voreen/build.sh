export toolName='voreen'
export toolVersion='5.3.0'

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
--base-image ubuntu:24.04 \
--pkg-manager apt \
--run="mkdir -p ${mountPointList}" \
--env DEBIAN_FRONTEND=noninteractive \
--install build-essential cmake libglew-dev libboost-all-dev libdevil-dev qtbase5-dev libqt5svg5-dev wget \
--workdir='/root' \
--run="wget https://www.uni-muenster.de/imperia/md/content/voreen/release/voreen-src-${toolVersion}-unix.tar.gz && \
    tar -xf voreen-src-${toolVersion}-unix.tar.gz" \
--run='cd /root/voreen-src-unix-nightly && \
    mkdir build && cd build && \
    cmake .. && \
    make -j8 && \
    export VRN_DEPLOYMENT=ON && \
    export VRN_ADD_INSTALL_TARGET=ON && \
    export CMAKE_INSTALL_PREFIX=/usr/share/voreen/ && \
    make install && \
    rm -rf /root/*' \
--env DEPLOY_BINS=voreenve:voreentool \
--copy README.md /README.md \
> ${toolName}_${toolVersion}.Dockerfile 

if [ "$1" != "" ]; then
   ./../main_build.sh
fi