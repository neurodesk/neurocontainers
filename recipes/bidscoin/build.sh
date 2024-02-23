#!/usr/bin/env bash
set -e

# this template file builds tools required for dicom conversion to bids
export toolName='bidscoin'
export toolVersion='4.3.0'    # Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export BIDSCOIN_DEBUG=TRUE
    export debug=$1
fi

source ../main_setup.sh

# Changes made to .def file: turned the apt-get install to neurodocker install and use Ubuntu/Qt6
neurodocker generate ${neurodocker_buildMode} `# Based on Singularity .def file provided by bidscoin at https://github.com/Donders-Institute/bidscoin/blob/master/apptainer.def` \
    --pkg-manager apt \
    --base-image debian:stable \
    --env DEBIAN_FRONTEND=noninteractive \
    --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
    --run="chmod +x /usr/bin/ll" \
    --run="mkdir -p ${mountPointList}" \
    `# Install the latest dcm2niix from source` \
    --install git build-essential cmake \
    --run "export GIT_SSL_NO_VERIFY=1 && git clone https://github.com/rordenlab/dcm2niix.git" `# first command solves some issue with the clone, see: https://stackoverflow.com/questions/21181231/server-certificate-verification-failed-cafile-etc-ssl-certs-ca-certificates-c` \
    --run "cd dcm2niix; mkdir build && cd build; cmake ..; make install" \
    `#  Install curl (sometimes needed by dcm2niix)` \
    --install curl \
    `# Install pigz (to speed up dcm2niix)` \
    --install pigz \
    `# Install the 4.2.1+Qt5 branch from Github` \
    `# NOTE: PyQt5 is installed as Debian package to solve dependencies issues occurring when installed with pip.` \
    --install python3-pyqt5 \
    --miniconda version=latest \
    conda_install="-c conda-forge -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/public/ fsl-libvis fsl-avwutils fsl-flirt" \
		pip_install="bidscoin[spec2nii2bids,deface]@git+https://github.com/Donders-Institute/bidscoin@v${toolVersion}+qt5" \
    --env FSLDIR=/opt/miniconda-latest FSLOUTPUTTYPE=NIFTI_GZ \
    --env DEPLOY_BINS=bidscoin:bidscoiner:bidseditor:bidsmapper:bidsparticipants:deface:dicomsort:echocombine:medeface:physio2tsv:plotphysio:rawmapper:slicereport:dcm2niix:spec2nii \
    --copy README.md /README.md \
  > ${toolName}_${toolVersion}.Dockerfile

if [ "$1" != "" ]; then
   ./../main_build.sh
fi

# Test image with:
# sudo docker run --rm -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix bidscoin_${toolVersion}:TAG bidscoin -t
# sudo docker run -it -v /root:/root --entrypoint /bin/bash -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix bidscoin_${toolVersion}:TAG
# bidscoin -t
