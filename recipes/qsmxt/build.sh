#!/usr/bin/env bash
set -e

export toolName='qsmxt'
export toolVersion='6.4.0'
# Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

# Version history:
# --qsmxtbase------------------------------------------------------------------------------------------------
# - 1.0.0: Release for preprint including FS7, Minc, FSL
# - 1.1.0: Change to Fastsurfer and replace minc with Ants
# - 1.1.1: update Julia and move packages to depot_Path + changed python version to 3.7
# - 1.1.2: changed python version back to 3.6 with older Miniconda version + fixed versions
# - 1.1.3: Removed FSL, bidscoin, matplotlib, seaborn; added bet2
# - 1.1.3 (fix): Fixed dependency problems occurring with Nipype (see github.com/QSMxT/QSMxT/runs/7553737387)
# --qsmxt----------------------------------------------------------------------------------------------------
# - 1.1.12: Combined qsmxt and qsmxtbase containers
# - 1.1.13: https://github.com/QSMxT/QSMxT/releases/tag/v1.1.13
# - 1.1.13 (container update): Added RomeoApp to Julia for nextQSM testing; removed run_1_fixGEphaseFFTshift.py from DEPLOY_BINS
# - 1.1.13 (container update): Added NeXtQSM
# - 1.2.0: Major update; added QSM.jl; ROMEO unwrapping; Laplacian unwrapping; V-SHARP; RTS QSM; major pipeline refactor
# - 1.3.0: Major update; added premade pipelines, interactive editor, PDF, TV, and fixed networkx version incompatibility
# - 1.3.0 (container update): Fixed FastSurfer to v1.1.1 due to seeming slowness in v2
# - ...
# - 3.2.0: Added fix for scikit-sparse due to Cython bug https://github.com/scikit-sparse/scikit-sparse/releases/tag/v0.4.9
# - 6.3.2: Note that Julia v1.10 is not compatible with QSM.jl - created issue https://github.com/kamesy/QSM.jl/issues/8

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:18.04 \
   --pkg-manager apt \
   --env DEBIAN_FRONTEND=noninteractive \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --install bzip2 ca-certificates wget unzip gcc cmake g++ dbus-x11 libgtk2.0-0 git graphviz wget \
      zip libgl1 libglib2.0 libglu1-mesa libsm6 libxrender1 libxt6 libxcomposite1 libfreetype6 \
      libasound2 libfontconfig1 libxkbcommon0 libxcursor1 libxi6 libxrandr2 libxtst6 qt5-default \
      libqt5svg5-dev wget libqt5opengl5-dev libqt5opengl5 libqt5gui5 libqt5core5a libsuitesparse-dev \
      libsqlite3-dev \
   --env PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
   --run="mkdir -p ${mountPointList}" \
   --workdir="/opt/bet2" \
   --run="git clone https://github.com/liangfu/bet2.git . \
       && cmake . && make \
       && ln -s /opt/bet2/bin/bet2 /bin/bet" \
   --workdir="/opt" \
   --env SUBJECTS_DIR="/tmp" \
   --ants version=2.3.4 \
   --dcm2niix method=source version=fe2f26005109f396a4f828aa438241f73fc25fe8 \
   --miniconda version=4.7.12.1 conda_install='python=3.8' \
   --run="rm -rf /usr/bin/python3.8 \
       && ln -s /opt/miniconda-latest/bin/python /usr/bin/python3.8 \
       && pip install qsmxt==${toolVersion} \
       && pip install git+https://github.com/astewartau/nii2dcm.git@main#egg=nii2dcm \
       && nextqsm --download_weights" \
   --env PATH="\${PATH}:/opt/miniconda-latest/bin" \
   --run="git clone --depth 1 --branch v1.1.1 https://github.com/Deep-MI/FastSurfer.git /opt/FastSurfer \
       && sed -i 's/cu113/cpu/g' /opt/FastSurfer/requirements.txt \
       && pip install -r /opt/FastSurfer/requirements.txt" \
   --env FASTSURFER_HOME="/opt/FastSurfer" \
   --env PATH="\${PATH}:/opt/FastSurfer" \
   --copy test.sh /test.sh \
   --workdir="/opt/bru2" \
   --run="wget https://github.com/neurolabusc/Bru2Nii/releases/download/v1.0.20180303/Bru2_Linux.zip \
       && unzip Bru2_Linux.zip \
       && rm Bru2_Linux.zip" \
   --env PATH="\${PATH}:/opt/bru2" \
   --workdir="/opt" \
   --run="wget https://julialang-s3.julialang.org/bin/linux/x64/1.9/julia-1.9.3-linux-x86_64.tar.gz \
       && tar zxvf julia-1.9.3-linux-x86_64.tar.gz \
       && rm -rf julia-1.9.3-linux-x86_64.tar.gz" \
   --env PATH="\${PATH}:/opt/julia-1.9.3/bin" \
   --workdir="/opt" \
   --copy install_packages.jl "/opt" \
   --env JULIA_DEPOT_PATH="/opt/julia_depot" \
   --run="julia install_packages.jl \
       && chmod -R 755 /opt/julia_depot/packages/RomeoApp" \
   --env JULIA_DEPOT_PATH="~/.julia:/opt/julia_depot" \
   --workdir="/opt" \
   --run="git clone --depth 1 --branch v0.93 https://github.com/QSMxT/QSMxT-UI" \
   --run="wget https://nodejs.org/dist/v14.17.0/node-v14.17.0-linux-x64.tar.xz \
       && tar xf node-v14.17.0-linux-x64.tar.xz \
       && rm node-v14.17.0-linux-x64.tar.xz" \
   --env PATH="\${PATH}:/opt/node-v14.17.0-linux-x64/bin" \
   --run="cd QSMxT-UI/frontend/ && npm install && CI=false npm run build" \
   --run="cd QSMxT-UI/api/ && npm install --unsafe-perm && npm i -g ts-node" \
   --env DEPLOY_PATH="/opt/ants-2.3.4/:/opt/FastSurfer" \
   --env DEPLOY_BINS="nipypecli:bet:dcm2niix:Bru2:Bru2Nii:tgv_qsm:julia:python3:python:pytest:predict_all.py:qsmxt:dicom-sort:dicom-convert:nifti-convert"  \
   --env LC_ALL="C.UTF-8" \
   --env LANG="C.UTF-8" \
   --run="wget https://raw.githubusercontent.com/QSMxT/QSMxT/main/docs/container_readme.md -O /README.md" \
   --run="sed -i \"s/toolVersion/${toolVersion}/g\" /README.md" \
  > ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi

# Explanation for Julia hack:
   # --env JULIA_DEPOT_PATH="/opt/julia_depot" \
   # --run="julia install_packages.jl" \
   # --env JULIA_DEPOT_PATH="~/.julia:/opt/julia_depot" \

   # The problem is that Julia packages install by default in the homedirectory
   # in singularity this homedirectory does not exist later on
   # so we have to set the Julia depot path to a path that's available in the image later
   # but: Julia assumes that this path is writable :( because it stores precompiled outputs there
   # solution is to to add a writable path before the unwritable path
   # behaviour: julia writes precompiled stuff to ~/.julia and searches for packages in both, but can't find them in ~/.julia and then searches in /opt/
   # if anyone has a better way of doing this, please let me know: @sbollmann_MRI (Twitter)

