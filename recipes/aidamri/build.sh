# this template file builds itksnap and is then used as a docker base image for layer caching
export toolName='aidamri'
export toolVersion='2.0'
export niftyreg_version='1.4.0'
export niftyreg_commit_sha='83d8d1182ed4c227ce4764f1fdab3b1797eecd8d'
# Don't forget to update version change in README.md!!!!!

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image 'ubuntu:20.04' \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install opts="--quiet" ca-certificates curl cmake make g++ \
   --workdir=/opt \
   --run="curl -fsSL --retry 5 https://github.com/aswendtlab/AIDAmri/archive/master.tar.gz | tar -xz -C ./" \
   --run="mv AIDAmri-master ${toolName}-${toolVersion}" \
   --workdir=/opt/niftyreg-builder \
   --run="curl -fsSL --retry 5 https://github.com/KCL-BMEIS/niftyreg/archive/${niftyreg_commit_sha}.tar.gz | tar -xz -C ./" \
   --run="mv niftyreg-${niftyreg_commit_sha} src && mkdir build" \
   --run="cmake -S src -B build -D CMAKE_INSTALL_PREFIX=/opt/niftyreg-${niftyreg_version}" \
   --run="cd build && make && make install" \
   --base-image dsistudio/dsistudio:latest \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install opts="--quiet" libgomp1 \
   --fsl version=5.0.11 exclude_paths='data' \
   --workdir=/opt/${toolName}-${toolVersion} \
   --copy-from '0' /opt/${toolName}-${toolVersion}/bin ./bin \
   --copy-from '0' /opt/${toolName}-${toolVersion}/lib ./lib \
   --copy-from '0' /opt/niftyreg-${niftyreg_version} ./NiftyReg \
   --run="echo /opt/dsi-studio/dsi_studio_64/dsi_studio > /opt/aidamri-1.1/bin/3.2_DTIConnectivity/dsi_studioPath.txt" \
   --miniconda version=4.7.12.1 \
            conda_install='python=3.6' \
              pip_install='nipype==1.1.2 lmfit==0.9.11 progressbar2==3.38.0 nibabel' \
   --env TOOLBOX_PATH=/opt/${toolName}-${toolVersion}/ \
   --env PATH=/opt/${toolName}-${toolVersion}:${PATH} \
   --env DEPLOY_PATH=/opt/${toolName}-${toolVersion}/bin/ \
   --copy README.md /README.md \
  > ${toolName}_${toolVersion}.Dockerfile

if [ "$1" != "" ]; then
   ./../main_build.sh
fi

