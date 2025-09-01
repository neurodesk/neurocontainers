export toolName='segmentator'
export toolVersion='1.6.1' 
# https://github.com/ofgulban/segmentator/releases 

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi
source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:20.04 \
   --env DEBIAN_FRONTEND=noninteractive \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --install opts="--quiet" wget git gcc curl ca-certificates unzip gfortran build-essential libxt6 libxext6 libxtst6 libgl1-mesa-glx libc6 libice6 libsm6 libx11-6 \
   --miniconda version=py38_22.11.1-1 \
               mamba=true \
               conda_install='python=3.8 numpy=1.22.0 matplotlib=3.1.1' \
               pip_install='nibabel==2.5.1 pytest-cov==2.7.1 compoda==0.3.5 scipy==1.3.1' \
   --workdir /opt \
   --run="wget https://github.com/ofgulban/segmentator/archive/refs/tags/v${toolVersion}.zip \
      && unzip v${toolVersion}.zip \
      && rm v${toolVersion}.zip \
      && cd segmentator-${toolVersion}/ \
      && python setup.py install" \
   --env DEPLOY_BINS=segmentator \
   --copy README.md /README.md \
   --copy test.sh /test.sh \
  > ${imageName}.${neurodocker_buildExt}


# 

# https://docs.anaconda.com/free/miniconda/miniconda-hashes/

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
