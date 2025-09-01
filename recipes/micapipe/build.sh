export toolName='micapipe'
export toolVersion='v0.2.3' 
# check if version is here: https://hub.docker.com/r/micalab/micapipe/tags
# Don't forget to update version change in README.md (or replace with toolVersion) !!!!!
if [ "$1" != "" ]; then
   echo "Entering Debug mode"
   export debug=$1
fi
source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image micalab/${toolName}:${toolVersion} \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --workdir /opt \
   --install wget git curl ca-certificates unzip \
   --env PATH='$PATH':/opt/${toolName}/:/opt/miniconda-22.11.1/envs/micapipe/bin/ \
   --env DEPLOY_PATH=/opt/${toolName}/ \
   --copy README.md /README.md \
   --copy test.sh /test.sh \
   --copy license.txt /opt/licence.txt \
> ${imageName}.${neurodocker_buildExt}

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
