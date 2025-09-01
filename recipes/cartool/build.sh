export toolName='cartool'
export toolVersion='7608' 

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi
source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image ubuntu:22.04 \
   --env DEBIAN_FRONTEND=noninteractive \
   --pkg-manager apt \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --workdir /opt \
   --install opts='--quiet' wget git curl ca-certificates unzip wine xvfb python3 python3-pip \
   --run="pip install gdown" \
   --env WINEPREFIX=/opt/wine \
   --run="winecfg" \
   --run="gdown -O cartool.exe https://drive.google.com/file/d/13zT9rzScUDSd5juY4kxt6ld_ObniPiWw" \
   --run="dpkg --add-architecture i386" \
   --install opts='--quiet' wine32 \
   --env PATH='$PATH':/opt/${toolName}-${toolVersion}/bin \
   --env DEPLOY_PATH=/opt/${toolName}-${toolVersion}/bin/ \
   --copy README.md /README.md \
   --copy license.txt /license.txt                          `# MANDATORY: include license file in container` \
  > ${imageName}.${neurodocker_buildExt}
   
   
   # this doesn't work yet -> zipping up installation folder for now on an interactive instance.
   # --run="Xvfb :0 -screen 0 1024x768x16 & DISPLAY=:0.0 wine Cartool64Setup.exe /S" \

  
if [ "$1" != "" ]; then
   ./../main_build.sh
fi

sudo dpkg --add-architecture i386 
sudo mkdir -pm755 /etc/apt/keyrings
sudo wget -O /etc/apt/keyrings/winehq-archive.key https://dl.winehq.org/wine-builds/winehq.key
sudo wget -NP /etc/apt/sources.list.d/ https://dl.winehq.org/wine-builds/ubuntu/dists/jammy/winehq-jammy.sources
sudo apt update
sudo apt install --install-recommends wine-stable xvfb
sudo apt install --install-recommends winehq-stable

export WINEPREFIX=/opt/wine
sudo mkdir -p /opt/wine
sudo chown $USER /opt/wine
wget -O Cartool64Setup.exe "https://drive.usercontent.google.com/u/0/uc?id=13zT9rzScUDSd5juY4kxt6ld_ObniPiWw&export=download"
# wine 
Xvfb :0 -screen 0 1024x768x16 & DISPLAY=:0.0 wine Cartool64Setup.exe /S

# dpkg --add-architecture i386 && apt-get update &&
# apt-get install wine32
# sudo dpkg --add-architecture i386
# sudo apt update
# sudo apt install wine32 wine
# sudo apt install libwine
# export WINEPREFIX=/opt/wine
# wget "https://drive.google.com/u/0/uc?id=13zT9rzScUDSd5juY4kxt6ld_ObniPiWw&export=download" -O Cartool64Setup.exe
