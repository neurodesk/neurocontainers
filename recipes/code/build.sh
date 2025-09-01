#!/usr/bin/env bash
set -e

export toolName='code'
export toolVersion='240320'
export juliaVersion='1.6.3'
export GO_VERSION="1.17.2" 
export SINGULARITY_VERSION="3.9.3" 
export OS=linux 
export ARCH=amd64

if [ "$1" != "" ]; then
    echo "Entering Debug mode"
    export debug=$1
fi

# vscode needs /run bind mounted to work!!!!

source ../main_setup.sh

neurodocker generate ${neurodocker_buildMode} \
   --base-image debian \
   --pkg-manager apt \
   --env DEBIAN_FRONTEND=noninteractive \
   --run="printf '#!/bin/bash\nls -la' > /usr/bin/ll" \
   --run="chmod +x /usr/bin/ll" \
   --run="mkdir -p ${mountPointList}" \
   --miniconda version=latest \
      conda_install="python=3.11 nipype jupyter nb_conda_kernels h5py seaborn numpy" \
      pip_install="osfclient" \
   --install midori xdg-utils  python-pyqt5 unzip git apt-transport-https ca-certificates coreutils \
      curl gnome-keyring gnupg libnotify4 wget libnss3 libxkbfile1 libsecret-1-0 libgtk-3-0 libxss1 libgbm1 libxshmfence1 libasound2 \
       cryptsetup squashfs-tools lua-bit32 lua-filesystem lua-json lua-lpeg lua-posix lua-term lua5.2 lmod imagemagick less nano tree \
       gcc graphviz libzstd1 zlib1g-dev zip build-essential uuid-dev libgpgme-dev libseccomp-dev pkg-config \
   --run="wget -O vscode.deb 'https://code.visualstudio.com/sha/download?build=stable&os=linux-deb-x64' \
      && apt install ./vscode.deb  \
      && rm -rf ./vscode.deb" \
   --workdir /opt \
   --run="wget https://julialang-s3.julialang.org/bin/linux/x64/${juliaVersion:0:3}/julia-${juliaVersion}-linux-x86_64.tar.gz \
      && tar zxvf julia-${juliaVersion}-linux-x86_64.tar.gz \
      && rm -rf julia-${juliaVersion}-linux-x86_64.tar.gz \
      && ln -s /opt/julia-${juliaVersion} /opt/julia-latest" \
   --run="code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension ms-python.python \
    && code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension julialang.language-julia \
    && code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension ms-python.vscode-pylance \
    && code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension ms-toolsai.jupyter \
    && code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension ms-toolsai.jupyter-keymap \
    && code --extensions-dir=/opt/vscode-extensions --user-data-dir=/opt/vscode-data --install-extension ms-toolsai.jupyter-renderers \
      && rm -rf /opt/vscode-extensions/ms-python.vscode-pylance-2023.1.10/dist/native/onnxruntime/napi-v3/darwin/x64/libonnxruntime.1.13.1.dylib \
      && rm -rf /opt/vscode-extensions/ms-python.vscode-pylance-2023.1.10/dist/native/onnxruntime/napi-v3/darwin/arm64/libonnxruntime.1.13.1.dylib \
      && rm -rf /opt/vscode-extensions/ms-python.vscode-pylance-2023.1.10/dist/native/onnxruntime/napi-v3/linux/x64/libonnxruntime.so.1.13.1 \
      && rm -rf /opt/vscode-extensions/ms-python.python-2022.16.1/out/client/extension.js.map.disabled \
      && rm -rf /opt/vscode-extensions/ms-python.vscode-pylance-2023.1.10/dist/native/onnxruntime/napi-v3/win32/x64/onnxruntime.dll \
      && rm -rf /opt/vscode-extensions/ms-toolsai.jupyter-renderers-1.0.9/out/client_renderer/vega.bundle.js \
      && rm -rf /opt/vscode-extensions/julialang.language-julia-1.38.2/dist/extension.js.map \
      && rm -rf /opt/vscode-extensions/ms-python.python-2022.16.1/pythonFiles/lib/python/debugpy/_vendored/pydevd/pydevd_attach_to_process/inject_dll_x86.pdb \
      && rm -rf /opt/vscode-extensions/ms-python.python-2022.16.1/pythonFiles/lib/python/debugpy/_vendored/pydevd/pydevd_attach_to_process/inject_dll_amd64.pdb \
    && rm -rf /opt/vscode-data/CachedExtensionVSIXs/" \
   --env DONT_PROMPT_WSL_INSTALL=1 \
   --env PATH='$PATH':/opt/julia-${juliaVersion}/bin \
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
   --copy README.md /README.md \
   --copy code /usr/local/sbin/ \
   --run="chmod a+x /usr/local/sbin/code" \
   --run="chmod a+rwx /opt/vscode-extensions/ -R" \
   --run="chmod a+rwx /opt/vscode-data -R" \
   --env DEPLOY_BINS=code \
   --copy module.sh /usr/share/ \
   --user neuro \
 > ${imageName}.${neurodocker_buildExt}


if [ "$1" != "" ]; then
   ./../main_build.sh
fi

# vscode needs /run bind mounted to work!!!!
