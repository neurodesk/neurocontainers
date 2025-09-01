#!/usr/bin/env bash
set -e

# this template file builds datalad and is then used as a docker base image for layer caching + it contains examples for various things like github install, curl, ...
export toolName='mrsiproc'
export toolVersion='0.2.0' # the version number cannot contain a "-" - try to use x.x.x notation always
export matlabVersion='2021b' # this has to match the version on which the matlab scripts were compiled
export matlabUpdateVersion='6'
export mincVersion='1.9.15'
export lcmodelVersion='6.3'
export hdbetVersion='1.0' # note the hdbet doesn't really have a version
export fslVersion='6.0.7.1' # build on ubuntu 20.04
export minicondaVersion='latest'
export dcm2niixVersion='003f0d19f1e57b0129c9dcf3e653f51ca3559028' # copied from qsmxt
export juliaVersion='1.10.4'
export freesurferVersion='7.4.1'

# Don't forget to update version change in README.md!!!!!
# toolName or toolVersion CANNOT contain capital letters or dashes or underscores (Docker registry does not accept this!)

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
##########################################################################################################################################
neurodocker generate ${neurodocker_buildMode} \
   --base-image vnmd/fsl_${fslVersion} \
   --pkg-manager apt \
   --run="mkdir -p ${mountPointList}" \
   --user=root `# otherwise some permission denied error occurs during docker build` \
   --dcm2niix method=source version=${dcm2niixVersion} \
   --minc version=${mincVersion} \
   \
   --install wget curl git ca-certificates ltrace strace libxml2 gcc build-essential gzip tar unzip datalad libfftw3-3 software-properties-common bc `# install apt-get packages` \
   \
   --run="sudo apt remove -y libjpeg62 \
      && wget http://ftp.br.debian.org/debian/pool/main/libj/libjpeg-turbo/libjpeg62-turbo_2.0.6-4_amd64.deb \
      && dpkg -i libjpeg62-turbo_2.0.6-4_amd64.deb \
      && rm libjpeg62-turbo_2.0.6-4_amd64.deb" `# LIBJPEGTURBO_6.2 is required by dcm2mnc` \
   \
   --miniconda version=${minicondaVersion} \
   \
   --workdir /opt `# HD-BET` \
   --run="git clone https://github.com/MIC-DKFZ/HD-BET" \
   --workdir /opt/HD-BET \
   --run="echo 'import os' > /opt/HD-BET/HD_BET/paths.py" \
   --run="echo 'folder_with_parameter_files = \"/opt/HD-BET/hd-bet_params\"' >> /opt/HD-BET/HD_BET/paths.py" \
   --run="mkdir -p /opt/HD-BET/hd-bet_params" \
   --run="curl -o /opt/HD-BET/hd-bet_params/0.model https://zenodo.org/record/2540695/files/0.model?download=1" \
   --run="curl -o /opt/HD-BET/hd-bet_params/1.model https://zenodo.org/record/2540695/files/1.model?download=1" \
   --run="curl -o /opt/HD-BET/hd-bet_params/2.model https://zenodo.org/record/2540695/files/2.model?download=1" \
   --run="curl -o /opt/HD-BET/hd-bet_params/3.model https://zenodo.org/record/2540695/files/3.model?download=1" \
   --run="curl -o /opt/HD-BET/hd-bet_params/4.model https://zenodo.org/record/2540695/files/4.model?download=1" \
   --run="pip install -e ." \
   \
   --workdir=/opt/lcmodel-${lcmodelVersion}/ `# install LCModel and things to make it work ` \
   --install curl ca-certificates libxft2 libxss1 libnet-ifconfig-wrapper-perl vim nano unzip gv unrar `# LCModel dependencies (libtk8.6 removed asks interactive question)` \
   --run="curl -o /opt/lcm-64.tar http://www.s-provencher.com/pub/LCModel/programs/lcm-64.tar && \
          tar xf /opt/lcm-64.tar && \
          rm -rf /opt/lcm-64.tar && \
          gunzip  -c  lcm-core.tar.gz  |  tar  xf  - && \
          rm -rf lcm-core.tar.gz" \
   --workdir=/opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/3t.zip http://www.s-provencher.com/pub/LCModel/3t.zip && \
         unzip /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/3t.zip && \
         rm -rf /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/3t.zip" \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/1.5t.zip http://www.s-provencher.com/pub/LCModel/1.5t.zip && \
         unzip /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/1.5t.zip && \
         rm -rf /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/1.5t.zip" \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/7t.zip http://www.s-provencher.com/pub/LCModel/7t.zip && \
         unzip /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/7t.zip && \
         rm -rf /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/7t.zip" \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/9.4t.zip http://www.s-provencher.com/pub/LCModel/9.4t.zip && \
         unzip /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/9.4t.zip && \
         rm -rf /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/9.4t.zip" \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/basisset_LCModel.zip https://www.ismrm.org/workshops/Spectroscopy16/mrs_fitting_challenge/basisset_LCModel.zip && \
         unzip /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/basisset_LCModel.zip && \
         rm -rf /opt/lcmodel-${lcmodelVersion}/.lcmodel/basis-sets/basisset_LCModel.zip" \
   --workdir=/opt/datasets \
   --run="curl -o /opt/lcmodel-${lcmodelVersion}/manual.pdf http://www.lcmodel.com/pub/LCModel/manual/manual.pdf" \
   --copy setup_lcmodel.sh  /opt/lcmodel-${lcmodelVersion}/.lcmodel/bin \
   --workdir /opt/lcmodel-${lcmodelVersion}/.lcmodel/profiles/1/control-defaults \
   --copy controlfiledefault  /opt/lcmodel-${lcmodelVersion}/.lcmodel/profiles/1/control-defaults/controlfiledefault \
   --copy gui-defaults  /opt/lcmodel-${lcmodelVersion}/.lcmodel/profiles/1/gui-defaults \
   --run="chmod a+rwx /opt/lcmodel-${lcmodelVersion} -R" \
   --env DEPLOY_PATH=/opt/lcmodel-${lcmodelVersion}/.lcmodel/bin/:/opt/lcmodel-${lcmodelVersion}/.lcmodel/ \
   --env PATH="\${PATH}:/opt/lcmodel-${lcmodelVersion}/.lcmodel/bin/:/opt/lcmodel-${lcmodelVersion}/.lcmodel/" \
   \
   `# Add MATLAB compiler runtime (doesn't need license)` \
   --install bc curl libncurses5 libxext6 libxmu6 libxpm-dev libxt6 unzip openjdk-8-jre dbus-x11 \
   --run="wget https://ssd.mathworks.com/supportfiles/downloads/R${matlabVersion}/Release/${matlabUpdateVersion}/deployment_files/installer/complete/glnxa64/MATLAB_Runtime_R${matlabVersion}_Update_${matlabUpdateVersion}_glnxa64.zip && \
          unzip MATLAB_Runtime_R${matlabVersion}_Update_${matlabUpdateVersion}_glnxa64.zip -d mcrtmp && \
          mcrtmp/install -mode silent -destinationFolder /opt/MATLAB_Runtime_R${matlabVersion} -agreeToLicense yes && \
          rm -rf mcrtmp && \
          rm -f MATLAB_Runtime_R${matlabVersion}_Update_${matlabUpdateVersion}_glnxa64.zip" \
   \
   --env LD_LIBRARY_PATH="\${LD_LIBRARY_PATH}:/opt/MATLAB_Runtime_R${matlabVersion}/R${matlabVersion}/runtime/glnxa64:/opt/MATLAB_Runtime_R${matlabVersion}/R${matlabVersion}/bin/glnxa64:/opt/MATLAB_Runtime_R${matlabVersion}/R${matlabVersion}/sys/os/glnxa64:/opt/MATLAB_Runtime_R${matlabVersion}/R${matlabVersion}/extern/bin/glnxa64" \
   --workdir /opt `# Add Julia with MRSI packages` \
   --run="wget https://julialang-s3.julialang.org/bin/linux/x64/${juliaVersion:0:4}/julia-${juliaVersion}-linux-x86_64.tar.gz && \
      tar zxvf julia-${juliaVersion}-linux-x86_64.tar.gz && \
      rm -rf julia-${juliaVersion}-linux-x86_64.tar.gz" \
   --env PATH="\${PATH}:/opt/julia-${juliaVersion}/bin" \
   --copy install_packages.jl "/opt" \
   --env JULIA_DEPOT_PATH="/opt/julia_depot" \
   --run="julia install_packages.jl \
      && chmod -R 755 /opt/julia_depot/packages/MRSI" \
   --env JULIA_DEPOT_PATH="~/.julia:/opt/julia_depot" \
   \
   --workdir /opt `# Add freesurfer` \
   --install language-pack-en gettext xterm x11-apps csh tcsh xorg xorg-dev xserver-xorg-video-intel libwayland-cursor0 \
   --run="wget --quiet https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/${freesurferVersion}/freesurfer_ubuntu20-${freesurferVersion}_amd64.deb \
            && dpkg -i freesurfer_ubuntu20-${freesurferVersion}_amd64.deb" \
   --env PATH="\${PATH}:/usr/local/freesurfer/${freesurferVersion}-1" \
   \
   --workdir /opt `# Add MRSI pipeline scripts` \
   --install parallel \
   --run="git clone https://github.com/korbinian90/mrsi_pipeline_neurodesk.git && \
      cd mrsi_pipeline_neurodesk && \
      git checkout 02ed0178bf2d7be3ec0406fee92333ae86de377d" \
   --env PATH="\${PATH}:/opt/mrsi_pipeline_neurodesk/Part1:/opt/mrsi_pipeline_neurodesk/Part2" \
   --copy update_mrsi.sh /opt/mrsi_pipeline_neurodesk \
   --run="chmod a+x /opt/mrsi_pipeline_neurodesk/update_mrsi.sh" \
   --env PATH="/neurodesktop-storage/mrsi_pipeline_neurodesk/Part1:/neurodesktop-storage/mrsi_pipeline_neurodesk/Part2:/opt/mrsi_pipeline_neurodesk:\${PATH}" \
   --env DEPLOY_PATH="/opt/mrsi_pipeline_neurodesk/Part1:/opt/mrsi_pipeline_neurodesk/Part2" \
   --env DEPLOY_BINS="julia:python:bc:dcm2niix:nii2mnc:dcm2mnc:rawtominc:mincresample:mincmath:minctoraw:mnc2nii:bet:hd-bet:mri_synthseg:lcmodel:parallel:fsl:fslmaths:Part1_ProcessMRSI.sh:Part2_EvaluateMRSI.sh:update_mrsi.sh:fid_1.300000ms.basis:LCModel_Control_Template.m:LCModel_Control_Volunteers_WatRef.m" \
   \
   --copy README.md /README.md                          `# include README file in container` \
   \
  > ${imageName}.${neurodocker_buildExt}                `# LAST COMMENT; NOT FOLLOWED BY BACKSLASH!`

if [ "$1" != "" ]; then
   ./../main_build.sh
fi
