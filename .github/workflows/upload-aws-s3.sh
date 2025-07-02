echo "[DEBUG] Attempting upload to AWS Object Storage:"

IMAGENAME=$1
export IMAGE_HOME="/storage/tmp"


# check if aws cli is installed
if ! command -v aws &>/dev/null; then
  echo "[DEBUG] Installing AWS CLI"
  curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip
fi

# uploading to aws in the background:
time aws s3 cp $IMAGE_HOME/${IMAGENAME}_${BUILDDATE}.simg s3://neurocontainers/temporary-builds-new/${IMAGENAME}_${BUILDDATE}.simg
# echo "[DEBUG] Done with uploading to AWS Object Storage!"

# if curl --output /dev/null --silent --head --fail "https://neurocontainers.neurodesk.org/temporary-builds-new/${IMAGENAME}_${BUILDDATE}.simg"; then
#   echo "[DEBUG] ${IMAGENAME}_${BUILDDATE}.simg was freshly build and exists now :)"
#   echo "[DEBUG] cleaning up $IMAGE_HOME/${IMAGENAME}_${BUILDDATE}.simg"
#   rm -rf $IMAGE_HOME/${IMAGENAME}_${BUILDDATE}.simg
# else
#   echo "[ERROR] ${IMAGENAME}_${BUILDDATE}.simg does not exist yet. Something is WRONG"
#   exit 2
# fi