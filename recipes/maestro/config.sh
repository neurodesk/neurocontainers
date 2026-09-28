#!/bin/bash
# Container paths must be independent of the caller's working directory.
export nnUNet_extTrainer=/opt/maestro/custom_trainers
export MODEL_PATH="${MODEL_PATH:-/opt/maestro/model_weights}"
