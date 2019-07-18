#!/bin/bash

IMAGE_NAME='xtract_tabular_image'

#DIRECTORY=$1
#FILENAME=$2
#CHUNKSIZE=${3:-10000}

#echo $CHUNKSIZE
args_array=("$@")
DIRECTORY=("${args_array[@]:0:1}")
echo "${DIRECTORY[@]}"   
CMD_ARGS=("${args_array[@]:1}")
echo "${CMD_ARGS[@]}"
docker run -it -v "${DIRECTORY[@]}":/"${DIRECTORY[@]}" $IMAGE_NAME "${CMD_ARGS[@]}"


