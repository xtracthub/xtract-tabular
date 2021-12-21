#!/bin/bash

IMAGE_NAME='xtract-tabular'

args_array=("$@")
DIRECTORY=("${args_array[@]:0:1}")
FILE_NAME=("${args_array[@]:1:1}")
CMD_ARGS=("${args_array[@]:2}")

docker run -v $DIRECTORY:/$DIRECTORY $IMAGE_NAME --path /$DIRECTORY/$FILE_NAME "${CMD_ARGS[@]}"
