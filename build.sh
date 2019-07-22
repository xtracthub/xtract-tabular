#!/bin/bash

IMAGE_NAME='xtract_tabular_image'

docker rmi -f $IMAGE_NAME

docker build -t $IMAGE_NAME .


