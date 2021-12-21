#!/bin/bash

IMAGE_NAME='xtract-tabular'

docker rmi -f $IMAGE_NAME

docker build -t $IMAGE_NAME .


