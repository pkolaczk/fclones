#!/bin/sh

# Builds a docker image used for generating various fclones packages
IMAGE="pkolaczk/fclones-builder"
docker build -t $IMAGE $(realpath "$(dirname $0)")
