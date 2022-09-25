#!/bin/sh

# This script generates packages for a release and places them in target/packages/<version>.
# Uses docker container for better reproducibility of builds.
# Additionally the pulled in docker container includes an older libc, therefore generated packages
# will be compatible with older Linux distributions.

FCLONES_HOME=$(realpath "$(dirname $0)/..")
echo $FCLONES_HOME
IMAGE="pkolaczk/fclones-builder"
docker run \
    -v "$FCLONES_HOME":/fclones \
    -u $(id -u ${USER}):$(id -g ${USER}) \
    -it $IMAGE /fclones/packaging/package-internal.sh
