#!/bin/bash

# Base name for the images
IMAGE_BASE_NAME="roby944/"

# AsyncIO tag
TAG="asyncio"

# Loop through each subfolder and build the Docker image
for dir in */; do
    echo "Building image for $dir"
    # Remove trailing slash
    dir=${dir%*/}

    # Remove the first part of the directory name up to the first underscore
    new_dir=${dir#*_}
    
    echo "Building image for ${new_dir}"
    
    # Build the Docker image
    docker build -t ${IMAGE_BASE_NAME}${new_dir}:${TAG} $dir
    
    # Push the Docker image to Docker Hub
    docker push ${IMAGE_BASE_NAME}${new_dir}:${TAG}
done
