# This Dockerfile provides instructions to build a docker image 
# which forms the image for running the command line interface  - cli.py in the root of this repo
# the cli allows us to call whatever python functions in this repo that we
# need for our SOMISANA operations
#
# The docker image is built by 
# .github/workflows/build_cli_image.yml (which calles .github/workflows/build_images.yml)
# and gets triggered whenever any changes are made to this repo
#
# TODO: maybe add a user and run the image as a user rather than as root?

# start with a mambaforge base image
# We're fixing the mambaforge version due to dependency issues which get introduced if left open ended
FROM condaforge/mambaforge:22.9.0-1

ENV DEBIAN_FRONTEND noninteractive

RUN mkdir /somisana-download
WORKDIR /somisana-download

# Install somisana-croco environment into base conda environment
COPY environment.yml .
RUN mamba env update -n base -f environment.yml

# add the somisana-croco code and install into the base environment
ADD . /somisana-croco
RUN pip install -e .
