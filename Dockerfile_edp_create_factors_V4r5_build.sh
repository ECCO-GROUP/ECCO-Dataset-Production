#!/usr/bin/env bash

# note that image tag cannot include uppercase characters:
docker build -f ./Dockerfile_edp_create_factors_V4r5 -t edp_create_factors_v4r5 .
