#!/usr/bin/env bash

# invoke Docker "executable" using single TASKLIST environment
# variable input:

docker run \
    --rm -it \
    --env TASKLIST=./input/SSH_native_latlon_mon_mean_tasks_V4r5.json \
    --mount type=bind,src=./input,dst=/input \
    --mount type=bind,src=./output,dst=/output \
    edp_generate_dataproducts
