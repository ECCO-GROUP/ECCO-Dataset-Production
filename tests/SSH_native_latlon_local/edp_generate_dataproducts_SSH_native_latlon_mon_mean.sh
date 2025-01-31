#!/usr/bin/env bash

#
# test granule generation using locally-stored ECCO results
#
# usage: edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

tasklist=SSH_native_latlon_mon_mean_tasks_${ver}.json

edp_generate_dataproducts \
    --tasklist ${tasklist} \
    --log DEBUG
