#!/usr/bin/env bash

#
# test granule generation using select, locally-stored ECCO results
#
# usage: edp_generate_dataproducts.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_generate_dataproducts.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

tasklist=tasks_${ver}.json

edp_generate_dataproducts \
    --tasklist ${tasklist} \
    --log DEBUG
