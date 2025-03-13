#!/usr/bin/env bash

#
# test granule generation using S3-stored ECCO results, local granule output:
#
# usage: edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_generate_dataproducts_SSH_native_latlon_mon_mean.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

TASKLIST=SSH_native_latlon_mon_mean_tasks_${ver}.json

# for testing within JPL environment:
KEYGEN=/usr/local/bin/aws-login-pub.darwin.amd64
PROFILE=saml-pub

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --log DEBUG \
    --keygen ${KEYGEN} \
    --profile ${PROFILE}
