#!/usr/bin/env bash

#
# test granule generation using select, S3-stored ECCO results, local granule output:
#
# usage: edp_generate_dataproducts.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_generate_dataproducts.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

TASKLIST=tasks_${ver}.json

# for testing in AWS IAM Identity Center (SSO) environment:
KEYGEN=/usr/local/bin/aws-login-pub.darwin.amd64
PROFILE=saml-pub

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --keygen ${KEYGEN} \
    --profile ${PROFILE} \
    --log DEBUG
