#!/usr/bin/env bash

# previously:
# download some selected ECCO results data from s3://ecco-model-granules/:
#   $ ../data/ecco_results/download_selected_data.sh
# and create an s3 testing bucket:
#   $ aws s3 mb s3://ecco-tmp --profile saml-pub

edp_aws_s3_sync \
    --src ../data/ecco_results/V4r4 \
    --dest s3://ecco-tmp/V4r4 \
    --nproc 2 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile saml-pub \
    --log DEBUG
    #--log INFO
    #--dryrun \

