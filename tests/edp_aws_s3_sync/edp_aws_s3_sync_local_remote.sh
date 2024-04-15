#!/usr/bin/env bash

# previously:
# aws s3 mb s3://ecco-tmp --profile saml-pub

edp_aws_s3_sync \
    --src ../ecco_model_granules/V4r4 \
    --dest s3://ecco-tmp/V4r4 \
    --nproc 2 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --log DEBUG
    #--log INFO
    #--dryrun \

