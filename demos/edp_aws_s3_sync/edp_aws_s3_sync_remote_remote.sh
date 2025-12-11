#!/usr/bin/env bash

# previously:
# upload test data to s3 testing bucket:
#   $ ./edp_aws_s3_sync_local_remote.sh
# create a temporary s3 bucket for remote copies:
#   $ aws s3 mb s3://ecco-tmp-1 --profile saml-pub

edp_aws_s3_sync \
    --src s3://ecco-tmp \
    --dest s3://ecco-tmp-1 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile saml-pub \
    --log DEBUG
    #--log INFO
    #--dryrun \

