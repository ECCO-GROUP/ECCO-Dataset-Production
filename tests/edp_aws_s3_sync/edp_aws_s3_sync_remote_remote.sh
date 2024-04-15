#!/usr/bin/env bash

# previously:
# aws s3 mb s3://ecco-tmp-1 --profile saml-pub

edp_aws_s3_sync \
    --src s3://ecco-tmp \
    --dest s3://ecco-tmp-1 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --log DEBUG
    #--log INFO
    #--dryrun \

