#!/usr/bin/env bash

edp_aws_s3_sync \
    --src s3://ecco-tmp-1 \
    --dest ./ecco-tmp-2 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --log DEBUG
    #--dryrun \

