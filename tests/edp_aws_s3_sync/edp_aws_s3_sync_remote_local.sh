#!/usr/bin/env bash

# previously:
# copy test data from s3 bucket to s3 tmp bucket:
#   $ ./edp_aws_s3_sync_remote_remote.sh
# afterwards, can compare local source data referenced in
# edp_aws_s3_sync_local_remote.sh with resulting local ./ecco-tmp-2

edp_aws_s3_sync \
    --src s3://ecco-tmp-1 \
    --dest ./ecco-tmp-2 \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile saml-pub \
    --log DEBUG
    #--dryrun \

