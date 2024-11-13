#!/usr/bin/env bash

#
# download ECCO mapping factors from AWS using
# ECCO-Dataset-Production's command-line edp_aws_s3_sync utility
# (which, for this simple case, could also be done just about as
# easily using the AWS CLI, "aws s3 cp"):
#

ver='V4r4'   # V4r5, etc.

src="s3://ecco-mapping-factors/${ver}/"
dest="./${ver}"
log='INFO'

# the following are only necessary if running within an AWS federated
# login service, e.g. JPL's SSO domain
# (ref. https://cloudwiki.jpl.nasa.gov/display/cloudcomputing/AWS+CLI+and+BOTO+with+JPL+Credentials):
keygen='/usr/local/bin/aws-login.darwin.amd64'
profile='saml-pub'

if [[ ! -d ${dest} ]]; then
    mkdir -p ${dest}
fi

edp_aws_s3_sync \
    --src ${src} \
    --dest ${dest} \
    --keygen ${keygen} \
    --profile ${profile} \
    --log ${log}
