#!/usr/bin/env bash

# download ECCO mapping factors from AWS using AWS CLI

# Note that it will be necessary to first log into AWS if
# running within an AWS federated login service, e.g. JPL's SSO domain
# (ref. https://cloudwiki.jpl.nasa.gov/display/cloudcomputing/AWS+CLI+and+BOTO+with+JPL+Credentials),
# hence the optional arguments keygen and profile

usage() { echo 'download.sh -v ver -k keygen -p profile   # ver = V4r4, V4r5, etc.'; }

while getopts ":v:k:p:h" opt; do
    case $opt in
    v ) ver=$OPTARG ;;
    k ) keygen=$OPTARG ;;
    p ) profile=$OPTARG ;;
    h ) usage
        exit 1 ;;
    \?) usage
        exit 1 ;;
    esac
done

if [[ ! -v ver ]]; then
    usage && exit 1
fi

s3_bucket=s3://ecco-mapping-factors/
s3_prefix=${ver}/
log='INFO'

src=${s3_bucket}${s3_prefix}
dest=./${ver}

if [[ -v keygen ]]; then
    ${keygen}
fi

if [[ ! -d ${dest} ]]; then
    mkdir -p ${dest}
fi

if [[ -v keygen && -v profile ]]; then
    edp_aws_s3_sync \
        --src ${src} \
        --dest ${dest} \
        --keygen ${keygen} \
        --profile ${profile} \
        --log ${log}
else
    edp_aws_s3_sync \
        --src ${src} \
        --dest ${dest} \
        --log ${log}
fi

