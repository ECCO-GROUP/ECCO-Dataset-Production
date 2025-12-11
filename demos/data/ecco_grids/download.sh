#!/usr/bin/env bash

# download ECCO grid data from AWS using AWS CLI (note that the CLI is
# used instead of edp_aws_s3_sync since only a single file is to be
# downloaded, and not an entire bucket/prefix).

# Also, note that it will be necessary to first log into AWS if
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

# test data location(s) in AWS S3; may need to be changed if cloud storage
# configuration changes:

s3_bucket=s3://ecco-model-granules/
s3_prefix=${ver}/llc90_grid/
gridfile=grid_ECCO${ver}.tar.gz

src=${s3_bucket}${s3_prefix}${gridfile}
dest=./${ver}

if [[ -v keygen ]]; then
    ${keygen}
fi

if [[ ! -d ${dest} ]]; then
    mkdir -p ${dest}
fi

if [[ -v profile ]]; then
    aws s3 cp ${src} ${dest} --profile ${profile}
else
    aws s3 cp ${src} ${dest}
fi

cd ${dest} && tar -xf *gz
