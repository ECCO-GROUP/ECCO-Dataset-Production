#!/usr/bin/env bash

# download ECCO grid data from AWS using AWS CLI (note that the CLI is
# used instead of edp_aws_s3_sync since only a single file is to be
# downloaded, and not an entire bucket/prefix).

# Also, note that it will be necessary to first log into AWS if
# running within an AWS federated login service, e.g. JPL's SSO domain
# (ref. https://cloudwiki.jpl.nasa.gov/display/cloudcomputing/AWS+CLI+and+BOTO+with+JPL+Credentials),
# hence the optional arguments keygen and profile

usage() { echo 'download.sh -k keygen -p profile'; }

ver='V4r4'   # V4r5, etc.
src="s3://ecco-model-granules/${ver}/llc90_grid/grid_ECCO${ver}.tar.gz"

while getopts ":k:p:h" opt; do
    case $opt in
	k ) keygen=$OPTARG ;;
	p ) profile=$OPTARG ;;
	h ) usage
	    exit 1 ;;
	\?) usage
	    exit 1 ;;
    esac
done

if [[ -v keygen ]]; then
    ${keygen}
fi

if [[ ! -d ${ver} ]]; then
    mkdir -p ${ver}
fi

cd ${ver}
if [[ -v profile ]]; then
    aws s3 cp ${src} . --profile ${profile}
else
    aws s3 cp ${src} .
fi

tar -xf *gz
