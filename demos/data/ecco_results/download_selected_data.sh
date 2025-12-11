#!/usr/bin/env bash
 
# get/update some representative ECCO results test data using same topology as
# is found in AWS S3 bucket

# Note that it will be necessary to first log into AWS if running within an AWS
# federated login service, e.g. JPL's SSO domain
# (ref. https://cloudwiki.jpl.nasa.gov/display/cloudcomputing/AWS+CLI+and+BOTO+with+JPL+Credentials),
# hence the optional arguments keygen and profile

# TODO: figure out why 'set -e' won't exit for loop if incorrect
# keygen/profile input

set -e

usage() { echo 'download_selected_data.sh -v ver -n file_pair_count -k keygen -p profile   # ver = V4r4, V4r5, etc., -n 3 default'; }

while getopts ":v:k:p:n:h" opt; do
    case $opt in
        v ) ver=$OPTARG ;;
        k ) keygen=$OPTARG ;;
        p ) profile=$OPTARG ;;
        n ) file_pair_count=$OPTARG ;;
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
s3_prefix=${ver}/diags_monthly/
ecco_prefixes=(
    ETAN_mon_mean/
    SALT_mon_mean/
    SSH_mon_mean/
    SSHIBC_mon_mean/
    SSHNOIBC_mon_mean/
    THETA_mon_mean/
    UVEL_mon_mean/
    VVEL_mon_mean/
    WVELMASS_mon_mean/)

if [[ -v keygen ]]; then
    ${keygen}
fi

if [[ -v profile ]]; then
    profileopt="--profile ${profile}"
fi

# go get the data:

root_dir=$(pwd)
for ecco_prefix in ${ecco_prefixes[@]}; do
    test_data_dir=./${s3_prefix}${ecco_prefix}
    if [[ ! -d ${test_data_dir} ]] ; then
        mkdir -p ${test_data_dir}
    fi
    cd ${test_data_dir}

    all_mds_files=($(aws s3 ls \
        ${s3_bucket}${s3_prefix}${ecco_prefix} \
        ${profileopt} \
        --output text | awk '{print $4}'))

    for f in ${all_mds_files[@]:0:$((${file_pair_count:?3}*2))}; do
        aws s3 cp ${s3_bucket}${s3_prefix}${ecco_prefix}${f} . ${profileopt}
    done
    cd ${root_dir}

done
