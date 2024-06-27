#!/usr/bin/env bash
#
# get/update some representative ECCO results test data using same topology as
# is found in AWS S3 bucket
#

# TODO: pass in credentials, profile via command-line, add options for some of
# the hard-code data, e.g., ecco_prefixes, file counts, etc.

# some configuration data that may need to be changed depending on the user's
# environment:

profile=saml-pub

# test data location(s) in AWS S3; may need to be changed if cloud storage
# configuration changes:

s3_bucket=s3://ecco-model-granules/
s3_prefix=V4r4/diags_monthly/
ecco_prefixes=(
    ETAN_mon_mean/
    SSH_mon_mean/
    SSHIBC_mon_mean/
    SSHNOIBC_mon_mean/
    UVEL_mon_mean/
    VVEL_mon_mean/
    WVELMASS_mon_mean/)

# number of mds file pairs (.data/.meta) to get:

file_pair_count=3

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
        --profile ${profile} \
        --output text | awk '{print $4}'))

    for f in ${all_mds_files[@]:0:$((file_pair_count*2))}; do
        aws s3 cp ${s3_bucket}${s3_prefix}${ecco_prefix}${f} . --profile ${profile}
    done
    cd ${root_dir}
done

