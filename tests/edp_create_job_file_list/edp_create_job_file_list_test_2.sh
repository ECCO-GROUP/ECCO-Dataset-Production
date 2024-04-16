#!/usr/bin/env bash

# test job task list generation using AWS S3-stored ECCO results:

# previously:
# $ aws s3 mb ecco-tmp
# $ ../edp_aws_s3_sync/edp_aws_s3_sync_local_remote.sh

edp_create_job_file_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root s3://ecco-tmp \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --outfile edp_create_job_file_list_test_2.json \
    --cfgfile ./product_generation_config.yaml \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile_name saml-pub \
    --log DEBUG
