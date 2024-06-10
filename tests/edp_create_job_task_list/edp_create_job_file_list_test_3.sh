#!/usr/bin/env bash

# test job task list generation using AWS S3-stored full ECCO results, s3 output
# endpoint.
#
# note: depends on presence of full results set at s3://ecco-model-granules

edp_create_job_task_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root s3://ecco-model-granules/V4r4 \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --outfile edp_create_job_file_list_test_3.json \
    --cfgfile ../../processing/configs/product_generation_config_updated.yaml \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile_name saml-pub \
    --log DEBUG
    #--cfgfile ./product_generation_config.yaml \
    #--log INFO
