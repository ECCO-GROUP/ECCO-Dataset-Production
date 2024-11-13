#!/usr/bin/env bash

# test job task list generation using a limited set of  AWS S3-stored ECCO
# results:

# previously:
# $ aws s3 mb ecco-tmp
# $ ../edp_aws_s3_sync/edp_aws_s3_sync_local_remote.sh

edp_create_job_task_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root s3://ecco-tmp/V4r4 \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --ecco_grid_loc ../data/ecco_grids/V4r4/grid_ECCOV4r4 \
    --ecco_mapping_factors_loc ../data/ecco_mapping_factors/V4r4 \
    --ecco_metadata_loc '../../ECCO-v4-Configurations/ECCOv4 Release 4/metadata' \
    --outfile edp_create_job_file_list_test_2.json \
    --cfgfile ../../processing/configs/product_generation_config_updated.yaml \
    --keygen $(which aws-login-pub.darwin.amd64) \
    --profile saml-pub \
    --log DEBUG
