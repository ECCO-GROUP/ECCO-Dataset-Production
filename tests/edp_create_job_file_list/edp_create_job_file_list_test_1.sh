#!/usr/bin/env bash

# test job task list generation using locally-stored ECCO results:

edp_create_job_file_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root ../ecco_model_granules \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --outfile edp_create_job_file_list_test_1.json \
    --cfgfile ./product_generation_config.yaml \
    --log DEBUG
    #--log INFO