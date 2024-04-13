#!/usr/bin/env bash

edp_create_job_file_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root ../ecco_model_granules \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --cfgfile ./product_generation_config.yaml \
    --log DEBUG
    #--log INFO
