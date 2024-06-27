#!/usr/bin/env bash

# test job task list generation using locally-stored ECCO results:

edp_create_job_task_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root ../data/ecco_results \
    --ecco_destination_root s3://ecco_dataset_production_test \
    --ecco_grid_dir ../data/ecco_grids/V4r4/grid_ECCOV4r4 \
    --outfile edp_create_job_file_list_test_1.json \
    --cfgfile ../../processing/configs/product_generation_config_updated.yaml \
    --log DEBUG
