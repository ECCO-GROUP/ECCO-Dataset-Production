#!/usr/bin/env bash

# test job task list generation using locally-stored ECCO results:

edp_create_job_task_list \
    --jobfile ./jobs_example.txt \
    --ecco_source_root ../data/ecco_results \
    --ecco_destination_root . \
    --ecco_grid_loc ../data/ecco_grids/V4r4/grid_ECCOV4r4 \
    --ecco_mapping_factors_loc ../data/ecco_mapping_factors/V4r4 \
    --ecco_metadata_loc '../../ECCO-v4-Configurations/ECCOv4 Release 4/metadata' \
    --outfile edp_create_job_file_list_test_1.json \
    --cfgfile ../../processing/configs/product_generation_config_updated.yaml \
    --log DEBUG
    #--ecco_destination_root s3://ecco_dataset_production_test \
