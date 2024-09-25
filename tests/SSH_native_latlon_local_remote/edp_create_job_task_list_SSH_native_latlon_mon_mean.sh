#!/usr/bin/env bash

# test job task list generation using locally-stored ECCO results:

ver='V4r4'

edp_create_job_task_list \
    --jobfile ./SSH_native_latlon_mon_mean_jobs.txt \
    --ecco_source_root s3://ecco-model-granules/${ver} \
    --ecco_destination_root ./${ver} \
    --ecco_grid_loc s3://ecco-model-granules/${ver}/llc90_grid/grid_ECCO${ver}.tar.gz \
    --ecco_mapping_factors_loc s3://ecco-mapping-factors/${ver} \
    --ecco_metadata_loc '../../ECCO-v4-Configurations/ECCOv4 Release 4/metadata' \
    --outfile SSH_native_latlon_mon_mean_tasks.json \
    --cfgfile ../../processing/configs/product_generation_config_updated.yaml \
    --log DEBUG
