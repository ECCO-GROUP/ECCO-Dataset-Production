#!/usr/bin/env bash

# test job task list generation using locally-stored ECCO results:

ver='V4r4'

edp_create_job_task_list \
    --jobfile ./SSH_native_latlon_mon_mean_jobs.txt \
    --ecco_source_root ../data/ecco_results/${ver} \
    --ecco_destination_root ./ \
    --ecco_grid_loc ../data/ecco_grids/${ver}/grid_ECCO${ver} \
    --ecco_mapping_factors_loc ../data/ecco_mapping_factors/${ver} \
    --ecco_metadata_loc '../ECCO-v4-Configurations/ECCOv4 Release 4/metadata' \
    --outfile SSH_native_latlon_mon_mean_tasks.json \
    --cfgfile ../../config/config_${ver}.yaml \
    --log DEBUG
