#!/usr/bin/env bash

#
# test job task list generation using locally-stored ECCO results
#
# usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

edp_create_job_task_list \
    --jobfile ./SSH_native_latlon_mon_mean_jobs.txt \
    --ecco_source_root ../data/ecco_results/${ver} \
    --ecco_destination_root ./ \
    --ecco_grid_loc ../data/ecco_grids/${ver}/grid_ECCO${ver} \
    --ecco_mapping_factors_loc ../data/ecco_mapping_factors/${ver} \
    --ecco_metadata_loc "../ECCO-v${ver:1:1}-Configurations/ECCOv${ver:1:1} Release ${ver:3:1}/metadata" \
    --ecco_cfg_loc ../../config/config_${ver}.yaml \
    --outfile SSH_native_latlon_mon_mean_tasks_${ver}.json \
    --log DEBUG
