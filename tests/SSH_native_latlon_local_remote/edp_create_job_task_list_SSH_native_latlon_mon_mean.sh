#!/usr/bin/env bash

#
# test job task list generation using S3-stored ECCO results, local granule output:
#
# usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

# depending on AWS S3 and/or local configuration:
ecco_source_root="s3://ecco-model-granules/${ver}"
ecco_grid_loc="s3://ecco-model-granules/${ver}/llc90_grid"
ecco_mapping_factors_loc="s3://ecco-mapping-factors/${ver}"

edp_create_job_task_list \
    --jobfile ./SSH_native_latlon_mon_mean_jobs.txt \
    --ecco_source_root ${ecco_source_root} \
    --ecco_destination_root ./ \
    --ecco_grid_loc ${ecco_grid_loc} \
    --ecco_mapping_factors_loc ${ecco_mapping_factors_loc} \
    --ecco_metadata_loc "../ECCO-v${ver:1:1}-Configurations/ECCOv${ver:1:1} Release ${ver:3:1}/metadata" \
    --ecco_cfg_loc ../../config/config_${ver}.yaml \
    --outfile SSH_native_latlon_mon_mean_tasks_${ver}.json \
    --log DEBUG
