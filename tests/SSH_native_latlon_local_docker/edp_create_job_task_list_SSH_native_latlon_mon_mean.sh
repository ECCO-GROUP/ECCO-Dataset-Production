#!/usr/bin/env bash

#
# variation on ../SSH_native_latlon_local/\
# edp_create_job_task_list_SSH_native_latlon_mon_mean.sh
# to demonstrate running in local Docker container mode:
# - sets up local bind-mountable ./input and ./output directories
# - calls edp_create_job_task_list to create a task list using these i/o directories
#
# usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>
# where: <ver> = 'V4r4', 'V4r5', etc.
#

ver=${1:?usage: edp_create_job_task_list_SSH_native_latlon_mon_mean.sh <ver>   # ver = 'V4r4', 'V4r5', etc.}

input_root=./input
output_root=./output

mkdir -p ${input_root} ${output_root}

cp -r ../data ${input_root}
cp -r "../ECCO-v${ver:1:1}-Configurations/ECCOv${ver:1:1} Release ${ver:3:1}/metadata" ${input_root}
cp -r ../../config ${input_root}

edp_create_job_task_list \
    --jobfile ./SSH_native_latlon_mon_mean_jobs.txt \
    --ecco_source_root ${input_root}/data/ecco_results/${ver} \
    --ecco_destination_root ${output_root} \
    --ecco_grid_loc ${input_root}/data/ecco_grids/${ver}/grid_ECCO${ver} \
    --ecco_mapping_factors_loc ${input_root}/data/ecco_mapping_factors/${ver} \
    --ecco_metadata_loc ${input_root}/metadata \
    --ecco_cfg_loc ${input_root}/config/config_${ver}.yaml \
    --outfile ${input_root}/SSH_native_latlon_mon_mean_tasks_${ver}.json \
    --log DEBUG
