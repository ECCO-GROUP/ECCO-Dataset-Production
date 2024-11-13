#!/usr/bin/env bash

# create a job task list from a jobfile
# first argument is the name of the job task file (text)
# second argument is the name of the output task file (json)


EDP_root_dir=../../../
echo ${EDP_root_dir}
ls ${EDP_root_dir}

edp_create_job_task_list \
    --jobfile ${1} \
    --ecco_source_root ../data/ecco_results/${ver} \
    --ecco_destination_root "./V4r4_sav_test" \
    --ecco_grid_loc "${EDP_root_dir}/ECCO-Dataset-Production/tests/data/ecco_grids/V4r4/grid_ECCOV4r4" \
    --ecco_mapping_factors_loc "${EDP_root_dir}/ECCO-Dataset-Production/tests/data/ecco_mapping_factors/V4r4" \
    --ecco_metadata_loc "${EDP_root_dir}/ECCO-v4-Configurations/ECCOv4 Release 4/metadata" \
    --outfile "${2}" \
    --cfgfile "${EDP_root_dir}/ECCO-Dataset-Production/processing/configs/product_generation_config_updated.yaml" \
    --log DEBUG

