#!/bin/bash
set -eo pipefail

# Replace the following paths with your own paths

SOURCE_ROOT="s3://.../source/V4r6/"
DEST_ROOT="s3://.../destination/V4r6/"
GRID_LOC="s3://.../ecco_grids/V4r5/grid_ECCOV4r5/"
FACTORS_LOC="s3://.../ecco-mapping-factors/V4r5/"
METADATA_DIR="s3://.../ecco-metadata/V4r6/"
CONFIG="s3://.../ecco-metadata/V4r6/config_V4r6.yaml"

mkdir -p ./tasklists
for jobfile in ./jobs/*_jobs.txt; do
    outfile="./tasklists/$(basename "${jobfile%.txt}").json"
    edp_create_job_task_list \
        --jobfile "$jobfile" \
        --ecco_source_root "$SOURCE_ROOT" \
        --ecco_destination_root "$DEST_ROOT" \
        --ecco_grid_loc "$GRID_LOC" \
        --ecco_mapping_factors_loc "$FACTORS_LOC" \
        --ecco_metadata_loc "$METADATA_DIR" \
        --ecco_cfg_loc "$CONFIG" \
        --outfile "$outfile" \
        -l INFO
done
