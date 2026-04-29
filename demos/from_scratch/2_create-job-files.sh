#!/bin/bash
set -eo pipefail
source venv/bin/activate

GROUPINGS_FILE="./ECCO-v4-Configurations/ECCOv4 Release 6/metadata/groupings_for_native_datasets.json"

mkdir -p ./jobs
edp_create_job_files \
  -l INFO \
  --groupings_file "$GROUPINGS_FILE" \
  --output ./jobs
