#!/bin/bash
set -eo pipefail
source venv/bin/activate

TASKLIST="./tasklists"

mkdir -p ./first-middle-last-tasklists
edp_subset_tasklists "$TASKLIST" \
  --output_dir ./first-middle-last-tasklists \
  --mode first-middle-last \
  -l INFO
