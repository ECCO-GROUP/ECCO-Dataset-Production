#!/bin/bash
set -eo pipefail

TASKLIST="./tasklists"

mkdir -p ./first-middle-last-tasklists
python3 ./ECCO-Dataset-Production/utils/extract_first_mid_last_tasks.py \
  --tasklist $TASKLIST \
  --dest ./first-middle-last-tasklists \
  --log INFO
