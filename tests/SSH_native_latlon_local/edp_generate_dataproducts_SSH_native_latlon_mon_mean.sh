#!/usr/bin/env bash

TASKLIST=SSH_native_latlon_mon_mean_tasks.json
CFGFILE=../../config/config_V4r4.yaml

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --cfgfile ${CFGFILE} \
    --log DEBUG
