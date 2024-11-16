#!/usr/bin/env bash

TASKLIST=SSH_native_latlon_mon_mean_tasks.json
CFGFILE=../../processing/configs/product_generation_config_updated.yaml

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --cfgfile ${CFGFILE} \
    --log DEBUG \
