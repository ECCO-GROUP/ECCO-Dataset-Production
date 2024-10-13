#!/usr/bin/env bash

#TASKLIST=SSH_native_latlon_mon_mean_tasks.json
CFGFILE=../../processing/configs/product_generation_config_updated.yaml
KEYGEN=/usr/local/bin/aws-login-pub.darwin.amd64
PROFILE=saml-pub

edp_generate_dataproducts \
    --tasklist ${1} \
    --cfgfile ${CFGFILE} \
    --log DEBUG 
#    --keygen ${KEYGEN} \
#    --profile ${PROFILE}
