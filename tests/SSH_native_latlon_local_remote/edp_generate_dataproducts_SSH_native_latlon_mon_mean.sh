#!/usr/bin/env bash

ver='V4r4'

TASKLIST=SSH_native_latlon_mon_mean_tasks.json
CFGFILE=../../config/config_${ver}.yaml
KEYGEN=/usr/local/bin/aws-login-pub.darwin.amd64
PROFILE=saml-pub

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --cfgfile ${CFGFILE} \
    --log DEBUG \
    --keygen ${KEYGEN} \
    --profile ${PROFILE}
