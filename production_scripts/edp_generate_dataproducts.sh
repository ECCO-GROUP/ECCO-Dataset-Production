#!/usr/bin/env bash

#
# ECCO Dataset Production Dockerfile ENTRYPOINT script
#

# Note: TASKLIST must be a defined environment variable

time edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --log DEBUG
