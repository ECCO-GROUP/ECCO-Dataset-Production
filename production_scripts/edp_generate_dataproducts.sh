#!/usr/bin/env bash

#
# ECCO Dataset Production ENTRYPOINT script
#

# Note: TASKLIST must be in the environment

edp_generate_dataproducts \
    --tasklist ${TASKLIST} \
    --log DEBUG
