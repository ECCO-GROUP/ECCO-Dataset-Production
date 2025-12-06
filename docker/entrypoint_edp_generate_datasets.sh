#!/usr/bin/env bash
#---------------------------------------------------------------------
#
# Name: entrypoint_edp_generate_datasets.sh
#
# Docker ENTRYPOINT script for ECCO Dataset Production
# edp_generate_datasets command-line utility.
#
# Usage: To be invoked via Docker ENTRYPOINT instruction.
#
# Notes: TASKLIST must be a defined environment variable.
#
#---------------------------------------------------------------------

time edp_generate_datasets \
    --tasklist ${TASKLIST} \
    --log DEBUG
