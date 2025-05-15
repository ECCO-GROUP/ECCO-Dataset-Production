#!/usr/bin/env bash


# Run in a directory with one or more json task files (extension .json)
# Loops through each one; sends them all off to run in parallel
# takes one argument, the root name of the task file json file(s)


EDP_root_dir=/home/jpluser/edp
CFGFILE="${EDP_root_dir}/ECCO-Dataset-Production/processing/configs/product_generation_config_updated.yaml"

# loop through all task files
TASKFILE=${1}

echo $TASKFILE

#KEYGEN='/usr/local/bin/aws-login.darwin.amd64'
#PROFILE='saml-pub'

edp_generate_dataproducts --tasklist ${TASKFILE} --cfgfile ${CFGFILE} --log DEBUG 
#> LOG_$TASKFILE.log 2> LOG_$TASKFILE.log 
#    #--keygen ${KEYGEN} \
#    #--profile ${PROFILE} \

