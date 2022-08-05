#!/bin/bash

# Access-Key-Generation is a JPL github repo: 
# https://github.jpl.nasa.gov/cloud/Access-Key-Generation

cred_type=$1
cred_file_name=$2
region=$3
login_file_dir=$4

# Create credentials using python (aws-login.py) file
if [[ $cred_type == "python" ]]; then
    python3 $login_file_dir/$cred_file_name -r $region
fi

# Create credentials using binary (.darwin or .linux) file
if [[ $cred_type == "binary" ]]; then
    $cred_file_name -r $region
fi