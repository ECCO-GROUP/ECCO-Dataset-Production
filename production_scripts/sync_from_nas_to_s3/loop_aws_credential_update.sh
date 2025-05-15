#!/bin/bash
CONDA_ENV=JPLAKG                                                                                                                                                            

# sourcing bashrc puts conda path in environment
source ~/.bashrc 

conda activate $CONDA_ENV 

# update AWS credential every 30minutes
# works for about 1 full day
while true; do date; source ./update_aws_credentials.sh ; sleep 1800; done
