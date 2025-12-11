#!/bin/sh

UNAME=ifenty
CONDA_ENV=JPLAKG
AKG_PATH=/nobackup/ifenty/git_repos_others/Access-Key-Generation

# sourcing bashrc puts conda path in environment
source ~/.bashrc

# activating this envionrment makes boto3 and other junk available
# this works on pfe. needs to have proper version of conda and libraries 
conda activate $CONDA_ENV

# remove the old aws credential log
rm ~/.aws/aws.cred.log

python $AKG_PATH/aws-login.py -pub -r us-west-2 -t 14400 -U $UNAME -l -v > ~/.aws/aws.cred.log
