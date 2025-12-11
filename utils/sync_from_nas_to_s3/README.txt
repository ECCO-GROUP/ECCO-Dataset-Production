Syncing ECCO mds output files to S3
-----------------------------------
Ian Fenty 2025-04-23


Preliminaries
================
Step 1. create a conda environment with the packages required to run the JPL Access-Key-Generation (AKG) codes

In this example, I name the conda environment "JPLAKG".  The package "spec list" is in the file
'conda_environment_JPL_AKG.txt'

$ conda create --name JPLAKG --file conda_environment_JPL_AKG.txt 


Step 2: Make sure you have the JPL Access-Key-Generation codes
--------------------------------------------------------------
I put mine here:
AKG_PATH=/nobackup/ifenty/git_repos_others/Access-Key-Generation


Step 3. identify location of model output 
-----------------------------------------------------------------------------------
the 'root directory' below which we have

diags_inst/
diags_mon/
diags_day/

I like to make my own custom directory and then softlink into Ou's files

For example, my path
ifenty@pfe23:~/nobackup/AWS_staging/V4r5_20250417/

has softlinks to Ou's V4r5 files:
diags_monthly -> /nobackupp18/owang/runs/V4r5/V4r5/diags_monthly 
diags_daily -> /nobackupp18/owang/runs/V4r5/V4r5/diags_daily 
diags_inst -> /nobackupp18/owang/runs/V4r5/V4r5/diags_inst 


Step 4: make custom version of sync program 
----------------------------------------------------------------------
The sync program: sync_v4r5_to_s3.sh
has hard-coded paths for V4r5.

Make a copy of this program and use the model output paths


Sync Execution
==============

After the preliminaries, syncing is simple

Sync Step 1: spin up a while loop of aws credential updating
----------------------------------------------------------------------
note: requires that JPLAKG conda environment is installed and good to go

Run this command in a tmux window:

$ ./loop_aws_credential_update.sh


 
Sync Step 2: run your sync program 
----------------------------------------------------------------------
after making your own sync program you should be OK to run
$ custom_sync_program.sh diags_daily
$ custom_sync_program.sh diags_mon
$ custom_sync_program.sh diags_inst

