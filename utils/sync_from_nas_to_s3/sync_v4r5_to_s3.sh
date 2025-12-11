#!/bin/bash

# Sync files/directories from nobackup to S3
# --> IMPORTANT NOTE: REQUIRES AN AWS CREDENTIAL FILE
# --> WITH IDENTITY CALLED SAML-PUB

# uses GNU parallel, NPROCS=15 processes (see last line)

# program takes one command line argument, one of 
# diags_daily
# diags_monthly
# diags_inst

# The command-line argument goes into the bash environment variable $1

# The routine expects subdirectories under $ROOTDIR/$1 
# to contain mitgcm diagnostic variable output like,
# ./ETAN_mon_mean

# How it works:
# -------------
# first go into $SYNC_DIR
# then for each subdirectory below $SYNC_DIR,
# create an "aws s3 sync" command and append/pipe that command to the file
#   $1_sync_instructions.txt

# so you get something like "diags_daily_sync_instructions.txt"

# each line of the sync instructions file looks something like
#    aws s3 sync SOURCE_PATH S3_DESTINATION_PATH --SOME ARGUMENTS

# then it reverses the order of the "aws s3 sync" commands in a 
# new text file (don't ask me why)

# the reversed order filename is called 
#   "$sync_fname"_r.txt

# so you get something like "diags_daily_sync_instructions_r.txt"

# then it uses gnu parallel to execute the aws s3 sync commands
# using NPROCS at a time

# LOCATION OF FILES ON NOBACKUP
# ... ROOT_DIR is the path above the diags_daily/ diags_mon/ and diags_inst/
ROOT_DIR="/home5/ifenty/nobackup/AWS_staging/V4r5_20250417"
# ... SYNC_DIR is the $ROOT_DIR/diags_X directory
SYNC_DIR=$ROOT_DIR/$1

# DESTINATION OF FILES ON S3
S3_BUCKET="s3://ecco-model-granules/V4r5_20250417/$1"

# Number of parallel 'aws s3 sync' processes.
# I found 15 worked ok pfe23 head node. Not optimized for performance
NPROCS=15


echo $ROOT_DIR
echo $SYNC_DIR
echo $S3_BUCKET


# build a list of directories to sync
cd $ROOT_DIR

# define the sync_instructions file
sync_fname="$1"_sync_instructions.txt
rm $sync_fname 
touch $sync_fname 

# make a directory with transfer logs
log_dir="$1"_logs
mkdir -p $log_dir
# clear it out
rm $log_dir/*.log

# loop through each subdirectory of $SYNC_DIR
for d in "$SYNC_DIR"/*; do
   echo "destination " "$S3_BUCKET/$(basename "$d")"

   # define the name of the log file
   logfilename=$(basename "$d").log

   # echo the aws s3 sync command for this subdirectory to $sync_fname
   # note use of 'saml-pub' profile
   echo "aws s3 sync "$d" "$S3_BUCKET/$(basename "$d")" \
 --exact-timestamps \
 --follow-symlinks \
 --profile saml-pub \
 --no-progress > "$log_dir"/"$logfilename" " >> $sync_fname 

done

# show the $sync_fname exists
ls -l $sync_fname
# reverse its order
tac $sync_fname > "$sync_fname"_r.txt

# run through the reversed aws s3 sync commands using GNU parallel
parallel -j $NPROCS < "$sync_fname"_r.txt
