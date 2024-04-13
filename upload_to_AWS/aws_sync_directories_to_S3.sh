#!/bin/bash

cred_type=$1
cred_file_name=$2
region=$3
login_file_dir=$4
s3_dir=$5
files_dir=$6
update_AWS_cred_file=$7
dryrun=$8

# update_AWS_cred_file=../processing/src/utils/aws_login/update_AWS_cred_ecco_production.sh

# note current directory
cur_dir=`pwd`

# get PID of *this* bash job
bash_pid=$$

# count number of sub-directories here
# num_sub_dirs=`find . -type d -maxdepth 1 | wc -l`
num_sub_dirs=`find $files_dir/* -type d -maxdepth 0 | wc -l`
# gjm:
echo 'num_sub_dirs: ' ${num_sub_dirs}

# update AWS credentials 
# there are many ways to do this.  Here we
# use the JPL Github Package "Access-Key-Generation"
# linked to the ecco_production AWS account
echo ".. updating credentials, elapsed seconds $SECONDS"
sh $update_AWS_cred_file $cred_type $cred_file_name $region $login_file_dir
let "time_of_last_cred_update=SECONDS"
echo "time of last cred update $time_of_last_cred_update"

# make logs dir
mkdir $cur_dir/logs

# spawn up to max_synchronous_uploads upload jobs
# gjm testing:
#max_synchronous_uploads=1
max_synchronous_uploads=20

# loop through all sub-directories, spawn off
# upload jobs using "aws sync"
dir_counter=0
for dir in $files_dir/*; do

  # count number of child processes. each child
  # process is uploading one subdirectory
  children=`ps -eo ppid |grep -w $bash_pid`
  num_children=`echo $children | wc -w`
  let "num_children = num_children -1"

  # if the number of child processes < max_synchronous_uploads
  # then proceed, otherwise, go into a sleep/wait loop
  # until at least one child process is finished
  while [ $num_children -ge $max_synchronous_uploads ]
  do
    # wait 5 seconds then check the number of 
    # child processes again.  
    sleep 5 
    # count number of child processes NOW
    children=`ps -eo ppid |grep -w $bash_pid`
    num_children=`echo $children | wc -w`
    let "num_children = num_children -1"
  done 

  # now we have an open upload process slot
  echo "$dir"
  # cd $cur_dir/$dir
  cd $dir

  # get file dir (i.e. "diags_monthly/SSH_mon_mean")
  file_dir=$(basename "$(dirname "$dir")")/$(basename "$dir")

  # create current file log dir
  mkdir -p $cur_dir/logs/$file_dir

  # before uploading, update AWS credentials
  # if it's been an hour or more since the last credential update
  let "delta=SECONDS - time_of_last_cred_update"
  if [ $delta -ge 3600 ]
  then
    echo ".. updating credentials, elapased seconds $SECONDS"
    sh $update_AWS_cred_file $cred_type $cred_file_name $region $login_file_dir
    let "time_of_last_cred_update=SECONDS"
  fi

  # spawn off a new upload process
  if [ $dryrun = "True" ]; then
    aws s3 sync . s3://$s3_dir/$file_dir --dryrun --no-progress --profile saml-pub > $cur_dir/logs/$file_dir/log.txt &
  else
    aws s3 sync . s3://$s3_dir/$file_dir --no-progress --profile saml-pub > $cur_dir/logs/$file_dir/log.txt &
  fi


  # increment dir_counter by 1
  let "dir_counter=dir_counter+1"

  echo "spawned transfer $dir_counter of $num_sub_dirs directories"
  echo "total elapsed time $SECONDS"
# end loop through all sub-directories
done

echo "waiting for the final set of directories to complete their upload .."
wait
echo "all done!"
echo "total elapsed time $SECONDS"
