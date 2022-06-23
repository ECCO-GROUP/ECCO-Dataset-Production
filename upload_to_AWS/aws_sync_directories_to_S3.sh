#!/bin/bash

# note current directory
cur_dir=`pwd`

# get PID of *this* bash job
bash_pid=$$

# count number of sub-directories here
num_sub_dirs=`find . -type d -maxdepth 1 |wc -l`

# update AWS credentials 
# there are many ways to do this.  Here we
# use the JPL Github Package "Access-Key-Generation"
# linked to the ecco_production AWS account
echo ".. updating credentials, elapsed seconds $SECONDS"
sh ./update_AWS_cred_ecco_production.sh
let "time_of_last_cred_update=SECONDS"
echo "time of last cred update $time_of_last_cred_update"


# spawn up to max_synchronous_uploads upload jobs
max_synchronous_uploads=20

# loop through all sub-directories, spawn off
# upload jobs using "aws sync"
dir_counter=0
for dir in *; do

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
  cd $cur_dir/$dir
  # delete log file
  rm /tmp/$dir

  # before uploading, update AWS credentials
  # if it's been an hour or more since the last credential update
  let "delta=SECONDS - time_of_last_cred_update"
  if [ $delta -ge 3600 ]
  then
    echo ".. updating credentials, elapased seconds $SECONDS"
    sh ~/bash-scripts/update_AWS_cred_ecco_production.sh
    let "time_of_last_cred_update=SECONDS"
  fi

  # spawn off a new upload process
  aws s3 sync . s3://ecco-model-granules/V4r4/diags_inst/$dir --no-progress --profile saml-pub > /tmp/$dir &

  # increment dir_counter by 1
  let "dir_counter=dir_counter+1"

  echo "spawned transfer of $dir_counter of $num_sub_dirs directories"
  echo "total elapsed time $SECONDS"
# end loop through all sub-directories
done

echo "waiting for the final set of directories to complete their upload .."
wait
echo "all done!"
echo "total elapsed time $SECONDS"
