#!usr/bin/env bash

# pass yml file as first argument
file=$1
past_header=false
while read -r line; do

    echo $line
    echo "past header ${past_header}"

    if [ "$past_header" = true ]
    then
     repo="${line:2}"
     tmp="--yes"
     conda_cmd="conda install ${tmp} ${repo} " 
     echo "$conda_cmd"
     eval "${conda_cmd}"
    fi

    if [ "$line" = "dependencies:" ]
    then
      echo "dependencies"
      past_header=true
    fi
done <$file 
