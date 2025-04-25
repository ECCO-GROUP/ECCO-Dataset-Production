#!/bin/bash

## takes one argument, one of

#freq="day_mean"
#freq="mon_mean"
#freq="day_snap"

freq=$1

for f in OBP_"$freq"*.meta; do
  echo $f

  # Rename the file
  newfile="${f/OBP_$freq/OBPAnoma_$freq}"

  cp "$f" "$newfile"

  # Replace OBP with OBPAnoma inside the new file
  sed -i "s/OBP\s\s\s\s\s/OBPAnoma/" "$newfile"


done
