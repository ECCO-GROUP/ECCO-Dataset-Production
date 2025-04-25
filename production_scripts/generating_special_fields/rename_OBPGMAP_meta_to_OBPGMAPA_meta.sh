#!/bin/bash

## takes one argument, one of

#freq="day_mean"
#freq="mon_mean"
#freq="day_snap"

freq=$1

for f in OBPGMAP_"$freq"*.meta; do
  echo $f

  # Rename the file
  newfile="${f/OBPGMAP_$freq/OBPGMAPA_$freq}"

  cp "$f" "$newfile"

  # Replace OBP with OBPAnoma inside the new file
  sed -i "s/OBPGMAP\s/OBPGMAPA/" "$newfile"

done
