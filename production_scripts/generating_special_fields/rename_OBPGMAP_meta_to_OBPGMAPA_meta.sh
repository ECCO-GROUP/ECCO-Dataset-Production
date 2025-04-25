#!/bin/bash

# run from the OBPGMAPA_* directory
# assumes the OBPGMAP_* directory is one level up at ../OBPGMAP_*

## takes one argument, one of

#freq="day_mean"
#freq="mon_mean"
#freq="day_snap"

freq=$1

# delete existing meta files
rm ./*meta

# copy over original meta files 
cp ../OBPGMAP_"$freq"/*meta .

for f in OBPGMAP_"$freq"*.meta; do
  echo $f

  # Rename the file
  newfile="${f/OBPGMAP_$freq/OBPGMAPA_$freq}"

  cp "$f" "$newfile"

  # Replace OBP with OBPAnoma inside the new file
  sed -i "s/OBPGMAP\s/OBPGMAPA/" "$newfile"
 
  rm "$f"

done
