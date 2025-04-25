#!/bin/bash


# run from the OBPAnoma_* directory
# original metadata files are in ../OBP_* directory

## takes one argument, one of

#freq="day_mean"
#freq="mon_mean"
#freq="day_snap"

freq=$1

# deletes all old meta files in this directory
rm *meta

# copies over original meta files from ../OBP_*/
cp ../OBP_"$freq"/*meta .

for f in OBP_"$freq"*.meta; do
  echo $f

  # Rename the file
  newfile="${f/OBP_$freq/OBPAnoma_$freq}"

  cp "$f" "$newfile"

  # Replace OBP with OBPAnoma inside the new file
  sed -i "s/OBP\s\s\s\s\s/OBPAnoma/" "$newfile"

  rm "$f"

done
