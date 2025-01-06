#!/usr/bin/env bash

# one-off script to create V4r4 configuration submodule metadata soft links,
# allowing metadata files to be accessed as ecco_dataset_production package
# resource data

relpath=../../../../../
srcdir='ECCO-v4-Configurations/ECCOv4 Release 4/metadata'
latest_podaac_metadata_file=PODAAC_datasets-revised_20210226.5.csv

for filename in $(ls "${relpath}/${srcdir}"); do
    extension=${filename##*.}
    if [ $extension = json ] || [ $filename = $latest_podaac_metadata_file ]; then
        ln -s "${relpath}/${srcdir}/${filename}" ${filename}
    fi
done
