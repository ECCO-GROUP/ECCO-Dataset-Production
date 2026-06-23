#!/Users/brucel/miniforge3/envs/ecco_env1/bin/bash

fileList=("$1"/*)

searchString="coverage_content_type"

varTypeArray=("float" "int")

for fileName in "${fileList[@]}"; do

    if [[ "$fileName" == ""  ]]; then
        continue
    fi

    coordCommandArray=("ncatted -a metadata_link,global,o,c,ShortName=TEEBEEDEEbutWeirdNum $fileName")
    coordCommandArray+=("ncatted -a long_name,rSurfC,o,c,TEEBEEDEE $fileName")
    coordCommandArray+=("ncatted -a long_name,rLowC,o,c,TEEBEEDEE $fileName")
    coordCommandArray+=("ncatted -a long_name,dyU,o,c,TEEBEEDEE $fileName")
    coordCommandArray+=("ncatted -a long_name,dyF,o,c,TEEBEEDEE $fileName")
    coordCommandArray+=("ncatted -a long_name,dxV,o,c,TEEBEEDEE $fileName")

    variableCommandArray=("ncatted -a metadata_link,global,o,c,TBDShortName=TBD $fileName")

    grepResults=( "$(ncks -M $fileName | grep $searchString)")

    if [[ ${grepResults[0]} != ""  ]]; then
        echo "attributes added to coordinate file: $fileName"
        for commandString in "${coordCommandArray[@]}"; do
            $commandString
        done
    else
        echo "attributes added to variable file: $fileName"
        for commandString in "${variableCommandArray[@]}"; do
            $commandString
        done
    fi

done
