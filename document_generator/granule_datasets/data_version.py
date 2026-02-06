#BL: THIS WAS NOT WORKING IF A DIRECTORY BEGINNING WITH "v" DIDN'T ALREADY EXIST!!!!!!

import json
import subprocess
import os
# README First
# This script is to specify the dataset version and release info. 
# This information is saved into a json file to be used in the 
# following step for the document generator. Do this first !!!
#----------------------------------------------------------------

data_version = str(input("Insert the dataset version number you're willing \n to generate the document for. Note, only \n the number is required ...:  "))
data_release = str(input("Insert the dataset release number you're willing \n to generate the document for. Note, only \n the number is required ...:  "))
data_vers = f"v{data_version}r{data_release}"
data_vers_json = {"version": str(data_version),
                  "release": str(data_release),
                  "dataset_version": data_vers}

# A bit clunky - if a directory beginning with "v" is found, this try block (which I added) will run.  If not, then the body of the "else"
# clause in this try block needs to be run separately
try:
    output = subprocess.check_output('ls -d v*/', shell=True, text=True)

    if data_vers in output:
        # Write the variable to a JSON file
        with open('data_version.json', 'w') as f:
            json.dump(data_vers_json, f)
        print("Dataset sample exists already. You're all set! Go for it!")
    else:
        print("Folder need to be created for "+data_vers)
        # Creating the folder the dataset version sample
        os.system("mkdir "+data_vers)
        # Write the variable to a JSON file
        with open('data_version.json', 'w') as f:
            json.dump(data_vers_json, f)
        print("Folder is now created for "+data_vers+". You're all set! Go for it!")

# Act of faith that an exit code of 1 simply means we didn't find a match (I can't figure out how to print more than that)
except subprocess.CalledProcessError as error:
    if error.returncode == 1:
        print("Folder need to be created for "+data_vers)
        # Creating the folder the dataset version sample
        os.system("mkdir "+data_vers)
        # Write the variable to a JSON file
        with open('data_version.json', 'w') as f:
            json.dump(data_vers_json, f)
        print("Folder is now created for "+data_vers+". You're all set! Go for it!")

