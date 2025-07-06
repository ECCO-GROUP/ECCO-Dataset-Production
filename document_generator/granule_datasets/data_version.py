import json
import subprocess
import os
# README First
# This script is to specify the dataset version and release info. 
# This information is saved into a json file to be used in the 
# following step for the document generator. Do this first !!!
#----------------------------------------------------------------
data_version = int(input("Insert the dataset version number you're willing \n to generate the document for. Note, only \n the number is required ...:  "))
data_release = str(input("Insert the dataset release number you're willing \n to generate the document for. Note, only \n the number is required ...:  "))
data_vers = "v"+str(data_version)+"r"+data_release
data_vers_json = {"version": str(data_version),
                  "release": str(data_release),
                  "dataset_version": data_vers}
print(data_vers)

output = subprocess.check_output('ls -d v*/', shell=True, text=True)
print(type(output))
print(output)
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
    print("Folder is now created for"+data_vers+". You're all set! Go for it!")
#----------------------------------------------------------------