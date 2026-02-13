import os
import re
import xarray as xr
import glob
import subprocess
import yaml
from pathlib import Path
import sys
import requests
import netrc

utilities_base_dir = str(Path(__file__).parent.parent)
sys.path.append(utilities_base_dir)

# ---------- pretend "def ____" was here

try:
    # Parse the .netrc file
    netrc_info = netrc.netrc()

    # Get the login, account, and password for a specific machine
    auth_info = netrc_info.authenticators('urs.earthdata.nasa.gov')
    if auth_info:
        login, account, password = auth_info
        # Use the requests library with the credentials
        url = "https://<hostname>/path/to/file"
        response = requests.get(url, auth=(login, password))
        response.raise_for_status() # Raise an exception for bad status codes
        print(f"Successfully downloaded {url} using requests.")
        # Process the response content as needed
    else:
        print(f"No entry found for <hostname> in .netrc file.")

except FileNotFoundError:
    print("Error: .netrc file not found.")
except netrc.NetrcParseError as e:
    print(f"Error parsing .netrc file: {e}")
except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")


version_string = "v4r4"
overwrite_granules = True


config_file_static = os.path.join(utilities_base_dir, "config_files", version_string, "config_static.yaml")
with open(config_file_static,'r') as stream:
    config_dictionary_static = yaml.safe_load(stream)

possible_grid_types = config_dictionary_static["possible_grid_types"]
grid_types_to_ignore = []

config_file_user = os.path.join(utilities_base_dir, "config_files", version_string, "config_user.yaml")
with open(config_file_user,'r') as stream:
    config_dictionary_user = yaml.safe_load(stream)

for grid_type in possible_grid_types:
    for key in config_dictionary_user.keys():
        if grid_type in key and grid_type not in grid_types_to_ignore:
            grid_types_to_ignore.append(grid_type)

            for granule_type in config_dictionary_static[f"possible_granule_types_{grid_type}"]:
                dataset_dir_pre = config_dictionary_static[f"{granule_type}_files_{grid_type}_dir"]
                dataset_dir = os.path.join(os.path.realpath(utilities_base_dir), dataset_dir_pre)
                os.makedirs(os.path.join(dataset_dir), exist_ok=True)                
                
                url_list = config_dictionary_user[f"{grid_type}_{granule_type}_file_paths_remote"]
               
                

                #if not overwrite_granules:
                    #existing_files = glob.glob(f"{dataset_dir}/*.nc")
                    #for local_path in existing_files:
                    #    local_path_not_in_url_list_index_list = [Path(local_path).stem not in url for url in url_list]
                    #    url_list = [url for url, boolean in zip(url_list, local_path_not_in_url_list_index_list) if boolean]
                
                """
                # No error handling for now
                for url in url_list:
                    if not overwrite_granules:
                        subprocess.run(["wget", "--no-verbose", "--no-clobber", "--continue", url, "-P", dataset_dir])
                    else:
                        subprocess.run(["wget",  "--no-verbose", "--continue", url, "-P", dataset_dir])
                """




