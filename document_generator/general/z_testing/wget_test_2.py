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


dataset_dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/utilities/document_files/v4r4/granule_datasets/granule_files/granules_latlon"
#dataset_dir = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/utilities/document_files/v4r4/granule_datasets/coordinate_files/coordinate_files_latlon"

try:
    

    # This should be read from the .netrc file
    hostname = "urs.earthdata.nasa.gov"

    

    # Parse the .netrc file
    netrc_info = netrc.netrc()


    # Get the login, account, and password for a specific machine
    auth_info = netrc_info.authenticators(hostname)
    if auth_info:
        login, account, password = auth_info
        # Use the requests library with the credentials
        #url = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_GEOMETRY_05DEG_V4R4/GRID_GEOMETRY_ECCO_V4r4_latlon_0p50deg.nc"
        url = "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ECCO_L4_SSH_05DEG_DAILY_V4R4/SEA_SURFACE_HEIGHT_day_mean_2017-12-29_ECCO_V4r4_latlon_0p50deg.nc"
        response = requests.get(url, auth=(login, password), stream=True)
        response.raise_for_status() # Raise an exception for bad status codes
       
        filename = os.path.join(dataset_dir,Path(url).name)

        print(filename)

        with open(filename, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=8192):
                fd.write(chunk)
        

        print(f"Successfully downloaded {url} using requests.")
        # Process the response content as needed
    else:
        print(f"No entry found for {hostname} in .netrc file.")

except FileNotFoundError:
    print("Error: .netrc file not found.")
except netrc.NetrcParseError as e:
    print(f"Error parsing .netrc file: {e}")
except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")
