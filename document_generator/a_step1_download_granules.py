# Feels funny that the user must construct a json "groupings" file for each of their granules/variables, 
# but maybe that's fine.  Just seems like those should be made automatically based on the config
# file, since I think the data they hold comes from there....

# MUST FIX LACK OF READABLE INDENTATION IN THE EXAMPLE TABLES (cdf_extract.py)

import os
import general.utility_scripts.utils_general as utils_general

def main():
    
    ecco_version_string = "v4r4" # Should not be hardcoded!   
   
    overwrite_granules_switch = True
    #overwrite_granules_switch = False 

    print("\n downloading granules: \n")

    utils_general.download_granules(ecco_version_string, overwrite_granules_switch)

if __name__ == "__main__":
    main()
