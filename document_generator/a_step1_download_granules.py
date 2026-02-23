import os
import general.utility_scripts.utils_general as utils_general

def main():
    ecco_version_string = "v4r4" # Should not be hardcoded!   
    overwrite_granules_switch = True
    #overwrite_granules_switch = False 
    print("\ndownloading granules:\n")
    utils_general.download_granules(ecco_version_string, overwrite_granules_switch)
    print()
if __name__ == "__main__":
    main()
