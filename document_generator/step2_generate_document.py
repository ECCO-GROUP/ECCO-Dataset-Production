# Feels funny that the user must construct a json "groupings" file for each of their granules/variables, 
# but maybe that's fine.  Just seems like those should be made automatically based on the config
# file, since I think the data they hold comes from there....

# MUST FIX LACK OF READABLE INDENTATION IN THE EXAMPLE TABLES (cdf_extract.py)

import os
import general.utility_scripts.utils_docgen as utils_docgen
import general.utility_scripts.latex_outline as latex_outline

def main():
    
    version_string = "v4r4" # Should not be hardcoded!   
   
    overwrite_files_switch = False # User can choose to avoid overwriting files - should also not be hardcoded!

    latex_outline.write_data_attributes_tables(version_string, overwrite_files_switch)
    latex_outline.write_datasets(version_string, overwrite_files_switch)

if __name__ == "__main__":
    main()
