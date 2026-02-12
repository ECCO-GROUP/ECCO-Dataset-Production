# MUST FIX LACK OF READABLE INDENTATION IN THE EXAMPLE TABLES (cdf_extract.py)

import os
import z_utility_scripts.utils_docgen as utils_docgen
import z_utility_scripts.latex_outline as latex_outline

def main():
    
    version_string = "v4r4" # Should not be hardcoded!   
   
    overwrite_granules = False # User can choose to skip downloading previousy downloaded granules

    #z_utility_scripts.utils_docgen.download_granules(version_string, overwrite_granules)
    #z_utility_scripts.latex_outline.write_data_attributes_tables(version_string)
    #z_utility_scripts.latex_outline.write_datasets(version_string)
    
    utils_docgen.download_granules(version_string, overwrite_granules)
    latex_outline.write_data_attributes_tables(version_string)
    latex_outline.write_datasets(version_string)


if __name__ == "__main__":
    main()
