import os
import argparse
from pathlib import Path
import sys
import yaml

from . import utils_general as utils_general
from . import utils_json as utils_json
from . import cdf_extract as cdf_extract


def write_data_attributes_tables(base_dir, config_dictionary):
    """
        This function writes the data product tables to the latex document.
    """
    
    # Write attribute (global and variable) tables
    utils_json.write_attributes_tables_tex(base_dir, config_dictionary)

    # Write example variable tables
    variable_granules_parent_directory = os.path.join(base_dir, "/".join(config_dictionary["variable_files_native_dir"].split("/")[:-1]))
    variable_granule_directories = [root for root, dirs, files in os.walk(variable_granules_parent_directory) if not dirs]
    for granule_directory in variable_granule_directories:
        granule_type, grid_type = utils_general.get_granule_and_grid_types(granule_directory)
        cdf_extract.latex_example_netcdf(base_dir, config_dictionary, grid_type)


def write_datasets(base_dir, config_dictionary):

    # The following line depends on the project file tree structure
    granules_parent_directory = os.path.join(base_dir, "/".join(config_dictionary["coordinate_files_native_dir"].split("/")[:-2]))
    granule_directories = [root for root, dirs, files in os.walk(granules_parent_directory) if not dirs]

    for granule_directory in granule_directories:
        print(f"writing latex table and figure files for granules in the '{'/'.join(granule_directory.split('/')[-2:])}' directory" )
        cdf_extract.data_products(base_dir, config_dictionary, granule_directory)




if __name__ == '__main__':
    """
        This script generates the LaTeX outline for the dataset.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument('--type', required=True, type=str,
                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
    args = parser.parse_args()

    write_datasets(args.type)

