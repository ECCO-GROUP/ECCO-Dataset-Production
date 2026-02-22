# BL: added "overwrite_switch" as an argument, thinking that we should allow for only re-plotting newly downloaded/overwritten granules....
# But maybe I'll implement that later

import os
import argparse
from pathlib import Path
import sys
import yaml

general_base_dir = str(Path(__file__).parent.parent)
sys.path.append(general_base_dir)

import utility_scripts.readJSON as readJSON
import utility_scripts.cdf_extract as cdf_extract
import utility_scripts.utils_docgen as utils


def write_data_attributes_tables(ecco_version_string, overwrite_switch):
    """
        This function writes the data product tables to the latex document.
    """

    # Construct path to the configuration file, and load the file into a dictionary
    config_file = os.path.join(general_base_dir, "config_files", ecco_version_string, "config.yaml")
    with open(config_file,'r') as stream:
        config_dictionary = yaml.safe_load(stream)

    # Construct the "global_attributes.tex" file
    global_attributes_latex_lines = config_dictionary['global_attributes_latex_lines']
    global_attributes_json_list = readJSON.obtain_json_data(config_dictionary['global_attributes_json_file'])
    global_attributes_latex_lines.extend(readJSON.establish_table(global_attributes_json_list))
    global_attributes_latex_lines.append(r'\end{longtable}')
    global_attributes_latex_output_file = os.path.join(general_base_dir, config_dictionary["global_attributes_tex_file"])
    Path(global_attributes_latex_output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(global_attributes_latex_output_file, 'w') as output_file:
        output_file.write('\n'.join(global_attributes_latex_lines))

    # Construct the "variable_attributes.tex" file
    variable_attributes_latex_lines = config_dictionary['variable_attributes_latex_lines']
    variable_attributes_json_list = readJSON.obtain_json_data(config_dictionary['variable_attributes_json_file'])
    variable_attributes_latex_lines.extend(readJSON.establish_table(variable_attributes_json_list))
    variable_attributes_latex_lines.append(r'\end{longtable}')
    variable_attributes_latex_output_file = os.path.join(general_base_dir, config_dictionary["variable_attributes_tex_file"])
    Path(variable_attributes_latex_output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(variable_attributes_latex_output_file, 'w') as output_file:
        output_file.write('\n'.join(variable_attributes_latex_lines))

    # Construct the "example_{grid_type}_table.tex" files (which draw from variable granules, not coordinate granules)
    possible_grid_types = config_dictionary["possible_grid_types"]
    grid_types_to_ignore = []

    config_file_user = os.path.join(general_base_dir, "config_files", ecco_version_string, "config_user.yaml")
    with open(config_file_user,'r') as stream:
        config_dictionary_user = yaml.safe_load(stream)
    
    for grid_type in possible_grid_types:
        for key in config_dictionary_user.keys():
            if grid_type in key and "variable" in key and grid_type not in grid_types_to_ignore:
                grid_types_to_ignore.append(grid_type)
                grid_example_latex_lines = cdf_extract.latex_example_netcdf(grid_type, config_dictionary[f"variable_files_{grid_type}_dir"])
                grid_example_latex_output_file = os.path.join(general_base_dir, config_dictionary[f"example_{grid_type}_table_tex_file"])
                Path(grid_example_latex_output_file).parent.mkdir(parents=True, exist_ok=True)
                with open(grid_example_latex_output_file, 'w') as output_file:
                    output_file.write('\n'.join(grid_example_latex_lines))
                break





def write_datasets(ecco_version_string, overwrite_switch):

    config_file = os.path.join(general_base_dir, "config_files", ecco_version_string, "config.yaml")
    with open(config_file,'r') as stream:
        config_dictionary = yaml.safe_load(stream)
   
    # Obtain relative path of the parent granules directory (may need adjustment of following line if file structure changes), then make list of all granules
    granules_parent_directory = os.path.join(general_base_dir, "/".join(config_dictionary["coordinate_files_native_dir"].split("/")[:-2]))
    granule_directories = [root for root, dirs, files in os.walk(granules_parent_directory) if not dirs]

    for granule_directory in granule_directories:
        granule_latex_lines = cdf_extract.data_products(ecco_version_string, config_dictionary, granule_directory)

        #granule_latex_output_file = os.path.join(general_base_dir, config_dictionary[f'{granule_type}_table_{grid_type}_tex_file'])
        #Path(granule_latex_output_file).parent.mkdir(parents=True, exist_ok=True)
        #with open(granule_latex_output_file, 'w') as output_file:
        #    output_file.write('\n'.join(granule_latex_lines))




if __name__ == '__main__':
    """
        This script generates the LaTeX outline for the dataset.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument('--type', required=True, type=str,
                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
    args = parser.parse_args()

    write_datasets(args.type)

