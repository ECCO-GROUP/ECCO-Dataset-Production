# BL: It's annoying that both "native" and "natives" is used in directory/filenames in previous work - 
# this adds hackiness to the code below, and will make it less portable.  So, we should address this.
# (I won't try to make things uniform for the moment, since I don't want to break 'dependencies' in the project)

import argparse
from pathlib import Path
import sys
import os

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# BL: This adds the base "document_generator" directory to the Python PATH, so that modules and files(?) with paths relative to it can be used
# (Maybe this should only be a temporary approach).

document_generator_dir = str(Path(__file__).parent.parent)
sys.path.append(document_generator_dir)

import readJSON
import cdf_extract
import sections.dataset_sections as dataset_sections


def write_coordinate_dataset_tables(config_dict_static, config_dict_for_functions, data_type):
    for json_filepath in config_dict_static[config_dict_for_functions['config_static_json_list_name']]:
        if f"{data_type}_coords" in json_filepath or f"{data_type[:-1]}_coords" in json_filepath:
            for dataset_directory in config_dict_static[config_dict_for_functions['config_static_dataset_dir_list_name']]:
                if f"{data_type}_coords" in dataset_directory or f"{data_type[:-1]}_coords" in dataset_directory:
                    for image_directory in config_dict_static[config_dict_for_functions['config_static_image_dir_list_name']]:
                        if f"{data_type}_plots_coords" in image_directory or f"{data_type[:-1]}_plots_coords" in image_directory:
                            for latex_filepath in config_dict_static[config_dict_for_functions['config_static_latex_list_name']]:
                                if f"{data_type}_coords_dataset_tables" in latex_filepath or f"{data_type[:-1]}_coords_dataset_tables" in latex_filepath:
                                    #latex_lines_coordinates = dataset_sections.data_products(json_filepath, dataset_directory, 
                                    #                                                         image_directory, data_type)
                                    latex_lines_coordinates = dataset_sections.data_products(json_filepath, f"{dataset_directory}/", 
                                                                                             f"{image_directory}/", data_type)
                                    with open(latex_filepath, 'w') as output_file:
                                        output_file.write('\n'.join(latex_lines_coordinates))
                                    return


def write_variable_dataset_tables(config_dict_static, config_dict_for_functions, data_type):
# Ok, the directory names are annoying, because I can't use simple logic to differentiate between
# coordinate directories and dataset directories.  But, I don't want to change path names now,
# since so much is hardcoded in this project...  TO DO!
    for json_filepath in config_dict_static[config_dict_for_functions['config_static_json_list_name']]:
        if json_filepath.split("/")[-1] == f"{data_type}" or json_filepath.split("/")[-1] == f"{data_type[:-1]}": 
            for dataset_directory in config_dict_static[config_dict_for_functions['config_static_dataset_dir_list_name']]:
                if dataset_directory.split("/")[-1] == f"{data_type}" or dataset_directory.split("/")[-1] == f"{data_type[:-1]}":
                    for image_directory in config_dict_static[config_dict_for_functions['config_static_image_dir_list_name']]:
                        if image_directory.split("/")[-1] == f"{data_type}_plots" or image_directory.split("/")[-1] == f"{data_type[:-1]}_plots":
                            for latex_filepath in config_dict_static[config_dict_for_functions['config_static_latex_list_name']]:
                                if f"{data_type}_dataset_tables" in latex_filepath or f"{data_type[:-1]}_dataset_tables" in latex_filepath:
                                    #latex_lines_variabes = dataset_sections.data_products(json_filepath, dataset_directory, 
                                    #                                                         image_directory, data_type)
                                    latex_lines_variabes = dataset_sections.data_products(json_filepath, f"{dataset_directory}/", 
                                                                                             f"{image_directory}/", data_type)
                                    with open(latex_filepath, 'w') as output_file:
                                        output_file.write('\n'.join(latex_lines_variabes))
                                    return
        

def write_data_attributes_tables(config_file_static, config_file_user):
    """
        This function writes the data product tables to the latex document.
    """
    
    with open(config_file_static,'r') as stream:
        config_dict_static = yaml.safe_load(stream)
   
    # These should be called "keys", since they're key names in a dictionary
    config_static_json_list_name = 'ecco_attributes'

    config_static_latex_list_name = 'global_attributes_latex_lines'
    global_attributes_latex_lines = config_dict_static[config_static_latex_list_name]
    
    config_static_latex_list_name = 'latex_support_data_product'
    
    found_flag = False
    search_string = 'global_attributes'
    for json_filepath in config_dict_static[config_static_json_list_name]:
        if search_string in json_filepath:
            global_attributes_dictionary_list = readJSON.obtain_json_data(json_filepath)
            global_attributes_latex_lines.extend(readJSON.establish_table(global_attributes_dictionary_list))
            global_attributes_latex_lines.append(r'\end{longtable}')
            for latex_filepath in config_dict_static[config_static_latex_list_name]:
                if search_string in latex_filepath:
                    with open(latex_filepath, 'w') as output_file:
                        output_file.write('\n'.join(global_attributes_latex_lines))
                    found_flag = True
                    break
            break

    if not found_flag:
        sys.exit(f"'{search_string}' elements not found in config file {config_file_static}")

    config_static_latex_list_name = 'variable_attributes_latex_lines'
    variable_attributes_latex_lines = config_dict_static[config_static_latex_list_name]
    
    config_static_latex_list_name = 'latex_support_data_product'
    
    found_flag = False
    search_string = 'variable_attributes'
    for json_filepath in config_dict_static[config_static_json_list_name]:
        if search_string in json_filepath:
            variable_attributes_dictionary_list = readJSON.obtain_json_data(json_filepath)
            variable_attributes_latex_lines.extend(readJSON.establish_table(variable_attributes_dictionary_list))
            variable_attributes_latex_lines.append(r'\end{longtable}')
            for latex_filepath in config_dict_static[config_static_latex_list_name]:
                if search_string in latex_filepath:
                    with open(latex_filepath, 'w') as output_file:
                        output_file.write('\n'.join(variable_attributes_latex_lines))
                    found_flag = True
                    break
            break

    if not found_flag:
        sys.exit(f"'{search_string}' elements not found in config file {config_file_static}")
    
    with open(config_file_user,'r') as stream:
        config_dict_user = yaml.safe_load(stream)
    
    config_user_variable_list_name = 'variable_type_list'
    for data_type in config_dict_user[config_user_variable_list_name]:
        example_latex_lines = cdf_extract.latex_example_netcdf(data_type)
        for latex_filepath in config_dict_static[config_static_latex_list_name]:
            if f"example_{data_type}_table" in latex_filepath:
                with open(latex_filepath, 'w') as output_file:
                    output_file.write('\n'.join(example_latex_lines))


def write_datasets(config_file_static, config_file_user):
    
    with open(config_file_static,'r') as stream:
        config_dict_static = yaml.safe_load(stream)
    with open(config_file_user,'r') as stream:
        config_dict_user = yaml.safe_load(stream)

    config_dict_for_functions = {}    
    config_dict_for_functions['config_static_json_list_name'] = 'ecco_groupings'
    config_dict_for_functions['config_static_dataset_dir_list_name'] = 'dataset_directories'
    config_dict_for_functions['config_static_image_dir_list_name'] = 'image_directories'
    config_dict_for_functions['config_static_latex_list_name'] = 'latex_support_dataset'
   
    config_user_variable_list_name = 'variable_type_list'
    
    for data_type in config_dict_user[config_user_variable_list_name]:
        write_coordinate_dataset_tables(config_dict_static, config_dict_for_functions, data_type)
        write_variable_dataset_tables(config_dict_static, config_dict_for_functions, data_type)

#    else:
#        print(f"Invalid dataset type: {dataset_type}. Please select from 'Native', 'Latlon', '1D'.")



# Commenting-out "main" for now

#if __name__ == '__main__':
#    """
#        This script generates the LaTeX outline for the dataset.
#    """
#    parser = argparse.ArgumentParser(description='Write datasets to latex')
#    parser.add_argument('--type', required=True, type=str,
#                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
#    args = parser.parse_args()

#    write_datasets(args.type)

# usage: main.py [-h] --type TYPE
# python sections/latex_outline.py --type 1D

