# BL:  I am still a bit confused - we are loading in Latex tables, and then... writing to them... so they are templates?
# i.e. document/latex/data_product/variable_attributes.tex
# I guess I'm wondering if that will be provided to the user to begin with.  I know that there are other template Tex files
# which must be provided, but for these ones, which are written to with "w", I'm not sure if that's the intention?

import argparse
from pathlib import Path
import sys
import os

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# BL: I modified these imports, because some of the absolute paths were wrong.  It still feels perhaps risky
# to hardcode these paths, i.e.:   document_generator_dir = str(Path(__file__).parent.parent); sys.path.append(document_generator_dir);
# This adds the base "document_generator" directory to the Python PATH, so that modules and files(?) with paths relative to it can be used
# (Maybe this should only be a temporary approach).

document_generator_dir = str(Path(__file__).parent.parent)
sys.path.append(document_generator_dir)

import readJSON
import cdf_extract
import sections.dataset_sections as dataset_sections

def write_data_attributes_tables(config_file_static, config_file_user):
#def write_data_attributes_tables(config_file):
    """
        This function writes the data product tables to the latex document.
    """

    with open(config_file_static,'r') as stream:
        config_dict_static = yaml.safe_load(stream)
    
    config_json_list_name = 'ecco_attributes'
    config_latex_list_name = 'latex_support_data_product'

    config_latex_list_name = 'global_attributes_latex_lines'
    global_attributes_latex_lines = config_dict_static[config_latex_list_name]
    
    found_flag = False
    search_string = 'global_attributes'
    for i_config in range(len(config_dict_static[config_json_list_name])):
        if search_string  in config_dict_static[config_json_list_name][i_config]:
            global_attributes_dictionary_list = readJSON.obtain_json_data(config_dict_static[config_json_list_name][i_config])
            global_attributes_latex_lines.extend(readJSON.establish_table(global_attributes_dictionary_list))
            global_attributes_latex_lines.append(r'\end{longtable}')
            for j_config in range(len(config_dict_static[config_latex_list_name])):
                if search_string in config_dict_static[config_latex_list_name][j_config]:
                    with open(config_dict_static[config_latex_list_name][j_config], 'w') as output_file:
                        output_file.write('\n'.join(global_attributes_latex_lines))
                    found_flag = True
                    break
            break

    if not found_flag:
        sys.exit(f"'{search_string}' elements not found in config file {config_file_static}")


    config_latex_list_name = 'variable_attributes_latex_lines'
    variable_attributes_latex_lines = config_dict_static[config_latex_list_name]

    found_flag = False
    search_string = 'variable_attributes'
    for i_config in range(len(config_dict_static[config_json_list_name])):
        if search_string  in config_dict_static[config_json_list_name][i_config]:
            variable_attributes_dictionary_list = readJSON.obtain_json_data(config_dict_static[config_json_list_name][i_config])
            variable_attributes_latex_lines.extend(readJSON.establish_table(variable_attributes_dictionary_list))
            variable_attributes_latex_lines.append(r'\end{longtable}')
            for j_config in range(len(config_dict_static[config_latex_list_name])):
                if search_string in config_dict_static[config_latex_list_name][j_config]:
                    with open(config_dict_static[config_latex_list_name][j_config], 'w') as output_file:
                        output_file.write('\n'.join(variable_attributes_latex_lines))
                    found_flag = True
                    break
            break

    if not found_flag:
        sys.exit(f"'{search_string}' elements not found in config file {config_file_static}")



    with open(config_file_user,'r') as stream:
        config_dict_user = yaml.safe_load(stream)
    
    config_list_name = 'variable_type_list'
    for data_type in config_dict_user[config_list_name]:
        example_latex_lines = cdf_extract.latex_example_netcdf(data_type)
        for i_config in range(len(config_dict_user[config_latex_list_name])):
            if f"example_{data_type}_table" in config_dict_user[config_latex_list_name][i_config]:
                with open(config_dict_user[config_latex_list_name][i_config], 'w') as output_file:
                    output_file.write('\n'.join(example_latex_lines))




def write_datasets(dataset_type:str)->None:
    data_version_to_get, _ ,_ = cdf_extract.get_dataset_version()

    # -----------------------------------------------------------------------------------------------------------
    # All of these json files seem internal to ECCO, with some internal to a specific release; 
    # shouldn't they be safely stored higher up in the file tree?
    # -----------------------------------------------------------------------------------------------------------
    #native_coords_groupings = 'granule_datasets/'+data_version_to_get+'/natives_coords.json'
    native_coords_groupings = 'granule_datasets/'+data_version_to_get+'/native_coords.json'
    native_groupings_json = 'granule_datasets/'+data_version_to_get+'/ECCOv4r4_groupings_for_native_datasets.json'
    latlon_coords_groupings = 'granule_datasets/'+data_version_to_get+'/latlon_coords.json'
    latlon_groupings_json = 'granule_datasets/'+data_version_to_get+'/ECCOv4r4_groupings_for_latlon_datasets.json'
    oneD_groupings_json = 'granule_datasets/'+data_version_to_get+'/ECCOv4r4_groupings_for_1D_datasets.json'
    # -----------------------------------------------------------------------------------------------------------

    native_coords_dir = 'granule_datasets/'+data_version_to_get+'/natives_coords/'
    native_ds_dir = 'granule_datasets/'+data_version_to_get+'/natives/'
    latlon_coords_dir = 'granule_datasets/'+data_version_to_get+'/latlon_coords/'
    latlon_ds_dir = 'granule_datasets/'+data_version_to_get+'/latlon/'
    oneD_ds_dir = 'granule_datasets/'+data_version_to_get+'/oneD/'

    native_coords_images_dir = 'images/plots/'+data_version_to_get+'/native_plots_coords/'
    native_images_dir = 'images/plots/'+data_version_to_get+'/native_plots/'
    latlon_coords_images_dir = 'images/'+data_version_to_get+'/plots/latlon_plots_coords/'
    latlon_images_dir = 'images/plots/'+data_version_to_get+'/latlon_plots/'
    oneD_images_dir = 'images/plots/'+data_version_to_get+'/oneD_plots/'

    if dataset_type == 'native':
        native_coord_latex_lines = dataset_sections.data_products(native_coords_groupings, native_coords_dir, 
                                                                  native_coords_images_dir, dataset_type + " Coordinates")
        with open('document/latex/dataset/native_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_coord_latex_lines))

        native_latex_lines = dataset_sections.data_products(native_groupings_json, native_ds_dir,
                                                            native_images_dir, dataset_type)
        with open('document/latex/dataset/native_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_latex_lines))

    elif dataset_type == 'latlon':
        latlon_coord_latex_lines = dataset_sections.data_products(latlon_coords_groupings, latlon_coords_dir,
                                                                  latlon_coords_images_dir, dataset_type) #+ " Coordinates"
        with open('document/latex/dataset/latlon_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_coord_latex_lines))

        latlon_latex_lines = dataset_sections.data_products(latlon_groupings_json, latlon_ds_dir,
                                                            latlon_images_dir, dataset_type)
        with open('document/latex/dataset/latlon_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_latex_lines))

    elif dataset_type == '1D':
        oneD_latex_lines = dataset_sections.data_products(oneD_groupings_json, oneD_ds_dir,
                                                          oneD_images_dir, dataset_type)
        with open('document/latex/dataset/oneD_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(oneD_latex_lines))

    else:
        print(f"Invalid dataset type: {dataset_type}. Please select from 'Native', 'Latlon', '1D'.")


if __name__ == '__main__':
    """
        This script generates the LaTeX outline for the dataset.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument('--type', required=True, type=str,
                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
    args = parser.parse_args()

    write_datasets(args.type)

# usage: main.py [-h] --type TYPE
# python sections/latex_outline.py --type 1D

