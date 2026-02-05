#BL: Everything that's hardcoded should be read from a config file (like the hardcoded tex lines, etc)

#import cdf_reader
import argparse
from pathlib import Path
import sys
import os

# BL: I modified these imports, because some of the absolute paths were wrong.  It still feels perhaps risky
# to hardcode these paths, i.e.:   docgen_dir = str(Path(__file__).parent.parent); sys.path.append(docgen_dir);

docgen_dir = str(Path(__file__).parent.parent)
sys.path.append(docgen_dir)

import readJSON
import cdf_extract
import sections.dataset_sections as ds_s

def write_data_attributes_tables():
    """
        This function writes the data product tables to the latex document.
    """
    global_attributes_lines = [
        r'% Table 8-1 Mandatory global attributes for GDS 2.0 netCDF data files',
        r'\begin{longtable}{|p{0.276\textwidth}|p{0.092\textwidth}|p{0.46\textwidth}|p{0.092\textwidth}|}',
        r'\caption{Mandatory global attributes for GDS 2.0 netCDF data files}',
        r'\label{tab:global-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{lightgray} \textbf{Global Attribute Name} & \textbf{Type} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]
    global_attributes_dictionary_list = readJSON.obtain_json_data('data/global_attributes.json')
    global_attributes_lines.extend(readJSON.establish_table(global_attributes_dictionary_list))
    global_attributes_lines.append(r'\end{longtable}')
    with open('document/latex/data_product/global_attributes.tex', 'w') as output_file:
        output_file.write('\n'.join(global_attributes_lines))

    var_attributes_lines = [
        r'% Table 8-2 Variable attributes for GDS 2.0 netCDF data files',
        r'\begin{longtable}{|p{0.168\textwidth}|p{0.20\textwidth}|p{0.46\textwidth}|p{0.092\textwidth}|}',
        r'\caption{Table 8-2. Variable attributes for GDS 2.0 netCDF data files}',
        r'\label{tab:variable-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{lightgray} \textbf{Variable Attribute Name} & \textbf{Format} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]
    variable_attributes_dictionary_list = readJSON.obtain_json_data('data/variable_attributes.json')
    var_attributes_lines.extend(readJSON.establish_table(variable_attributes_dictionary_list))
    var_attributes_lines.append(r'\end{longtable}')
    with open('document/latex/data_product/variable_attributes.tex', 'w') as output_file:
        output_file.write('\n'.join(var_attributes_lines))

# DO WE REALLY WANT ALL OF THIS HARDCODING???????

    example_native_lines = cdf_extract.latex_example_netcdf('native')
    with open('document/latex/data_product/example_native_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_native_lines))


    example_latlon_lines = cdf_extract.latex_example_netcdf('latlon')
    with open('document/latex/data_product/example_latlon_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_latlon_lines))

    example_1D_lines = cdf_extract.latex_example_netcdf('1D')
    with open('document/latex/data_product/example_oneD_table.tex', 'w') as output_file:
        output_file.write('\n'.join(example_1D_lines))








def write_datasets(dataset_type:str)->None:
    data_version_to_get, _ ,_ = cdf_extract.get_dataset_version()

    # -----------------------------------------------------------------------------------------------------------
    # All of these json files seem internal to ECCO, with some internal to a specific release; 
    # shouldn't they be safely stored higher up in the file tree?
    # -----------------------------------------------------------------------------------------------------------
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

        native_coord_ds_lines = ds_s.data_products(native_coords_groupings, native_coords_dir,
                                                   native_coords_images_dir, dataset_type + " Coordinates")
        with open('document/latex/dataset/native_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_coord_ds_lines))


        native_ds_lines = ds_s.data_products(native_groupings_json, native_ds_dir,
                                            native_images_dir, dataset_type)
        with open('document/latex/dataset/native_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(native_ds_lines))
            #output_file.write('example of native dataset table\n')

    elif dataset_type == 'latlon':
        latlon_coord_ds_lines = ds_s.data_products(latlon_coords_groupings, latlon_coords_dir,
                                                   latlon_coords_images_dir, dataset_type) #+ " Coordinates"
        with open('document/latex/dataset/latlon_coords_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_coord_ds_lines))



        latlon_ds_lines = ds_s.data_products(latlon_groupings_json, latlon_ds_dir,
                                            latlon_images_dir, dataset_type)
        with open('document/latex/dataset/latlon_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(latlon_ds_lines))
            #output_file.write('example of latlon dataset table\n')

    elif dataset_type == '1D':
        oneD_ds_lines = ds_s.data_products(oneD_groupings_json, oneD_ds_dir,
                                        oneD_images_dir, dataset_type)
        with open('document/latex/dataset/oneD_dataset_tables.tex', 'w') as output_file:
            output_file.write('\n'.join(oneD_ds_lines))

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

