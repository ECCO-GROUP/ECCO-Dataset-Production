import os
import argparse

document_generator_dir = str(Path(__file__).parent.parent)
sys.path.append(document_generator_dir)
import z_utility_scripts.readJSON
import z_utility_scripts.cdf_extract
import z_utility_scripts.sections.dataset_sections as dataset_sections

def write_data_attributes_tables(version_string):
    """
        This function writes the data product tables to the latex document.
    """

    # Construct path to the static configuration file, and load the file into a dictionary
    config_file_static = os.path.join(document_generator_dir, "z_config_files", version_string, "config_static.yaml")
    with open(config_file_static,'r') as stream:
        config_dictionary_static = yaml.safe_load(stream)

    # Construct the "global_attributes.tex" file
    global_attributes_latex_lines = config_dictionary_static['global_attributes_latex_lines']
    global_attributes_json_list = readJSON.obtain_json_data(config_dictionary_static['global_attributes_json_file'])
    global_attributes_latex_lines.extend(readJSON.establish_table(global_attributes_json_list))
    global_attributes_latex_lines.append(r'\end{longtable}')
    with open(config_dictionary_static['global_attributes_tex_file'], 'w') as output_file:
        output_file.write('\n'.join(global_attributes_latex_lines))

    # Construct the "variable_attributes.tex" file
    variable_attributes_latex_lines = config_dictionary_static['variable_attributes_latex_lines']
    variable_attributes_json_list = readJSON.obtain_json_data(config_dictionary_static['variable_attributes_json_file'])
    variable_attributes_latex_lines.extend(readJSON.establish_table(variable_attributes_json_list))
    variable_attributes_latex_lines.append(r'\end{longtable}')
    with open(config_dictionary_static['variable_attributes_tex_file'], 'w') as output_file:
        output_file.write('\n'.join(variable_attributes_latex_lines))


    # Construct the "example_{grid_type}_table.tex" files
    possible_grid_types = config_dictionary_static["possible_grid_types"]:
    grid_types_to_ignore = []

    config_file_user = os.path.join(document_generator_dir, "z_config_files", version_string, "config_user.yaml")
    with open(config_file_user,'r') as stream:
        config_dictionary_user = yaml.safe_load(stream)

    for grid_type in possible_grid_types:
        for key in config_dictionary_user.keys():
            if grid_type in key and grid_type not in grid_types_to_ignore:
                grid_types_to_ignore.append(grid_type)
                grid_example_latex_lines = cdf_extract.latex_example_netcdf(grid_type)
                grid_example_latex_output_file = config_dictionary_static[f"example_{grid_type}_table_tex_file"]
                with open(grid_example_latex_output_file, 'w') as output_file:
                    output_file.write('\n'.join(grid_example_latex_lines))
                break







def write_datasets(version_string):

    config_file_static = os.path.join(document_generator_dir, "z_config_files", version_string, "config_static.yaml")
    with open(config_file_static,'r') as stream:
        config_dictionary_static = yaml.safe_load(stream)
    
    config_file_user = os.path.join(document_generator_dir, "z_config_files", version_string, "config_user.yaml")
    with open(config_file_user,'r') as stream:
        config_dictionary_user = yaml.safe_load(stream)

    possible_grid_types = config_dictionary_static["possible_grid_types"]:
    grid_types_to_ignore = []

    for grid_type in possible_grid_types:
        for key in config_dictionary_user.keys():
            if grid_type in key and grid_type not in grid_types_to_ignore:
                grid_types_to_ignore.append(grid_type)
    
                # Coordinates
                if grid_type != "1D":
                    coordinate_latex_lines = dataset_sections.data_products(config_dictionary_static["groupings_coordinates_{grid_type}_json_file"],
                                                                            config_dictionary_static["coordinate_files_{grid_type}_dir"],
                                                                            config_dictionary_static["figures_coordinates_{grid_type}_dir"],
                                                                            grid_type)
                    with open(config_dictionary_static['coordinate_table_{grid_type}_tex_file'], 'w') as output_file:
                        output_file.write('\n'.join(coordinate_latex_lines))

                # Variables
                variables_latex_lines = dataset_sections.data_products(config_dictionary_static["groupings_variables_{grid_type}_json_file"],
                                                                        config_dictionary_static["variable_files_{grid_type}_dir"],
                                                                        config_dictionary_static["figures_variables_{grid_type}_dir"],
                                                                        grid_type)
                with open(config_dictionary_static['dataset_tables_{grid_type}_tex_file'], 'w') as output_file:
                    output_file.write('\n'.join(coordinate_latex_lines))







if __name__ == '__main__':
    """
        This script generates the LaTeX outline for the dataset.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument('--type', required=True, type=str,
                        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'.")
    args = parser.parse_args()

    write_datasets(args.type)

