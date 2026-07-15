import pdb
import copy
import xarray as xr
import json
from pathlib import Path
import sys
import os

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils_general



def check_for_attributes(base_dir: str, config_dictionary: dict, required_ratio: float) -> None:

    #var_types_to_consider = ['data_vars','coords']
    var_types_to_consider = ['data_vars','coords', 'dims']
    attr_string_buffer = 30

    # Collect all global and variable attributes present in the granules downloaded by the user
    all_granule_paths = [str(p) for p in (Path(base_dir) / config_dictionary["user_generated_granules_dir_relative"]).rglob('*.nc') if p.is_file()]

    granules_attributes_dictionary = {}
    granules_attributes_dictionary['global'] = {}
    granules_attributes_dictionary['variable'] = {}

    for granule_path in all_granule_paths:
        dataset = xr.open_dataset(granule_path)
        granules_attributes_dictionary['global'][dataset.attrs['product_name']] = [s.strip() for s in list(dataset.attrs.keys())]

        data_vars = {}
        coords = {}
        dims = {}

        for var in dataset.data_vars:
            data_vars[var] = [s.strip() for s in list(dataset[var].attrs.keys())]
            #variable_attributes_temp_dict[var] = [s.strip() for s in list(dataset[var].attrs.keys())]
        for var in dataset.coords:
            coords[var] = [s.strip() for s in list(dataset[var].attrs.keys())]
            #variable_attributes_temp_dict[var] = [s.strip() for s in list(dataset[var].attrs.keys())]
        for var in dataset.dims:
            dims[var] = [s.strip() for s in list(dataset[var].attrs.keys())]
            #variable_attributes_temp_dict[var] = [s.strip() for s in list(dataset[var].attrs.keys())]

        temp_dict = {}
        temp_dict['data_vars'] = data_vars
        temp_dict['coords'] = coords
        temp_dict['dims'] = dims

        granules_attributes_dictionary['variable'][dataset.attrs['product_name']] = temp_dict
        #granules_attributes_dictionary['variable'][dataset.attrs['product_name']] = variable_attributes_temp_dict

    

    # Determine which attributes are "common"
    attribute_ratios_global = {}
    attribute_ratios_variable = {}
    attribute_ratios_variable['data_vars'] = {}
    attribute_ratios_variable['coords'] = {}
    attribute_ratios_variable['dims'] = {}
    
    num_granules = len(all_granule_paths)
    num_variables = {}
    num_variables['data_vars'] = 0 
    num_variables['coords'] = 0 
    num_variables['dims'] = 0 


    for global_attributes_list in granules_attributes_dictionary['global'].values():
        for attr in global_attributes_list:
            if attr not in attribute_ratios_global.keys():
                attribute_ratios_global[attr] = 1 
            else:
                attribute_ratios_global[attr] += 1 

    for temp_dict in granules_attributes_dictionary['variable'].values():
        for var_type in temp_dict.keys():
            for attr_list in temp_dict[var_type].values():
                num_variables[var_type] += 1
                for attr in attr_list:
                    if attr not in attribute_ratios_variable[var_type].keys():
                        attribute_ratios_variable[var_type][attr] = 1 
                    else:
                        attribute_ratios_variable[var_type][attr] += 1 

    
    for attr in attribute_ratios_global.keys():
        attribute_ratios_global[attr] /= num_granules

    for var_type in attribute_ratios_variable.keys():
        for attr in attribute_ratios_variable[var_type].keys():
            attribute_ratios_variable[var_type][attr] /= num_variables[var_type]


    for attribute_type in ['global', 'variable']:

        dictionary_list_from_json = obtain_json_data(base_dir, config_dictionary[f"{attribute_type}_attributes_json_file"])

        # NoTE:
        # The algolrithm below for extracting attribute names from dictionaries makes the assumption that the attribute name was
        # the first field inserted into the dictionary, ie is the first field listed in the json file.  This logic will break
        # if that is ever not the case.
        attributes_list_from_json = []
        for dictionary in dictionary_list_from_json:
            attributes_list_from_json.append(list(dictionary.values())[0]) 

        if attribute_type == 'global':
            print("global attributes MISSING from all granules:")        
            print('-----------------------------------------------------------------------')
            print()
            attributes_from_json_absent_from_all = []
            for attribute in attributes_list_from_json:
                if attribute not in attribute_ratios_global.keys():
                    attributes_from_json_absent_from_all.append(attribute)
                    print(attribute)

            print()
            print()
            print("global attributes NEW to all granules:")        
            print('-----------------------------------------------------------------------')
            print()
            first_granule_switch = True
            for granule_name in granules_attributes_dictionary[attribute_type].keys():
                new_attributes_flags = [True] * len(granules_attributes_dictionary[attribute_type][granule_name])
                for ii in range(len(granules_attributes_dictionary[attribute_type][granule_name])):
                    if granules_attributes_dictionary[attribute_type][granule_name][ii] in attributes_list_from_json:
                        new_attributes_flags[ii] = False
                    new_attrs_granule = set([attr for attr,flag in zip(granules_attributes_dictionary[attribute_type][granule_name], new_attributes_flags) if flag])
                    if first_granule_switch:
                        attributes_new_for_all = new_attrs_granule
                    else:
                        attributes_new_for_all = attributes_new_for_all & new_attrs_granule

            for attribute in sorted(list(attributes_new_for_all)):
                print(attribute)

            print()
            print()
            print()

        else:
            attributes_from_json_absent_from_all = {} 
            for var_type in var_types_to_consider:
                print()
                print(f"variable attributes MISSING from all {var_type} variables:")        
                print('-----------------------------------------------------------------------')
                print()
                attributes_from_json_absent_from_all[var_type] = [] 
                for attribute in attributes_list_from_json:
                    if attribute not in attribute_ratios_variable[var_type].keys():
                        attributes_from_json_absent_from_all[var_type].append(attribute)
                        print(attribute)

            print()
            print()

            attributes_new_for_all = {} 
            for var_type in var_types_to_consider:
                print()
                print(f"variable attributes NEW for all {var_type} variables:")        
                print('-----------------------------------------------------------------------')
                print()
                first_granule_switch = True
                for granule_name in granules_attributes_dictionary[attribute_type].keys():

                    #for ii in range(len(granules_attributes_dictionary[attribute_type][granule_name][var_type])):
                    for attr_list in granules_attributes_dictionary[attribute_type][granule_name][var_type].values():
                        new_attributes_flags = [True] * len(attr_list)
                        for ii in range(len(attr_list)):
                        #if granules_attributes_dictionary[attribute_type][granule_name][var_type][ii] in attributes_list_from_json:
                            if attr_list[ii] in attributes_list_from_json:
                                new_attributes_flags[ii] = False
                        new_attrs_granule = set([attr for attr,flag in zip(attr_list, new_attributes_flags) if flag])
                        if first_granule_switch:
                            attributes_new_for_all[var_type] = new_attrs_granule
                        else:
                            attributes_new_for_all[var_type] = attributes_new_for_all & new_attrs_granule

                for attribute in sorted(list(attributes_new_for_all[var_type])):
                    print(attribute)

            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print("Now, to deal with attributes which are not new/missing across all granules")
            print()
            print("NOTE:")
            print(f"-  For global attributes, missing attributes are only printed if they are present in over {int(required_ratio*100)} percent of variables across all granules.")
            print("    (There are actually no global attributes missing which aren't missing across all granules)")
            print(f"-  For variable attributes, missing attributes are only printed if they are present in over {int(required_ratio*100)} percent of variables of a given variable type (data_vars, coords, dims) across all granules.")
            print()
            print(f"This filtering of printed output is based on your input value of {required_ratio} for the parameter 'required_ratio', which defaults to 0.")
            print()
            print()
            print()
            print()
            print()
            print()
            print()
            print()


            for granule_name in granules_attributes_dictionary[attribute_type].keys():
                
                if attribute_type == 'global':

                    attr_dict_print = {}
                    attr_dict_print['new'] = []
                    attr_dict_print['missing'] = []
                    
                    for attribute in sorted(granules_attributes_dictionary[attribute_type][granule_name]):
                        if attribute not in attributes_list_from_json and attribute not in attributes_new_for_all:
                            attr_dict_print['new'].append(attribute)
                    
                    for attribute in sorted(attributes_list_from_json):
                        if attribute in attributes_from_json_absent_from_all:
                            continue
                        if attribute not in granules_attributes_dictionary[attribute_type][granule_name]:
                            if attribute_ratios_global[attribute] > required_ratio: 
                                attr_dict_print['missing'].append(attribute)

                    if len(attr_dict_print['missing']) + len(attr_dict_print['new']) > 0:

                        print()
                        print()
                        print(f'granule: {granule_name}')
                        print('-----------------------------------------------------------------------')
                        print(f'{attribute_type} attributes:')

                        if len(attr_dict_print['new']) > 0:
                            print('    NEW:')
                            for attribute in attr_dict_print['new']:
                                print(f"            {attribute}")
                            
                        if len(attr_dict_print['missing']) > 0:
                            print()
                            print('    MISSING:')
                            for attribute in attr_dict_print['missing']:
                                print(f"            {attribute}")

                    print()
                    print()
                 
                else:

                    attr_dict_print = {}
                    attr_dict_print['new'] = {}
                    attr_dict_print['missing'] = {}

                    for var_type in var_types_to_consider:

                        temp_dict = granules_attributes_dictionary[attribute_type][granule_name][var_type]
                            
                        for var in temp_dict.keys():

                            for attribute in sorted(temp_dict[var]):
                                if attribute not in attributes_list_from_json and attribute not in attributes_new_for_all[var_type]:
                                    if var_type in attr_dict_print['new'].keys():
                                        if attribute in attr_dict_print['new'][var_type].keys():
                                            attr_dict_print['new'][var_type][attribute].append(var)
                                        else:
                                            attr_dict_print['new'][var_type][attribute] = [var]
                                    else:
                                        attr_dict_print['new'][var_type] = {}
                                        attr_dict_print['new'][var_type][attribute] = [var]

                            for attribute in sorted(attributes_list_from_json):
                                if attribute in attributes_from_json_absent_from_all[var_type]:
                                    continue
                                if attribute not in temp_dict[var]:
                                    if attribute_ratios_variable[var_type][attribute] > required_ratio: 

                                        if var_type in attr_dict_print['missing'].keys():
                                            if attribute in attr_dict_print['missing'][var_type].keys():
                                                attr_dict_print['missing'][var_type][attribute].append(var)
                                            else:
                                                attr_dict_print['missing'][var_type][attribute] = [var]
                                        else:
                                            attr_dict_print['missing'][var_type] = {}
                                            attr_dict_print['missing'][var_type][attribute] = [var]



                    if len(attr_dict_print['missing'].keys()) + len(attr_dict_print['new'].keys()) > 0:

                        print()
                        print()
                        print()
                        print()
                        print()
                        print()
                        print()
                        print()
                        print('--------------------------------------------------------------------------------------------------------------')
                        print(f'granule: {granule_name}')
                        print('--------------------------------------------------------------------------------------------------------------')
                        print(f"'{attribute_type} attributes' table:")
                        print('--------------------------------------------------------------------------------------------------------------')
                        print(f"{'':<{attr_string_buffer}} {'variable_type':<{attr_string_buffer}} {'attribute':<{attr_string_buffer}} {'variable'}")

                        if len(attr_dict_print['new'].keys()) > 0:
                            print('--------------------------------------------------------------------------------------------------------------')
                            first_entry = True

                            #print('    NEW:')

                            for var_type in attr_dict_print['new'].keys():
                                new_type_switch = True
                                for attr in attr_dict_print['new'][var_type].keys():
                                    new_attr_switch = True
                                    for var in attr_dict_print['new'][var_type][attr]:

                                        if new_type_switch and new_attr_switch:
                                            if not first_entry:
                                                print()
                                            print(f"{'NEW':<{attr_string_buffer}} {var_type:<{attr_string_buffer}} {attr:<{attr_string_buffer}} {var}")
                                            new_type_switch = False
                                            new_attr_switch = False
                                            first_entry = False
                                        elif new_attr_switch:
                                            print()
                                            print(f"{'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {attr:<{attr_string_buffer}} {var}")
                                            new_attr_switch = False
                                        else:
                                            print(f"{'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {var}")


                        if len(attr_dict_print['missing'].keys()) > 0:
                            print('--------------------------------------------------------------------------------------------------------------')
                            first_entry = True

                            #print('    MISSING:')
                            #print(f"{'MISSING':<{attr_string_buffer}} {'variable_type':<{attr_string_buffer}} {'attribute':<{attr_string_buffer}} {'variable'}")
                            #print(f"                {'---------':<{attr_string_buffer}} {'--------':<{attr_string_buffer}} {'-------------'}")

                            for var_type in attr_dict_print['missing'].keys():
                                missing_type_switch = True
                                for attr in attr_dict_print['missing'][var_type].keys():
                                    missing_attr_switch = True
                                    for var in attr_dict_print['missing'][var_type][attr]:

                                        if missing_type_switch and missing_attr_switch:
                                            if not first_entry:
                                                print()
                                            print(f"{'MISSING':<{attr_string_buffer}} {var_type:<{attr_string_buffer}} {attr:<{attr_string_buffer}} {var}")
                                            missing_type_switch = False
                                            missing_attr_switch = False
                                            first_entry = False
                                        elif missing_attr_switch:
                                            print()
                                            print(f"{'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {attr:<{attr_string_buffer}} {var}")
                                            missing_attr_switch = False
                                        else:
                                            print(f"{'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {'':<{attr_string_buffer}} {var}")

                        print('--------------------------------------------------------------------------------------------------------------')



                                

    #for attribute_type in ['global', 'variable']:
    '''
        print()
        print()
        print()



                                

    #for attribute_type in ['global', 'variable']:
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print()
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print(f'{attribute_type.upper()} attribute diagnostic information:')
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print()
        print()
        print()
        print()
    '''



def write_attributes_tables_tex(base_dir: str, config_dictionary: dict) -> None:
    """
    Generate and write LaTeX longtable files for each unique attribute type in the config.

    Iterates over the config dictionary to identify all attribute types (keys
    containing ``"_attributes_"``), validates that the three required config keys
    are present for each type, then builds and writes a ``.tex`` file for each
    unique attribute type by combining header lines from the config with rows
    derived from a JSON data source.

    .. note::
        The ``latex_lines`` list retrieved from ``config_dictionary`` is copied
        before modification to avoid mutating the original config. Output
        directories are created automatically if they do not exist.

    :param base_dir: Root directory used to resolve input JSON file paths and
        output ``.tex`` file paths.
    :type base_dir: str
    :param config_dictionary: Configuration mapping containing attribute metadata.
        For each attribute type the following keys must be present:

        - ``{attribute_type}_attributes_latex_lines`` (list[str]) — LaTeX lines
          forming the table preamble.
        - ``{attribute_type}_attributes_json_file`` (str) — path to the JSON file
          containing table row data, relative to ``base_dir``.
        - ``{attribute_type}_attributes_tex_file`` (str) — path for the output
          ``.tex`` file, relative to ``base_dir``.

    :type config_dictionary: dict
    :returns: None
    :raises KeyError: If any of the three required config keys are missing for
        a discovered attribute type.
    """
    # Collect all global and non-global attributes present in the granules downloaded by the user
    global_attributes_from_granules = set()
    non_global_attributes_from_granules = set()

    #all_granule_paths = utils_general.list_files_pathlib(os.path.join(base_dir, config_dictionary["user_generated_granules_dir_relative"]))
    all_granule_paths = [str(p) for p in (Path(base_dir) / config_dictionary["user_generated_granules_dir_relative"]).rglob('*.nc') if p.is_file()]

    global_attributes_granules = set()
    non_global_attributes_granules = set()

    for granule_path in all_granule_paths:
        dataset = xr.open_dataset(granule_path)
        #pdb.set_trace()
        global_attributes_granules.update([el.lower() for el in list(dataset.attrs.keys())])
        for var in dataset.data_vars:
            non_global_attributes_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])
        for var in dataset.coords:
            non_global_attributes_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])
        for var in dataset.dims:
            non_global_attributes_granules.update([s.strip() for s in list(dataset[var].attrs.keys())])


    required = ["latex_lines", "json_file", "tex_file"]
    processed_attribute_types = []

    for key in config_dictionary.keys():
        if "_attributes_" in key:
            # Extract the attribute type prefix, e.g. "global" from "global_attributes_json_file"
            attribute_type = key.split("_attributes_")[0]

            if attribute_type not in processed_attribute_types:
                # Validate that all three required config keys exist before proceeding
                for suffix in required:
                    if f"{attribute_type}_attributes_{suffix}" not in config_dictionary:
                        raise KeyError(f"Missing config key: {attribute_type}_attributes_{suffix}")

                print(f"writing '{attribute_type}_attributes' latex table")
                processed_attribute_types.append(attribute_type)


                #latex_lines = write_table(base_dir, config_dictionary, attribute_type)
                if attribute_type == "global":
                    write_table(base_dir, config_dictionary, attribute_type, global_attributes_granules)
                else:
                    write_table(base_dir, config_dictionary, attribute_type, non_global_attributes_granules)

                '''
                latex_output_file = os.path.join(base_dir, config_dictionary[f"{attribute_type}_attributes_tex_file"])
                # Create any missing parent directories for the output path
                Path(latex_output_file).parent.mkdir(parents=True, exist_ok=True)
                with open(latex_output_file, 'w') as output_file:
                    output_file.writelines(line + '\n' for line in latex_lines)
                '''



def obtain_json_data(base_dir: str, filename: str) -> list:
    """
    Read JSON data from a file and return its contents as a list.

    :param base_dir: Root directory used to resolve the file path.
    :type base_dir: str
    :param filename: Path to the JSON file, relative to ``base_dir``.
    :type filename: str
    :returns: Parsed contents of the JSON file. Each element represents one
        table record (row).
    :rtype: list[dict]
    """
    with open(os.path.join(base_dir, filename), "r") as file:
        json_data_dictionary_list = json.load(file)
    return json_data_dictionary_list


def modify_json_add_introduction_field_to_groupings(json_data: list, filename: str, config_dict: dict) -> list:

    modified_json_data = []
    
    #print("------")
    #print(filename)
    #print("------")

    for dictionary in json_data:

        modified_dict = copy.deepcopy(dictionary)
        intro_string = f"{config_dict['dataset_text_dict']['opening_text']}{modified_dict['name']}"
        frequency_list_crude = [f.strip() for f in modified_dict['frequency'].split(',')]
        frequency_list_verbose = []

        for frequency_crude in config_dict['dataset_text_dict']['frequency_dict'].keys():
            if frequency_crude in frequency_list_crude:
                frequency_list_verbose.append(config_dict['dataset_text_dict']['frequency_dict'][frequency_crude])
        
        if len(frequency_list_verbose) == 0:
            intro_string = f"{intro_string}{config_dict['dataset_text_dict']['time_resolution_none_text']}"

        elif len(frequency_list_verbose) == 1:
            intro_string = f"{intro_string}{config_dict['dataset_text_dict']['time_resolution_single_text']}"
            intro_string = intro_string.format(frequency_string = frequency_list_verbose[0])

        elif len(frequency_list_verbose) == 2:
            intro_string = f"{intro_string}{config_dict['dataset_text_dict']['time_resolution_multiple_text']}"
            frequency_string = f"{frequency_list_verbose[0]} and {frequency_list_verbose[1]}"
            intro_string = intro_string.format(frequency_string = frequency_string)

        else:
            intro_string = f"{intro_string}{config_dict['dataset_text_dict']['time_resolution_multiple_text']}"
            frequency_string = ", ".join(frequency_list_verbose[:-1])
            frequency_string = f"{frequency_string}, and {frequency_list_verbose[-1]}"
            intro_string = intro_string.format(frequency_string = frequency_string)

        # POTENTIAL BUG HERE SINCE I DON'T HANDLE CASE WHERE NO GRID TYPE IS MATCHED.  WON'T FAIL, BUT PRINTED STRING WILL BE WRONG 
        for grid_type in [g.replace("-","") for g in config_dict['possible_grid_types']]: 
            if grid_type.casefold() in filename.casefold():
                intro_string = f"{intro_string}{config_dict['dataset_text_dict'][f'grid_text_{grid_type}']}"
                break

        #print()
        #print('intro string')
        #print(intro_string)
        #print()

        modified_dict['Introduction'] = intro_string
        modified_json_data.append(modified_dict)

    return modified_json_data

    
def modify_json_add_product_field_to_groupings(json_data: list, grid_type: str) -> list:
    modified_json_data = []
    for dictionary in json_data:
        modified_dict = copy.deepcopy(dictionary)
        modified_dict['product'] = grid_type
        modified_json_data.append(modified_dict)
    return modified_json_data


def obtain_keys(json_data: list) -> set:
    """
    Extract all unique keys from a list of dictionaries.

    :param json_data: Data parsed from a JSON file, where each element
        represents a record.
    :type json_data: list[dict]
    :returns: Union of all keys found across every dictionary in the list.
    :rtype: set[str]
    """
    keys = set()
    for element in json_data:
        # Union the current key set with the keys of this record
        keys |= set(element)
    return keys


def write_table(base_dir: str, config_dictionary: dict, attribute_type: str, attributes_granules: set) -> list:
    """
    Build LaTeX table row lines from a list of JSON records.

    Each record is rendered as a ``\\rowcolor{LightCyan}`` LaTeX row. Rows
    with fewer columns than the widest row encountered are padded with empty
    cells to ensure consistent column alignment.

    .. note::
        Missing values for any key are filled with ``"N/A"``. Rows shorter
        than the widest row encountered are padded with empty strings.

    :param dictionary_list_from_json: Records loaded from a JSON file, each
        representing one table row. Keys define the columns.
    :type dictionary_list_from_json: list[dict]
    :param config_dictionary: Configuration mapping passed through to the
        sanitization utilities.
    :type config_dictionary: dict
    :returns: LaTeX row strings, each formatted as a ``\\rowcolor{LightCyan}``
        row ending with ``\\\\ \\hline``.
    :rtype: list[str]
    """
    # Copy header lines to avoid mutating the original config list
    latex_lines = list(config_dictionary[f"{attribute_type}_attributes_latex_lines"])
    dictionary_list_from_json = obtain_json_data(base_dir, config_dictionary[f"{attribute_type}_attributes_json_file"])

    max_col = 0  # Track the widest row to pad narrower rows consistently
    
    # NoTE:
    # The algolrithm below for extracting attribute names from dictionaries makes the assumption that the attribute name was
    # the first field inserted into the dictionary, ie is the first field listed in the json file.  This logic will break
    # if that is ever not the case.
    attributes_list_from_json = []
    for dictionary in dictionary_list_from_json:
        attributes_list_from_json.append(list(dictionary.values())[0].lower()) 
        #pdb.set_trace()


    mystery_attributes = []

    for attribute in sorted(list(attributes_granules)):
        if attribute.lower() not in attributes_list_from_json:
            
            #raise KeyError(f"Attribute ‘{attribute}’ from your downloaded granules is not present in the list of approved attributes (see {config_dictionary[f'{attribute_type}_attributes_json_file']})")

            mystery_attributes.append(attribute)

        else:
            dictionary_index = attributes_list_from_json.index(attribute)
            formatted_dictionary_as_list = [
                utils_general.sanitize_with_url(config_dictionary, str(dictionary_list_from_json[dictionary_index].get(key, "N/A")))
                for key in dictionary
            ]

            # Update the max column count seen so far
            max_col = len(formatted_dictionary_as_list) if len(formatted_dictionary_as_list) > max_col else max_col

            # Pad with empty cells if this row has fewer columns than the widest row
            if len(formatted_dictionary_as_list) < max_col:
                formatted_dictionary_as_list.extend([""] * (max_col - len(formatted_dictionary_as_list)))

            latex_lines.append(
                r'\rowcolor{' + config_dictionary[f"{attribute_type}_attribute_rowcolor"] + '} ' + '\n' + ' & '.join(formatted_dictionary_as_list) + r' \\ \hline' + '\n'
            )

    latex_lines.append(r'\end{longtable}')

    latex_output_file = os.path.join(base_dir, config_dictionary[f"{attribute_type}_attributes_tex_file"])
    # Create any missing parent directories for the output path
    Path(latex_output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(latex_output_file, 'w') as output_file:
        output_file.writelines(line + '\n' for line in latex_lines)

    print() 
    print(f"The following attributes from your local granules were not included in the ‘{attribute_type} attributes’ table, as they do not exist in the official list of possible {attribute_type} attributes:")
    print(mystery_attributes)
    print() 
    print(f"For a list of allowed {attribute_type} attributes, see ‘{config_dictionary[f'{attribute_type}_attributes_json_file']}’.")
    print() 
    print() 
    print() 

    #return latex_lines




