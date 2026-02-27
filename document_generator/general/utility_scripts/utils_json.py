import json
from pathlib import Path
import sys
import os
from . import utils_general as utils_general


def write_attributes_tables_tex(base_dir, config_dictionary):
    processed_attribute_types = []
    for key in config_dictionary.keys():
        if "_attributes_" in key:
            attribute_type = key.split("_")[0]
            if not attribute_type in processed_attribute_types:
                print(f"writing '{attribute_type}_attributes' latex table")
                processed_attribute_types.append(attribute_type)
                latex_lines = config_dictionary[f"{attribute_type}_attributes_latex_lines"]
                json_list = obtain_json_data(base_dir, config_dictionary[f"{attribute_type}_attributes_json_file"])
                latex_lines.extend(establish_table(json_list, config_dictionary))
                latex_lines.append(r'\end{longtable}')
                latex_output_file = os.path.join(base_dir, config_dictionary[f"{attribute_type}_attributes_tex_file"])
                Path(latex_output_file).parent.mkdir(parents=True, exist_ok=True)
                with open(latex_output_file, 'w') as output_file:
                    output_file.write('\n'.join(latex_lines))
                

def obtain_json_data(base_dir, filename: str) -> list:
    """
    Read JSON data from a file and return a dictionary.
    :param filename: string
    :return: list of dictionaries
    """
    with open(os.path.join(base_dir,filename), "r") as file:
        json_data_dictionary_list = json.load(file)
    return json_data_dictionary_list


def obtain_keys(json_data: list) -> set:
    """
    Extract keys from a list of dictionaries and return a set of unique keys.
    :param json_data: list of dictionaries
    :return: set of strings
    """
    keys = set()
    for element in json_data:
        keys |= set(element)
    return keys


def verify_columns(available_columns: set, user_columns: list) -> list:
    """
    Return the intersection between available_columns and user_columns.
    :param available_columns: set of available columns
    :param user_columns: list of user-defined columns
    :return: list of strings
    """
    return [utils_general.sanitize(config_dictionary, col) for col in user_columns if col in available_columns]


def establish_table(dictionary_list_from_json:list, config_dictionary)->list:
    """
    Establishes the table for the json data
    :param dictionary_list_from_json: list of dictionaries from a json file
    :return: list of strings
    """
    latex_lines = []

    max_col = 0
    for dictionary in dictionary_list_from_json:
        formatted_dictionary_as_list = [utils_general.sanitize_with_url(config_dictionary, str(dictionary.get(key, "N/A"))) for key in dictionary]
        max_col = len(formatted_dictionary_as_list) if len(formatted_dictionary_as_list) > max_col else max_col
        if len(formatted_dictionary_as_list) < max_col:
            formatted_dictionary_as_list.extend([""] * (max_col - len(formatted_dictionary_as_list)))
        latex_lines.append(r'\rowcolor{LightCyan} ' + ' & '.join(formatted_dictionary_as_list) + r' \\ \hline' + '\n')

    return latex_lines

"""Create adjustable LaTeX table code from JSON data and return a list of strings."""
def set_table(json_data: dict, caption: str = None, col_names: list = None, wider_col: str = None, wider_col_width: float = None) -> list: # type: ignore
    if col_names is None:
        col_names = ["name"]
    if caption is None:
        caption = "default caption"

    num_cols = len(col_names)

    # Customize column widths
    total_width = 5.5
    if wider_col and wider_col_width:
        other_col_width = (total_width - wider_col_width) / (num_cols - 1)
        col_widths = [wider_col_width if col == wider_col else other_col_width for col in col_names]
    else:
        col_widths = [total_width / num_cols] * num_cols

    latex_table = ["\\begin{longtable}[c]{|" + "|".join([f"p{{{width}in}}" for width in col_widths]) + "|}\n",
                   f"\\caption{{{caption}}}\\\\\n",
                   "\\hline\n",
                   "\\endfirsthead \\hline \\endhead \\hline \\endfoot \\hline \\endlastfoot\n",
                   "\\rowcolor{Gray}\n",
                   " & ".join([f"\\textbf{{{col}}}" for col in col_names]) + "\\\\\n",
                   "\\hline\n"]

    for row in json_data[1:]:
        latex_table.append("\\rowcolor{LightCyan}\n")
        formatted_row = [utils_general.sanitize(config_dictionary, str(row.get(key, "N/A"))) for key in col_names]
        latex_table.append(" & ".join(formatted_row) + " \\\\\n")
        latex_table.append("\\hline\n")

    latex_table.append("\\end{longtable}\n")
    latex_table.append('\\end{document}\n')

    return latex_table


