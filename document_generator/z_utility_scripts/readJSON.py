import json
from pathlib import Path
import sys

document_generator_dir = str(Path(__file__).parent.parent)
sys.path.append(document_generator_dir)

import z_utility_scripts.utils_docgen as utils


def obtain_json_data(filename: str) -> list:
    """
    Read JSON data from a file and return a dictionary.
    :param filename: string
    :return: list of dictionaries
    """
    with open(filename, "r") as file:
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
    return [utils.sanitize(col) for col in user_columns if col in available_columns]


def establish_table(dictionary_list_from_json:list)->list:
    """
    Establishes the table for the json data
    :param dictionary_list_from_json: list of dictionaries from a json file
    :return: list of strings
    """
    latex_lines = []

    max_col = 0
    for dictionary in dictionary_list_from_json:
        formatted_dictionary_as_list = [utils.sanitize_with_url(str(dictionary.get(key, "N/A"))) for key in dictionary]
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
        formatted_row = [utils.sanitize(str(row.get(key, "N/A"))) for key in col_names]
        latex_table.append(" & ".join(formatted_row) + " \\\\\n")
        latex_table.append("\\hline\n")

    latex_table.append("\\end{longtable}\n")
    latex_table.append('\\end{document}\n')

    return latex_table


