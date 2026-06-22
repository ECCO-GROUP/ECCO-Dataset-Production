import json
from pathlib import Path
import sys
import os
from . import utils_general as utils_general


def write_attributes_tables_tex(base_dir:str, config_dictionary:dict) -> None:

    """
    Generate and write LaTeX longtable files for each unique attribute type in the config.

    Iterates over the config dictionary to identify all attribute types, validates
    that required config keys are present, then builds and writes a ``.tex`` file
    for each unique attribute type by combining header lines with rows derived
    from a JSON data source.

    Parameters
    ----------
    base_dir : str
        Root directory used to resolve input JSON file paths and output ``.tex``
        file paths.
    config_dictionary : dict
        Configuration mapping containing attribute metadata. For each attribute
        type, the following keys must be present:

        - ``{attribute_type}_attributes_latex_lines`` : list of str
            LaTeX lines forming the table preamble (e.g. ``\\begin{longtable}``,
            column spec, header row).
        - ``{attribute_type}_attributes_json_file`` : str
            Path to the JSON file containing table row data, relative to ``base_dir``.
        - ``{attribute_type}_attributes_tex_file`` : str
            Path for the output ``.tex`` file, relative to ``base_dir``.

    Raises
    ------
    KeyError
        If any of the three required config keys are missing for a discovered
        attribute type.

    Notes
    -----
    The ``latex_lines`` list retrieved from ``config_dictionary`` is copied before
    modification to avoid mutating the original config. Output directories are
    created automatically if they do not exist.
    """

    required = ["latex_lines", "json_file", "tex_file"]
    processed_attribute_types = []
    for key in config_dictionary.keys():
        if "_attributes_" in key:
            # Extract prefix e.g. "node" from "node_attributes_json_file"
            attribute_type = key.split("_attributes_")[0]
            if attribute_type not in processed_attribute_types:
                # Ensure all three required config keys exist before proceeding
                for suffix in required:
                    if f"{attribute_type}_attributes_{suffix}" not in config_dictionary:
                        raise KeyError(f"Missing config key: {attribute_type}_attributes_{suffix}")
                print(f"writing '{attribute_type}_attributes' latex table")
                processed_attribute_types.append(attribute_type)
                # Copy header lines to avoid mutating the original config list
                latex_lines = list(config_dictionary[f"{attribute_type}_attributes_latex_lines"])
                json_list = obtain_json_data(base_dir, config_dictionary[f"{attribute_type}_attributes_json_file"])
                latex_lines.extend(establish_table(json_list, config_dictionary))
                latex_lines.append(r'\end{longtable}')
                latex_output_file = os.path.join(base_dir, config_dictionary[f"{attribute_type}_attributes_tex_file"])
                # Create any missing parent directories for the output path
                Path(latex_output_file).parent.mkdir(parents=True, exist_ok=True)
                with open(latex_output_file, 'w') as output_file:
                    output_file.writelines(line + '\n' for line in latex_lines)

def obtain_json_data(base_dir:str, filename:str) -> list:
    """
    Read JSON data from a file and return its contents as a list.

    Parameters
    ----------
    base_dir : str
        Root directory used to resolve the file path.
    filename : str
        Path to the JSON file, relative to ``base_dir``.

    Returns
    -------
    list of dict
        Parsed contents of the JSON file.
    """
    with open(os.path.join(base_dir, filename), "r") as file:
        json_data_dictionary_list = json.load(file)
    return json_data_dictionary_list


def obtain_keys(json_data:list) -> set:
    """
    Extract all unique keys from a list of dictionaries.

    Parameters
    ----------
    json_data : list of dict
        Data parsed from a JSON file, where each element represents a record.

    Returns
    -------
    set of str
        Union of all keys found across every dictionary in the list.
    """
    keys = set()
    for element in json_data:
        # Union the current key set with the keys of this record
        keys |= set(element)
    return keys


def establish_table(dictionary_list_from_json:list, config_dictionary:dict) -> list:
    """
    Build LaTeX table row lines from a list of JSON records.

    Each record is rendered as a colored, ruled LaTeX row. Rows with fewer
    columns than the widest row are padded with empty cells to ensure
    consistent column alignment.

    Parameters
    ----------
    dictionary_list_from_json : list of dict
        Records loaded from a JSON file, each representing one table row.
    config_dictionary : dict
        Configuration mapping passed to sanitization utilities.

    Returns
    -------
    list of str
        LaTeX row strings, each formatted as a ``\\rowcolor{LightCyan}``
        row ending with ``\\\\ \\hline``.

    Notes
    -----
    Missing values for any key are filled with ``"N/A"``. Rows shorter than
    the widest row encountered are padded with empty strings.
    """
    latex_lines = []
    max_col = 0
    for dictionary in dictionary_list_from_json:
        # Sanitize each cell value for LaTeX, falling back to "N/A" if the key is absent
        formatted_dictionary_as_list = [
            utils_general.sanitize_with_url(config_dictionary, str(dictionary.get(key, "N/A")))
            for key in dictionary
        ]

        formatted_dictionary_as_list = [
            utils_general.sanitize_with_url(config_dictionary, str(dictionary.get(key, "N/A")))
            for key in dictionary
        ]

        # Track the widest row seen so narrower rows can be padded to match
        max_col = len(formatted_dictionary_as_list) if len(formatted_dictionary_as_list) > max_col else max_col
        # Pad with empty cells if this row has fewer columns than the widest row
        if len(formatted_dictionary_as_list) < max_col:
            formatted_dictionary_as_list.extend([""] * (max_col - len(formatted_dictionary_as_list)))
        latex_lines.append(r'\rowcolor{LightCyan} ' + ' & '.join(formatted_dictionary_as_list) + r' \\ \hline' + '\n')
    return latex_lines
