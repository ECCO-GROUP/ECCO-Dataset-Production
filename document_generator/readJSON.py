import json
import utils as u


def obtain_json_data(filename: str) -> dict:
    """
    Read JSON data from a file and return a dictionary.
    :param filename: string
    :return: dictionary
    """
    with open(filename, "r") as file:
        json_data = json.load(file)
    return json_data


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
    return [u.sanitize(col) for col in user_columns if col in available_columns]


def establish_table(json:dict)->list:
    """
    Establishes the table for the json data
    :param json: json data
    :return: list of strings
    """
    latex_lines = []

    max_col = 0
    for row in json:
        formatted_row = [u.sanitize_with_url(str(row.get(key, "N/A"))) for key in row]
        max_col = len(formatted_row) if len(formatted_row) > max_col else max_col
        if len(formatted_row) < max_col:
            formatted_row.extend([""] * (max_col - len(formatted_row)))
        latex_lines.append(r'\rowcolor{LightCyan} ' + ' & '.join(formatted_row) + r' \\ \hline' + '\n')

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
        formatted_row = [u.sanitize(str(row.get(key, "N/A"))) for key in col_names]
        latex_table.append(" & ".join(formatted_row) + " \\\\\n")
        latex_table.append("\\hline\n")

    latex_table.append("\\end{longtable}\n")
    latex_table.append('\\end{document}\n')

    return latex_table
def main():
    filename = "./data/sorted_metadata_for_V4r4_v2.json"

    # Validate file name
    #while True:
    #    verify = input(f'Is "{filename}" the correct file? (Y/N): ')
    #    if verify.lower() == "y":
    #        break
    #    filename = input("Enter absolute/relative file name: ")

    # Obtain data and column keys from JSON file

    """
    json_data = obtain_json_data(filename)
    available_columns = obtain_keys(json_data)

    # Set the caption and user-defined column names
    caption = "Mandatory global attributes for GDS 2.0 netCDF data files"
    user_columns = ["Name", "Format", "Description", "Source"]
    verified_columns = verify_columns(available_columns, user_columns)

    # Set up list of strings to write to the LaTeX file
    latex_lines = set_preamble() + set_table(json_data, caption, verified_columns, wider_col="Description", wider_col_width=2.5)

    # Write the LaTeX file
    with open('variables.tex', 'w') as file:
        file.writelines(latex_lines)

    print("LaTeX file has been written")

    """