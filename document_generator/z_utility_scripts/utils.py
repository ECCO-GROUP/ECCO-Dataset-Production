import re
import xarray as xr
def sanitize(string:str)->str:
    """
    Sanitizes a string for use in LaTeX by replacing any reserved characters
    with their LaTeX equivalent.

    Parameters:
        string (str): The input string to sanitize.

    Returns:
        str: The sanitized string.
    """

    # Define a dictionary of characters to replace
    replacements = {
        r"&": r"\&",
        r"%": r"\%",
        r"$": r"\$",
        r"#": r"\#",
        r"_": r"\_",
        r"{": r"\{",
        r"}": r"\}",
        r"~": r"\textasciitilde",
        r"^": r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|": r"\textbar",
        "\t": r"\hspace*{0.5cm} ",
        #r"'": r'\'', might not be needed
    }

    # Replace any reserved characters with their LaTeX equivalent
    for key, value in replacements.items():
        string = string.replace(key, value)

    return string



def sanitize_with_math(string:str)->str:
    """
    Sanitizes a string for use in LaTeX by replacing any reserved characters
    with their LaTeX equivalent, except within math environments (delimited by $).

    Parameters:
        string (str): The input string to sanitize.

    Returns:
        str: The sanitized string.
    """

    # Define a dictionary of characters to replace
    replacements = {
        r"&": r"\&",
        r"%": r"\%",
        r"#": r"\#",
        r"_": r"\_",
        r"{": r"\{",
        r"}": r"\}",
        r"~": r"\textasciitilde",
        r"^": r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|": r"\textbar",
        "\t": r"\hspace{0.5cm}",
        #r"'": '\'',
    }

    # Split the string into parts that are inside and outside of math environments
    parts = string.split('$')

    if len(parts) != 1:
        # Replace any reserved characters with their LaTeX equivalent in the parts
        # that are outside of math environments (these are the even-indexed parts)
        for i in range(0, len(parts), 2):
            for key, value in replacements.items():
                parts[i] = parts[i].replace(key, value)
    else:
        # If there are no math environments, replace all reserved characters
        for key, value in replacements.items():
            parts[0] = parts[0].replace(key, value)

    # Reassemble the string
    return '$'.join(parts)

def sanitize_with_url(string:str)->str:
    """
    Sanitizes a string for use in LaTeX by replacing any reserved characters
    with their LaTeX equivalent, ignoring parts between \\url{ and }.

    Parameters:
        string (str): The input string to sanitize.

    Returns:
        str: The sanitized string.
    """

    url_pattern = re.compile(r'\\url\{.*?\}')
    urls = re.findall(url_pattern, string)
    placeholders = [f'PLACEHOLDER{i}' for i in range(len(urls))]
    
    # Replace the URLs with placeholders temporarily
    for url, placeholder in zip(urls, placeholders):
        string = string.replace(url, placeholder)

    # Define a dictionary of characters to replace
    replacements = {
        r"&": r"\&",
        r"%": r"\%",
        r"$": r"\$",
        r"#": r"\#",
        r"_": r"\_",
        r"{": r"\{",
        r"}": r"\}",
        r"~": r"\textasciitilde",
        r"^": r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|": r"\textbar",
        "\t": r"\hspace{0.5cm} ",
        #r"'": r'\'',
    }

    # Replace any reserved characters with their LaTeX equivalent
    for key, value in replacements.items():
        string = string.replace(key, value)

    # Replace back the placeholders with the original URLs
    for url, placeholder in zip(urls, placeholders):
        string = string.replace(placeholder, url)

    return string




def get_substring(input_string:str)->str:
    """
        Returns the substring between the first pair of parentheses in the given string.
        Parameters:
            input_string (str): The input string to search.
        Returns:
            str: The substring between the first pair of parentheses.
    """
    start_pos = input_string.find('(') + 1
    end_pos = input_string.find(')')
    return input_string[start_pos:end_pos]



def add_to_line(line:str, before:str, after:str)->str:
    """
        Returns the line with the before string replaced with the after string.
        Parameters:
            line (str): The input string to search.
            before (str): The string to replace.
            after (str): The string to replace with.
        Returns:
            str: The line with the before string replaced with the after string.
    """
    # loop through the line and find before and change into after
    while line.find(before) != -1:
        start_pos = line.find(before)
        end_pos = start_pos + len(before)
        line = line[:start_pos] + after + line[end_pos:]
    return line



# Function takes in a datasettitle and returns a string of the dataset title
def get_ds_title(ds:xr.Dataset)->str:
    """
        Returns the dataset title of the given dataset.
        Parameters:
            ds (Dataset): The dataset to get the title of.
        Returns:
            str: The dataset title.
    """
    fullTitle = ds.title
    title = ''
    for word in fullTitle.split():
        if word == 'ECCO':
            continue
        elif word == '-':
            break
        else:
            title += word + '_'
    return title[:-1]
        


