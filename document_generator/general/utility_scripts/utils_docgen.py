import os
import re
import xarray as xr
import glob
import subprocess
import yaml
from pathlib import Path
import sys
import netrc
import requests

general_base_dir = str(Path(__file__).parent.parent)
sys.path.append(general_base_dir)


def download_granules(version_string, overwrite_granules = False):
    
    config_file_static = os.path.join(general_base_dir, "config_files", version_string, "config_static.yaml")
    with open(config_file_static,'r') as stream:
        config_dictionary_static = yaml.safe_load(stream)

    possible_grid_types = config_dictionary_static["possible_grid_types"]
    grid_types_to_ignore = []

    config_file_user = os.path.join(general_base_dir, "config_files", version_string, "config_user.yaml")
    with open(config_file_user,'r') as stream:
        config_dictionary_user = yaml.safe_load(stream)

    hostname = config_dictionary_static["remote_server_hostname"]

    # Parse the local .netrc file (the user must create this according to the README instructions)
    netrc_info = netrc.netrc()

    # Get the login, account, and password for a specific machine
    auth_info = netrc_info.authenticators(hostname)
    
    if auth_info:
        login, account, password = auth_info
        for grid_type in possible_grid_types:
            for key in config_dictionary_user.keys():
                if grid_type in key and grid_type not in grid_types_to_ignore:
                    grid_types_to_ignore.append(grid_type)

                    for granule_type in config_dictionary_static[f"possible_granule_types_{grid_type}"]:

                        if f"{grid_type}_{granule_type}_file_paths_remote" in config_dictionary_user.keys():
                        
                            dataset_dir_pre = config_dictionary_static[f"{granule_type}_files_{grid_type}_dir"]
                            dataset_dir = os.path.join(os.path.realpath(general_base_dir), dataset_dir_pre)
                            os.makedirs(os.path.join(dataset_dir), exist_ok=True)                
                            
                            url_list = config_dictionary_user[f"{grid_type}_{granule_type}_file_paths_remote"]
                            
                            if overwrite_granules:
                                existing_files = glob.glob(f"{dataset_dir}/*.nc")
                                for local_path in existing_files:
                                    local_path_not_in_url_list_index_list = [Path(local_path).stem not in url for url in url_list]
                                    url_list = [url for url, boolean in zip(url_list, local_path_not_in_url_list_index_list) if boolean]

                            for url in url_list:
                                local_filename = os.path.join(dataset_dir,Path(url).name)
                                if not overwrite_granules:
                                    if os.path.exists(local_filename):
                                        continue
                                try:
                                    response = requests.get(url, auth=(login, password), stream=True)
                                    response.raise_for_status() 
                                    with open(local_filename, 'wb') as fd:
                                        for chunk in response.iter_content(chunk_size=8192):
                                            fd.write(chunk)
                                    print(f"successfully downloaded:      {url}")
                                except requests.exceptions.RequestException as e:
                                    print(f"An error occurred: {e}")
    else:
        print(f"No entry found for {hostname} in .netrc file.  Please refer to the README for the ECCO document generator for help")


def get_a_file_with_max_num_vars(nc_dir):
    """
    Determines the number occurrances of the string "long_name" in the attribute strings of a netCDF file,
    which is assumed to be the number of variables (coordinate and data) represented in a file.  
    Returns a path to a netCDF file containing the maximum number of variables found in a file.

    Parameters:
        string (nc_dir): The directory in which to search

    Returns:
        str: A path to a file containing the maximum number of variables found across all netCDF files in <nc_dir>
    """

    num_vars_per_file_list = []
    num_vars_max = 0
    nc_files = glob.glob(f"{os.path.join(general_base_dir,nc_dir)}/*.nc")
    for nc_file in nc_files:
        cmd1 = ["ncdump", "-h", nc_file]
        cmd2 = ["grep", "long_name"]
        cmd3 = ["wc", "-l"]
        p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, text=True)
        p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
        p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        p2.stdout.close()
        p1.stdout.close()
        stdout, stderr = p3.communicate()
        if stdout:
            num_vars = int(stdout)
            num_vars_per_file_list.append(num_vars)
            num_vars_max = num_vars if num_vars > num_vars_max else num_vars_max
        if stderr:
            print("STDERR Output:")
            print(stderr)
    #print(f"{num_vars_max}, {nc_files[num_vars_per_file_list.index(num_vars_max)]}")
    return nc_files[num_vars_per_file_list.index(num_vars_max)]


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
        


