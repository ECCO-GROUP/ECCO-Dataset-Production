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
from PIL import Image


def write_latex_lines_to_file(latex_lines: list, output_file: str) -> None:
    """
    Write a list of LaTeX lines to a file, creating parent directories as needed.

    :param latex_lines: LaTeX content to write, one element per line.
    :type latex_lines: list[str]
    :param output_file: Destination file path. Parent directories are created
        if they do not exist.
    :type output_file: str
    :returns: None
    """
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as output_file:
        output_file.write('\n'.join(latex_lines))


def append_hanging_indentation_commands_cm_latex(
    config_dictionary: dict, num_tabs: int, latex_lines_list: list
) -> list:
    """
    Append LaTeX hanging-indentation commands to a list of LaTeX lines.

    Inserts ``\\hangindent`` and ``\\hangafter1`` commands sized in centimetres
    based on the tab width defined in the config. Used to visually indent
    nested CDL attribute lines in the output tables.

    :param config_dictionary: Configuration mapping. Must contain
        ``'tab_char'`` (str, the character used as a tab in source strings) and
        ``'tab_width_cm'`` (float, width of one tab stop in centimetres).
    :type config_dictionary: dict
    :param num_tabs: Depth of indentation. The hang indent is set to
        ``(num_tabs + 1) * tab_width_cm`` cm.
    :type num_tabs: int
    :param latex_lines_list: Existing list of LaTeX lines to append to.
    :type latex_lines_list: list[str]
    :returns: The input list with the two indentation commands appended.
    :rtype: list[str]
    """
    latex_lines_list.append(
        sanitize_with_math(
            config_dictionary,
            f"\\hangindent={(num_tabs + 1) * config_dictionary['tab_width_cm']}cm"
        )
    )
    latex_lines_list.append(r"\hangafter1")
    return latex_lines_list


def download_granules(base_dir: str, config_dictionary: dict) -> None:
    """
    Download ECCO granule files from a remote server to local directories.

    Reads a list of granule URLs from the file specified in the config, then
    authenticates using credentials stored in the user's ``.netrc`` file. Each
    URL is matched against grid-type substrings to determine the correct local
    destination directory. Files that already exist are skipped unless
    ``overwrite_switch`` is enabled in the config.

    .. note::
        The ``.netrc`` file must have an entry for the hostname specified in
        ``config_dictionary['remote_server_hostname']``. See the project README
        for setup instructions.

    :param base_dir: Root directory of the project; used to resolve relative
        paths in the config.
    :type base_dir: str
    :param config_dictionary: Configuration mapping. Expected keys include:

        - ``'granules_url_list_file_relative'`` (str) — path to the URL list file.
        - ``'remote_server_hostname'`` (str) — hostname used for ``.netrc`` lookup.
        - ``'url_grid_type_substrings'`` (list[str]) — substrings identifying grid types.
        - ``'url_coordinate_substring'`` (str) — substring identifying coordinate files.
        - ``'overwrite_switch'`` (bool) — if ``True``, re-download existing files.
        - ``'coordinate_files_{grid_type}_dir'`` / ``'variable_files_{grid_type}_dir'``
          (str) — relative paths for saving coordinate vs. variable granules.

    :type config_dictionary: dict
    :returns: None
    """
    granule_list_filepath = os.path.join(base_dir, config_dictionary['granules_url_list_file_relative'])

    # Read the URL list, skipping blank lines and comment lines starting with '#'
    granule_url_list = []
    with open(granule_list_filepath, 'r', encoding='utf-8') as file:
        for line in file:
            if not line.strip().startswith('#'):
                if line.strip():
                    granule_url_list.append(line.strip())

    hostname = config_dictionary["remote_server_hostname"]

    # Retrieve credentials from the user's local .netrc file
    netrc_info = netrc.netrc()
    auth_info = netrc_info.authenticators(hostname)

    if auth_info:
        login, account, password = auth_info

        # Iterate over all grid types (e.g. 'native', 'latlon', '1D')
        for grid_type_substring in config_dictionary["url_grid_type_substrings"]:
            for granule_url in granule_url_list:
                # Normalise 'lat-lon' -> 'latlon' for URL matching, since URLs omit the hyphen
                if "".join(grid_type_substring.split("-")) in granule_url:

                    # Determine whether this URL points to a coordinate or variable file
                    if config_dictionary["url_coordinate_substring"] in granule_url:
                        # Strip leading underscore from grid type substring if present
                        if grid_type_substring.startswith("_"):
                            dataset_dir_pre = config_dictionary[f"coordinate_files_{grid_type_substring[1:]}_dir"]
                        else:
                            dataset_dir_pre = config_dictionary[f"coordinate_files_{grid_type_substring}_dir"]
                    else:
                        if grid_type_substring.startswith("_"):
                            dataset_dir_pre = config_dictionary[f"variable_files_{grid_type_substring[1:]}_dir"]
                        else:
                            dataset_dir_pre = config_dictionary[f"variable_files_{grid_type_substring}_dir"]

                    dataset_dir = os.path.join(os.path.realpath(base_dir), dataset_dir_pre)
                    os.makedirs(dataset_dir, exist_ok=True)
                    local_filename = os.path.join(dataset_dir, Path(granule_url).name)

                    # Skip download if file already exists and overwrite is disabled
                    if not config_dictionary['overwrite_switch']:
                        if os.path.exists(local_filename):
                            continue

                    try:
                        response = requests.get(granule_url, auth=(login, password), stream=True)
                        response.raise_for_status()
                        # Write in chunks to avoid loading large files fully into memory
                        with open(local_filename, 'wb') as fd:
                            for chunk in response.iter_content(chunk_size=8192):
                                fd.write(chunk)
                        print(f"successfully downloaded:      {granule_url}")
                    except requests.exceptions.RequestException as e:
                        print(f"An error occurred: {e}")
    else:
        print(
            f"No entry found for {hostname} in .netrc file.  "
            "Please refer to the README for the ECCO document generator for help"
        )


def get_a_file_with_min_num_vars(base_dir: str, nc_dir: str) -> str:
    """
    Return the path to the NetCDF file with the fewest variables in a directory.

    Variable count is estimated by the number of occurrences of the string
    ``"long_name"`` in the output of ``ncdump -h``, which corresponds to the
    number of coordinate and data variables that carry a ``long_name`` attribute.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param nc_dir: Path to the directory containing ``.nc`` files, relative
        to ``base_dir``.
    :type nc_dir: str
    :returns: Absolute path to the ``.nc`` file with the minimum variable count.
    :rtype: str
    """
    num_vars_per_file_list = []
    num_vars_min = 9999  # Sentinel value; assumes no file has more variables than this
    nc_files = glob.glob(f"{os.path.join(base_dir, nc_dir)}/*.nc")

    for nc_file in nc_files:
        # Shell pipeline equivalent: ncdump -h FILE | grep long_name | wc -l
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
            num_vars_min = num_vars if num_vars < num_vars_min else num_vars_min
        if stderr:
            print("STDERR Output:")
            print(stderr)

    return nc_files[num_vars_per_file_list.index(num_vars_min)]


def get_a_file_with_max_num_vars(base_dir: str, nc_dir: str) -> str:
    """
    Return the path to the NetCDF file with the most variables in a directory.

    Variable count is estimated by the number of occurrences of the string
    ``"long_name"`` in the output of ``ncdump -h``, which corresponds to the
    number of coordinate and data variables that carry a ``long_name`` attribute.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param nc_dir: Path to the directory containing ``.nc`` files, relative
        to ``base_dir``.
    :type nc_dir: str
    :returns: Absolute path to the ``.nc`` file with the maximum variable count.
    :rtype: str
    """
    num_vars_per_file_list = []
    num_vars_max = 0
    nc_files = glob.glob(f"{os.path.join(base_dir, nc_dir)}/*.nc")

    for nc_file in nc_files:
        # Shell pipeline equivalent: ncdump -h FILE | grep long_name | wc -l
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

    return nc_files[num_vars_per_file_list.index(num_vars_max)]


def sanitize(config_dictionary: dict, string: str) -> str:
    """
    Escape LaTeX special characters in a plain-text string.

    Replaces all LaTeX reserved characters (``& % $ # _ { } ~ ^ \\``) with
    their LaTeX-safe equivalents. Tab characters defined in the config are
    replaced with an ``\\hspace`` command of the configured width.

    .. note::
        This function does **not** preserve math environments. For strings that
        may contain ``$...$`` math, use :func:`sanitize_with_math` instead.

    :param config_dictionary: Configuration mapping. Must contain
        ``'tab_char'`` (str) and ``'tab_width_cm'`` (float).
    :type config_dictionary: dict
    :param string: The input string to sanitize.
    :type string: str
    :returns: The sanitized string, safe for direct inclusion in LaTeX source.
    :rtype: str
    """
    replacements = {
        r"&":  r"\&",
        r"%":  r"\%",
        r"$":  r"\$",
        r"#":  r"\#",
        r"_":  r"\_",
        r"{":  r"\{",
        r"}":  r"\}",
        r"~":  r"\textasciitilde",
        r"^":  r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|":  r"\textbar",
        config_dictionary['tab_char']: f"\\hspace{{{config_dictionary['tab_width_cm']}cm}}",
    }

    for key, value in replacements.items():
        string = string.replace(key, value)

    return string


def sanitize_with_math(config_dictionary: dict, string: str) -> str:
    """
    Escape LaTeX special characters while preserving inline math environments.

    Splits the string on ``$`` delimiters so that content inside ``$...$``
    math environments is left untouched. Only the non-math segments (even
    indices after splitting) are sanitized.

    .. note::
        ``$`` itself is intentionally excluded from the replacement table so
        that math delimiters are preserved. Compare with :func:`sanitize`,
        which does escape ``$``.

    :param config_dictionary: Configuration mapping. Must contain
        ``'tab_char'`` (str) and ``'tab_width_cm'`` (float).
    :type config_dictionary: dict
    :param string: The input string, which may contain ``$...$`` inline math.
    :type string: str
    :returns: The sanitized string with math environments intact.
    :rtype: str
    """
    replacements = {
        r"&":  r"\&",
        r"%":  r"\%",
        r"#":  r"\#",
        r"_":  r"\_",
        r"{":  r"\{",
        r"}":  r"\}",
        r"~":  r"\textasciitilde",
        r"^":  r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|":  r"\textbar",
        config_dictionary['tab_char']: f"\\hspace{{{config_dictionary['tab_width_cm']}cm}}",
    }

    # Split on '$' to separate math and non-math regions.
    # After splitting: even indices are outside math, odd indices are inside math.
    parts = string.split('$')

    if len(parts) != 1:
        # Only sanitize the non-math segments (even-indexed parts)
        for i in range(0, len(parts), 2):
            for key, value in replacements.items():
                parts[i] = parts[i].replace(key, value)
    else:
        # No math delimiters found — sanitize the whole string
        for key, value in replacements.items():
            parts[0] = parts[0].replace(key, value)

    return '$'.join(parts)


def sanitize_with_url(config_dictionary: dict, string: str) -> str:
    """
    Escape LaTeX special characters while preserving ``\\url{...}`` commands.

    Temporarily replaces any ``\\url{...}`` substrings with numbered
    placeholders, sanitizes the remaining text, then restores the original
    URL commands.

    :param config_dictionary: Configuration mapping. Must contain
        ``'tab_char'`` (str) and ``'tab_width_cm'`` (float).
    :type config_dictionary: dict
    :param string: The input string, which may contain ``\\url{...}`` commands.
    :type string: str
    :returns: The sanitized string with ``\\url{...}`` commands intact.
    :rtype: str
    """
    url_pattern = re.compile(r'\\url\{.*?\}')
    urls = re.findall(url_pattern, string)
    placeholders = [f'PLACEHOLDER{i}' for i in range(len(urls))]

    # Swap URLs out before sanitizing so their contents are not escaped
    for url, placeholder in zip(urls, placeholders):
        string = string.replace(url, placeholder)

    replacements = {
        r"&":  r"\&",
        r"%":  r"\%",
        r"$":  r"\$",
        r"#":  r"\#",
        r"_":  r"\_",
        r"{":  r"\{",
        r"}":  r"\}",
        r"~":  r"\textasciitilde",
        r"^":  r"\textasciicircum",
        r"\\": r"\textbackslash",
        r"|":  r"\textbar",
        config_dictionary['tab_char']: f"\\hspace{{{config_dictionary['tab_width_cm']}cm}}",
    }

    for key, value in replacements.items():
        string = string.replace(key, value)

    # Restore the original \url{...} commands
    for url, placeholder in zip(urls, placeholders):
        string = string.replace(placeholder, url)

    return string


def get_substring(input_string: str) -> str:
    """
    Extract the substring enclosed by the first pair of parentheses.

    :param input_string: The string to search for parentheses.
    :type input_string: str
    :returns: The content between the first ``(`` and the first ``)``.
        Returns an empty string if no parentheses are found.
    :rtype: str
    """
    start_pos = input_string.find('(') + 1
    end_pos = input_string.find(')')
    return input_string[start_pos:end_pos]


def add_to_line(line: str, before: str, after: str) -> str:
    """
    Replace all occurrences of a substring in a string.

    .. note::
        Equivalent to ``line.replace(before, after)`` but implemented as an
        explicit loop. Prefer the built-in ``str.replace`` for new code.

    :param line: The string to perform replacements on.
    :type line: str
    :param before: The substring to find and replace.
    :type before: str
    :param after: The replacement string.
    :type after: str
    :returns: The modified string with all occurrences of ``before`` replaced
        by ``after``.
    :rtype: str
    """
    while line.find(before) != -1:
        start_pos = line.find(before)
        end_pos = start_pos + len(before)
        line = line[:start_pos] + after + line[end_pos:]
    return line


def get_ds_title(ds: xr.Dataset) -> str:
    """
    Extract a compact title slug from an ECCO dataset's ``title`` attribute.

    Iterates over whitespace-separated words in ``ds.title``, skipping the
    word ``"ECCO"`` and stopping at ``"-"``. The remaining words are joined
    with underscores.

    :param ds: An ECCO NetCDF dataset with a ``title`` global attribute,
        e.g. ``"ECCO Ocean Temperature Salinity - Version 4 Release 4"``.
    :type ds: xr.Dataset
    :returns: Underscore-joined title slug,
        e.g. ``"Ocean_Temperature_Salinity"``.
    :rtype: str
    """
    fullTitle = ds.title
    title = ''
    for word in fullTitle.split():
        if word == 'ECCO':
            continue
        elif word == '-':
            # Stop at the dash that separates the product name from version info
            break
        else:
            title += word + '_'
    return title[:-1]  # Strip the trailing underscore


def get_granule_and_grid_types(granule_directory: str) -> tuple:
    """
    Infer the granule type and grid type from the last two path components.

    Assumes the directory structure follows the convention
    ``.../<granule_type>_<...>/<...>_<grid_type>/``. For example,
    ``".../variable_files/data_native"`` yields ``("variable", "native")``.

    :param granule_directory: Absolute or relative path to a granule directory.
    :type granule_directory: str
    :returns: A 2-tuple of ``(granule_type, grid_type)`` where
        ``granule_type`` is the prefix of the second-to-last path component
        (typically ``"coordinate"`` or ``"variable"``) and ``grid_type`` is
        the suffix of the last path component (typically ``"native"``,
        ``"latlon"``, or ``"1D"``).
    :rtype: tuple[str, str]
    """
    # Take the last two directory components to determine type metadata
    relevant_strings_list = granule_directory.split("/")[-2:]
    return (
        relevant_strings_list[0].split("_")[0],   # granule_type from parent dir
        relevant_strings_list[1].split("_")[-1]   # grid_type from leaf dir
    )
