import xarray as xa
import utils as u
import subprocess
import re


def get_non_coord_vars(ds_grid: xa.Dataset) -> dict:
    """
    Return a dictionary of non-coordinate variables from a dataset.

    :param ds_grid: The dataset to inspect.
    :type ds_grid: xa.Dataset
    :returns: Mapping of variable name to ``xa.Variable`` for all variables
        that are **not** listed in ``ds_grid.coords``.
    :rtype: dict[str, xa.Variable]
    """
    non_coord_vars = {
        key: value
        for key, value in ds_grid.variables.items()
        if key not in ds_grid.coords
    }
    return non_coord_vars


def readVarAttr(varName: str, var: xa.Variable) -> dict:
    """
    Extract attributes from a single netCDF variable into a flat dictionary.

    Prefixes all attribute keys with the variable name so they can be
    identified unambiguously in the returned dict.

    :param varName: The name of the variable (e.g. ``'THETA'``).
    :type varName: str
    :param var: The xarray Variable object whose attributes are to be extracted.
    :type var: xa.Variable
    :returns: Dictionary with keys ``'Variable Name'``,
        ``'Storage Type Definition'``, ``'Description'``, ``'Units'``,
        ``'Comment'``, ``'tag'``, plus one key per attribute prefixed with
        ``varName + ' '``.
    :rtype: dict
    """
    # Prefix all attribute keys with the variable name to avoid key collisions
    var.attrs = {
        varName + " " + (key[len(varName) + 1:] if key.startswith(varName + " ") else key): value
        for key, value in var.attrs.items()
    }

    d = dict()
    d["Variable Name"]           = varName
    d["Storage Type Definition"] = var.dtype
    d["Description"]             = var.attrs.get(f'{varName} description', 'N/A')
    d["Units"]                   = var.attrs.get(f'{varName} units', 'N/A')
    d["Comment"]                 = var.attrs.get(f'{varName} comment', 'N/A')
    d["tag"]                     = str(var.dtype) + " " + varName + " " + str(var.dims).replace("\'", "")

    # Include all raw attributes for completeness
    for attr, value in var.attrs.items():
        d[attr] = value

    return d


def readAllVarAttrs(ds_grid: xa.Dataset) -> list:
    """
    Extract attributes for every non-coordinate variable in a dataset.

    :param ds_grid: The dataset to inspect.
    :type ds_grid: xa.Dataset
    :returns: One dictionary per non-coordinate variable, as returned by
        :func:`readVarAttr`.
    :rtype: list[dict]
    """
    vars = get_non_coord_vars(ds_grid)
    a = list()
    for var in vars:
        a.append(readVarAttr(var, vars[var]))
    return a


# ---------------------------------------------------------------------------
# Functions for grouping tasks in native_sets.py, 1D.py, and latlong.py
# ---------------------------------------------------------------------------

def process_dict_items(data: dict, sanitation_func) -> list:
    """
    Apply a sanitation function to every value in a dictionary.

    :param data: The dictionary whose values are to be sanitized.
    :type data: dict
    :param sanitation_func: A callable that accepts a single string value and
        returns a sanitized string.
    :type sanitation_func: callable
    :returns: Sanitized values in the same iteration order as
        ``data.items()``.
    :rtype: list
    """
    results = []
    for key, value in data.items():
        sanitized_value = sanitation_func(value)
        results.append(sanitized_value)
    return results


def data_var_table(fieldName: str, da: dict, ds_name: str) -> list:
    """
    Build a LaTeX ``longtable`` for a single data variable (legacy reader version).

    This is an earlier version of the table builder used with the legacy
    :func:`readVarAttr` / :func:`compute_ds_dict` pipeline. For new code,
    prefer ``cdf_extract.data_var_table``.

    :param fieldName: Short name of the variable, used in the caption and label.
    :type fieldName: str
    :param da: Attribute dictionary as returned by :func:`compute_ds_dict`.
        Expected keys: ``'Storage Type Definition'``, ``'Variable Name'``,
        ``'Description'``, ``'Units'``, ``'Comment'``,
        ``'Example CDL Description'``.
    :type da: dict
    :param ds_name: Short name of the parent dataset, used in the caption
        and label.
    :type ds_name: str
    :returns: LaTeX lines forming a complete ``longtable`` environment.
    :rtype: list[str]
    """
    new_sani = u.sanitize(ds_name)

    la = [
        r'\begin{longtable}{|p{0.1\textwidth}|p{0.35\textwidth}|p{0.45\textwidth}|p{0.1\textwidth}|}',
        fr"\caption{{CDL example description of {new_sani}\'s {u.sanitize(fieldName)} variable}}",
        fr'\label{{tab:table-{ds_name}_{fieldName}}} \\',
        r'\hline \endhead \hline \endfoot',
    ]

    storageType = u.sanitize(str(da["Storage Type Definition"]))
    varName     = u.sanitize(da["Variable Name"])
    description = u.sanitize(da["Description"])
    unit        = u.sanitize(da["Units"])
    comment     = u.sanitize(da["Comment"])

    # CDL description may contain math; use the math-aware sanitizer
    cdl_description = u.sanitize_with_math(da['Example CDL Description'])
    cdl_description = cdl_description.replace(r'\\', '\'')
    cdl_description = cdl_description.replace('\n', '\\\\\\\n')

    # Header and data rows
    la.append(r'\rowcolor{lightgray} \textbf{Storage Type} & \textbf{Variable Name} & \textbf{Description} & \textbf{Unit} \\ \hline')
    la.append(rf'{storageType} & {varName} & {description} & {unit} \\ \hline')
    la.append(r'\rowcolor{lightgray}  \multicolumn{4}{|p{1.08\textwidth}|}{\textbf{CDL Description}} \\ \hline')
    la.append(
        r'\multicolumn{4}{|p{1.08\textwidth}|}' +
        r'{\makecell{\parbox{1.08\textwidth}' + rf'{{{cdl_description}}}' + r'}} \\ \hline'
    )
    la.append(r'\rowcolor{lightgray} \multicolumn{4}{|p{1.08\textwidth}|}{\textbf{Comments}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1.08\textwidth}|}' + rf'{{{comment}}}' + r' \\ \hline')
    la.append(r'\end{longtable}')
    la.append(r"")

    return la


def compute_ds_dict(varName: str, var: xa.DataArray) -> dict:
    """
    Build a metadata dictionary from an xarray DataArray for use in table generation.

    Constructs a CDL-style attribute listing where each attribute is formatted
    as ``varName:attr = value ;``, stored as a sub-dictionary keyed by
    attribute name.

    :param varName: The short name of the variable (e.g. ``'THETA'``).
    :type varName: str
    :param var: The DataArray whose attributes are to be extracted.
    :type var: xa.DataArray
    :returns: Dictionary with keys ``'Variable Name'``,
        ``'Storage Type Definition'``, ``'Description'``, ``'Units'``,
        ``'Example CDL Description'`` (a sub-dict of CDL-formatted attribute
        strings plus a ``'tag'`` entry), and ``'Comment'``.
    :rtype: dict
    """
    d = dict()
    attrDict = dict()

    # Tag line: dtype, variable name, and dimension tuple in CDL format
    attrDict["tag"] = str(var.dtype) + " " + varName + " " + str(var.dims).replace("\'", "") + ' ;'

    # Format each attribute as a CDL assignment; quote string values
    for attr, value in var.attrs.items():
        formatted_value = f'"{value}"' if isinstance(value, str) else value
        attrDict[attr] = varName + ':' + attr + ' = ' + str(formatted_value) + ' ;'

    d["Variable Name"]           = varName
    d["Storage Type Definition"] = str(var.dtype)
    d["Description"]             = var.attrs.get('long_name', 'N/A')
    d["Units"]                   = var.attrs.get('units', 'N/A')
    d['Example CDL Description'] = attrDict
    d["Comment"]                 = var.attrs.get('comment', 'N/A')

    return d


def read_data_vars(ds: xa.Dataset) -> dict:
    """
    Build a metadata dictionary for every data variable in a dataset.

    :param ds: The dataset to inspect.
    :type ds: xa.Dataset
    :returns: Mapping of variable name (str) to the metadata dict returned by
        :func:`compute_ds_dict` for that variable.
    :rtype: dict[str, dict]
    """
    ds_dict = dict()
    for vars in ds.data_vars:
        varsStr = str(vars)
        ds_dict[vars] = compute_ds_dict(varsStr, ds.data_vars[varsStr])
    return ds_dict


# ---------------------------------------------------------------------------
# CDL generation utilities
# ---------------------------------------------------------------------------

def generate_CDL(original_nc_path: str, new_nc_path: str) -> str:
    """
    Copy a NetCDF file and return its CDL header as a string.

    Uses ``nccopy`` to create a copy of the file at ``new_nc_path``, then
    runs ``ncdump -h`` to obtain the CDL (Common Data Language) header.
    The header-only flag (``-h``) avoids loading potentially large data arrays.

    :param original_nc_path: Path to the source NetCDF file.
    :type original_nc_path: str
    :param new_nc_path: Path where the copied file should be written.
    :type new_nc_path: str
    :returns: The ``ncdump -h`` output for the copied file.
    :rtype: str
    """
    # Copy the file first so the original is never modified
    nccopy_command = f'nccopy {original_nc_path} {new_nc_path}'
    subprocess.run(nccopy_command, shell=True, check=True)

    ncdump_command = f'ncdump -h {new_nc_path}'
    ncdump_result  = subprocess.run(ncdump_command, shell=True, check=True, capture_output=True, text=True)

    return ncdump_result.stdout


def cdl_to_latex(cdl_string: str, name: str = "example") -> list:
    """
    Convert a CDL header string into a LaTeX ``longtable`` representation.

    Parses the CDL output of ``ncdump -h`` line by line and applies
    colour-coded row backgrounds:

    - **YellowGreen** — dimension entries.
    - **Apricot** — variable declarations and their attributes.

    :param cdl_string: The raw CDL header string, as returned by
        :func:`generate_CDL`.
    :type cdl_string: str
    :param name: Human-readable dataset name used in the table caption and
        label. Default is ``"example"``.
    :type name: str
    :returns: LaTeX lines forming a complete ``longtable`` environment.
    :rtype: list[str]
    """
    lines = cdl_string.split('\n')

    latex_lines = [
        r'\begin{longtable}{|p{\textwidth}|}',
        r'\caption{Example CDL description of ' + name + r' dataset}',
        r'\label{tab:cdl-' + name + r'} \\',
        r'\hline \endhead',
        r'\hline \endfoot',
    ]

    variables_start  = False
    dimensions_start = False
    independent_vars = []

    for line in lines:
        new_line = u.sanitize_with_math(line)

        if line.startswith('netcdf'):
            latex_lines.append(new_line + r'\\')
            continue

        if "dimensions:" in line:
            # Switch into dimension-parsing mode
            dimensions_start = True
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            continue
        elif "variables:" in line:
            # Switch from dimension-parsing to variable-parsing mode
            dimensions_start = False
            variables_start  = True
            latex_lines.append(r'\hline')
            latex_lines.append(new_line + r'\\')
            latex_lines.append(r'\hline')
            continue

        if dimensions_start:
            # Dimension lines get a yellow-green background
            latex_lines.append(r'\rowcolor{YellowGreen}' + new_line + r'\\')
            independent_vars.append(u.get_substring(new_line))

        elif variables_start:
            data_var = u.get_substring(line)
            # Detect the end of the variables block: no double-tab indent means
            # we've reached the global attributes section
            if "\t\t" not in line and data_var + "(" + data_var + ")" not in line:
                variables_start = False
                latex_lines.append(r'\hline')
                latex_lines.append(new_line + r'\\')
            else:
                # Variable declarations and attributes get an apricot background
                latex_lines.append(r'\rowcolor{Apricot}' + new_line + r'\\')
        else:
            latex_lines.append(new_line + r'\\')

    latex_lines.append(r'\hline')
    latex_lines.append(r'\end{longtable}')
    return latex_lines
