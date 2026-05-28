import re
import os
import numpy as np
import xarray as xr
import json
import subprocess
from pathlib import Path
import sys
import pdb
import matplotlib.pyplot as plt

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils_general
import src.document_generator.utils.utils_json as utils_json
import src.document_generator.utils.cdf_plotter as cdf_plotter



# ---------------------------------------------------------------------------
# -------------------- Extracting CDL For Examples -------------------------
# ---------------------------------------------------------------------------

def fieldTable(config_dictionary: dict, dataset: xr.Dataset, is_coord: bool, grid_type: str) -> list:
    """
    Build a LaTeX ``longtable`` listing all coordinates and variables in a dataset.

    Produces a two-section table: one section for coordinate variables and one
    for data variables, each with columns for variable name, description, and
    unit. Column widths are chosen dynamically based on the longest variable name.

    :param config_dictionary: Configuration mapping passed to sanitization utilities.
    :type config_dictionary: dict
    :param dataset: The open NetCDF dataset to describe.
    :type dataset: xr.Dataset
    :param is_coord: Unused in the current implementation; reserved for future
        filtering logic.
    :type is_coord: bool
    :param grid_type: Human-readable grid type string included in the table
        caption, e.g. ``"native"`` or ``"latlon"``.
    :type grid_type: str
    :returns: LaTeX lines forming a complete ``longtable`` environment.
    :rtype: list[str]
    """
    product_name = get_product_name(dataset)
    datavar_shortname_list, datavar_longname_list, datavar_units_list = get_variable_names_in_dataset(dataset=dataset, isCoord=False)
    coordvar_shortname_list, coordvar_longname_list, coordvar_units_list = get_variable_names_in_dataset(dataset=dataset, isCoord=True)

    # NoTE: Bruce: THIS METHOD HAS A THRESHOLD, PAST WHICH IT ASSIGNS A FIXED LENGTH.  THAT'S WHY "ocean_column_thickness" is spilling into the next column!
    # Compute column widths based on the longest variable name across both lists
    a, b = table_cellSize(datavar_shortname_list + coordvar_shortname_list)

    # ----------------------------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------------------------
    # Bruce - HACK CITY
    # ----------------------------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------------------------
    coordvar_shortname_list_hyphenated = hyphenate_shortnames(coordvar_shortname_list)
    datavar_shortname_list_hyphenated = hyphenate_shortnames(datavar_shortname_list)
    # ----------------------------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------------------------

    latex_lines = []
    latex_lines.append(r'\begin{longtable}{|m{' + str(a) + r'\textwidth}|m{' + str(b) + r'\textwidth}|m{0.12\textwidth}|}')
    latex_lines.append(fr"\caption{{Coordinates and variables from {utils_general.sanitize(config_dictionary, product_name)} ({grid_type})}}")
    latex_lines.append(fr'\label{{tab:table-{dataset}-fields}} \\ ')
    latex_lines.append(r"\hline \endfirsthead \endhead \endfoot \hline \endlastfoot")

    # --- Coordinates section ---
    latex_lines.append(
        r'\rowcolor{lightgray} \multicolumn{1}{|c|}{\textbf{Coordinates}} '
        r'& \multicolumn{1}{|c|}{\textbf{Description of data coordinates}} '
        r'& \multicolumn{1}{|c|}{\textbf{Unit}}\\ \hline'
    )

    for ij in np.arange(len(coordvar_shortname_list)):
        latex_lines.append(
            f'{utils_general.sanitize(config_dictionary, coordvar_shortname_list_hyphenated[ij])} &'
            f'{utils_general.sanitize(config_dictionary, coordvar_longname_list[ij])} &'
            rf'{utils_general.sanitize(config_dictionary, coordvar_units_list[ij])}  \\ \hline'
        )

    # --- Data variables section ---
    latex_lines.append(
        r'\rowcolor{lightgray} \multicolumn{1}{|c|}{\textbf{Variables}} '
        r'& \multicolumn{1}{|c|}{\textbf{Description of data variables}} '
        r'& \multicolumn{1}{|c|}{\textbf{Unit}}\\ \hline'
    )

    for ij in np.arange(len(datavar_shortname_list)):
        latex_lines.append(
            f'{utils_general.sanitize(config_dictionary, datavar_shortname_list_hyphenated[ij])} &'
            f'{utils_general.sanitize(config_dictionary, datavar_longname_list[ij])} &'
            rf'{utils_general.sanitize(config_dictionary, datavar_units_list[ij])}  \\ \hline'
        )

    latex_lines.append(r'\end{longtable}')
    latex_lines.append(r"")
    return latex_lines


def latex_example_netcdf(base_dir: str, config_dictionary: dict, grid_type: str) -> None:
    """
    Write a LaTeX ``longtable`` showing an example NetCDF file's CDL structure.

    Opens the example granule for the given grid type (specified by name in the
    config), then renders its dimensions, coordinate variables (with all
    attributes), and non-coordinate data variables into a colour-coded longtable
    and writes the result to the output ``.tex`` file path defined in the config.

    Colour coding follows CDL convention:

    - **YellowGreen** — dimension entries.
    - **Apricot** — coordinate variable declarations and their attributes.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param config_dictionary: Configuration mapping. Expected keys include
        ``'variable_files_{grid_type}_dir'``, ``'example_granule_{grid_type}'``,
        ``'example_table_first_latex_lines_{grid_type}'``, and
        ``'example_{grid_type}_table_tex_file'``.
    :type config_dictionary: dict
    :param grid_type: Grid type identifier, e.g. ``"native"``, ``"latlon"``,
        or ``"1D"``.
    :type grid_type: str
    :returns: None
    """
    granule_directory = os.path.join(base_dir, config_dictionary[f"variable_files_{grid_type}_dir"])

    # Use the hardcoded example granule filename as requested by the project lead
    example_granule = os.path.join(granule_directory, config_dictionary[f"example_granule_{grid_type}"])

    # Open without decoding so raw attribute values are preserved in the CDL output
    dataset = xr.open_dataset(
        example_granule,
        decode_times=False, decode_cf=False,
        decode_coords=False, decode_timedelta=False
    )

    latex_lines = []
    latex_lines.append(r'\begin{longtable}{|p{\textwidth}|}')
    latex_lines.extend(config_dictionary[f"example_table_first_latex_lines_{grid_type}"])
    #latex_lines.append(r'granule:  ' + utils_general.sanitize(config_dictionary, f'{config_dictionary[f"example_granule_{grid_type}"]}') + r'\\')
    #latex_lines.append('granule: ' + utils_general.sanitize(config_dictionary, f'‘{config_dictionary[f"example_granule_{grid_type}"]}’') + r'\\')
    #latex_lines.append(r'\noindent from: \hfill' + utils_general.sanitize(config_dictionary, f'‘{config_dictionary[f"example_granule_{grid_type}"]}’') + r'\hfill\null \\')
    latex_lines.append(r'\noindent granule: \hfill ' + utils_general.sanitize(config_dictionary, f'{config_dictionary[f"example_granule_{grid_type}"]}') + r'\hfill\null \\')
    latex_lines.append(r'\hline')
    latex_lines.append(r'dimensions: \\')
    latex_lines.append(r'\hline')

    # --- Dimensions block (highlighted in yellow-green) ---
    for dimension_name in dataset.sizes:
        num_tabs = 1
        latex_lines.append(r'\rowcolor{YellowGreen}')
        latex_lines = utils_general.append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines)
        latex_lines.append(
            utils_general.sanitize_with_math(config_dictionary, f'{dimension_name} = {len(dataset[dimension_name])}') + "\\\\"
        )

    latex_lines.append(r'\hline')
    latex_lines.append(r'coordinates: \\')
    latex_lines.append(r'\hline')

    # --- Coordinates block (highlighted in apricot) ---
    for coord_name in dataset.coords:
        coord = dataset[coord_name]
        coord_dt = str(coord.dtype)
        coord_dims = ', '.join([str(x) for x in coord.dims])

        num_tabs = 1
        latex_lines.append(r'\rowcolor{Apricot}')
        latex_lines = utils_general.append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines)
        latex_lines.append(
            utils_general.sanitize_with_math(
                config_dictionary,
                f'{config_dictionary["tab_char"] * num_tabs}{coord_dt} {coord.name} ({coord_dims})'
            ) + "\\\\"
        )
        # Indent each attribute one level deeper than the variable declaration
        num_tabs += 1
        for coord_attr in coord.attrs:
            latex_lines.append(r'\rowcolor{Apricot}')
            latex_lines = utils_general.append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines)
            latex_lines.append(
                utils_general.sanitize_with_math(
                    config_dictionary,
                    f'{config_dictionary["tab_char"] * num_tabs}{coord.name}:{coord_attr} = "{coord.attrs[coord_attr]}"'
                ) + "\\\\"
            )

    # Separate data vars by coverage_content_type into true data variables
    # and those that are coordinate-typed but stored in data_vars
    coords = []     # variables flagged as coordinates within data_vars
    data_vars = []  # true scientific data variables

    latex_lines.append(r'\hline')
    latex_lines.append(r'data variables: \\')
    latex_lines.append(r'\hline')

    for datavar_name in dataset.data_vars:
        if dataset[datavar_name].attrs['coverage_content_type'] == 'coordinate':
            coords.append(dataset[datavar_name])
        else:
            data_vars.append(dataset[datavar_name])

    # Note: coordinate-typed data_vars are intentionally not rendered here.
    # See the commented-out block in the original source for context.

    for datavar in data_vars:
        datavar_dt = str(datavar.dtype)
        datavar_dims = ', '.join([str(x) for x in datavar.dims])

        num_tabs = 1
        latex_lines = utils_general.append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines)
        latex_lines.append(
            utils_general.sanitize_with_math(
                config_dictionary,
                f'{config_dictionary["tab_char"] * num_tabs}{datavar_dt} {datavar.name} ({datavar_dims})'
            ) + "\\\\"
        )
        num_tabs += 1
        for datavar_attr in datavar.attrs:
            latex_lines = utils_general.append_hanging_indentation_commands_cm_latex(config_dictionary, num_tabs, latex_lines)
            latex_lines.append(
                utils_general.sanitize_with_math(
                    config_dictionary,
                    f'{config_dictionary["tab_char"] * num_tabs}{datavar.name}:{datavar_attr} = "{datavar.attrs[datavar_attr]}"'
                ) + "\\\\"
            )

    latex_lines.append(r'\end{longtable}')

    latex_output_file = os.path.join(base_dir, config_dictionary[f"example_{grid_type}_table_tex_file"])
    Path(latex_output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(latex_output_file, 'w') as output_file:
        output_file.write('\n'.join(latex_lines))


# ---------------------------------------------------------------------------
# -------------------- Extracting CDL For Datasets -------------------------
# ---------------------------------------------------------------------------

def get_non_coordinate_vars(filename: str) -> list:
    """
    Return the non-coordinate data variables from a NetCDF file.

    A variable is considered non-coordinate if its ``coverage_content_type``
    attribute is **not** ``'coordinate'``. Results are sorted alphabetically
    by variable name.

    :param filename: Path to the NetCDF file.
    :type filename: str
    :returns: DataArrays for all non-coordinate variables, sorted by name.
    :rtype: list[xr.DataArray]
    """

    #print()
    #print(filename)


    dataset = xr.open_dataset(
        filename,
        decode_times=False, decode_coords=False,
        decode_cf=False, decode_timedelta=False
    )
    non_coordinate = []

    for var in dataset.data_vars:
        try:
            if dataset[var].attrs['coverage_content_type'] != 'coordinate':
                non_coordinate.append(var)
        except:
            pdb.set_trace()
    non_coordinate = sorted(non_coordinate)
    data_array_list = [dataset[field] for field in non_coordinate]

    return data_array_list


def get_coordinate_vars(filename: str) -> list:
    """
    Return the coordinate-typed data variables from a NetCDF file.

    The logic for identifying coordinate variables differs between native and
    lat-lon grids:

    - **Native** — variables whose dimensions include ``'tile'`` and whose
      name does not contain ``'bnds'``.
    - **Lat-lon** — all data variables whose name does not contain ``'bnds'``.

    .. note::
        The heuristic for distinguishing native from lat-lon datasets checks
        whether the string ``'native'`` appears in
        ``dataset.attrs['product_name']``. Consider using an explicit config
        flag for robustness.

    :param filename: Path to the NetCDF file.
    :type filename: str
    :returns: DataArrays for all coordinate-typed variables found in the file.
    :rtype: list[xr.DataArray]
    """
    dataset = xr.open_dataset(
        filename,
        decode_times=False, decode_coords=False,
        decode_cf=False, decode_timedelta=False
    )
    coordinate = []

    # Determine dataset type from product_name attribute
    dataset_type = 'native' if 'native' in dataset.attrs['product_name'] else 'lat-lon'

    if dataset_type == 'native':
        # Native grids use LLC tiles; coordinate variables span the tile dimension
        for var in dataset.data_vars:
            var = dataset[var]
            if 'tile' in var.dims and 'bnds' not in var.name:
                coordinate.append(var)
    else:
        # Lat-lon grids: all data variables except bounds arrays are coordinates
        for var in dataset.data_vars:
            var = dataset[var]
            if "bnds" not in var.name:
                coordinate.append(var)

    data_array_list = [dataset[field.name] for field in coordinate]
    return data_array_list


def extract_field_info(field: xr.DataArray) -> dict:
    """
    Extract metadata from a DataArray into a flat dictionary for table rendering.

    Builds a CDL-style description string from the variable's attributes (sorted
    alphabetically, as per CDL convention), then packages it alongside the key
    metadata fields needed to populate a LaTeX variable table.

    .. note::
        The CDL description is built via a chain of string substitutions using
        the sentinel strings ``'-Coovi-Paul-Houndegnonto-'``, ``'Victory'``, and
        ``'I will have a job soon'`` as temporary delimiters. These strings are
        assumed to be absent from any real attribute values. They allow safe
        handling of commas and trailing newlines without regex.

    :param field: The data variable to describe.
    :type field: xr.DataArray
    :returns: Dictionary with keys ``'Variable Name'``, ``'Storage Type'``,
        ``'Description'``, ``'Units'``, ``'CDL Description'``, and
        ``'Comments'``.
    :rtype: dict
    """
    name = str(field.name)
    storageType = str(field.dtype)
    dims = str(field.dims).replace("'", "")

    # Format the CDL variable header: e.g. "float32 THETA(time, tile, k, j, i)"
    if dims[-2] == ',':
        dims = dims[:-2] + ')'
    fieldHeader = storageType + ' ' + name + dims

    # Build a sorted attribute dict using unique sentinel strings as key separators.
    # Commas in attribute values are temporarily replaced to avoid confusion with CSV joins.
    temp = {
        name + '-Coovi-Paul-Houndegnonto-' + k: str(v).replace(',', 'I will have a job soon').replace('_', ' ')
        for (k, v) in field.attrs.items()
        if k != 'comment'
    }
    mykeys = sorted(list(temp.keys()))
    temp = {i: temp[i] for i in mykeys}

    # Mark the last attribute so we can append a trailing newline after it
    Last_key = list(temp.keys())[-1]
    temp[Last_key] = str(temp[Last_key]) + ' Victory'

    # Convert the dict to a multi-line CDL string via a chain of replacements
    stringTemp = str(temp)
    stringTemp = stringTemp.replace('{', '')
    stringTemp = stringTemp.replace('}', '')
    stringTemp = stringTemp.replace("'", '')
    stringTemp = stringTemp.replace('"', '')
    stringTemp = stringTemp.replace(',', '\n')          # each attribute on its own line
    stringTemp = stringTemp.replace('\n ', '\n')
    stringTemp = stringTemp.replace(':', ' =')
    stringTemp = stringTemp.replace('-Coovi-Paul-Houndegnonto-', ': ')  # restore "varname: attr"
    stringTemp = stringTemp.replace("I will have a job soon", ',')       # restore commas in values
    stringTemp = stringTemp.replace(' Victory', '\n')                    # final newline after last attr

    # Indent the variable name itself with four spaces to match CDL convention
    stringTemp = stringTemp.replace(name, f'    {name}')
    stringTemp = fieldHeader + '\n' + stringTemp

    data = dict()
    data["Variable Name"] = name
    data['Storage Type'] = storageType
    dims = str(field.dims).replace("'", "")
    data['Description'] = field.attrs.get('long_name', 'N/A')
    data['Units'] = field.attrs.get('units', 'N/A')
    data['CDL Description'] = stringTemp
    data['Comments'] = field.attrs.get('comment', 'N/A')

    return data


def search_and_extract(
    granule_filename_truncated_stem: str,
    granule_directory: str,
    is_coord: bool = False
) -> tuple:
    """
    Find a NetCDF file by a stem substring and extract its variables.

    Walks ``granule_directory`` recursively to find the first ``.nc`` file
    whose name contains ``granule_filename_truncated_stem``, then returns
    either the coordinate or non-coordinate variables depending on ``is_coord``.

    :param granule_filename_truncated_stem: Substring to match against file
        names (case-sensitive).
    :type granule_filename_truncated_stem: str
    :param granule_directory: Root directory to search recursively.
    :type granule_directory: str
    :param is_coord: If ``True``, return coordinate variables; if ``False``
        (default), return non-coordinate data variables.
    :type is_coord: bool
    :returns: A 2-tuple of ``(data_array_list, dataset)`` where
        ``data_array_list`` is a list of :class:`xr.DataArray` objects and
        ``dataset`` is the open :class:`xr.Dataset` for the matched file.
    :rtype: tuple[list[xr.DataArray], xr.Dataset]
    :raises ValueError: If no ``.nc`` file containing the given stem is found
        in the directory.
    """
    for root, dirs, files in os.walk(granule_directory):
        for file in files:
            if granule_filename_truncated_stem in file and file.endswith(".nc"):
                filepath = os.path.join(root, file)
                if is_coord:
                    data_array_list = get_coordinate_vars(filepath)
                else:
                    data_array_list = get_non_coordinate_vars(filepath)
                dataset = xr.open_dataset(filepath)

                #for da in data_array_list:
                #    print(filepath)
                #    print(da.name)

                return data_array_list, dataset
            #else:
        return None
    
    # NoTE: The error below triggers whenever a grouping is not found to have a corresponding granule.
    # This is wrong - we must allow users to have any set of granule files

    #raise ValueError(
    #    f"No NetCDF file containing '{granule_filename_truncated_stem}' "
    #    f"found in granule_directory '{granule_directory}'"
    #)


def data_var_table(
    config_dictionary: dict,
    field_name: str,
    attrs: dict,
    dataset_name: str,
    grid_type: str
) -> list:
    """
    Build a LaTeX ``longtable`` for a single data variable.

    Creates a four-column table with a header row (storage type, variable name,
    description, unit), a full-width CDL description block rendered in
    typewriter font, and a full-width comments row.

    :param config_dictionary: Configuration mapping passed to sanitization
        utilities.
    :type config_dictionary: dict
    :param field_name: Short name of the variable, used in the caption and label.
    :type field_name: str
    :param attrs: Attribute dictionary as returned by :func:`extract_field_info`.
        Expected keys: ``'Storage Type'``, ``'Variable Name'``,
        ``'Description'``, ``'Units'``, ``'CDL Description'``, ``'Comments'``.
    :type attrs: dict
    :param dataset_name: Short name of the parent dataset, used in the caption
        and label.
    :type dataset_name: str
    :param grid_type: Human-readable grid type included in the caption,
        e.g. ``"native"``.
    :type grid_type: str
    :returns: LaTeX lines forming a complete ``longtable`` environment for this
        variable.
    :rtype: list[str]
    """
    dataset_name_formatted = utils_general.sanitize(config_dictionary, dataset_name)

    storageType = utils_general.sanitize(config_dictionary, attrs["Storage Type"])
    varName     = utils_general.sanitize(config_dictionary, attrs["Variable Name"])
    description = utils_general.sanitize(config_dictionary, attrs["Description"])
    unit        = utils_general.sanitize(config_dictionary, attrs["Units"])
    comment     = utils_general.sanitize(config_dictionary, attrs["Comments"])

    # CDL description may contain math; use the math-aware sanitizer
    cdl_description = utils_general.sanitize_with_math(config_dictionary, attrs['CDL Description'])
    cdl_description = cdl_description.replace(r'\\', '\'')
    cdl_description = cdl_description.replace('\n', '\\\\\\\n')
    cdl_description = cdl_description.replace('    ', r'\hspace*{0.5cm}')

    # Choose column widths based on length of variable name
    if len(varName) >= 29:
        a = 0.44; b = 0.38
    else:
        a = 0.3;  b = 0.45

    la = [
        r'\begin{longtable}{|m{0.06\textwidth}|m{' + str(a) + r'\textwidth}|m{' + str(b) + r'\textwidth}|m{0.12\textwidth}|}',
        fr"\caption{{{utils_general.sanitize(config_dictionary, field_name)} from {dataset_name_formatted} ({grid_type})}}"
        fr'\label{{tab:table-{dataset_name}_{field_name}}} \\ ',
        r'\hline \endhead \hline \endfoot',
    ]

    # Header row
    la.append(r'\rowcolor{lightgray} \textbf{Storage Type} & \textbf{Variable Name} & \textbf{Description} & \textbf{Unit} \\ \hline')
    la.append(rf'{storageType} & {varName} & {description.capitalize()} & {unit} \\ \hline')

    # CDL description block — rendered in monospace font via \fontfamily{lmtt}
    #la.append(r'\multicolumn{4}{|c|}{\cellcolor{lightgray}{\textbf{Description of the variable in Common Data language (CDL)}}} \\ \hline')
    la.append(r'\multicolumn{4}{|c|}{\cellcolor{lightgray}{\textbf{Common Data format Language (CDL) description}}} \\ \hline')
    la.append(
        r'\multicolumn{4}{|c|}' +
        r'{\fontfamily{lmtt}\selectfont{\makecell{\parbox{.95\textwidth}' +
        r'{\vspace*{0.25cm} \footnotesize{' + rf'{cdl_description}' + r'}}}}} \\ \hline'
    )

    # Comments row
    la.append(r'\rowcolor{lightgray} \multicolumn{4}{|c|}{\textbf{Comments}} \\ \hline')
    la.append(r'\multicolumn{4}{|p{1\textwidth}|}{\footnotesize{' + rf'{{{comment.capitalize()}}}' + r'}} \\ \hline')
    la.append(r'\end{longtable}')
    la.append(r"")

    return la


# ---------------------------------------------------------------------------
# ----------------------------- Helper Functions ----------------------------
# ---------------------------------------------------------------------------

def get_product_name(dataset: xr.Dataset) -> str:
    """
    Extract the all-uppercase product name prefix from a dataset's ``product_name`` attribute.

    Iterates over underscore-separated tokens in the ``product_name`` global
    attribute and collects consecutive all-uppercase tokens, e.g.
    ``"OCEAN_TEMPERATURE_SALINITY"`` from a longer product string.

    :param dataset: An ECCO NetCDF dataset with a ``product_name`` global
        attribute.
    :type dataset: xr.Dataset
    :returns: The uppercase product name prefix,
        e.g. ``"OCEAN_TEMPERATURE_SALINITY"``.
    :rtype: str
    """
    h = dataset.attrs['product_name'].split('_')
    product_name = ''
    for i in h:
        if i.isupper():
            product_name += i + '_'
        else:
            # First non-uppercase token marks the end of the product name prefix
            product_name = product_name[:-1]
            break
    return product_name


def get_variable_names_in_dataset(dataset: xr.Dataset, isCoord: bool = False) -> tuple:
    """
    Extract short names, long names, and units for either coordinates or data variables.

    :param dataset: The dataset to inspect.
    :type dataset: xr.Dataset
    :param isCoord: If ``False`` (default), extract from ``dataset.data_vars``.
        If ``True``, extract from ``dataset.coords``.
    :type isCoord: bool
    :returns: A 3-tuple of ``(shortnames_list, longnames_list, units_list)``
        where each element is a list of strings. Units default to
        ``'--none--'`` when the ``units`` attribute is absent.
    :rtype: tuple[list[str], list[str], list[str]]
    """
    var_list = list(dataset.coords if isCoord else dataset.data_vars)
    shortnames_list = []
    longnames_list  = []
    units_list      = []

    for ij in np.arange(len(var_list)):
        shortnames_list.append(var_list[ij])
        longnames_list.append(str(dataset[var_list[ij]].long_name).capitalize())
        if 'units' in dataset[var_list[ij]].attrs.keys():
            units_list.append(dataset[var_list[ij]].units)
        else:
            units_list.append('--none--')

    return shortnames_list, longnames_list, units_list


def table_cellSize(field_var: list) -> tuple:
    """
    Compute proportional column widths for a LaTeX table based on variable name length.

    Returns fractions of ``\\textwidth`` for the "Variable Name" column (``a``)
    and the "Description" column (``b``), widening the name column if any name
    exceeds 28 characters.

    :param field_var: Variable (or coordinate) short names to consider.
    :type field_var: list[str]
    :returns: A 2-tuple ``(a, b)`` where ``a`` is the fraction of
        ``\\textwidth`` for the variable name column and ``b`` is the fraction
        for the description column.
    :rtype: tuple[float, float]
    """
    maxVarlen = max(len(v) for v in field_var)

    if maxVarlen >= 29:
        a = 0.4;  b = 0.39
    else:
        a = 0.15; b = 0.64

    return a, b


def global_attrs_for_ECCOnetCDF(
    jsonFileRef: str,
    GlobalAttrsCollect: list,
    tableCaption: str,
    latexFilename: str,
    saveTo: str
) -> None:
    """
    Generate a LaTeX ``longtable`` describing selected global NetCDF attributes.

    Reads a reference JSON file that maps attribute names to their type,
    description, and source, then writes a table covering the requested
    subset of attributes. Attributes not found in the reference are filled
    with ``"TBD"``.

    :param jsonFileRef: Path to the JSON reference file. Each key is an
        attribute name; each value is a dict with ``'type'``, ``'description'``,
        and ``'sourc'`` keys.
    :type jsonFileRef: str
    :param GlobalAttrsCollect: Ordered list of attribute names to include in
        the table.
    :type GlobalAttrsCollect: list[str]
    :param tableCaption: LaTeX caption string for the generated table.
    :type tableCaption: str
    :param latexFilename: Filename of the output ``.tex`` file (without
        directory prefix).
    :type latexFilename: str
    :param saveTo: Directory path where the output file should be written.
    :type saveTo: str
    :returns: None
    """
    GlobAttrsFilledECCO = {}

    with open(jsonFileRef, 'r') as json_file:
        data = json.load(json_file)

    AttrsRef = list(data.keys())

    # Build the ordered attribute dict, inserting "TBD" for any unknown attributes
    for itk in GlobalAttrsCollect:
        if itk in AttrsRef:
            GlobAttrsFilledECCO.update({
                itk: {
                    "type": data[itk]['type'],
                    "description": data[itk]['description'],
                    "sourc": data[itk]['sourc']
                }
            })
        else:
            GlobAttrsFilledECCO.update({itk: {"type": "TBD", "description": "TBD", "sourc": "TBD"}})

    latex_lines = [
        r'\begin{longtable}{|p{0.28\textwidth}|p{0.06\textwidth}|p{0.51\textwidth}|p{0.07\textwidth}|}',
        r'\caption{' + rf'{tableCaption}' + r'}',
        r'\label{tab:variable-attributes} \\ ',
        r'\hline \endhead',
        r'\hline \endfoot',
        r'\rowcolor{blue!25} \textbf{Attribute Name} & \textbf{Format} & \textbf{Description} & \textbf{Source} \\ \hline',
    ]

    for i in list(GlobAttrsFilledECCO.keys()):
        GAttrsNam     = i
        GAFormat      = GlobAttrsFilledECCO[i]["type"]
        GAdescription = GlobAttrsFilledECCO[i]["description"]
        GASource      = GlobAttrsFilledECCO[i]["sourc"]
        latex_lines.append(r'\rowcolor{cyan!25}')
        latex_lines.append(
            rf'{utils_general.sanitize(config_dictionary, GAttrsNam)} & {GAFormat} & '
            rf'{utils_general.sanitize(config_dictionary, GAdescription)} & {GASource} \\ \hline'
        )

    latex_lines.append(r'\end{longtable}')
    latex_lines.append(r"")

    with open(saveTo + latexFilename, 'w') as output_file:
        output_file.write('\n'.join(latex_lines))


def get_Global_or_CoordsDimsVarsList(netCDFpath: str, jsonFileName: str, saveTo: str) -> None:
    """
    Collect unique global attribute names across a set of NetCDF files and save as JSON.

    Useful for building a reference list of all attribute names that appear
    across an ECCO dataset collection (geometry, grid, and 1-D data files).

    :param netCDFpath: Path to a directory containing example NetCDF files.
        All files in the directory are opened.
    :type netCDFpath: str
    :param jsonFileName: Filename for the output JSON file.
    :type jsonFileName: str
    :param saveTo: Directory where the JSON file should be saved.
    :type saveTo: str
    :returns: None
    """
    contentlist = sorted(os.listdir(path=netCDFpath))
    GlobalAttrsCollect = []

    for i in range(len(contentlist)):
        dataset = xr.open_dataset(netCDFpath + contentlist[i])
        GlobalAttrsCollect = GlobalAttrsCollect + list(dataset.attrs)

    # Deduplicate and sort for a stable, human-readable output
    GlobalAttrsCollect = sorted(list(set(GlobalAttrsCollect)))

    with open(os.path.join(saveTo, jsonFileName), 'w') as output_file:
        output_file.write(str(json.dumps(GlobalAttrsCollect)))


def data_products(
    base_dir: str,
    config_dictionary: dict,
    granule_directory: str,
    overwrite_switch: bool
) -> None:
    """
    Generate all LaTeX content for one granule directory and write it to a ``.tex`` file.

    For each dataset group defined in the corresponding JSON groupings file,
    this function:

    1. Opens the matched NetCDF granule.
    2. Writes an overview field table (coordinates + variables).
    3. For each variable, writes a detailed CDL attribute table.
    4. Generates (or reuses) a thumbnail plot figure.
    5. Embeds the figure with a caption and label.

    All output is accumulated and written to a single ``.tex`` file per granule
    type / grid type combination, as specified in the config.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param config_dictionary: Configuration mapping. Must contain keys for JSON
        groupings file paths, granule directories, image directories, output
        ``.tex`` file paths, and section title strings.
    :type config_dictionary: dict
    :param granule_directory: Absolute path to the directory containing the
        NetCDF granules to document. The last two path components are used to
        infer granule type and grid type.
    :type granule_directory: str
    :param overwrite_switch: If ``True``, regenerate plot images even if they
        already exist on disk.
    :type overwrite_switch: bool
    :returns: None
    """
    ecco_version_string = config_dictionary["ecco_version_string"]
    latex_lines = []

    # Infer granule type (e.g. "coordinate", "variable") and grid type
    # (e.g. "native", "latlon", "1D") from the directory path structure
    granule_type, grid_type = utils_general.get_granule_and_grid_types(granule_directory)
    is_coord = granule_type == "coordinate"

    granule_document_section_title = config_dictionary["table_section_titles"][f"{granule_type}_{grid_type}"]
    granule_document_section_title = utils_general.sanitize(config_dictionary, granule_document_section_title)
    latex_lines.append(r'\section{' + f'{granule_document_section_title}' + r'}')

    json_groupings_filepath = os.path.join(base_dir, config_dictionary[f"groupings_{granule_type}_{grid_type}_json_file"])
    granule_directory       = os.path.join(base_dir, config_dictionary[f"{granule_type}_files_{grid_type}_dir"])
    image_directory         = os.path.join(base_dir, config_dictionary[f"figures_{granule_type}_{grid_type}_dir"])

    #print("*****")
    #print(json_groupings_filepath)
    #print(is_coord)
    #print("*****")

    with open(json_groupings_filepath, 'r') as json_file:
        list_of_json_dictionaries = json.load(json_file)

    # Modify variable groupings by adding an "introduction" field for each variable dataset
    if not is_coord:
        list_of_json_dictionaries = utils_json.modify_json_add_product_field_to_groupings(list_of_json_dictionaries, grid_type)
        list_of_json_dictionaries = utils_json.modify_json_add_introduction_field_to_groupings(list_of_json_dictionaries, json_groupings_filepath, config_dictionary)

        # Determine which granules exist locally, so only relevant groupings are used
        all_granule_paths = [str(p) for p in (Path(base_dir) / config_dictionary["user_generated_granules_dir_relative"]).rglob('*.nc') if p.is_file()]
        all_grid_granule_paths = [p for p in all_granule_paths if re.search(grid_type.replace('-','_?').replace('_','_?'), p)]
        all_grid_granule_paths_megastring = ("_").join(all_grid_granule_paths)

        #print(all_grid_granule_paths_megastring)

    # Each entry in the JSON groupings file corresponds to one document subsection
    for json_dictionary in list_of_json_dictionaries:
        granule_filename_truncated_stem = json_dictionary["filename"]

        if not is_coord:
            if granule_filename_truncated_stem not in all_grid_granule_paths_megastring:
                #print(granule_filename_truncated_stem)
                continue

        #print()
        #print('looping over <list_of_json_dictionaries> in cdf_extract')
        #print(granule_filename_truncated_stem)

        if search_and_extract(
            granule_filename_truncated_stem,
            os.path.join(granule_directory),
            is_coord
            ) is None:

            print()
            print()
            print(grid_type)
            print('the following file returned "None" from <search_and_extract>')
            print(granule_filename_truncated_stem)
            print()
            print()
         
            continue
        else:

            data_array_list, dataset = search_and_extract(
                granule_filename_truncated_stem,
                os.path.join(granule_directory),
                is_coord
            )


        granule_filename_truncated_stem_formatted = utils_general.sanitize(config_dictionary, granule_filename_truncated_stem)
        latex_lines.append(fr'\subsection{{{granule_filename_truncated_stem_formatted}}}')
        latex_lines.append(fr"\subsubsection{{Overview}}")
        latex_lines.append(r'\newp')
        latex_lines.append(utils_general.sanitize(config_dictionary, json_dictionary["Introduction"]))
        latex_lines.append(r"\\\\")

        # Optional note field from the JSON groupings file
        if "comment" in json_dictionary.keys():
            latex_lines.append(utils_general.sanitize(config_dictionary, f"Note: {json_dictionary['comment']}"))
            latex_lines.append(r"\\")

        # Overview table listing all variables and coordinates
        latex_lines.extend(fieldTable(config_dictionary, dataset, is_coord, grid_type))
        latex_lines.append(r'\newp')

        # One sub-subsection per variable: CDL table + thumbnail figure
        for variable in data_array_list:
            attributes_dictionary = extract_field_info(variable)

            variable_name = attributes_dictionary['Variable Name']
            variable_name_formatted = utils_general.sanitize(config_dictionary, variable_name)
            variable_descriptor_string = f"{variable_name_formatted} ({grid_type})"

            latex_lines.append(r'\pagebreak')
            latex_lines.append(fr'\subsubsection{{{variable_descriptor_string}}}')

            # Detailed CDL attribute table for this variable
            dataVarTable = data_var_table(config_dictionary, variable_name, attributes_dictionary, granule_filename_truncated_stem, grid_type)
            latex_lines.extend(dataVarTable)

            #print()
            #print('data_var_plot call in cdf_extract')
            #print(variable_name)

            # Generate (or retrieve cached) plot and embed as a figure
            dataVarPlot = cdf_plotter.data_var_plot(
                config_dictionary, dataset, dataset[variable_name], image_directory, overwrite_switch
            )
            latex_lines.append(r'\begin{figure}[H]')
            latex_lines.append(r'\centering')
            latex_lines.append(dataVarPlot)
            latex_lines.append(fr'\caption{{{variable_descriptor_string}}}')
            latex_lines.append(fr'\label{{tab:table-{granule_filename_truncated_stem}_{variable_name}-Plot}}')
            latex_lines.append(r'\end{figure}')
            latex_lines.append(r'\newpage')

        # Write the accumulated lines to the section's .tex file after each grouping entry
        granule_latex_output_file = os.path.join(
            base_dir, config_dictionary[f'{granule_type}_table_{grid_type}_tex_file']
        )
        utils_general.write_latex_lines_to_file(latex_lines, granule_latex_output_file)


def get_word_width(word):
    
    fig, ax = plt.subplots()
    # Set up LaTeX rendering environment
    plt.rc('text', usetex=True)

    # Measure width of the word in inches (convert to points by multiplying by 72)
    text_obj = ax.text(0, 0, word, fontsize=12, fontfamily='serif')
    renderer = fig.canvas.get_renderer()
    bbox = text_obj.get_window_extent(renderer=renderer)

    width_in_points = bbox.width
    plt.close()
    return width_in_points

def hyphenate_shortnames(shortname_list):
    # Bruce - hypthenated name hack to prevent spilling into adjacent columns
    # Now that this is a function, these parameters should be arguments which are defined in a config file...

    # Taking WILD stabs at how "wide" the column in question is
    width_max = 180
    #width_max = 190
    starting_length_check = 10
    
    shortnames_hyphenated = []
    for name in shortname_list:
        if get_word_width(name) <= width_max:
            shortnames_hyphenated.append(name)
        else:
            #print(f"{name} width: {get_word_width(name)}")
            name_pieces = [] 
            dex_start = 0
            dex_end = starting_length_check
            while True:
                if get_word_width(name[dex_start:]) < width_max:
                    name_pieces.append(name[dex_start:])
                    break
                while True:
                    if get_word_width(name[dex_start:dex_end]) < width_max:
                        dex_end += 1
                    else:
                        name_pieces.append(name[dex_start:dex_end])
                        dex_start = dex_end
                        dex_end += 1
                        break

            if len(name_pieces[-1]) == 0:
                name_pieces = name_pieces[:-1]
            shortnames_hyphenated.append(r"- \newline ".join(name_pieces))

    return shortnames_hyphenated



