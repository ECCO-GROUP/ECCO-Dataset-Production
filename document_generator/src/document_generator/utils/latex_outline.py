import os
import argparse
from pathlib import Path
import sys
import yaml
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils_general
import src.document_generator.utils.utils_json as utils_json
import src.document_generator.utils.cdf_extract as cdf_extract


def write_data_attributes_tables(base_dir: str, config_dict_static: dict, config_dict_user: dict) -> None:
    """
    Write all attribute and example variable tables to their respective ``.tex`` files.

    This is the first of two document-generation passes (the second is
    :func:`write_datasets`). It handles:

    1. **Attribute tables** — global and variable attribute reference tables,
       generated from JSON source files via
       :func:`utils_json.write_attributes_tables_tex`.
    2. **Example variable tables** — one CDL-style longtable per grid type
       (native, latlon, 1D), generated from the example granule specified in
       the config via :func:`cdf_extract.latex_example_netcdf`.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param config_dict_static: Configuration mapping. Must contain
        ``'variable_files_native_dir'`` (used to locate the parent directory
        of all variable granule directories) plus all keys required by
        :func:`utils_json.write_attributes_tables_tex` and
        :func:`cdf_extract.latex_example_netcdf`.
    :type config_dict_static: dict
    :param overwrite_switch: Passed through to downstream functions; if
        ``True``, existing files are regenerated rather than skipped.
    :type overwrite_switch: bool
    :returns: None
    """

    # --- Step 1: Write global and variable attribute reference tables ---
    utils_json.write_attributes_tables_tex(base_dir, config_dict_static, config_dict_user)

    for grid_type in config_dict_user["grid_types_considered"]:
        variable_files_dir = Path(base_dir) / config_dict_static[f"variable_files_{grid_type}_dir"].format(ecco_version_string=config_dict_user["ecco_version_string"])
        if variable_files_dir.exists():
            granules_parent_directory = Path("/", *str(variable_files_dir).split("/")[:-config_dict_static["negative_steps_to_variable_granules_dir"]])
            break

    # Collect only leaf directories (those with no subdirectories); each leaf
    # corresponds to one grid type (native, latlon, or 1D)
    variable_granule_directories = [
        root for root, dirs, files in os.walk(str(granules_parent_directory))
        if not dirs
    ]

    # --- Step 2: Write one example CDL table per grid-type leaf directory ---
    for granule_directory in variable_granule_directories:
        granule_type, grid_type = utils_general.get_granule_and_grid_types(granule_directory)
        cdf_extract.latex_example_netcdf(base_dir, config_dict_static, config_dict_user, grid_type)

    # --- Step 3: Modify "filenames_conventions.tex" to reflect the example granules currently being analyzed ---
    for grid_type in config_dict_user["grid_types_considered"]:
        cdf_extract.append_to_filenames_conventions_file(base_dir, config_dict_static, config_dict_user, grid_type)


def write_datasets(base_dir: str, config_dict_static: dict, config_dict_user: dict) -> None:
    """
    Write all dataset variable and coordinate tables to their ``.tex`` files.

    Walks the granule parent directory to discover every leaf directory (one
    per granule type × grid type combination), then calls
    :func:`cdf_extract.data_products` for each, which generates the full set
    of LaTeX sections, tables, and figures for that directory.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param config_dict_static: Configuration mapping. Must contain
        ``'coordinate_files_native_dir'`` (used to locate the shared parent
        directory of all granule directories) plus all keys required by
        :func:`cdf_extract.data_products`.
    :type config_dict_static: dict
    :param overwrite_switch: If ``True``, plot images are regenerated even if
        they already exist on disk.
    :type overwrite_switch: bool
    :returns: None
    """
    # Navigate two levels up from a <grid_type> coordinate dir to reach the
    # common ancestor of all granule type / grid type directories
    for grid_type in config_dict_user["grid_types_considered"]:
        coordinate_files_dir = Path(base_dir) / config_dict_static[f"coordinate_files_{grid_type}_dir"].format(ecco_version_string=config_dict_user["ecco_version_string"])
        if coordinate_files_dir.exists():
            granules_parent_directory = Path("/", *str(coordinate_files_dir).split("/")[:-config_dict_static["negative_steps_to_variable_granules_dir"]])
            break
    
    # Each leaf directory contains granules for one (type, grid) combination
    granule_directories = [root for root, dirs, files in os.walk(str(granules_parent_directory)) if not dirs]

    for granule_directory in granule_directories:
        dir_path_string = ('/').join(granule_directory.split('/')[-config_dict_static['negative_steps_to_variable_granules_dir']:])
        print(f"writing latex table and figure files for granules in the {dir_path_string} directory")
        cdf_extract.data_products(base_dir, config_dict_static, config_dict_user, granule_directory)

