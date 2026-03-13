import os
import argparse
from pathlib import Path
import sys
import yaml

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils_general
import src.document_generator.utils.utils_json as utils_json
import src.document_generator.utils.cdf_extract as cdf_extract


def write_data_attributes_tables(base_dir: str, config_dictionary: dict, overwrite_switch: bool) -> None:
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
    :param config_dictionary: Configuration mapping. Must contain
        ``'variable_files_native_dir'`` (used to locate the parent directory
        of all variable granule directories) plus all keys required by
        :func:`utils_json.write_attributes_tables_tex` and
        :func:`cdf_extract.latex_example_netcdf`.
    :type config_dictionary: dict
    :param overwrite_switch: Passed through to downstream functions; if
        ``True``, existing files are regenerated rather than skipped.
    :type overwrite_switch: bool
    :returns: None
    """
    # --- Step 1: Write global and variable attribute reference tables ---
    utils_json.write_attributes_tables_tex(base_dir, config_dictionary)

    # Navigate up one level from the native variable dir to find the shared
    # parent of all variable granule directories (native, latlon, 1D)
    variable_granules_parent_directory = os.path.join(
        base_dir,
        "/".join(config_dictionary["variable_files_native_dir"].split("/")[:-1])
    )

    # Collect only leaf directories (those with no subdirectories); each leaf
    # corresponds to one grid type (native, latlon, or 1D)
    variable_granule_directories = [
        root for root, dirs, files in os.walk(variable_granules_parent_directory)
        if not dirs
    ]

    # --- Step 2: Write one example CDL table per grid-type leaf directory ---
    for granule_directory in variable_granule_directories:
        granule_type, grid_type = utils_general.get_granule_and_grid_types(granule_directory)
        cdf_extract.latex_example_netcdf(base_dir, config_dictionary, grid_type)


def write_datasets(base_dir: str, config_dictionary: dict, overwrite_switch: bool) -> None:
    """
    Write all dataset variable and coordinate tables to their ``.tex`` files.

    Walks the granule parent directory to discover every leaf directory (one
    per granule type × grid type combination), then calls
    :func:`cdf_extract.data_products` for each, which generates the full set
    of LaTeX sections, tables, and figures for that directory.

    :param base_dir: Root directory of the project.
    :type base_dir: str
    :param config_dictionary: Configuration mapping. Must contain
        ``'coordinate_files_native_dir'`` (used to locate the shared parent
        directory of all granule directories) plus all keys required by
        :func:`cdf_extract.data_products`.
    :type config_dictionary: dict
    :param overwrite_switch: If ``True``, plot images are regenerated even if
        they already exist on disk.
    :type overwrite_switch: bool
    :returns: None
    """
    # Navigate two levels up from the native coordinate dir to reach the
    # common ancestor of all granule type / grid type directories
    granules_parent_directory = os.path.join(
        base_dir,
        "/".join(config_dictionary["coordinate_files_native_dir"].split("/")[:-2])
    )

    # Each leaf directory contains granules for one (type, grid) combination
    granule_directories = [
        root for root, dirs, files in os.walk(granules_parent_directory)
        if not dirs
    ]

    for granule_directory in granule_directories:
        print(
            f"writing latex table and figure files for granules in the "
            f"'{'/'.join(granule_directory.split('/')[-2:])}' directory"
        )
        cdf_extract.data_products(base_dir, config_dictionary, granule_directory, overwrite_switch)


if __name__ == '__main__':
    """
    Command-line entry point for writing dataset tables directly.

    Usage::

        python latex_outline.py --type {Native,Latlon,1D}

    :param --type: The dataset type to write. Must be one of ``'Native'``,
        ``'Latlon'``, or ``'1D'``.
    """
    parser = argparse.ArgumentParser(description='Write datasets to latex')
    parser.add_argument(
        '--type', required=True, type=str,
        help="Type of the dataset to write. Should be one of 'Native', 'Latlon', '1D'."
    )
    args = parser.parse_args()
    write_datasets(args.type)
