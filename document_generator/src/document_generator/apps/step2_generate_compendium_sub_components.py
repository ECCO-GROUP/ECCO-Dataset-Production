"""
Step 2 — Generate LaTeX table and figure source files.

Processes the downloaded NetCDF granules to produce:

- Attribute reference tables (global and variable), sourced from JSON.
- Example CDL (Common Data Language) listings for one representative
  granule per grid type.
- Per-variable attribute tables and thumbnail plots for every dataset
  listed in the JSON groupings files.

All output is written as ``.tex`` and ``.png`` files ready for compilation
by Step 3.

Usage::

    python step2_generate_document.py
"""

import sys
import yaml
from pathlib import Path
import argparse

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.latex_outline as latex_outline
import src.document_generator.utils.utils_general as utils


parser = argparse.ArgumentParser()
parser.add_argument('skipSed', nargs='?')
args = parser.parse_args()

skip_sed = args.skipSed


config_file_static = Path(base_dir) / "files_general/resource_files/universal_input/config_static_DoNotModifyMe/config_static.yaml"
config_file_user = Path(base_dir) / "files_general/resource_files/config_user_ModifyMe/config_user.yaml"

with open(config_file_static, 'r') as stream:
    config_dict_static = yaml.safe_load(stream)

with open(config_file_user, 'r') as stream:
    config_dict_user = yaml.safe_load(stream)

overwrite_switch = config_dict_user['figure_generation_overwrite_switch']


def main() -> None:
    """
    Entry point for the LaTeX source generation step.

    Runs two passes over the granule directories:

    1. :func:`latex_outline.write_data_attributes_tables` — writes attribute
       reference and example CDL tables.
    2. :func:`latex_outline.write_datasets` — writes per-dataset variable
       tables and generates plot figures.

    :returns: None
    """

    if skip_sed is None:
        file_type_to_modify = "json"
        utils.sed_replacement(base_dir, config_dict_static, config_dict_user, file_type_to_modify)

    print("\nGenerating supporting latex table and image files:\n")
    latex_outline.write_data_attributes_tables(base_dir, config_dict_static, config_dict_user, overwrite_switch)
    latex_outline.write_datasets(base_dir, config_dict_static, config_dict_user, overwrite_switch)
    print()


if __name__ == "__main__":
    main()
