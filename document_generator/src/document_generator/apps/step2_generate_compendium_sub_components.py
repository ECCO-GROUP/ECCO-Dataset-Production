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

    python a_step2_generate_document.py
"""

import os
import sys
import yaml
from pathlib import Path

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.latex_outline as latex_outline


# Path to the static configuration file — update this for your environment
config_static_file = ('/').join([base_dir, "files_general/resource_files/universal_input/config_files/config_static.yaml"])

with open(config_static_file, 'r') as stream:
    config_static_dictionary = yaml.safe_load(stream)

config_user_file = ('/').join([base_dir, f"files_general/resource_files/version_specific/{config_static_dictionary['ecco_version_string']}/input_and_templates/config/config_user.yaml"])

with open(config_user_file, 'r') as stream:
    config_user_dictionary = yaml.safe_load(stream)

overwrite_switch = config_user_dictionary['figure_generation_overwrite_switch ']


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
    print("\nGenerating supporting latex table and image files:\n")
    latex_outline.write_data_attributes_tables(base_dir, config_static_dictionary, overwrite_switch)
    latex_outline.write_datasets(base_dir, config_static_dictionary, overwrite_switch)
    print()


if __name__ == "__main__":
    main()
