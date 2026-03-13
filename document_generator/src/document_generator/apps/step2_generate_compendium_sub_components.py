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



# If True, regenerate all output files even if they already exist on disk
overwrite_switch = True
# overwrite_switch = False  # uncomment to skip files that already exist

# Path to the YAML configuration file — update this for your environment
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file, 'r') as stream:
    config_dictionary = yaml.safe_load(stream)


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
    latex_outline.write_data_attributes_tables(base_dir, config_dictionary, overwrite_switch)
    latex_outline.write_datasets(base_dir, config_dictionary, overwrite_switch)
    print()


if __name__ == "__main__":
    main()
