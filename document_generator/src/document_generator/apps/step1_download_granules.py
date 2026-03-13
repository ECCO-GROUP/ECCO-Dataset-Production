"""
Step 1 — Download ECCO granule files from the remote server.

Reads granule URLs from the list file specified in the YAML config and
downloads each file to the appropriate local directory, authenticating
via the user's ``.netrc`` file. See the project README for ``.netrc``
setup instructions.

Usage::

    python a_step1_download_granules.py
"""

import os
import sys
from pathlib import Path
import yaml

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils


# Path to the YAML configuration file — update this for your environment
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file, 'r') as stream:
    config_dictionary = yaml.safe_load(stream)


def main() -> None:
    """
    Entry point for the granule download step.

    Calls :func:`utils.download_granules` with the project base
    directory and the loaded configuration dictionary. Prints a header and
    footer message for visibility in pipeline logs.

    :returns: None
    """
    print("\ndownloading granules:\n")
    utils.download_granules(base_dir, config_dictionary)
    print()


if __name__ == "__main__":
    main()
