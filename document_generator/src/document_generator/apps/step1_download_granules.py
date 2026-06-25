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
import argparse
import pdb

parser = argparse.ArgumentParser()
parser.add_argument('eccoversionstring')
args = parser.parse_args()

ecco_version_string = args.eccoversionstring

print(ecco_version_string)

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils

config_static_file = ('/').join([base_dir, "files_general/resource_files/universal_input/config_files/config_static.yaml"])

with open(config_static_file, 'r') as stream:
    config_static_dictionary = yaml.safe_load(stream)


config_user_file = ('/').join([base_dir, f"files_general/resource_files/version_specific/{ecco_version_string}/input_and_templates/config/config_user.yaml"])
#config_user_file = ('/').join([base_dir, f"files_general/resource_files/version_specific/{config_user_dictionary['ecco_version_string']}/input_and_templates/config/config_user.yaml"])

with open(config_user_file, 'r') as stream:
    config_user_dictionary = yaml.safe_load(stream)


def main() -> None:
    """
    Entry point for the granule download step.

    Calls :func:`utils.download_granules` with the project base
    directory and the loaded configuration dictionary. Prints a header and
    footer message for visibility in pipeline logs.

    :returns: None
    """
    print("\ndownloading granules:\n")
    utils.download_granules(base_dir, config_static_dictionary, config_user_dictionary)
    print()


if __name__ == "__main__":
    main()
