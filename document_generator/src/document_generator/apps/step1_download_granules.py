"""
Step 1 — Download ECCO granule files from the remote server.

Reads granule URLs from the list file specified in the YAML config and
downloads each file to the appropriate local directory, authenticating
via the user's ``.netrc`` file. See the project README for ``.netrc``
setup instructions.

Usage::

    python step1_download_granules.py
"""

import sys
from pathlib import Path
import yaml
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils


config_file_static = Path(base_dir) / "files_general/resource_files/universal_input/config_static_DoNotModifyMe/config_static.yaml"
config_file_user = Path(base_dir) / "files_general/resource_files/config_user_ModifyMe/config_user.yaml"

with open(config_file_static, 'r') as stream:
    config_dictionary_static = yaml.safe_load(stream)

with open(config_file_user, 'r') as stream:
    config_dictionary_user = yaml.safe_load(stream)


def main() -> None:
    """
    Entry point for the granule download step.

    Calls :func:`utils.download_granules` with the project base
    directory and the loaded configuration dictionary. Prints a header and
    footer message for visibility in pipeline logs.

    :returns: None
    """
    print("\ndownloading granules:\n")
    utils.download_granules(base_dir, config_dictionary_static, config_dictionary_user)
    print()

if __name__ == "__main__":
    main()
