"""
Description

Usage::

    python step0_generate_preliminary_file_tree.py
"""

import pdb
import filecmp
import sys
import shutil
from pathlib import Path
import yaml
import datetime
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)

config_file_static = Path(base_dir) / "files_general/resource_files/universal_input/config_static_DoNotModifyMe/config_static.yaml"
config_file_user = Path(base_dir) / "files_general/resource_files/config_user_ModifyMe/config_user.yaml"

with open(config_file_static, 'r') as stream:
    config_dict_static = yaml.safe_load(stream)

with open(config_file_user, 'r') as stream:
    config_dict_user = yaml.safe_load(stream)


def main() -> None:
    """
    docstring
    """
    ecco_version_string = config_dict_user["ecco_version_string"]
    granules_to_download_file_template = Path(base_dir) / config_dict_static["granules_url_list_template_file"]
    granules_to_download_file_userVersion = Path(base_dir) / config_dict_static[f"granules_url_list_file_relative"].format(ecco_version_string=ecco_version_string)
    granules_to_download_file_userVersion.parent.mkdir(parents=True, exist_ok=True)

    print(f"\nGenerating granule directory structure, in case the user wants to use granules already present on their machine in the compendium (see user guide/README).\n")

    if not granules_to_download_file_userVersion.is_file():
        print(f"Generating '{granules_to_download_file_userVersion.name}' for the user to edit by adding urls of granules they'd like to download for use in the compendium.\n")
        shutil.copy2(granules_to_download_file_template, granules_to_download_file_userVersion)
    else:
        # if the file is in its original (i.e. template) state, leave it be
        if filecmp.cmp(granules_to_download_file_template, granules_to_download_file_userVersion, shallow=False):
            print(f"Original version of {granules_to_download_file_userVersion.name} detected; it will remain unmodified and ready for user edits\n")
        else:
            if config_dict_user['url_list_overwrite_switch']:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                new_file = granules_to_download_file_userVersion.with_stem(granules_to_download_file_userVersion.stem + f"_movedOn_{timestamp}")
                print(f"'{granules_to_download_file_userVersion.name}' already exists; moving it to {new_file.name} and generating new, empty version\n")
                granules_to_download_file_userVersion.rename(new_file)
                granules_to_download_file_userVersion = Path(base_dir) / config_dict_static[f"granules_url_list_file_relative"].format(ecco_version_string=ecco_version_string)
                shutil.copy2(granules_to_download_file_template, granules_to_download_file_userVersion)
            else:
                print(f"'{granules_to_download_file_userVersion.name}' already exists and will remain unmodified for continued use and user edits (set 'url_list_overwrite_switch' to 'True' in 'config_user.yaml' to generate a fresh '{granules_to_download_file_userVersion.name}' file while automatically renaming the old one)\n")

    (Path(base_dir) / config_dict_static[f"variable_files_native_dir"].format(ecco_version_string=ecco_version_string)).mkdir(parents=True, exist_ok=True)
    (Path(base_dir) / config_dict_static[f"variable_files_latlon_dir"].format(ecco_version_string=ecco_version_string)).mkdir(parents=True, exist_ok=True)
    (Path(base_dir) / config_dict_static[f"variable_files_1D_dir"].format(ecco_version_string=ecco_version_string)).mkdir(parents=True, exist_ok=True)
    (Path(base_dir) / config_dict_static[f"coordinate_files_native_dir"].format(ecco_version_string=ecco_version_string)).mkdir(parents=True, exist_ok=True)
    (Path(base_dir) / config_dict_static[f"coordinate_files_latlon_dir"].format(ecco_version_string=ecco_version_string)).mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    main()
