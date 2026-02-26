import os
import sys
from pathlib import Path
import general.utility_scripts.utils_general as utils_general
import yaml

base_dir = str(Path(__file__).parent)
sys.path.append(base_dir)

# User must specify path to config file; this could be given as input, or handled another way...
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/general/files_general/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file,'r') as stream:
    config_dictionary = yaml.safe_load(stream)

def main():
    print("\ndownloading granules:\n")
    utils_general.download_granules(base_dir, config_dictionary)
    print()

if __name__ == "__main__":
    main()
