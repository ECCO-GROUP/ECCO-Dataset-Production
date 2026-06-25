import os
import sys
import yaml
from pathlib import Path
import argparse

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_json as utils_json

parser = argparse.ArgumentParser()
parser.add_argument("requiredRatio", nargs="?", type=float)
args = parser.parse_args()

required_ratio = 0 
if args.requiredRatio is not None:
    required_ratio = args.requiredRatio 

# Path to the YAML configuration file — update this for your environment
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/V4r6/input_and_templates/config/config.yaml"

with open(config_file, 'r') as stream:
    config_dictionary = yaml.safe_load(stream)


def main() -> None:
    utils_json.check_for_attributes(base_dir, config_dictionary, required_ratio)


if __name__ == "__main__":
    main()
