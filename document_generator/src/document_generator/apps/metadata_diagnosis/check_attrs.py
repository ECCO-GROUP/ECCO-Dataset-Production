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

config_file_static = Path(base_dir) / "files_general/resource_files/universal_input/config_static_DoNotModifyMe/config_static.yaml"
config_file_user = Path(base_dir) / "files_general/resource_files/config_user_ModifyMe/config_user.yaml"

with open(config_file_static, 'r') as stream:
    config_dictionary_static = yaml.safe_load(stream)

with open(config_file_user, 'r') as stream:
    config_dictionary_user = yaml.safe_load(stream)

def main() -> None:
    utils_json.check_for_attributes(base_dir, config_dictionary_static, config_dictionary_user, required_ratio)

if __name__ == "__main__":
    main()
