import os
import sys
import yaml
from pathlib import Path
import general.utility_scripts.latex_outline as latex_outline

base_dir = str(Path(__file__).parent)
sys.path.append(base_dir)

# Should not be hardcoded?
overwrite_switch = True
#overwrite_switch = False

# User must specify path to config file; this could be given as input, or handled another way...
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/general/files_general/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file,'r') as stream:
    config_dictionary = yaml.safe_load(stream)

def main() -> None:
    print("\nGenerating supporting latex table and image files:\n")
    latex_outline.write_data_attributes_tables(base_dir, config_dictionary, overwrite_switch)
    latex_outline.write_datasets(base_dir, config_dictionary, overwrite_switch)
    print()

if __name__ == "__main__":
    main()
