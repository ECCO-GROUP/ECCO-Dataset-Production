'''
sed "s/REPLACE-WITH-ISSUE-DATE/$(date +%B) $(date +%d), $(date +%Y)/" front_pages.tex

WORKED!
'''

import os
import sys
import subprocess
import yaml
from pathlib import Path
import datetime

# Ensure the project root is on the path so relative imports resolve correctly
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)


# Path to the YAML configuration file — update this for your environment
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r6/input_and_templates/config/config.yaml"
#config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file, 'r') as stream:
    config_dictionary = yaml.safe_load(stream)

def main() -> None:
    Path(f"{base_dir}/{config_dictionary['latex_modified_input_files']}").mkdir(parents=True, exist_ok=True)
    latex_template_files = [f.name for f in Path(f"{base_dir}/{config_dictionary['latex_template_files']}").iterdir() if f.is_file() and f.suffix == ".tex"]
    for latex_file_name in latex_template_files: 
        format_map_context_dict = {
            'file_in': f"{base_dir}/{config_dictionary['latex_template_files']}/{latex_file_name}",
            'file_out': f"{base_dir}/{config_dictionary['latex_modified_input_files']}/{latex_file_name}"
        }
        try:
            for sed_command in config_dictionary['latex_template_modification_commands_list']:
                result = subprocess.run(
                        sed_command.format_map(format_map_context_dict),
                        check=True, shell=True
                        )
        except:
            print('Bash call to modify file did not work')

if __name__ == "__main__":
    main()
