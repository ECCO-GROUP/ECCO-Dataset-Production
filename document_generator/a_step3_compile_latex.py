import os
import sys
import subprocess
import yaml
from pathlib import Path

base_dir = str(Path(__file__).parent)
sys.path.append(base_dir)

# User must specify path to config file; this could be given as input, or handled another way...
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/general/files_general/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file,'r') as stream:
    config_dictionary = yaml.safe_load(stream)


def main():
    
    static_support_tex_dir_absolute = f"{base_dir}/{config_dictionary['static_support_tex_dir_relative']}"
    user_generated_tex_dir_absolute = f"{base_dir}/{config_dictionary['user_generated_tex_dir_relative']}"
    os.environ["TEXINPUTS"] = f"{static_support_tex_dir_absolute}:{user_generated_tex_dir_absolute}:"
    print(config_dictionary['compendium_compilation_runtime_message_string'])
    output_directory = os.path.join(base_dir, config_dictionary["final_compendium_files_dir"])
    os.makedirs(output_directory, exist_ok=True)
    try:
        result = subprocess.run(['pdflatex', '-halt-on-error', f'--output-directory={output_directory}',
                                 config_dictionary['compendium_tex_filepath']], 
                                check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during pdflatex execution:")
        print(e.stderr)
        print(e.stdout)
    except FileNotFoundError:
        print("Please install the 'pdflatex' python package, perhaps via 'conda install pdflatex'")

if __name__ == "__main__":
    main()
