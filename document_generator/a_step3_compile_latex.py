import os
import subprocess
from pathlib import Path
import yaml

general_base_dir = str(Path(__file__).parent)

def main():
    ecco_version_string = "v4r4"
    config_file_dir_relative_path = f"general/files_general/version_specific/{ecco_version_string}/input_and_templates/config"
    config_filepath = os.path.join(general_base_dir, config_file_dir_relative_path, "config.yaml")
    with open(config_filepath,'r') as stream:
        config_dictionary = yaml.safe_load(stream)
    static_support_tex_dir_absolute = f"{general_base_dir}/{config_dictionary['static_support_tex_dir_relative']}"
    user_generated_tex_dir_absolute = f"{general_base_dir}/{config_dictionary['user_generated_tex_dir_relative']}"
    os.environ["TEXINPUTS"] = f"{static_support_tex_dir_absolute}:{user_generated_tex_dir_absolute}:"
    print(config_dictionary['compendium_compilation_runtime_message_string'])
    output_directory = os.path.join(general_base_dir, config_dictionary["final_compendium_files_dir"])
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
