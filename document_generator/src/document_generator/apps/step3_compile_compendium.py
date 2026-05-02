"""
Step 3 — Compile the LaTeX compendium to PDF.

Sets the ``TEXINPUTS`` environment variable so that ``pdflatex`` can locate
both the static support ``.tex`` files and the user-generated ``.tex`` files
produced by Step 2, then invokes ``pdflatex`` with a timestamped job name so
successive runs do not silently overwrite each other.

Usage::

    python a_step3_compile_latex.py
"""

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
config_file = "/Users/brucel/ecco/yip/ECCO-Dataset-Production/document_generator/files_general/resource_files/version_specific/v4r4/input_and_templates/config/config.yaml"

with open(config_file, 'r') as stream:
    config_dictionary = yaml.safe_load(stream)


def main() -> None:
    """
    Entry point for the LaTeX compilation step.

    Resolves absolute paths for the static support and user-generated TeX
    directories, sets ``TEXINPUTS`` so ``pdflatex`` can find ``\\input``
    targets in both locations, then runs ``pdflatex`` with
    ``-halt-on-error`` and a timestamped ``--jobname`` to produce the final
    PDF.

    .. note::
        Install ``pdflatex`` via your TeX distribution (e.g. TeX Live or
        MiKTeX). On conda environments, ``conda install pdflatex`` may also
        work.

    :returns: None
    """
    static_support_tex_dir_absolute = f"{base_dir}/{config_dictionary['static_support_tex_dir_relative']}"
    user_generated_tex_dir_absolute = f"{base_dir}/{config_dictionary['user_generated_tex_dir_relative']}"

    # TEXINPUTS tells pdflatex where to search for \input and \include targets.
    # The trailing colon preserves the default TeX search path.
    os.environ["TEXINPUTS"] = f"{static_support_tex_dir_absolute}:{user_generated_tex_dir_absolute}:"

    print(config_dictionary['compendium_compilation_runtime_message_string'])

    output_directory = os.path.join(base_dir, config_dictionary["final_compendium_files_dir"])
    os.makedirs(output_directory, exist_ok=True)

    # Timestamp the output filename so repeated runs don't overwrite each other
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    base_tex_stem = Path(config_dictionary["compendium_tex_filepath"]).stem
                
    compendium_template_path = os.path.join(base_dir, config_dictionary['compendium_tex_filepath'])

    # Set the "issue date" to the current date
    try:
        result = subprocess.run(
                [
                    'sed',
                    "s/REPLACE-WITH-ISSUE-DATE/$(date +%B) $(date +%d), $(date +%Y)/",
                    config_dictionary['component_tex_files_to_modify']['front_pages'][0],
                    '>',
                    config_dictionary['component_tex_files_to_modify']['front_pages'][1]
                ],
                check=True, text=True, capture_output=True
                )
    except:
        print('Bash call to modify file did not work')


    try:
        result = subprocess.run(
            [
                'pdflatex',
                '-halt-on-error',
                f'--jobname={base_tex_stem}',
                #f'--jobname={base_tex_stem}_{timestamp}',
                f'--output-directory={output_directory}',
                compendium_template_path
            ],
            check=True, text=True, capture_output=True
        )
    except subprocess.CalledProcessError as e:
        print("An error occurred during pdflatex execution:")
        print(e.stderr)
        print(e.stdout)
    except FileNotFoundError:
        print("Please install the 'pdflatex' python package, perhaps via 'conda install pdflatex'")


if __name__ == "__main__":
    main()
