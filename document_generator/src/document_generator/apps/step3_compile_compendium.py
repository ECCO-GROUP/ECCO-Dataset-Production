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
base_dir = str(Path(__file__).parent.parent.parent.parent.resolve())
sys.path.append(base_dir)
import src.document_generator.utils.utils_general as utils


config_file_static = Path(base_dir) / "files_general/resource_files/universal_input/config_static_DoNotModifyMe/config_static.yaml"
config_file_user = Path(base_dir) / "files_general/resource_files/config_user_ModifyMe/config_user.yaml"

with open(config_file_static, 'r') as stream:
    config_dict_static = yaml.safe_load(stream)

with open(config_file_user, 'r') as stream:
    config_dict_user = yaml.safe_load(stream)


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
    ecco_version_string = config_dict_user['ecco_version_string']
    compendium_template_path = Path(base_dir) / config_dict_static['compendium_tex_filepath'].format(ecco_version_string=ecco_version_string)
    base_tex_stem = compendium_template_path.stem

    input_tex_dir_absolute = Path(base_dir) / config_dict_static['input_tex_dir_relative'].format(ecco_version_string=ecco_version_string)
    output_component_tex_dir_absolute = Path(base_dir) / config_dict_static['output_component_tex_dir_relative'].format(ecco_version_string=ecco_version_string)

    # TEXINPUTS tells pdflatex where to search for \input and \include targets.
    # The trailing colon preserves the default TeX search path.
    os.environ["TEXINPUTS"] = f"{input_tex_dir_absolute}:{output_component_tex_dir_absolute}:"

    print(config_dict_static['compendium_compilation_runtime_message_string'])

    output_directory = Path(base_dir) / config_dict_static["final_compendium_files_dir"]
    output_directory.mkdir(parents=True, exist_ok=True)

    for ii in range(config_dict_user['num_pdflatex_calls']):

        print(f"pdflatex call {ii+1}/{config_dict_user['num_pdflatex_calls']}")
    
        # Attempt compilation of final latex document
        try:
            result = subprocess.run(
                [
                    'pdflatex',
                    '-halt-on-error',
                    f'--jobname={base_tex_stem}',
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
            print("Please install the 'pdflatex' program onto your computer in order to compile a latex document")


if __name__ == "__main__":
    main()
