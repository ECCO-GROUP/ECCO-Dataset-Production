import os
import subprocess
from pathlib import Path

general_base_dir = str(Path(__file__).parent)

def main():

    ecco_version_string = "v4r4"
    output_directory = "final_compendium_files"
    
    os.makedirs(output_directory, exist_ok=True)

    static_support_tex_dir_rel = r"general/document_files/v4r4/static/latex//"
    user_generated_tex_dir_rel = r"general/document_files/v4r4/user_generated/latex//"
    static_support_tex_dir_abs = f"{general_base_dir}/{static_support_tex_dir_rel}"
    user_generated_tex_dir_abs = f"{general_base_dir}/{user_generated_tex_dir_rel}"
    os.environ["TEXINPUTS"] = f"{static_support_tex_dir_abs}:{user_generated_tex_dir_abs}:"

    message_string = (
        "\n Compiling the final latex document (if successful, will be found in the directory 'final_compendium_files' \n"
        "Note: if there is an error during compilation, it should print to the screen.  If compilation seems hung up, "
        "we suggest you terminate this process (e.g. type'ctrl + c' and hit 'return'), and look for error messages in the "
        "'__.log' file, which should be found in the 'final_compendium_files' directory.\n"
    )

    print(message_string)

    try:
        result = subprocess.run(['pdflatex', '-halt-on-error', f'--output-directory={output_directory}',
                                 'general/document_files/v4r4/static/latex/do_not_modify/ECCO_V4r4_Dataset_Catalog_and_Variable_Compendium.tex'], 
                                check=True, text=True, capture_output=True)
                                #check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, capture_output=True)
        '''
        result = subprocess.run(['pdflatex', '-halt-on-error', f'--output-directory={output_directory}',
                                 'general/document_files/v4r4/compilation_of_final_document/ECCO_V4r4_Dataset_Catalog_and_Variable_Compendium.tex'], 
                                check=True, text=True, capture_output=True)
                                #check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, capture_output=True)
        '''

    except subprocess.CalledProcessError as e:
        print(f"An error occurred during pdflatex execution:")
        print(e.stderr)
        print(e.stdout)
    except FileNotFoundError:
        print("Please install the 'pdflatex' python package, perhaps via 'conda install pdflatex'")


if __name__ == "__main__":
    main()
