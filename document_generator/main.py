# MUST FIX LACK OF READABLE INDENTATION IN THE EXAMPLE TABLES (cdf_extract.py)

import os
import sections.latex_outline as outline

def main():

    document_generator_dir = os.path.dirname(os.path.realpath(__file__))
    #version_string = input("Enter four-letter ECCO version string for compendium generation (e.g. v4r4):").lower()

    version_string = "v4r4"

    config_file_static = os.path.join(document_generator_dir, "config_files", f"config_files_{version_string}", f"config_static_{version_string}.yaml")
    config_file_user = os.path.join(document_generator_dir, "config_files", f"config_files_{version_string}", f"config_user_{version_string}.yaml")

    outline.write_data_attributes_tables(config_file_static, config_file_user)
    outline.write_datasets(config_file_static, config_file_user)

    #outline.write_datasets('native')
    #outline.write_datasets('latlon')
    #outline.write_datasets('1D')

if __name__ == "__main__":
    main()
