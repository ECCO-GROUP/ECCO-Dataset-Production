"""
ECCO Dataset Production Setup

Author: Duncan Bark

Setup script for ECCO Dataset Production. Prepares code files, directories, etc. for processing.

"""

import os
import sys
import yaml
import shutil
from pathlib import Path

# Local imports
main_path = Path(__file__).parent.resolve().parent.resolve()
sys.path.append(f'{main_path}')
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
import print_utils as print_utils


def prepare_product_generation_config():
    print_utils.printc(f'\nPreparing product_generation_config', 'blue')
    # Load 'product_generation_config.yaml'
    product_generation_config = yaml.safe_load(open(main_path / 'configs' / 'product_generation_config.yaml'))

    ecco_version = product_generation_config['ecco_version']

    # Prepare directories in product_generation_config
    # Default directories
    mapping_factors_dir_default = str(main_path / 'mapping_factors' / ecco_version)
    diags_root_default = str(main_path / 'tmp' / 'tmp_model_output' / ecco_version)
    metadata_default = str(main_path / 'metadata' / ecco_version)
    ecco_grid_dir_default = str(main_path / 'ecco_grids' / ecco_version)
    ecco_grid_dir_mds_default = str(main_path / 'ecco_grids' / ecco_version)
    processed_output_dir_base_default = str(main_path / 'tmp' / 'tmp_output' / ecco_version)

    # Set config values to default values if none are included in the config yaml
    if product_generation_config['mapping_factors_dir'] == '':
        product_generation_config['mapping_factors_dir'] = str(Path(mapping_factors_dir_default))
    if product_generation_config['model_output_dir'] == '':
        product_generation_config['model_output_dir'] = str(Path(diags_root_default))
    if product_generation_config['metadata_dir'] == '':
        product_generation_config['metadata_dir'] = str(Path(metadata_default))
    if product_generation_config['ecco_grid_dir'] == '':
        product_generation_config['ecco_grid_dir'] = str(Path(ecco_grid_dir_default))
    if product_generation_config['ecco_grid_dir_mds'] == '':
        product_generation_config['ecco_grid_dir_mds'] = str(Path(ecco_grid_dir_mds_default))
    if product_generation_config['processed_output_dir_base'] == '':
        product_generation_config['processed_output_dir_base'] = str(Path(processed_output_dir_base_default))

    # If using custom factors, replace 'mapping_factors_dir' with the 'custom_factors_dir' path provided.
    # This makes it so all future references to the factors (when doing any mapping) is done so with
    # the custom factors that get, or are already, made.
    if product_generation_config['custom_grid_and_factors']:
        product_generation_config['mapping_factors_dir'] = product_generation_config['custom_factors_dir']

    # ECCO-ACCESS and ECCO code directories
    ecco_code_name_default = f'ECCO{ecco_version[:2].lower()}-py'
    if product_generation_config['ecco_code_name'] == '':
        if product_generation_config['ecco_code_dir'] == '':
            product_generation_config['ecco_code_name'] = ecco_code_name_default
        else:
            # get the last directory name from config file (handles if trailing / is included)
            split_dir = product_generation_config['ecco_code_dir'].split('/')
            if split_dir[-1] == '/':
                product_generation_config['ecco_code_name'] = split_dir[-2]
            else:
                product_generation_config['ecco_code_name'] = split_dir[-1]
    
    ecco_code_dir_default = str(main_path.parent.resolve().parent.resolve() / product_generation_config['ecco_code_name'])
    if product_generation_config['ecco_code_dir'] == '':
        product_generation_config['ecco_code_dir'] = ecco_code_dir_default

    # ECCO Configuration values
    ecco_configurations_name_default = f'ECCO-{ecco_version[:2].lower()}-Configurations'
    ecco_configurations_subfolder_default = f'ECCO{ecco_version[:2].lower()} Release {ecco_version[-1]}'
    if product_generation_config['ecco_configurations_name'] == '':
        product_generation_config['ecco_configurations_name'] = ecco_configurations_name_default
    if product_generation_config['ecco_configurations_subfolder'] == '':
        product_generation_config['ecco_configurations_subfolder'] = ecco_configurations_subfolder_default

    print_utils.printc('Preparing product_generation_config -- DONE', 'green')

    return product_generation_config


def retrieve_outside_code_files():
    product_generation_config = prepare_product_generation_config()
    ecco_version = product_generation_config['ecco_version']

    # ========== <prepare ecco_cloud_utils and ecco_code> =========================================
    print_utils.printc(f'\nPreparing ecco_cloud_utils and ecco_code', 'blue')

    # copy files from ECCO-ACCESS/ecco_cloud_utils to ECCO-Dataset-Production/processing/src/ecco_code
    ecco_access_dir = main_path.parent.resolve().parent.resolve() / 'ECCO-ACCESS'
    ecco_cloud_utils_dir = ecco_access_dir / 'ecco-cloud-utils' / 'ecco_cloud_utils'
    new_ecco_cloud_utils_file_dir = main_path / 'src' / 'ecco_code'
    if not os.path.exists(new_ecco_cloud_utils_file_dir):
        os.makedirs(new_ecco_cloud_utils_file_dir, exist_ok=True)

    for ecco_cloud_utils_file in os.listdir(ecco_cloud_utils_dir):
        if ecco_cloud_utils_file != '__init__.py' and '.py' in ecco_cloud_utils_file:
            ecu_file_path = ecco_cloud_utils_dir / ecco_cloud_utils_file
            new_ecu_file_path = new_ecco_cloud_utils_file_dir / ecco_cloud_utils_file
            shutil.copyfile(ecu_file_path, new_ecu_file_path)

    # copy code files from ECCOvX-py/ecco_vX_py/ to ECCO-Dataset-Production/processing/src/ecco_code
    new_ecco_code_file_dir = main_path / 'src' / 'ecco_code'
    if not os.path.exists(new_ecco_code_file_dir):
        os.makedirs(new_ecco_code_file_dir, exist_ok=True)

    ecco_code_dir = Path(product_generation_config['ecco_code_dir']) / f'ecco_{ecco_version[:2].lower()}_py'
    for ecco_code_file in os.listdir(ecco_code_dir):
        if ecco_code_file != '__init__.py' and '.py' in ecco_code_file:
            ec_file_path = ecco_code_dir / ecco_code_file
            new_ec_file_path = new_ecco_code_file_dir / ecco_code_file
            shutil.copyfile(ec_file_path, new_ec_file_path)
            
    print_utils.printc('Preparing ecco_cloud_utils and ecco_code -- DONE', 'green')
    # ========== </prepare ecco_cloud_utils and ecco_code> ========================================

    return


if __name__ == "__main__":
    retrieve_outside_code_files()