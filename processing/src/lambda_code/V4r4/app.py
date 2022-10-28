"""
ECCO Dataset Production AWS Lambda app file

Author: Duncan Bark

Contains the handler() function which each Lambda job calls when started, and the run_script()
function which runs the processing script passed via product_generation_config.yaml

"""

import time
import logging
import importlib

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ==========================================================================================================================
# RUN SCRIPT
# ==========================================================================================================================
def run_script(payload):
    """
    Import processing function and call with using the passed the dictionary payload

    Args:
        payload (dict): Contains all the information required to process the passed job:
            grouping_to_process (int): Grouping number from groupings json file for current dataset
            product_type (str): String product type (i.e. 'latlon', 'native')
            output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
            time_steps_to_process (list): List of timesteps to process for the current job
            field_files (defaultdict(list)): Dictionary with field names as keys, and S3/local file paths for each timestep as values
            product_generation_config (dict): Dictionary of product_generation_config.yaml config file
            aws_config (dict): Dictionary of aws_config.yaml config file
            local (bool): Boolean for whether or not processing is to occur locally (no S3, no Lambda)
            use_lambda (bool): Boolean for whether or not processing is to occur on Lambda
            credentials (dict): Dictionary containaing credentials information for AWS
            processing_code_filename (only for lambda, str): Name of this file, used to call it from the lambda_code app.py file

    Returns:
        None
    """
    # Import processing file and time how long it takes
    processing_file = payload['product_generation_config']['processing_code_filename']
    print(f'Importing file: {processing_file}')
    im_st = time.time()
    script = importlib.import_module(processing_file)
    logger.info(f'DURATION\tIMPORT\t{time.time() - im_st}\tseconds')

    # Run processing function with the dictionary "payload"  and time how long it takes
    run_st = time.time()
    script.generate_netcdfs(payload)
    logger.info(f'DURATION\tRUN\t{time.time() - run_st}\tseconds')

    return


# ==========================================================================================================================
# LAMBDA HANDLER
# ==========================================================================================================================
def handler(payload, context):
    """
    Import processing function and call with using the passed the dictionary called payload

    Args:
        payload (dict): Contains all the information required to process the passed job:
            grouping_to_process (int): Grouping number from groupings json file for current dataset
            product_type (str): String product type (i.e. 'latlon', 'native')
            output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
            time_steps_to_process (list): List of timesteps to process for the current job
            field_files (defaultdict(list)): Dictionary with field names as keys, and S3/local file paths for each timestep as values
            product_generation_config (dict): Dictionary of product_generation_config.yaml config file
            aws_config (dict): Dictionary of aws_config.yaml config file
            local (bool): Boolean for whether or not processing is to occur locally (no S3, no Lambda)
            use_lambda (bool): Boolean for whether or not processing is to occur on Lambda
            credentials (dict): Dictionary containaing credentials information for AWS
            processing_code_filename (only for lambda, str): Name of this file, used to call it from the lambda_code app.py file
        
        context (AWS Lambda context object): AWS Lambda context, ignored

    Returns:
        None
    """
    logger.info('START')
    print('Inside handler')

    # Call run_script() function with dictionary called payload, and time how long it takes
    all_st = time.time()
    run_script(payload)
    logger.info(f'DURATION\tALL\t{time.time() - all_st}\tseconds')

    return