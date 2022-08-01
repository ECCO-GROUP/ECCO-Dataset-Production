"""
Created May 18, 2022

Author: Duncan Bark

"""
import os
import ast
import sys
import json
import time
import yaml
import boto3
import argparse
import platform
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

# Local imports
main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path}')
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
import ecco_cloud_utils as ea
import jobs_utils as jobs_utils
import lambda_utils as lambda_utils
import logging_utils as logging_utils
import credentials_utils as credentials_utils
import create_factors_utils as create_factors_utils

def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--process_data', default=False, action='store_true',
                        help='Starts processing model data using config file values')

    parser.add_argument('--use_cloud', default=False, action='store_true',
                        help='Process data using AWS cloud services')

    parser.add_argument('--use_lambda', default=False, action='store_true',
                        help='Completes processing via AWS lambda')

    parser.add_argument('--debug', default=False, action='store_true',
                        help='Sets debug flag (additional print outs and skips processing')

    parser.add_argument('--force_reconfigure', default=False, action='store_true',
                        help='Force code to re-run code to get AWS credentials')

    parser.add_argument('--create_factors', default=False, action='store_true',
                        help='ONLY creates all factors: 2D/3D factors, landmask, latlon_grid fields, and sparse matricies')

    parser.add_argument('--require_input', default=False, action='store_true',
                        help='Requests approval from user to start executing lambda jobs for each job (eg. 0,latlon,AVG_MON,all)')

    parser.add_argument('--include_all_timesteps', default=False, action='store_true',
                        help='Includes all timesteps and all submitted time steps for all lambda jobs in logs')

    parser.add_argument('--log_name', default='', required=False,
                        help='Name to use in the saved log file(s)')

    parser.add_argument('--logs_only', default='', required=False,
                        help='Only does logging, loads provided log file and gets all logs and creates a job_logs json file for it.')
    
    parser.add_argument('--enable_logging', default=False, action='store_true',
                        help='Enables logging for lambda jobs')

    parser.add_argument('--force', default=False, action='store_true',
                        help='Skips all required user input')

    parser.add_argument('--create_jobs', default=False, action='store_true',
                        help='Prompts user on jobs they want to process')
    return parser


if __name__ == "__main__":
    # ========== <create intial time values> ======================================================
    ms_to_sec = 0.001
    master_start_time = time.localtime() # datetime
    start_time = int(time.time() / ms_to_sec) # miliseconds since 1970
    # ========== </create inital time values> =====================================================


    # ========== <parse command line arguments> ===================================================
    parser = create_parser()
    args = parser.parse_args()
    dict_key_args = {key: value for key, value in args._get_kwargs()}

    process_data = dict_key_args['process_data']
    debug_mode = dict_key_args['debug']
    local = not dict_key_args['use_cloud']
    use_lambda = dict_key_args['use_lambda']
    force_reconfigure = dict_key_args['force_reconfigure']

    # if use lambda, then local is False (only use S3 for data)
    if use_lambda:
        local = False

    # Verify user does not want to enable logging
    # Logging only happens when processing data via AWS Lambda
    if (dict_key_args['process_data']) and (dict_key_args['use_lambda']) and (not dict_key_args['enable_logging']) and (not dict_key_args['force']):
        logging_check = input(f'Logging has not been enabled, continue? (y/n)\t').lower().strip()
        if logging_check != 'y':
            print(f'Exiting')
            sys.exit()
    # ========== </parse command line arguments> ==================================================


    # ========== <prepare product generation configuration> =======================================
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

    extra_prints = product_generation_config['extra_prints']
    # ========== </prepare product generation configuration> ======================================


    # ========== <create mapping factors> =========================================================
    # Creates mapping_factors (2D and 3D), landmask, latlon_grid, and sparse matrix files
    # Not needed unless changes have been made to the factors code and you need
    # to update the factors/mask in the lambda docker image
    if dict_key_args['create_factors']:
        status = create_factors_utils.create_all_factors(ea, 
                                                         product_generation_config, 
                                                         ['2D', '3D'], 
                                                         debug_mode=debug_mode, 
                                                         extra_prints=extra_prints)
        if status != 'SUCCESS':
            print(status)
            sys.exit()
        print('\nCompleted creation of all factors. Exiting')
        sys.exit()
    # ========== </create mapping factors> ========================================================


    # ========== <process metadata and jobs> ======================================================
    # Get grouping information
    groupings_metadata = [f for f in os.listdir(product_generation_config['metadata_dir']) if 'groupings' in f]

    # Load each grouping json and create corresponding groupings dicts
    metadata = {}
    for mf in groupings_metadata:
        with open(str(Path(product_generation_config['metadata_dir']) / mf), 'r') as fp:
            metadata[mf] = json.load(fp)

    groupings_for_datasets = {}
    for mf_name, mf in metadata.items():
        if '1D' in mf_name:
            groupings_for_datasets['1D'] = mf
        elif 'latlon' in mf_name:
            groupings_for_datasets['latlon'] = mf
        elif 'native' in mf_name:
            groupings_for_datasets['native'] = mf

    # Call create_jobs which prompts user to select datasets to process
    if dict_key_args['create_jobs']:
        jobs_filename = jobs_utils.create_jobs(groupings_for_datasets)
    else:
        jobs_filename = 'jobs.txt'

    # Parse jobs text file and create list of all_jobs to process
    all_jobs = []
    with open(main_path / 'configs' / jobs_filename, 'r') as j:
        for line in j:
            line = line.strip()
            if '#' in line or line == '':
                continue
            if line == 'done':
                break
            if line == 'all':
                all_jobs = jobs_utils.calculate_all_jobs(groupings_for_datasets)
                break

            if '[' in line:
                line_vals = ast.literal_eval(line)
            else:
                line_vals = line.split(',')

            if line_vals[2] == 'TI':
                print(f'Time-invariant groupings not currently tested/supported. Exiting')
                sys.exit()

            # if number of time steps to process is all, leave it, otherwise make sure it is an integer
            # if line_vals[3] == 'all':
            if not isinstance(line_vals[3], list) and line_vals != 'all':
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], int(line_vals[3])])
            else:
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], line_vals[3]])
    # ========== </process metadata and jobs> =====================================================


    # ========== <non-local processing preparation> ===============================================
    # Setup all AWS required variables and information
    # Includes authentication, lambda function creation
    credentials = {}
    aws_config = {}
    if not local:
        # ========== <prepare aws configuration metadata> =========================================
        # Load 'aws_config.yaml'
        aws_config = yaml.safe_load(open(main_path / 'configs' / 'aws_config.yaml'))

        # AWS defaults
        aws_profile_name_default = 'saml-pub'
        aws_region_default = 'us-west-2'
        model_granule_bucket_default = 'ecco-model-granules'
        processed_data_bucket_default = 'ecco-processed-data'
        if aws_config['credential_method_type'] == 'bash':
            aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'update_AWS_cred_ecco_production.sh')
        elif aws_config['credential_method_type'] == 'binary':
            if 'linux' in platform.platform().lower():
                aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'aws-login.linux.amd64')
            else:
                aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'aws-login.darwin.amd64')

        # Set config values to default values if none are included in the config yaml
        if aws_config['profile_name'] == '':
            aws_config['profile_name'] = aws_profile_name_default
        if aws_config['region'] == '':
            aws_config['region'] = aws_region_default
        if aws_config['source_bucket'] == '':
            aws_config['source_bucket'] = model_granule_bucket_default
        if aws_config['output_bucket'] == '':
            aws_config['output_bucket'] = processed_data_bucket_default
        if aws_config['aws_credential_path'] == '':
            aws_config['aws_credential_path'] = aws_credential_path_default

        source_bucket = aws_config['source_bucket']
        source_bucket_folder_name = aws_config['bucket_subfolder']
        function_name_prefix = aws_config['function_name_prefix']
        image_uri = aws_config['image_uri']
        role = aws_config['role']
        account_id = aws_config['account_id']
        region = aws_config['region']

        # Memory sizes dictionary where key is the function name and the value is the memory for it
        memory_sizes = {
            f'{function_name_prefix}_2D_latlon': aws_config['memory_size_2D_latlon'],
            f'{function_name_prefix}_2D_native': aws_config['memory_size_2D_native'],
            f'{function_name_prefix}_3D_latlon': aws_config['memory_size_3D_latlon'],
            f'{function_name_prefix}_3D_native': aws_config['memory_size_3D_native']
        }
        # ========== </prepare aws configuration metadata> ========================================


        # ========== <verify AWS credentials> =====================================================
        # Verify credentials
        credential_method = {}
        credential_method['region'] = region
        credential_method['type'] = aws_config['credential_method_type']
        credential_method['aws_credential_path'] = aws_config['aws_credential_path'] 
        credentials = credentials_utils.get_credentials_helper()
        try:
            if force_reconfigure:
                # Getting new credentials
                credentials = credentials_utils.get_aws_credentials(credential_method)
            elif credentials != {}:
                boto3.setup_default_session(profile_name=credentials['profile_name'])
                try:
                    boto3.client('s3').list_buckets()
                except:
                    # Present credentials are invalid, try to get new ones
                    credentials = credentials_utils.get_aws_credentials(credential_method)
            else:
                # No credentials present, try to get new ones
                credentials = credentials_utils.get_aws_credentials(credential_method)
        except Exception as e:
            print(f'Unable to login to AWS. Exiting')
            print(e)
            sys.exit()

        # Setup AWS session and S3 client
        boto3.setup_default_session(profile_name=credentials['profile_name'])
        s3 = boto3.client('s3')
        # ========== </verify AWS credentials =====================================================
    # ========== </non-local processing preparation> ==============================================


    # ========== <AWS Lambda preparation> =========================================================
    if use_lambda:
        lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())

        # Create arn
        prefix = 'aws'
        arn = f'arn:{prefix}:iam::{account_id}:role/{role}'

        # Create lambda client
        lambda_client = boto3.client('lambda')

        # values for cost estimation
        MB_to_GB = 0.0009765625
        USD_per_GBsec = 0.0000166667


        # ========== <only logging> ===============================================================
        # If a log file is passed in via the logs_only argument, load it in and process logs on CloudWatch
        if dict_key_args['logs_only'] != '':
            curr_job_logs = json.load(open(dict_key_args['logs_only']))
            start_time = 0
            num_jobs = 0
            for j_id, job in curr_job_logs['Jobs'].items():
                if job['end']:
                    num_jobs += 1
            num_jobs = curr_job_logs['Number of Lambda Jobs'] - num_jobs
            # If Cost Information is empty, make a new defaultdict, otherwise
            # fill a defaultdict with the current values in Cost Information
            if curr_job_logs['Cost Information'] == {}:
                curr_job_logs['Cost Information'] = defaultdict(float)
            else:
                cost_info = defaultdict(float)
                for key, value in curr_job_logs['Cost Information'].itmes():
                    cost_info[key] = value
            job_logs = logging_utils.lambda_logging(curr_job_logs, 
                                                    start_time, 
                                                    ms_to_sec, 
                                                    MB_to_GB, 
                                                    USD_per_GBsec, 
                                                    lambda_start_time, 
                                                    num_jobs, 
                                                    credential_method, 
                                                    dict_key_args['log_name'], 
                                                    main_path)
            sys.exit()
        # ========== </only logging> ==============================================================
        

        # ========== <Lambda function creation/updating> ==========================================
        # get ECR image info
        ecr_client = boto3.client('ecr')

        # Get information about current ECR image (name, tag, last push date/time)
        repo_name_and_tag = image_uri.split('/')[-1]
        repo_name, image_tag = repo_name_and_tag.split(':')
        repo_images = ecr_client.list_images(repositoryName=repo_name)
        image_ids = ''
        for image in repo_images['imageIds']:
            if ('imageTag' in image) and (image['imageTag'] == image_tag):
                image_ids = image
        image_info = ecr_client.describe_images(repositoryName=repo_name, imageIds=[image_ids])
        image_push_time = image_info['imageDetails'][0]['imagePushedAt'].astimezone(tz=timezone.utc)
        image_push_time = datetime.strftime(image_push_time, format='%Y-%m-%dT%H:%M:%S')

        # Compare each function's last modified time to the ECR image's last push time
        # and add the function to the functions_to_update list if an update is needed
        all_functions = []
        functions_to_update = []
        lambda_functions = lambda_client.list_functions()['Functions']
        for func in lambda_functions:
            if function_name_prefix in func['FunctionName']:
                all_functions.append(func['FunctionName'])
                func_modified = func['LastModified'].split('.')[0]
                if func_modified < image_push_time:
                    functions_to_update.append(func['FunctionName'])

        print(f'\nCreating and updating lambda functions')
        for (grouping_to_process, product_type, output_freq_code, num_time_steps_to_process) in all_jobs:
            # Get grouping dictionary for job's product type
            curr_grouping = groupings_for_datasets[product_type][grouping_to_process]

            # get fields and dimension for current job
            fields = curr_grouping['fields'].split(', ')
            fields = [f.strip() for f in fields]
            dimension = curr_grouping['dimension']

            # define the function name using the configuration function_name_prefix,
            # the job's dimension, and product type
            if product_type == 'latlon':
                if dimension == '2D':
                    function_name = f'{function_name_prefix}_2D_latlon'
                elif dimension == '3D':
                    function_name = f'{function_name_prefix}_3D_latlon'
                else:
                    print(f'Dimension ({dimension}) not currently supported for Lambda. Exiting.')
                    sys.exit()
            elif product_type == 'native':
                if dimension == '2D':
                    function_name = f'{function_name_prefix}_2D_native'
                elif dimension == '3D':
                    function_name = f'{function_name_prefix}_3D_native'
                else:
                    print(f'Dimension ({dimension}) not currently supported for Lambda. Exiting.')
                    sys.exit()
            
            # create lambda functions for jobs, or update it if it already exists
            if function_name not in all_functions:
                # create function since it does not exist
                memory_size = memory_sizes[function_name]
                status = lambda_utils.create_lambda_function(lambda_client, 
                                                             function_name, 
                                                             arn, 
                                                             memory_sizes[function_name], 
                                                             image_uri)
                if status != 'SUCCESS':
                    print(status)
                    sys.exit()
                all_functions.append(function_name)
            elif function_name in functions_to_update:
                # update function using newest ECR image
                status = lambda_utils.update_lambda_function(lambda_client, 
                                                             function_name, 
                                                             image_uri)
                if status != 'SUCCESS':
                    print(status)
                    sys.exit()
                functions_to_update.remove(function_name)
        print(f'\nAll necessary functions up to date!\n')
        # ========== </Lambda function creation/updating> =========================================

        # Create inital jobs_log
        num_jobs = 0
        job_logs = {}
        job_logs['Run Name'] = dict_key_args['log_name']
        job_logs['Run Date'] = time.strftime('%Y%m%d:%H%M%S', master_start_time)
        job_logs['Master Script Total Time (s)'] = 0
        job_logs['Cost Information'] = defaultdict(float)
        job_logs['Number of Lambda Jobs'] = 0
        if dict_key_args['include_all_timesteps']:
            job_logs['All timesteps'] = []
            job_logs['Timesteps submitted'] = []
        job_logs['Timesteps failed'] = []
        job_logs['Jobs'] = {}
    # ========== </AWS Lambda preparation> ========================================================


    # ========== <Job processing> =================================================================
    # loop through all jobs and run them. If using lambda and logging is enabled, then lambda logs are
    # saved and automatic job resubmission occurs if the number of retries in aws_config is > 0
    if process_data:
        print(f'\n=== PROCESSING START ===')
        num_jobs = 0
        for current_job in all_jobs:
            temp_num_jobs, job_logs, status = jobs_utils.run_job(current_job, groupings_for_datasets, dict_key_args, product_generation_config, aws_config, debug_mode, s3=s3, lambda_client=lambda_client, job_logs=job_logs, credentials=credentials)
            num_jobs += temp_num_jobs
            if status != 'SUCCESS':
                print(f'Skipping job')
                print(f'\t{status}')
            print(f'Total number of Lambda jobs: {num_jobs}')

        # ========== <Lambda logging> =============================================================
        if use_lambda and dict_key_args['enable_logging']:
            # Call function to process lambda logs until all jobs are finished
            job_logs = logging_utils.lambda_logging(job_logs, 
                                                    start_time, 
                                                    ms_to_sec, 
                                                    MB_to_GB, 
                                                    USD_per_GBsec, 
                                                    lambda_start_time, 
                                                    num_jobs, 
                                                    credential_method, 
                                                    dict_key_args['log_name'], 
                                                    main_path)

            # ========== <Lambda job resubmission> ================================================
            # Automatically resubmit jobs if num_retry > 0 and 1 or more timesteps failed
            if aws_config['num_retry'] > 0 and len(job_logs['Timesteps failed']) > 0:
                for retry_num in range(aws_config['num_retry']):
                    num_jobs = 0
                    if retry_num == 0:
                        last_job_logs = job_logs
                    lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())
                    start_time = int(time.time()/ms_to_sec)

                    # Create new job_logs just for the retries
                    retry_job_logs = {}
                    retry_job_logs['Master Script Total Time (s)'] = 0
                    retry_job_logs['Cost Information'] = defaultdict(float)
                    retry_job_logs['Number of Lambda Jobs'] = 0
                    if dict_key_args['include_all_timesteps']:
                        retry_job_logs['All timesteps'] = []
                        retry_job_logs['Timesteps submitted'] = []
                    retry_job_logs['Timesteps failed'] = []
                    retry_job_logs['Jobs'] = {}

                    # Create dictionary where the keys are jobs (i.e. "0, native, AVG_MON", etc.)
                    # and the values are the timesteps to process for that job (i.e. ["00001776"], etc.)
                    retry_jobs = defaultdict(list)
                    for ts_failed in last_job_logs['Timesteps failed']:
                        ts_failed = ts_failed.strip().split(' ')
                        failed_job = ts_failed[1:-1]
                        failed_job[0] = int(failed_job[0])
                        retry_jobs[str(failed_job)].append(ts_failed[0])

                    # Re-run the failed timesteps as new jobs, and produce the new retry logs
                    for job, ts in retry_jobs.items():
                        current_job = ast.literal_eval(job)
                        current_job.append(ts)
                        temp_num_jobs, retry_job_logs, status = jobs_utils.run_job(current_job, groupings_for_datasets, dict_key_args, product_generation_config, aws_config, debug_mode, s3=s3, lambda_client=lambda_client, job_logs=retry_job_logs, credentials=credentials)
                        num_jobs += temp_num_jobs
                        if status != 'SUCCESS':
                            print(f'Skipping job')
                            print(f'\t{status}')
                        print(f'Total number of Lambda jobs: {num_jobs}')
                    
                    last_job_logs = logging_utils.lambda_logging(retry_job_logs, 
                                                                start_time, 
                                                                ms_to_sec, 
                                                                MB_to_GB, 
                                                                USD_per_GBsec, 
                                                                lambda_start_time, 
                                                                num_jobs, 
                                                                credential_method, 
                                                                dict_key_args['log_name'], 
                                                                main_path, 
                                                                retry=retry_num)
            # ========== </Lambda job resubmission> ===============================================
        # ========== </Lambda logging> ============================================================

        # Delete lambda function(s)
        # if use_lambda:
        #     for function_name in current_functions:
        #         if 'ecco_processing' in function_name:
        #             print(f'Deleting function: {function_name}')
        #             lambda_client.delete_function(FunctionName=function_name)
    # ========== </Job processing> ================================================================

    master_total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
    print(f'\n=== PROCESSING COMPLETE ===')
    print(f'Master script total time: {master_total_time:.2f}s')
    print(f'=== EXECUTION COMPLETE ===')