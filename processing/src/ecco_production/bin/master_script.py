#!/usr/bin/env python3

"""
ECCO Dataset Production Master Script

Author: Duncan Bark

Primary script for all ECCO Processing. From this script, all functions of ECCO Processing can be started including:
- Create mapping factors, land mask, and sparse matrices
- Uploading local files to AWS S3
- Prompt user to create jobs file from available groupings in metadata files
- Process 2D/3D native/latlon granules, sourced locally, locally
- Process 2D/3D native/latlon granules, sourced from AWS S3, locally 
- Process 2D/3D native/latlon granules, sourced from AWS S3, via AWS Lambda
- Process logs created from Lambda executions

"""

import argparse
import sys
import textwrap
import time

from ..utils import mapping_factors_utils
from ..utils import print_utils

#!import os
#!import ast
#!import json
#!import yaml
#!import boto3
#!import shutil
#!import platform
#!import subprocess
#!from pathlib import Path
#!from collections import defaultdict
#!from datetime import datetime, timezone
#!
#!# Local imports
#!main_path = Path(__file__).parent.resolve().parent.resolve()
#!sys.path.append(f'{main_path}')
#!sys.path.append(f'{main_path / "src"}')
#!sys.path.append(f'{main_path / "src" / "utils"}')
#!
#!try:
#!    import setup as setup
#!    import jobs_utils as jobs_utils
#!    import print_utils as print_utils
#!    import lambda_utils as lambda_utils
#!    import logging_utils as logging_utils
#!    import credentials_utils as credentials_utils
#!    import mapping_factors_utils as mapping_factors_utils
#!except Exception as e:
#!    print(f'Error importing code files. Have you run setup.py? (Error: {e}, {repr(e)})')
#!    sys.exit()


def create_parser():
    """Set up list of command-line arguments to master_script.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""
            Primary script for all ECCO release dataset processing. From this script, all
            functions of ECCO Processing can be started including:
                - Create mapping factors, land mask, and sparse matrices
                - Uploading local files to AWS S3
                - Prompt user to create jobs file from available groupings in metadata files
                - Process 2D/3D native/latlon granules, sourced locally, locally
                - Process 2D/3D native/latlon granules, sourced from AWS S3, locally
                - Process 2D/3D native/latlon granules, sourced from AWS S3, via AWS Lambda
                - Process logs created from Lambda executions"""))

    parser.add_argument('--process_data', action='store_true', help="""
        Starts processing model data using config file values""")

    parser.add_argument('--use_S3', action='store_true', help="""
        Source model granules from AWS S3 and save processed files to S3""")

    parser.add_argument('--use_lambda', action='store_true', help="""
        Completes processing via AWS Lambda""")

    parser.add_argument('--force_reconfigure', action='store_true', help="""
        Force code to re-run code to get AWS credentials""")

    parser.add_argument('--create_factors', action='store_true', help="""
        Create all mapping factors and exit: 2D/3D factors, landmask,
        latlon_grid fields, and sparse matrices""")

    parser.add_argument('--require_input', action='store_true', help="""
        Requests approval from user to start executing AWS Lambda jobs for each
        job (eg. 0,latlon,AVG_MON,all)""")

    parser.add_argument('--log_name', default='', help="""
        Name to use in the saved log file(s)""")

    parser.add_argument('--logs_only', default='', help="""
        ONLY does logging. Loads provided log file and collects logs from AWS
        CloudWatch and produces new log file""")

    parser.add_argument('--enable_logging', action='store_true', help="""
        Enables logging for AWS Lambda jobs""")

    parser.add_argument('--create_jobs', action='store_true', help="""
        Prompts user on jobs they want to process""")

    parser.add_argument('--push_ecr', action='store_true', help="""
        Re-builds Docker image and pushes it to AWS ECR""")

    parser.add_argument('--upload_to_S3', action='store_true', help="""
        ONLY uploads files from "local_file_dir_to_upload" in
        product_generation_config.yaml, to "S3_upload_path" in aws_config.yaml.
        Exits when complete""")

    parser.add_argument('--dryrun', action='store_true', help="""
        Does a dryrun of uploading files to S3 (TODO: dryrun of all processing
        code)""")

    parser.add_argument('--dont_delete_local', action='store_true', help="""
        Prevents deletion of any files downloaded locally (model granules and
        processed files). Files local to Lambdas are still deleted.""")

    parser.add_argument('--dont_delete_cloudwatch', action='store_true', help="""
        Prevents deletion of any AWS CloudWatch log files during AWS Lambda
        logging.""")

    return parser


def master_script(
    create_factors=False):

    # ========== <create intial time values> ======================================================
    ms_to_sec = 0.001
    master_start_time = time.localtime() # datetime
    start_time = int(time.time() / ms_to_sec) # miliseconds since 1970
    # ========== </create inital time values> =====================================================


#!    # ========== <parse command line arguments> ===================================================
#!    print_utils.printc(f'Parsing arguments', 'blue')
#!    parser = create_parser()
#!    args = parser.parse_args()
#!    #dict_key_args = {key: value for key, value in args._get_kwargs()}
#!
#!    #process_data = dict_key_args['process_data']
#!    #use_S3 = dict_key_args['use_S3']
#!    #use_lambda = dict_key_args['use_lambda']
#!    #push_ecr = dict_key_args['push_ecr']
#!    #force_reconfigure = dict_key_args['force_reconfigure']
#!    #upload_to_S3 = dict_key_args['upload_to_S3']
#!
#!    # Verify user does not want to enable logging
#!    # Logging only happens when processing data via AWS Lambda
#!    if args.process_data and args.use_lambda and not args.enable_logging:
#!        logging_check = input(f'Logging has not been enabled, continue? (y/n)\t').lower().strip()
#!        if logging_check != 'y':
#!            print_utils.printc('EXITING', 'red')
#!            sys.exit()
#!    print_utils.printc('Parsing arguments -- DONE', 'green')
#!    # ========== </parse command line arguments> ==================================================


#!    # ========== <prepare product generation configuration> =======================================
#!    product_generation_config = setup.prepare_product_generation_config()
#!    extra_prints = product_generation_config['extra_prints']
#!    ecco_version = product_generation_config['ecco_version']
#!    ecco_configuration_dir = main_path.parent.resolve().parent.resolve() / product_generation_config['ecco_configurations_name']
#!    # ========== </prepare product generation configuration> ======================================


    # ========== <create mapping factors> ========================================================
    # Creates mapping_factors (2D and 3D), landmask, latlon_grid, and sparse matrix files
    # Not needed unless changes have been made to the factors code and you need
    # to update the factors/mask in the lambda docker image
    if create_factors:
    #if dict_key_args['create_factors']:
        print_utils.printc('\nCreating factors', 'blue')
        status = mapping_factors_utils.create_all_factors(
            product_generation_config, ['2D', '3D'], extra_prints=extra_prints)
        if status != 'SUCCESS':
            print_utils.printc(status, 'red')
            sys.exit()
        print_utils.printc('Creating factors -- DONE', 'green')
        sys.exit()
    # ========== </create mapping factors> ========================================================


#!    # ========== <process metadata and jobs> ======================================================
#!    # make sure processing metadata folder(s) exists
#!    print_utils.printc(f'\nPreparing metadata', 'blue')
#!    if not os.path.exists(Path(product_generation_config['metadata_dir'])):
#!        os.makedirs(Path(product_generation_config['metadata_dir']), exist_ok=True)
#!
#!    # get metadata from {ecco_configuration_name}/{ecco_configurations_subfolder}/metadata/ and save it to 
#!    # the metadata directory in product_generation_config.
#!    # assumes {ecco_configurations_name}/ exists on the same level as ECCO-Dataset-Production
#!    ec_metadata_dir = ecco_configuration_dir / product_generation_config['ecco_configurations_subfolder'] / 'metadata'
#!    ec_metadata_files = os.listdir(ec_metadata_dir)
#!    for metadata_file in ec_metadata_files:
#!        # Only get .json files and the PODAAC csv metadata file
#!        if '.json' in metadata_file or metadata_file == product_generation_config['podaac_metadata_filename']:
#!            # clean up the name if it has ECCOv4r4_ (or similar) in the name
#!            if f'ECCO{ecco_version.lower()}_' in metadata_file:
#!                new_metadata_file = metadata_file.replace(f'ECCO{ecco_version.lower()}_', '')
#!            else:
#!                new_metadata_file = metadata_file
#!
#!            # get ECCO Configurations file path, and the new ECCO-Dataset-Production filepath
#!            ec_metadata_file_path = ec_metadata_dir / metadata_file
#!            processing_metadata_file_path = Path(product_generation_config['metadata_dir']) / new_metadata_file
#!
#!            # copy .json (or PODAAC csv) metadata file from ECCO Configurations to a local ECCO-Dataset-Production directory
#!            shutil.copyfile(ec_metadata_file_path, processing_metadata_file_path)
#!
#!    # Get grouping information
#!    groupings_metadata = [f for f in os.listdir(product_generation_config['metadata_dir']) if 'groupings' in f]
#!
#!    # Load each grouping json and create corresponding groupings dicts
#!    metadata = {}
#!    for mf in groupings_metadata:
#!        with open(str(Path(product_generation_config['metadata_dir']) / mf), 'r') as fp:
#!            metadata[mf] = json.load(fp)
#!
#!    # Create dictionary containing groupings dictionaries for each product type
#!    groupings_for_datasets = {}
#!    for mf_name, mf in metadata.items():
#!        if '1D' in mf_name:
#!            groupings_for_datasets['1D'] = mf
#!        elif 'latlon' in mf_name:
#!            groupings_for_datasets['latlon'] = mf
#!        elif 'native' in mf_name:
#!            groupings_for_datasets['native'] = mf
#!    print_utils.printc('Preparing metadata -- DONE', 'green')
#!
#!    print_utils.printc(f'\nProcessing jobs', 'blue')
#!    # Call create_jobs which prompts user to select datasets to process
#!    jobs_filename = 'jobs.txt'
#!    if args.create_jobs:
#!    #if dict_key_args['create_jobs']:
#!        jobs_utils.create_jobs(groupings_for_datasets, jobs_filename)
#!
#!    # Parse jobs text file and create list of all_jobs to process
#!    all_jobs = []
#!    with open(main_path / 'configs' / jobs_filename, 'r') as j:
#!        for i, line in enumerate(j):
#!            line = line.strip()
#!            # comment/blank line, skip
#!            if '#' in line or line == '':
#!                continue
#!
#!            # "done" line, break out of file, dont add anymore jobs
#!            if line == 'done':
#!                break
#!
#!            # "all" line, process ALL groupings, for ALL timesteps
#!            if line == 'all':
#!                all_jobs = jobs_utils.calculate_all_jobs(groupings_for_datasets)
#!                break
#!
#!            # if timesteps is a list, evaluate it, otherwise split the job on commas
#!            if '[' in line:
#!                try:
#!                    line_vals = ast.literal_eval(line)
#!                except:
#!                    print_utils.printc(f'Unable to parse job: {line} (line {i+1}). Skipping.', 'red')
#!                    continue
#!            else:
#!                line_vals = line.split(',')
#!
#!            # if the frequency is time invariant, exit as it is not currently tested or supported
#!            if line_vals[2] == 'TI':
#!                print_utils.printc(f'Time-invariant groupings not currently tested/supported. Skipping', 'red')
#!                continue
#!
#!            # the the number of time steps is a list or 'all', leave it. Otherwise, make sure it is an int
#!            if not isinstance(line_vals[3], list) and line_vals[3] != 'all':
#!                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], int(line_vals[3])])
#!            else:
#!                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], line_vals[3]])
#!
#!    print_utils.printc('Processing jobs -- DONE', 'green')
#!    # ========== </process metadata and jobs> =====================================================


#!    # ========== <non-local processing preparation> ===============================================
#!    # Setup all AWS required variables and information
#!    # Includes authentication, lambda function creation
#!    credentials = {}
#!    aws_config = {}
#!    s3 = None
#!    if args.use_S3 or args.use_lambda or args.push_ecr or args.upload_to_S3:
#!        print_utils.printc(f'\nPreparing AWS', 'blue')
#!        # ========== <prepare aws configuration metadata> =========================================
#!        # Load 'aws_config.yaml'
#!        aws_config = yaml.safe_load(open(main_path / 'configs' / 'aws_config.yaml'))
#!
#!        # AWS defaults
#!        aws_profile_name_default = 'saml-pub'
#!        aws_region_default = 'us-west-2'
#!        model_granule_bucket_default = 'ecco-model-granules'
#!        processed_data_bucket_default = 'ecco-processed-data'
#!        aws_credentials_bash_filepath_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'update_AWS_cred_ecco_production.sh')
#!
#!        # Set config values to default values if none are included in the config yaml
#!        if aws_config['profile_name'] == '':
#!            aws_config['profile_name'] = aws_profile_name_default
#!        if aws_config['region'] == '':
#!            aws_config['region'] = aws_region_default
#!        if aws_config['source_bucket'] == '':
#!            aws_config['source_bucket'] = model_granule_bucket_default
#!        if aws_config['output_bucket'] == '':
#!            aws_config['output_bucket'] = processed_data_bucket_default
#!        if aws_config['aws_credentials_bash_filepath'] == '':
#!            aws_config['aws_credentials_bash_filepath'] = aws_credentials_bash_filepath_default
#!
#!        source_bucket = aws_config['source_bucket']
#!        source_bucket_folder_name = aws_config['bucket_subfolder']
#!        function_name_prefix = aws_config['function_name_prefix']
#!        image_uri = aws_config['image_uri']
#!        role = aws_config['role']
#!        account_id = aws_config['account_id']
#!        region = aws_config['region']
#!
#!        S3_upload_path_default = f'{source_bucket}/{source_bucket_folder_name}'
#!        if aws_config['S3_upload_path'] == '':
#!            aws_config['S3_upload_path'] = S3_upload_path_default
#!
#!        # Memory sizes dictionary where key is the function name and the value is the memory for it
#!        memory_sizes = {
#!            f'{function_name_prefix}_1D': aws_config['memory_size_1D'],
#!            f'{function_name_prefix}_2D_latlon': aws_config['memory_size_2D_latlon'],
#!            f'{function_name_prefix}_2D_native': aws_config['memory_size_2D_native'],
#!            f'{function_name_prefix}_3D_latlon': aws_config['memory_size_3D_latlon'],
#!            f'{function_name_prefix}_3D_native': aws_config['memory_size_3D_native']
#!        }
#!        # ========== </prepare aws configuration metadata> ========================================


#!        # ========== <verify AWS credentials> =====================================================
#!        # Verify credentials
#!        credential_method = {}
#!        credential_method['region'] = region
#!        credential_method['type'] = aws_config['credential_method_type']
#!        credential_method['bash_filepath'] = aws_config['aws_credentials_bash_filepath']
#!
#!        # get the aws login file name based on the credental method type and operating system
#!        if credential_method['type'] == 'python':
#!            credential_method['aws_login_file'] = 'aws-login.py'
#!        elif credential_method['type'] == 'binary':
#!            if 'linux' in platform.platform().lower():
#!                credential_method['aws_login_file'] = 'aws-login.linux.amd64'
#!            else:
#!                credential_method['aws_login_file'] = 'aws-login.darwin.amd64'
#!        
#!        # get AWS credentials and make sure they are valid
#!        credentials = credentials_utils.get_aws_credentials()
#!        try:
#!            if args.force_reconfigure:
#!                # Getting new credentials
#!                credentials = credentials_utils.get_aws_credentials(credential_method)
#!            elif credentials != {}:
#!                boto3.setup_default_session(profile_name=credentials['profile_name'])
#!                try:
#!                    boto3.client('s3').list_buckets()
#!                except:
#!                    # Present credentials are invalid, try to get new ones
#!                    credentials = credentials_utils.get_aws_credentials(credential_method)
#!            else:
#!                # No credentials present, try to get new ones
#!                credentials = credentials_utils.get_aws_credentials(credential_method)
#!
#!            # test credentials to verify they are valid. If they are not valid, this will
#!            # produce an error which is then caught and execution is ended.
#!            boto3.setup_default_session(profile_name=credentials['profile_name'])
#!            boto3.client('s3').list_buckets()
#!        except Exception as e:
#!            print_utils.printc(f'Unable to login to AWS ({e}). Exiting', 'red')
#!            sys.exit()
#!
#!        # Setup AWS session and S3 client
#!        boto3.setup_default_session(profile_name=credentials['profile_name'])
#!        s3 = boto3.client('s3')
#!        # ========== </verify AWS credentials =====================================================
#!        print_utils.printc(f'Preparing AWS -- DONE', 'green')
#!    # ========== </non-local processing preparation> ==============================================


#!    # ========== <Upload local files to AWS S3> ===================================================
#!    if args.upload_to_S3:
#!        print_utils.printc('Uploading files to S3', 'blue')
#!        local_file_dir = product_generation_config['local_file_dir_to_upload']
#!        if local_file_dir != '':
#!            if os.path.exists(local_file_dir):
#!                if not args.dryrun:
#!                    user_resp = input(f'No dryrun argument passed, continue with uploading files to AWS S3? (y/n)\t')
#!                    if user_resp.lower().strip() != 'y':
#!                        print_utils.printc('EXITING', 'red')
#!                        sys.exit()
#!                upload_to_AWS_path = main_path.parent.resolve() / 'upload_to_AWS'
#!                aws_sync_script_path = upload_to_AWS_path / 'aws_sync_directories_to_S3.sh'
#!                
#!                # get the current working directory
#!                orig_cwd = os.getcwd()
#!
#!                # change the working directory to that of the aws_sync script, so that the log files
#!                # saved from within the script are saved to the upload_to_AWS/logs/ directory
#!                os.chdir(os.path.join(os.path.abspath(sys.path[0]), upload_to_AWS_path))
#!
#!                # run uploading script
#!                # Arguments are as follows:
#!                # cred_type=$1
#!                # cred_file_name=$2
#!                # region=$3
#!                # login_file_dir=$4
#!                # s3_dir=$5
#!                # files_dir=$6
#!                # update_AWS_cred_file=$7
#!                # dryrun=$8
#!                subprocess.run([aws_sync_script_path, 
#!                                credential_method['type'], 
#!                                credential_method['aws_login_file'], 
#!                                region, 
#!                                str(Path(credential_method['bash_filepath']).parent), 
#!                                aws_config['S3_upload_path'], 
#!                                local_file_dir, 
#!                                credential_method['bash_filepath'], 
#!                                args.dryrun])
#!                
#!                # change working directory to original working directory (eg. the directory of master_script.py)
#!                os.chdir(orig_cwd)
#!        print_utils.printc('Uploading files to S3 -- DONE', 'green')
#!        sys.exit()
#!    # ========== </Upload local files to AWS S3> ==================================================


#!    # ========== <AWS Lambda preparation> =========================================================
#!    # if "push_ecr" arugment is passed, then call ecr_pus.sh script to re-build Docker image and
#!    # push it to ECR
#!    if args.push_ecr:
#!        print_utils.printc(f'\nPushing to AWS ECR', 'blue')
#!        # get the current working directory
#!        orig_cwd = os.getcwd()
#!
#!        # get the container name and tag to use
#!        container_name_and_tag = image_uri.split('/')[-1].split(':')
#!        container_name = container_name_and_tag[0]
#!        image_tag = container_name_and_tag[1]
#!        ecr_push_path = str(main_path / 'ecr_push.sh')
#!
#!        # change working directory to that of the ecr_push script. This is because when you build
#!        # the docker image, all paths within the Dockerfile are relative to the where the script
#!        # is run. When using subprocess.run() the working directory does not change to that of the
#!        # file being run, so we change the directory.
#!        os.chdir(os.path.join(os.path.abspath(sys.path[0]), main_path))
#!
#!        # run ecr_push.sh
#!        subprocess.run([ecr_push_path, container_name, image_tag, ecco_version], check=True)
#!
#!        # change working directory to original working directory (eg. the directory of master_script.py)
#!        os.chdir(orig_cwd)
#!        print_utils.printc(f'Pushing to AWS ECR -- DONE', 'green')
#!    
#!    lambda_client = None
#!    job_logs = {}
#!    if args.use_lambda:
#!        print_utils.printc(f'\nPreparing AWS Lambda', 'blue')
#!        lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())
#!
#!        # Create arn
#!        prefix = 'aws'
#!        arn = f'arn:{prefix}:iam::{account_id}:role/{role}'
#!
#!        # Create lambda client
#!        lambda_client = boto3.client('lambda')
#!
#!        # values for cost estimation
#!        MB_to_GB = 0.0009765625
#!        USD_per_GBsec = aws_config['USD_per_GBms'] * 1000.
#!
#!
#!        # ========== <only logging> ===============================================================
#!        # If a log file is passed in via the logs_only argument, load it in and process logs on CloudWatch
#!        if args.logs_only != '':
#!        #if dict_key_args['logs_only'] != '':
#!            curr_job_logs = json.load(open(args.logs_only))
#!            #curr_job_logs = json.load(open(dict_key_args['logs_only']))
#!            start_time = 0
#!            num_jobs = 0
#!            for j_id, job in curr_job_logs['Jobs'].items():
#!                if job['end']:
#!                    num_jobs += 1
#!            num_jobs = curr_job_logs['Number of Lambda Jobs'] - num_jobs
#!            # If Cost Information is empty, make a new defaultdict, otherwise
#!            # fill a new defaultdict with the current values in Cost Information
#!            if curr_job_logs['Cost Information'] == {}:
#!                curr_job_logs['Cost Information'] = defaultdict(float)
#!            else:
#!                cost_info = defaultdict(float)
#!                for key, value in curr_job_logs['Cost Information'].items():
#!                    cost_info[key] = value
#!            job_logs = logging_utils.lambda_logging(curr_job_logs, 
#!                                                    start_time, 
#!                                                    ms_to_sec, 
#!                                                    MB_to_GB, 
#!                                                    USD_per_GBsec, 
#!                                                    lambda_start_time, 
#!                                                    num_jobs, 
#!                                                    credential_method, 
#!                                                    args.log_name, 
#!                                                    #dict_key_args['log_name'], 
#!                                                    main_path)
#!            sys.exit()
#!        # ========== </only logging> ==============================================================
#!        
#!
#!        # ========== <Lambda function creation/updating> ==========================================
#!        # get ECR image info
#!        ecr_client = boto3.client('ecr')
#!
#!        # Get information about current ECR image (name, tag, last push date/time)
#!        repo_name_and_tag = image_uri.split('/')[-1]
#!        repo_name, image_tag = repo_name_and_tag.split(':')
#!        repo_images = ecr_client.list_images(repositoryName=repo_name)
#!        image_ids = ''
#!        for image in repo_images['imageIds']:
#!            if ('imageTag' in image) and (image['imageTag'] == image_tag):
#!                image_ids = image
#!        image_info = ecr_client.describe_images(repositoryName=repo_name, imageIds=[image_ids])
#!        image_push_time = image_info['imageDetails'][0]['imagePushedAt'].astimezone(tz=timezone.utc)
#!        image_push_time = datetime.strftime(image_push_time, format='%Y-%m-%dT%H:%M:%S')
#!        image_sha = image_info['imageDetails'][0]['imageDigest'].split(':')[-1]
#!
#!        # Compare each function's last modified time to the ECR image's last push time
#!        # and add the function to the functions_to_update list if an update is needed
#!        all_functions = []
#!        functions_code_to_update = []
#!        functions_memory_to_update = []
#!        lambda_functions = lambda_client.list_functions()['Functions']
#!        for func in lambda_functions:
#!            if function_name_prefix in func['FunctionName']:
#!                all_functions.append(func['FunctionName'])
#!
#!                # Get function information values
#!                func_modified = func['LastModified'].split('.')[0]
#!                func_memory = func['MemorySize']
#!                func_product_type_and_freq = func['FunctionName'].split(f'{function_name_prefix}_')[-1]
#!                func_image_sha = func['CodeSha256']
#!
#!                # If updating the image to use for the function, add it to the list for function code updates
#!                if (func_modified < image_push_time) or (func_image_sha != image_sha):
#!                    functions_code_to_update.append(func['FunctionName'])
#!
#!                # If updating the memory for the lambda job, add it to the list for function memory updates
#!                if (func_memory != aws_config[f'memory_size_{func_product_type_and_freq}']):
#!                    functions_memory_to_update.append(func['FunctionName'])
#!
#!        print(f'\nCreating and updating lambda functions')
#!        for (grouping_to_process, product_type, output_freq_code, num_time_steps_to_process) in all_jobs:
#!            # Get grouping dictionary for job's product type
#!            curr_grouping = groupings_for_datasets[product_type][grouping_to_process]
#!
#!            # get fields and dimension for current job
#!            fields = curr_grouping['fields'].split(', ')
#!            fields = [f.strip() for f in fields]
#!            dimension = curr_grouping['dimension']
#!
#!            # define the function name using the configuration function_name_prefix,
#!            # the job's dimension, and product type
#!            if product_type == '1D':
#!                function_name = f'{function_name_prefix}_1D'
#!            elif product_type == 'latlon':
#!                if dimension == '2D':
#!                    function_name = f'{function_name_prefix}_2D_latlon'
#!                    lambda_memory = aws_config['memory_size_2D_latlon']
#!                elif dimension == '3D':
#!                    function_name = f'{function_name_prefix}_3D_latlon'
#!                    lambda_memory = aws_config['memory_size_3D_latlon']
#!                else:
#!                    print_utils.printc(f'Dimension ({dimension}) not currently supported for Lambda. Exiting.', 'red')
#!                    sys.exit()
#!            elif product_type == 'native':
#!                if dimension == '2D':
#!                    function_name = f'{function_name_prefix}_2D_native'
#!                    lambda_memory = aws_config['memory_size_2D_native']
#!                elif dimension == '3D':
#!                    function_name = f'{function_name_prefix}_3D_native'
#!                    lambda_memory = aws_config['memory_size_3D_native']
#!                else:
#!                    print_utils.printc(f'Dimension ({dimension}) not currently supported for Lambda. Exiting.', 'red')
#!                    sys.exit()
#!            
#!            # create lambda functions for jobs, or update it if it already exists
#!            if function_name not in all_functions:
#!                # create function since it does not exist
#!                memory_size = memory_sizes[function_name]
#!                status = lambda_utils.create_lambda_function(lambda_client, 
#!                                                             function_name, 
#!                                                             arn, 
#!                                                             memory_sizes[function_name], 
#!                                                             image_uri)
#!                if status != 'SUCCESS':
#!                    print_utils.printc(status, 'red')
#!                    sys.exit()
#!                all_functions.append(function_name)
#!            else:
#!                # Check to see if the function needs its memory or image updated
#!                # Set the other to none if only one of them are to be updated
#!                # i.e. if youre only updating the image, set the lambda_memory to none
#!                #       so that the update_lambda_function knows not to update the memory.
#!                update_code = function_name in functions_code_to_update
#!                update_memory = function_name in functions_memory_to_update
#!                if update_code and not update_memory:
#!                    lambda_memory = None
#!                elif not update_code and update_memory:
#!                    image_uri = None
#!
#!                # update function using ECR image and memory value
#!                status = lambda_utils.update_lambda_function(lambda_client, 
#!                                                            function_name, 
#!                                                            lambda_memory,
#!                                                            image_uri)
#!
#!                if status != 'SUCCESS':
#!                    print_utils.printc(status, 'red')
#!                    sys.exit()
#!                
#!                # Remove the function name from list of functions to update
#!                if update_code:
#!                    functions_code_to_update.remove(function_name)
#!                if update_memory:
#!                    functions_memory_to_update.remove(function_name)
#!                    
#!        print(f'\nAll necessary functions up to date!\n')
#!        import pdb; pdb.set_trace()
#!        # ========== </Lambda function creation/updating> =========================================
#!
#!        # Create inital jobs_log
#!        num_jobs = 0
#!        job_logs['Run Name'] = args.log_name
#!        #job_logs['Run Name'] = dict_key_args['log_name']
#!        job_logs['Run Date'] = time.strftime('%Y%m%d:%H%M%S', master_start_time)
#!        job_logs['Master Script Total Time (s)'] = 0
#!        job_logs['Cost Information'] = defaultdict(float)
#!        job_logs['Number of Lambda Jobs'] = 0
#!        job_logs['Timesteps failed'] = []
#!        job_logs['Jobs'] = {}
#!
#!        print_utils.printc(f'Preparing AWS Lambda -- DONE', 'green')
#!    # ========== </AWS Lambda preparation> ========================================================


#!    # ========== <Job processing> =================================================================
#!    # loop through all jobs and run them. If using lambda and logging is enabled, then lambda logs are
#!    # saved and automatic job resubmission occurs if the number of retries in aws_config is > 0
#!    if args.process_data:
#!        print_utils.printc(f'\n=== PROCESSING START ===', 'blue')
#!        num_jobs = 0
#!        for current_job in all_jobs:
#!            temp_num_jobs, job_logs, status = jobs_utils.run_job(current_job, 
#!                                                                 groupings_for_datasets, 
#!                                                                 -->pick up here. reason for dict_key_args??
#!                                                                 dict_key_args, 
#!                                                                 product_generation_config, 
#!                                                                 aws_config,
#!                                                                 s3=s3, 
#!                                                                 lambda_client=lambda_client, 
#!                                                                 job_logs=job_logs, 
#!                                                                 credentials=credentials)
#!            
#!            num_jobs += temp_num_jobs
#!            if status != 'SUCCESS':
#!                print_utils.printc(f'\tSKIPPING JOB ({status})', 'red')
#!            print(f'Total number of jobs: {num_jobs}')
#!
#!        # ========== <Lambda logging> =============================================================
#!        if args.use_lambda and dict_key_args['enable_logging']:
#!            # Call function to process lambda logs until all jobs are finished
#!            job_logs = logging_utils.lambda_logging(job_logs, 
#!                                                    start_time, 
#!                                                    ms_to_sec, 
#!                                                    MB_to_GB, 
#!                                                    USD_per_GBsec, 
#!                                                    lambda_start_time, 
#!                                                    num_jobs, 
#!                                                    credential_method, 
#!                                                    dict_key_args['log_name'], 
#!                                                    main_path,
#!                                                    dict_key_args['dont_delete_cloudwatch'])
#!
#!            # ========== <Lambda job resubmission> ================================================
#!            # Automatically resubmit jobs if num_retry > 0 and 1 or more timesteps failed
#!            if aws_config['num_retry'] > 0 and len(job_logs['Timesteps failed']) > 0:
#!                for retry_num in range(aws_config['num_retry']):
#!                    num_jobs = 0
#!                    if retry_num == 0:
#!                        last_job_logs = job_logs
#!                    lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())
#!                    start_time = int(time.time()/ms_to_sec)
#!
#!                    # Create new job_logs just for the retries
#!                    retry_job_logs = {}
#!                    retry_job_logs['Master Script Total Time (s)'] = 0
#!                    retry_job_logs['Cost Information'] = defaultdict(float)
#!                    retry_job_logs['Number of Lambda Jobs'] = 0
#!                    retry_job_logs['Timesteps failed'] = []
#!                    retry_job_logs['Jobs'] = {}
#!
#!                    # Create dictionary where the keys are jobs (i.e. "0, native, AVG_MON", etc.)
#!                    # and the values are the timesteps to process for that job (i.e. ["00001776"], etc.)
#!                    retry_jobs = defaultdict(list)
#!                    for ts_failed in last_job_logs['Timesteps failed']:
#!                        ts_failed = ts_failed.strip().split(' ')
#!                        failed_job = ts_failed[1:-1]
#!                        failed_job[0] = int(failed_job[0])
#!                        retry_jobs[str(failed_job)].append(ts_failed[0])
#!
#!                    # Re-run the failed timesteps as new jobs, and produce the new retry logs
#!                    for job, ts in retry_jobs.items():
#!                        current_job = ast.literal_eval(job)
#!                        current_job.append(ts)
#!                        temp_num_jobs, retry_job_logs, status = jobs_utils.run_job(current_job, 
#!                                                                                   groupings_for_datasets, 
#!                                                                                   dict_key_args, 
#!                                                                                   product_generation_config, 
#!                                                                                   aws_config,
#!                                                                                   s3=s3, 
#!                                                                                   lambda_client=lambda_client, 
#!                                                                                   job_logs=retry_job_logs, 
#!                                                                                   credentials=credentials)
#!                        num_jobs += temp_num_jobs
#!                        if status != 'SUCCESS':
#!                            print_utils.printc(f'\tSKIPPING JOB ({status})', 'red')
#!                        print(f'Total number of Lambda jobs: {num_jobs}')
#!                    
#!                    last_job_logs = logging_utils.lambda_logging(retry_job_logs, 
#!                                                                start_time, 
#!                                                                ms_to_sec, 
#!                                                                MB_to_GB, 
#!                                                                USD_per_GBsec, 
#!                                                                lambda_start_time, 
#!                                                                num_jobs, 
#!                                                                credential_method, 
#!                                                                dict_key_args['log_name'], 
#!                                                                main_path, 
#!                                                                dict_key_args['dont_delete_cloudwatch'],
#!                                                                retry=retry_num)
#!            # ========== </Lambda job resubmission> ===============================================
#!        # ========== </Lambda logging> ============================================================
#!    # ========== </Job processing> ================================================================

    master_total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
    print_utils.printc(f'=== PROCESSING COMPLETE ===', 'green')
    print(f'Master script total time: {master_total_time:.2f}s')
    print_utils.printc(f'=== EXECUTION COMPLETE ===', 'green')


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()
    master_script(
        args.create_factors)


if __name__=='__main__':
    """Support calling from top-level, e.g.:
    $ python master_script.py ...

    """
    main()

