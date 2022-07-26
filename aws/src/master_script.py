"""
Created May 18, 2022

Author: Duncan Bark

"""
import ast
import sys
import glob
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
import aws_utils as aws_utils
import ecco_cloud_utils as ea
import create_factors_utils as create_factors_utils
from ecco_gen_for_podaac_cloud import generate_netcdfs

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
    return parser


if __name__ == "__main__":
    master_start_time = time.localtime()
    ms_to_sec = 0.001
    start_time = int(time.time() / ms_to_sec)
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()
    dict_key_args = {key: value for key, value in args._get_kwargs()}

    # Verify user does not want to enable logging
    if dict_key_args['use_lambda'] and dict_key_args['process_data'] and not dict_key_args['enable_logging'] and not dict_key_args['force']:
        logging_check = input(f'Logging has not been enabled, continue? (y/n)\t').lower().strip()
        if logging_check != 'y':
            print(f'Exiting')
            sys.exit()

    process_data = dict_key_args['process_data']
    debug_mode = dict_key_args['debug']
    local = not dict_key_args['use_cloud']
    use_lambda = dict_key_args['use_lambda']
    force_reconfigure = dict_key_args['force_reconfigure']

    # if use lambda, then local is False (only use S3 for data)
    if use_lambda:
        local = False

    # Load 'product_generation_config.yaml'
    product_generation_config = yaml.safe_load(open(main_path / 'configs' / 'product_generation_config.yaml'))

    # Load directories (local vs AWS)
    # Default directories
    mapping_factors_dir_default = str(main_path / 'mapping_factors')
    diags_root_default = str(main_path / 'tmp' / 'tmp_model_output')
    metadata_default = str(main_path / 'metadata')
    ecco_grid_dir_default = str(main_path / 'ecco_grids')
    ecco_grid_dir_mds_default = str(main_path / 'ecco_grids')
    processed_output_dir_base_default = str(main_path / 'tmp' / 'tmp_output')

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
    if product_generation_config['model_output_dir_folder_name'] == '':
        product_generation_config['model_output_dir_folder_name'] = product_generation_config['ecco_version']

    extra_prints = product_generation_config['extra_prints']


    # Creates mapping_factors (2D and 3D), landmask, latlon_grid, and sparse matrix files
    # Not needed unless changes have been made to the factors code and you need
    # to update the factors/mask in the lambda docker image
    if dict_key_args['create_factors']:
        status = create_factors_utils.create_all_factors(ea, product_generation_config, ['2D', '3D'], debug_mode=debug_mode, extra_prints=extra_prints)
        if status == -1:
            print('Error creating all factors. Exiting')
            sys.exit()
        print('\nCompleted creation of all factors. Exiting')
        sys.exit()


    # Get grouping information
    metadata_fields = ['ECCOv4r4_groupings_for_latlon_datasets', 'ECCOv4r4_groupings_for_native_datasets']

    # load METADATA
    metadata = {}

    for mf in metadata_fields:
        mf_e = mf + '.json'
        with open(str(Path(product_generation_config['metadata_dir']) / mf_e), 'r') as fp:
            metadata[mf] = json.load(fp)

    groupings_for_latlon_datasets = metadata['ECCOv4r4_groupings_for_latlon_datasets']
    groupings_for_native_datasets = metadata['ECCOv4r4_groupings_for_native_datasets']


    # Get all configurations
    all_jobs = []
    with open(main_path / 'configs' / 'jobs.txt', 'r') as j:
        # /Users/bark/Documents/ECCO_GROUP/ECCO-Dataset-Production/aws/configs/jobs.txt
        for line in j:
            line = line.strip()
            if '#' in line or line == '':
                continue
            if line == 'done':
                break
            if line == 'all':
                all_jobs = aws_utils.calculate_all_jobs(groupings_for_latlon_datasets, groupings_for_native_datasets)
                break
            line_vals = line.split(',')
            if line_vals[3] == 'all':
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], line_vals[3]])
            else:
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], int(line_vals[3])])

    # Setup all AWS required variables and information
    # Includes authentication, lambda function creation
    credentials = {}
    aws_config_metadata = {}
    if not local:
        # Load 'aws_config.yaml'
        aws_config_metadata = yaml.safe_load(open(main_path / 'configs' / 'aws_config.yaml'))

        # AWS defaults
        aws_profile_name_default = 'saml-pub'
        aws_region_default = 'us-west-2'
        model_granule_bucket_default = 'ecco-model-granules'
        processed_data_bucket_default = 'ecco-processed-data'
        if aws_config_metadata['credential_method_type'] == 'bash':
            aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'update_AWS_cred_ecco_production.sh')
        elif aws_config_metadata['credential_method_type'] == 'binary':
            if 'linux' in platform.platform().lower():
                aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'aws-login.linux.amd64')
            else:
                aws_credential_path_default = str(main_path / 'src' / 'utils' / 'aws_login' / 'aws-login.darwin.amd64')

        if aws_config_metadata['profile_name'] == '':
            aws_config_metadata['profile_name'] = aws_profile_name_default
        if aws_config_metadata['region'] == '':
            aws_config_metadata['region'] = aws_region_default
        if aws_config_metadata['source_bucket'] == '':
            aws_config_metadata['source_bucket'] = model_granule_bucket_default
        if aws_config_metadata['output_bucket'] == '':
            aws_config_metadata['output_bucket'] = processed_data_bucket_default
        if aws_config_metadata['aws_credential_path'] == '':
            aws_config_metadata['aws_credential_path'] = aws_credential_path_default

        source_bucket = aws_config_metadata['source_bucket']
        source_bucket_folder_name = aws_config_metadata['bucket_subfolder']
        function_name_prefix = aws_config_metadata['function_name_prefix']
        image_uri = aws_config_metadata['image_uri']
        role = aws_config_metadata['role']
        account_id = aws_config_metadata['account_id']
        region = aws_config_metadata['region']

        memory_sizes = {
            f'{function_name_prefix}_2D_latlon': aws_config_metadata['memory_size_2D_latlon'],
            f'{function_name_prefix}_2D_native': aws_config_metadata['memory_size_2D_native'],
            f'{function_name_prefix}_3D_latlon': aws_config_metadata['memory_size_3D_latlon'],
            f'{function_name_prefix}_3D_native': aws_config_metadata['memory_size_3D_native']
        }

        # Verify credentials
        credential_method = dict()
        credential_method['region'] = region
        credential_method['type'] = aws_config_metadata['credential_method_type']
        credential_method['aws_credential_path'] = aws_config_metadata['aws_credential_path'] 
        credentials = aws_utils.get_credentials_helper()
        try:
            if force_reconfigure:
                # Getting new credentials
                credentials = aws_utils.get_aws_credentials(credential_method)
            elif credentials != {}:
                boto3.setup_default_session(profile_name=credentials['profile_name'])
                try:
                    boto3.client('s3').list_buckets()
                except:
                    # Present credentials are invalid, try to get new ones
                    credentials = aws_utils.get_aws_credentials(credential_method)
            else:
                # No credentials present, try to get new ones
                credentials = aws_utils.get_aws_credentials(credential_method)
        except Exception as e:
            print(f'Unable to login to AWS. Exiting')
            print(e)
            sys.exit()

        # Create arn
        prefix = 'aws'
        arn = f'arn:{prefix}:iam::{account_id}:role/{role}'

        # Setup AWS session and S3 client
        boto3.setup_default_session(profile_name=credentials['profile_name'])
        s3 = boto3.client('s3')

        # setup AWS Lambda
        if use_lambda:
            # Create lambda client
            lambda_client = boto3.client('lambda')

            lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())

            # values for cost estimation
            MB_to_GB = 0.0009765625
            USD_per_GBsec = 0.0000166667

            # only do logging if logs_only arg passed
            if dict_key_args['logs_only'] != '':
                curr_job_logs = json.load(open(dict_key_args['logs_only']))
                start_time = 0
                num_jobs = 0
                for j_id, job in curr_job_logs['Jobs'].items():
                    if job['end']:
                        num_jobs += 1
                num_jobs = curr_job_logs['Number of Lambda Jobs'] - num_jobs
                # unsure if this works in all cases (i.e. when Cost Information is not an empty dictionary)
                if curr_job_logs['Cost Information'] == {}:
                    curr_job_logs['Cost Information'] = defaultdict(float)
                job_logs = aws_utils.lambda_logging(curr_job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method, dict_key_args['log_name'], main_path)
                sys.exit()
                
            # job information
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

            # get ECR image info
            ecr_client = boto3.client('ecr')

            repo_name_and_tag = image_uri.split('/')[-1]
            repo_name, image_tag = repo_name_and_tag.split(':')

            repo_images = ecr_client.list_images(repositoryName=repo_name)
            image_ids = ''
            for image in repo_images['imageIds']:
                if 'imageTag' in image and image['imageTag'] == image_tag:
                    image_ids = image
            image_info = ecr_client.describe_images(repositoryName=repo_name, imageIds=[image_ids])
            image_push_time = image_info['imageDetails'][0]['imagePushedAt'].astimezone(tz=timezone.utc)
            image_push_time = datetime.strftime(image_push_time, format='%Y-%m-%dT%H:%M:%S')

            # get functions that need to be created, and updated
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
                # Get field time steps and field files
                if product_type == 'latlon':
                    curr_grouping = groupings_for_latlon_datasets[grouping_to_process]
                elif product_type == 'native':
                    curr_grouping = groupings_for_native_datasets[grouping_to_process]

                fields = curr_grouping['fields'].split(', ')
                fields = [f.strip() for f in fields]
                dimension = curr_grouping['dimension']

                if product_type == 'latlon':
                    if dimension == '2D':
                        function_name = f'{function_name_prefix}_2D_latlon'
                    else:
                        function_name = f'{function_name_prefix}_3D_latlon'
                elif product_type == 'native':
                    if dimension == '2D':
                        function_name = f'{function_name_prefix}_2D_native'
                    else:
                        function_name = f'{function_name_prefix}_3D_native'
                
                # create lambda functions for jobs, or update it if it already exists
                if function_name not in all_functions:
                    memory_size = memory_sizes[function_name]
                    status = aws_utils.create_lambda_function(lambda_client, function_name, arn, memory_sizes[function_name], image_uri)
                    if status != 'SUCCESS':
                        print(status)
                        sys.exit()
                    all_functions.append(function_name)
                elif function_name in functions_to_update:
                    status = aws_utils.update_lambda_function(lambda_client, function_name, image_uri)
                    if status != 'SUCCESS':
                        print(status)
                        sys.exit()
                    functions_to_update.remove(function_name)
            print(f'\nAll necessary functions up to date!\n')


    # loop through all jobs and either process them locally
    # or invoke the created lambda function
    if process_data:
        num_jobs = 0
        for current_job in all_jobs:
            # Get field time steps and field files
            (grouping_to_process, product_type, output_freq_code, num_time_steps_to_process) = current_job
            if product_type == 'latlon':
                curr_grouping = groupings_for_latlon_datasets[grouping_to_process]
            elif product_type == 'native':
                curr_grouping = groupings_for_native_datasets[grouping_to_process]

            if output_freq_code == 'AVG_DAY':
                freq_folder = 'diags_daily'
                period_suffix = 'day_mean'

            elif output_freq_code == 'AVG_MON':
                freq_folder = 'diags_monthly'
                period_suffix = 'mon_mean'

            elif output_freq_code == 'SNAPSHOT':
                freq_folder = 'diags_inst'
                period_suffix = 'day_inst'
            else:
                print('valid options are AVG_DAY, AVG_MON, SNAPSHOT')
                print(f'you provided {output_freq_code}. Skipping job')
                continue

            fields = curr_grouping['fields'].split(', ')
            dimension = curr_grouping['dimension']
            filename = curr_grouping['filename']
            
            if not local:
                s3_dir_prefix = f'{source_bucket_folder_name}/{freq_folder}'
                
                file_time_steps, status = aws_utils.get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, 
                                                                source_bucket, num_time_steps_to_process)
                if status == 'SKIP':
                    print(f'--- Skipping job:\n\tgrouping: {grouping_to_process}\n\tproduct_type: {product_type}\n\toutput_freq_code: {output_freq_code}\n\num_time_steps_to_process: {num_time_steps_to_process}')
                    continue
                else:
                    field_files, field_time_steps, time_steps = file_time_steps
            else:
                fields = curr_grouping['fields'].split(', ')
                field_files = {}
                field_time_steps = {}
                all_time_steps_all_vars = []
                for field in fields:
                    field_files[field] = sorted(glob.glob(f'{product_generation_config["model_output_dir"]}/{product_generation_config["model_output_dir_folder_name"]}/{freq_folder}/{field}_{period_suffix}/*.data'))
                    time_steps = [key.split('.')[-2] for key in field_files[field]]
                    if num_time_steps_to_process == 'all':
                        field_time_steps[field] = sorted(time_steps)
                        all_time_steps_all_vars.extend(time_steps)
                    elif num_time_steps_to_process > 0:
                        time_steps = sorted(time_steps)[:num_time_steps_to_process]
                        field_files[field] = sorted(field_files[field])[:num_time_steps_to_process]
                        field_time_steps[field] = time_steps
                    else:
                        print(f'Bad time steps provided ("{num_time_steps_to_process}"). Skipping job.')
                        print(f'--- Skipping job:\n\tgrouping: {grouping_to_process}\n\tproduct_type: {product_type}\n\toutput_freq_code: {output_freq_code}\n\tnum_time_steps_to_process: {num_time_steps_to_process}')
                        continue
                    all_time_steps_all_vars.extend(time_steps)
                    
                # check that each field has the same number of times
                time_steps = sorted(list(set(all_time_steps_all_vars)))
                skip_job = False
                for field in fields:
                    if time_steps == field_time_steps[field]:
                        continue
                    else:
                        print(f'Unequal time steps for field "{field}". Skipping job')
                        skip_job = True
                if skip_job:
                    continue

            # **********
            # CREATE LAMBDA REQUEST FOR EACH "JOB"
            # **********
            if use_lambda:
                num_jobs += aws_utils.invoke_lambda(lambda_client, job_logs, time_steps, dict_key_args, product_generation_config, aws_config_metadata, current_job, function_name_prefix, dimension, field_files, credentials, num_jobs, debug_mode)
                print()
            else:
                # Call local generate_netcdfs function
                # Note: You can update this to utilize parallel processing
                # if you mimic the lambda functionality of batches and creating
                # separate payloads and function calls for each batch.
                payload = {
                    'grouping_to_process': grouping_to_process,
                    'product_type': product_type,
                    'output_freq_code': output_freq_code,
                    'time_steps_to_process': time_steps,
                    'field_files': field_files,
                    'product_generation_config': product_generation_config,
                    'aws_metadata': aws_config_metadata,
                    'debug_mode': debug_mode,
                    'local': local,
                    'use_lambda': use_lambda,
                    'credentials': credentials,
                    'use_workers_to_download': product_generation_config['use_workers_to_download']
                }

                generate_netcdfs(payload)
        
        # Lambda logging ==========================================================================
        if use_lambda and dict_key_args['enable_logging']:
            # Call function to process lambda logs until all jobs are finished
            job_logs = aws_utils.lambda_logging(job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method, dict_key_args['log_name'], main_path)

            if aws_config_metadata['num_retry'] > 0:
                if len(job_logs['Timesteps failed']) > 0:
                    num_jobs = 0
                    for retry_num in range(aws_config_metadata['num_retry']):
                        lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())
                        start_time = int(time.time()/ms_to_sec)

                        retry_job_logs = {}
                        retry_job_logs['Master Script Total Time (s)'] = 0
                        retry_job_logs['Cost Information'] = defaultdict(float)
                        retry_job_logs['Number of Lambda Jobs'] = 0
                        if dict_key_args['include_all_timesteps']:
                            retry_job_logs['All timesteps'] = []
                            retry_job_logs['Timesteps submitted'] = []
                        retry_job_logs['Timesteps failed'] = []
                        retry_job_logs['Jobs'] = {}

                        retry_jobs = defaultdict(list)
                        for ts_failed in job_logs['Timesteps failed']:
                            ts_failed = ts_failed.strip().split(' ')
                            failed_job = ts_failed[1:-1]
                            failed_job[0] = int(failed_job[0])
                            retry_jobs[str(failed_job)].append(ts_failed[0])

                        for job, ts in retry_jobs.items():
                            current_job = ast.literal_eval(job)
                            current_job.append(ts)
                            (grouping_to_process, product_type, output_freq_code, _) = current_job

                            if product_type == 'latlon':
                                curr_grouping = groupings_for_latlon_datasets[grouping_to_process]
                            elif product_type == 'native':
                                curr_grouping = groupings_for_native_datasets[grouping_to_process]

                            if output_freq_code == 'AVG_DAY':
                                freq_folder = 'diags_daily'
                                period_suffix = 'day_mean'

                            elif output_freq_code == 'AVG_MON':
                                freq_folder = 'diags_monthly'
                                period_suffix = 'mon_mean'

                            elif output_freq_code == 'SNAPSHOT':
                                freq_folder = 'diags_inst'
                                period_suffix = 'day_inst'
                            else:
                                print('valid options are AVG_DAY, AVG_MON, SNAPSHOT')
                                print(f'you provided {output_freq_code}. Skipping job')
                                continue

                            fields = curr_grouping['fields'].split(', ')
                            dimension = curr_grouping['dimension']
                            filename = curr_grouping['filename']
                        
                            s3_dir_prefix = f'{source_bucket_folder_name}/{freq_folder}'
                
                            file_time_steps, status = aws_utils.get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, 
                                                                            source_bucket, num_time_steps_to_process)
                            if status == 'SKIP':
                                print(f'--- Skipping job:\n\tgrouping: {grouping_to_process}\n\tproduct_type: {product_type}\n\toutput_freq_code: {output_freq_code}\n\num_time_steps_to_process: {num_time_steps_to_process}')
                                continue
                            else:
                                field_files, field_time_steps, time_steps = file_time_steps

                            num_jobs += aws_utils.invoke_lambda(lambda_client, retry_job_logs, time_steps, dict_key_args, product_generation_config, aws_config_metadata, current_job, function_name_prefix, dimension, field_files, credentials, debug_mode)

                        job_logs = aws_utils.lambda_logging(retry_job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method, dict_key_args['log_name'], main_path, retry=retry_num)



            # Delete lambda function
            # for function_name in current_functions:
            #     if 'ecco_processing' in function_name:
            #         print(f'Deleting function: {function_name}')
                    # lambda_client.delete_function(FunctionName=function_name)
            

        # **********
        # TODO: Check output S3 bucket for data
        # **********

    master_total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
    if not use_lambda:
        print(f'\n=== PROCESSING COMPLETE ===')
    print(f'Master script total time: {master_total_time:.2f}s')
    print(f'=== EXECUTION COMPLETE ===')