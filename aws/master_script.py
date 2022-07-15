"""
Created May 18, 2022

Author: Duncan Bark

"""
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

# Local imports
sys.path.append(f'{Path(__file__).parent.resolve()}')
import aws_utils as aws_utils
import ecco_cloud_utils as ea
import create_factors_utils as create_factors_utils
from eccov4r4_gen_for_podaac_cloud import generate_netcdfs

def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--upload_model_to_S3', default=False, action='store_true',
                        help='Upload model output data to provided directory (or S3 bucket) in config file')

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
    return parser


if __name__ == "__main__":
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()
    dict_key_args = {key: value for key, value in args._get_kwargs()} 

    upload_to_S3 = dict_key_args['upload_model_to_S3']
    process_data = dict_key_args['process_data']
    debug_mode = dict_key_args['debug']
    local = not dict_key_args['use_cloud']
    use_lambda = dict_key_args['use_lambda']
    force_reconfigure = dict_key_args['force_reconfigure']

    # if use lambda, then local is False (only use S3 for data)
    if use_lambda:
        local = False

    # Load 'product_generation_config.yaml'
    product_generation_config = yaml.safe_load(open(Path(__file__).parent.resolve() / 'configs' / 'product_generation_config.yaml'))

    # Load directories (local vs AWS)
    # Default directories
    parent_dir = Path(__file__).parent.resolve()
    mapping_factors_dir_default = str(parent_dir / 'mapping_factors')
    diags_root_default = str(parent_dir / 'temp_model_output')
    metadata_default = str(parent_dir / 'metadata')
    ecco_grid_dir_default = str(parent_dir / 'ecco_grids')
    ecco_grid_dir_mds_default = str(parent_dir / 'ecco_grids')
    processed_output_dir_base_default = str(parent_dir / 'temp_output')

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

    if dict_key_args['create_factors']:
        # Creates mapping_factors (2D and 3D), landmask, latlon_grid, and sparse matrix files
        # Not needed unless changes have been made to the factors code and you need
        # to update the factors/mask in the lambda docker image
        status = create_factors_utils.create_all_factors(ea, product_generation_config, ['2D', '3D'], debug_mode=debug_mode, extra_prints=extra_prints)
        if status == -1:
            print('Error creating all factors. Exiting')
            sys.exit()
        
        print('\nCompleted creation of all factors. Exiting')
        sys.exit()


    # Get all configurations
    all_jobs = []
    with open(f'{Path(__file__).parent.resolve() / "configs" / "jobs.txt"}', 'r') as j:
        # /Users/bark/Documents/ECCO_GROUP/ECCO-Dataset-Production/aws/configs/jobs.txt
        for line in j:
            line = line.strip()
            if '#' in line or line == '':
                continue
            
            if line == 'done':
                break

            line_vals = line.split(',')
            if line_vals[3] == 'all':
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], line_vals[3]])
            else:
                all_jobs.append([int(line_vals[0]), line_vals[1], line_vals[2], int(line_vals[3])])

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

    # Setup all AWS required variables and information
    # Includes authentication, lambda function creation
    credentials = {}
    aws_config_metadata = {}
    if not local:
        # Load 'aws_config.yaml'
        aws_config_metadata = yaml.safe_load(open(Path(__file__).parent.resolve() / 'configs' / 'aws_config.yaml'))

        # AWS defaults
        aws_profile_name_default = 'saml-pub'
        aws_region_default = 'us-west-2'
        model_granule_bucket_default = 'ecco-model-granules'
        processed_data_bucket_default = 'ecco-processed-data'
        if aws_config_metadata['credential_method_type'] == 'bash':
            aws_credential_path_default = './update_AWS_cred_ecco_production.sh'
        elif aws_config_metadata['credential_method_type'] == 'binary':
            if 'linux' in platform.platform().lower():
                aws_credential_path_default = './aws-login.linux.amd64'
            else:
                aws_credential_path_default = 'aws-login.darwin.amd64'

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

        time_2D_latlon = aws_config_metadata['latlon_2D_time']
        time_2D_native = aws_config_metadata['native_2D_time']
        time_3D_latlon = aws_config_metadata['latlon_3D_time']
        time_3D_native = aws_config_metadata['native_3D_time']

        memory_sizes = {
            f'{function_name_prefix}_2D_latlon': aws_config_metadata['memory_size_2D_latlon'],
            f'{function_name_prefix}_2D_native': aws_config_metadata['memory_size_2D_native'],
            f'{function_name_prefix}_3D_latlon': aws_config_metadata['memory_size_3D_latlon'],
            f'{function_name_prefix}_3D_native': aws_config_metadata['memory_size_3D_native']
        }

        number_of_batches_to_process = aws_config_metadata['number_of_batches_to_process']

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

        # Upload data to S3 bucket
        if upload_to_S3:
            status = aws_utils.upload_S3(s3, product_generation_config['model_output_dir'], product_generation_config['model_output_dir_folder_name'], aws_config_metadata['source_bucket'])
            if not status:
                print(f'Uploading to S3 failed. Exiting')
                sys.exit()

        # setup AWS Lambda
        if use_lambda:
            # Create lambda client
            lambda_client = boto3.client('lambda')

            lambda_start_time = time.strftime('%Y%m%d:%H%M%S', time.localtime())

            # values for cost estimation
            ms_to_sec = 0.001
            MB_to_GB = 0.0009765625
            USD_per_GBsec = 0.0000166667

            # job information
            num_jobs = 0
            job_logs = {}
            job_logs['Master Script Total Time (s)'] = 0
            job_logs['Cost Information'] = defaultdict(float)
            job_logs['Number of Lambda Jobs'] = 0
            if dict_key_args['include_all_timesteps']:
                job_logs['All timesteps'] = []
                job_logs['Timesteps submitted'] = []
            job_logs['Timesteps failed'] = []
            job_logs['Jobs'] = {}

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
                
                # create lambda functions for jobs
                # get current functions
                current_functions = [f['FunctionName'] for f in lambda_client.list_functions()['Functions']]
                if function_name not in current_functions:
                    memory_size = memory_sizes[function_name]
                    aws_utils.create_lambda_function(lambda_client, function_name, arn, memory_sizes[function_name], image_uri)
                    current_functions.append(function_name)
    
            start_time = int(time.time()/ms_to_sec)

    # loop through all jobs and either process them locally
    # or invoke the created lambda function
    if process_data:
        request_ids = []
        for (grouping_to_process, product_type, output_freq_code, num_time_steps_to_process) in all_jobs:      
            # Get field time steps and field files
            if product_type == 'latlon':
                curr_grouping = groupings_for_latlon_datasets[grouping_to_process]
            elif product_type == 'native':
                curr_grouping = groupings_for_native_datasets[grouping_to_process]

            fields = curr_grouping['fields'].split(', ')
            dimension = curr_grouping['dimension']
            filename = curr_grouping['filename']

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

            if not local:
                s3_dir_prefix = f'{source_bucket_folder_name}/{freq_folder}'
                
                file_time_steps = aws_utils.get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, 
                                                                source_bucket, num_time_steps_to_process)
                if file_time_steps == -1:
                    print(f'--- Skipping job:\n\tgrouping: {grouping_to_process}\n\tproduct_type: {product_type}\n\toutput_freq_code: {output_freq_code}\n\tnum_time_steps_to_process: {num_time_steps_to_process}')
                    continue
                else:
                    field_files, field_time_steps, all_time_steps_all_vars = file_time_steps
            else:
                field_files = {}
                field_time_steps = {}
                all_time_steps_all_vars = []
                for field in fields:
                    field_files[field] = sorted(glob.glob(f'{product_generation_config["model_output_dir"]}/{freq_folder}/{field}_{period_suffix}/*.data'))
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
            all_time_steps = sorted(list(set(all_time_steps_all_vars)))
            skip_job = False
            for field in fields:
                if all_time_steps == field_time_steps[field]:
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
                if dict_key_args['include_all_timesteps']:
                    job_logs['All timesteps'].extend(all_time_steps)

                # group number of time steps and files to process based on time to execute
                if product_type == 'latlon':
                    if dimension == '2D':
                        exec_time_per_vl = time_2D_latlon
                        function_name = f'{function_name_prefix}_2D_latlon'
                    else:
                        exec_time_per_vl = time_3D_latlon
                        function_name = f'{function_name_prefix}_3D_latlon'
                elif product_type == 'native':
                    if dimension == '2D':
                        exec_time_per_vl = time_2D_native
                        function_name = f'{function_name_prefix}_2D_native'
                    else:
                        exec_time_per_vl = time_3D_native
                        function_name = f'{function_name_prefix}_3D_native'

                # NEW TECHNIQUE (USING TIME VALUES)
                max_execs = 0
                max_time = 900 #s
                total_vl_possible = int(max_time / exec_time_per_vl)
                num_vl = 1
                if dimension == '3D':
                    num_vl = product_generation_config['num_vertical_levels']
                max_execs = int(total_vl_possible / (len(fields) * num_vl))

                # If max_execs is 0, then the number of field (and vertical levels) per time step is too high and will cause
                # the lambda job to exceed it's 15min time limit. Work may be required to improve the speed of the algorithms
                # or to process the problematic timesteps separately.
                if max_execs == 0:
                    print(f'Max execs is 0, this means you cannot process a single vertical level in less than 15 minutes. Skipping job')
                    continue

                # If override_max_execs is given, use the lower value between the calculated max_execs
                # and the provided override_max_execs.
                if aws_config_metadata['override_max_execs'] != 0:
                    max_execs = min([max_execs, aws_config_metadata['override_max_execs']])

                # Split all_time_steps into groups with length=max_execs (the final batch will have any left over)
                # ex. all_time_steps = [1, 2, 3, 4, 5, 6, 7], max_execs=2
                #     time_steps_by_batch = [[1, 2], [3, 4], [5, 6], [7]]
                time_steps_by_batch = [all_time_steps[x:x+max_execs] for x in range(0,len(all_time_steps), max_execs)]
                number_of_batches = len(time_steps_by_batch)

                # For each field, split the field_files into groups with length=max_execs (in the same fashion as the time steps)
                # Due to there being multiple fields per batch, the batches are assigned a number which is used as the key to the 
                # dictionary containing the fields and their files for the corresponding batch number
                # ex. field_files = {'SSH':['file1', 'file2', 'file3'], 'SSHIBC':['file1', 'file2', 'file3']}, max_execs = 2
                #     field_files_by_batch = {1:{['SSH':['file1', 'file2']], 'SSHIBC':['file1', 'file2']}, 2:{['SSH':['file3']], 'SSHIBC':['file3']}}
                field_files_by_batch = {}
                for field in fields:
                    batched_field_files = [field_files[field][x:x+max_execs] for x in range(0, len(all_time_steps), max_execs)]
                    for batch_number, batch_field_files in enumerate(batched_field_files):
                        if batch_number not in field_files_by_batch.keys():
                            field_files_by_batch[batch_number] = {}
                        field_files_by_batch[batch_number][field] = batch_field_files

                number_of_batches = min([number_of_batches_to_process, number_of_batches])
                print(f'Job information -- {grouping_to_process}, {product_type}, {output_freq_code}, {num_time_steps_to_process}')
                print(f'Number of batches: {number_of_batches}')
                print(f'Number of time steps per batch: {len(field_files_by_batch[0][fields[0]])}')
                print(f'Length of final batch: {len(field_files_by_batch[number_of_batches-1][fields[0]])}')
                print(f'Total number of time steps to process: {len(all_time_steps)}')

                if dict_key_args['require_input']:
                    create_lambdas = input(f'Would like to start executing the lambda jobs (y/n)?\t').lower()
                    if create_lambdas != 'y':
                        print('Skipping job')
                        continue
                    print()
                    
                for i in range(number_of_batches):
                    # create payload for current lambda job
                    payload = {
                        'grouping_to_process': grouping_to_process,
                        'product_type': product_type,
                        'output_freq_code': output_freq_code,
                        'time_steps_to_process': time_steps_by_batch[i],
                        'field_files': field_files_by_batch[i],
                        'product_generation_config': product_generation_config,
                        'aws_metadata': aws_config_metadata,
                        'debug_mode': debug_mode,
                        'local': local,
                        'use_lambda': use_lambda,
                        'credentials': credentials,
                        'processing_code_filename': product_generation_config['processing_code_filename'],
                        'use_workers_to_download': product_generation_config['use_workers_to_download']
                    }

                    data_to_process= {
                        'grouping_to_process': grouping_to_process,
                        'product_type': product_type,
                        'output_freq_code': output_freq_code,
                        'dimension': dimension,
                        'time_steps_to_process': time_steps_by_batch[i]
                    }

                    # invoke lambda job
                    try:
                        if use_lambda:
                            print(f'Lambda Job requested: {num_jobs+1:4}', end='\r')
                            invoke_response = lambda_client.invoke(
                                FunctionName=function_name,
                                InvocationType='Event',
                                Payload=json.dumps(payload),   
                            )

                            request_id = invoke_response['ResponseMetadata']['RequestId'].strip()
                            job_logs['Jobs'][request_id] = {
                                'date':invoke_response['ResponseMetadata']['HTTPHeaders']['date'], 
                                'status': invoke_response['StatusCode'], 
                                'data': data_to_process, 
                                'report': {}, 
                                'start': True,
                                'end': False,
                                'success': False,
                                'any_failed': False,
                                'timesteps_failed': [],
                                'error': {}
                            }
                            request_ids.append(request_id)

                            if dict_key_args['include_all_timesteps']:
                                job_logs['Timesteps submitted'].extend(time_steps_by_batch[i])
                    
                            num_jobs += 1
                    except Exception as e:
                        print(f'Lambda invoke error: {e}')
                        print(f'\tTime Steps: {time_steps}')
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
                    'time_steps_to_process': all_time_steps,
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
        if use_lambda:
            # Call function to process lambda logs until all jobs are finished
            aws_utils.lambda_logging(job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method)

            # Delete lambda function
            for function_name in current_functions:
                if 'ecco_processing' in function_name:
                    print(f'Deleting function: {function_name}')
                    lambda_client.delete_function(FunctionName=function_name)

        # **********
        # TODO: Check output S3 bucket for data
        # **********