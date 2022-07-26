import os
import ast
import sys
import copy
import glob
import time
import json
import boto3
import subprocess
from pathlib import Path
from concurrent import futures
from collections import defaultdict




def calculate_all_jobs(latlon_groupings, native_groupings):
    all_groupings = {'latlon':latlon_groupings, 'native':native_groupings}
    jobs = defaultdict(list)
    for product_type, groupings in all_groupings.items():
        for i, grouping in enumerate(groupings):
            freqs = grouping['frequency'].split(', ')
            for freq in freqs:
                if grouping['dimension'] == '2D':
                    jobs[f'2D_{product_type}'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '3D':
                    jobs[f'3D_{product_type}'].append([i, product_type, freq, 'all'])

    all_jobs = []
    all_jobs.extend(jobs['3D_native'])
    all_jobs.extend(jobs['3D_latlon'])
    all_jobs.extend(jobs['2D_native'])
    all_jobs.extend(jobs['2D_latlon'])

    return all_jobs





# ==========================================================================================================================
# S3 and FILES
# ==========================================================================================================================
def get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, source_bucket, time_steps_to_process):
    """Create lists of files and timesteps for each field from files present on S3

    Args:
        s3 (botocore.client.S3): Boto3 S3 client initalized with necessary credentials
        fields (list): List of field names
        s3_dir_prefix (str): Prefix of files stored on S3 (i.e. 'V4r4/diags_monthly)
        period_suffix (str): Period suffix of files (i.e. 'mon_mean')
        source_bucket (str): Name of S3 bucket
        time_steps_to_process (str/int/list): String 'all', an integer specifing the number of time
                                                steps, or a list of time steps to process

    Returns:
        ()
    """
    status = 'SUCCESS'

    # time_steps_to_process must either be the string 'all', a number corresponding to the total number
    # of time steps to process over all jobs (if using lambda), or overall (when local), or a list of timesteps
    if time_steps_to_process != 'all' and not isinstance(time_steps_to_process, int) and not isinstance(time_steps_to_process, list):
        print(f'Bad time steps provided ("{time_steps_to_process}"). Skipping job.')
        return -1

    print(f'\nGetting timesteps and files for fields: {fields} for {time_steps_to_process} {period_suffix} timesteps')

    # Construct the list of field paths
    # i.e. ['aws/temp_model_output/SSH', 'aws/temp_model_output/SSHIBC', ...]
    s3_field_paths = []
    for field in fields:
        s3_field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(source_bucket)
    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    time_steps_all_vars = []

    # NEW THREADED TECHNIQUE
    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    time_steps_all_vars = []
    if True:
        num_workers = len(s3_field_paths)
        print(f'Using {num_workers} workers to get time steps and files for {len(fields)} fields')

        # get files function
        def fetch(s3_field_path):
            field = s3_field_path.split('/')[-1].split(period_suffix)[0][:-1]
            num_time_steps = 0
            # loop through all the objects in the source_bucket with a prefix matching the s3_field_path
            for obj in bucket.objects.filter(Prefix=s3_field_path):
                obj_key = obj.key
                file_timestep = obj_key.split('.')[-2]
                if isinstance(time_steps_to_process, list) and file_timestep not in time_steps_to_process:
                    continue
                if num_time_steps == time_steps_to_process:
                    break
                if isinstance(time_steps_to_process, list) and num_time_steps == len(time_steps_to_process):
                    break
                if '.meta' in obj_key:
                    continue
                field_files[field].append(obj_key)
                field_time_steps[field].append(obj_key.split('.')[-2])
                time_steps_all_vars.append(obj_key.split('.')[-2])
                num_time_steps += 1
            field_files[field] = sorted(field_files[field])
            field_time_steps[field] = sorted(field_time_steps[field])
            return field

        # create workers and assign each one a field to look for times and files for
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_key = {executor.submit(fetch, path) for path in s3_field_paths}

            for future in futures.as_completed(future_to_key):
                field = future.result()
                exception = future.exception()
                if exception:
                    print(f'ERROR getting field times/files: {field} ({exception})')
                else:
                    print(f'Got times/files: {field}')

        time_steps_all_vars = sorted(time_steps_all_vars)

    # check that each field has the same number of times
    time_steps = sorted(list(set(time_steps_all_vars)))
    skip_job = False
    for field in fields:
        if time_steps == field_time_steps[field]:
            continue
        else:
            print(f'Unequal time steps for field "{field}". Skipping job')
            skip_job = True
    if skip_job:
        status = 'SKIP'

    return ((field_files, field_time_steps, time_steps), status)


# TODO: Remove and replace with updated script Ian and Duncan developed
def upload_S3(s3, source_path, source_path_folder_name, bucket, check_list=True):
    # Upload provided file to the provided bucket via the provided credentials.

    # Collect list of files within source_path
    data_files = glob.glob(f'{source_path}/**/*.data', recursive=True)
    num_files = len(data_files)

    # Collect files currently on the S3 bucket
    # If, when uploading, the name exists in this list, skip it.
    files_on_s3 = []
    if check_list:
        response = s3.list_objects(Bucket=bucket)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f'Unable to collect objects in bucket {bucket}')
            return -1
        else:
            files_on_s3 = [k['Key'] for k in response['Contents']]

    # Upload photos from source_path to S3 bucket
    upload = input(f'About to upload {len(data_files)} files, from {source_path}, to bucket {bucket}. Continue? y/n \n')
    if upload.strip().lower() == 'y':
        print('\nUploading files')
        for i, data_file in enumerate(data_files):
            print(f'\t{i+1:7} / {num_files}', end='\r')
            name = f'{source_path_folder_name}/{data_file.split(f"/{source_path_folder_name}/")[-1]}'
            if name in files_on_s3:
                continue
            try:
                response = s3.upload_file(data_file, bucket, name)
                print(f'Uploaded {data_file} to bucket {bucket}')
            except:
                print(f'Unable to upload file {data_file} to bucket {bucket}')
        print()

    return 1


# ==========================================================================================================================
# LAMBDA FUNCTION CREATION and UPDATING
# ==========================================================================================================================
def update_lambda_function(client, function_name, image_uri):
    status = 'SUCCESS'

    # Update lambda_function with current image_uri
    print(f'\nUpdating lambda function ({function_name})')
    try:
        client.update_function_code(
            FunctionName=function_name,
            ImageUri=image_uri
        )
    except Exception as e:
        status = f'ERROR updating lambda function ({function_name})\n\terror: {e}'
        return status

    print(f'Verifying lambda function update ({function_name})...')
    while True:
        last_update_status = client.get_function_configuration(FunctionName=function_name)['LastUpdateStatus']
        if last_update_status == "Failed":
            status = f'\tFailed to update function ({function_name}). Try again'
            return status
        elif last_update_status == 'Successful':
            print(f'\tFunction updated successfully')
            break
        time.sleep(2)
    return status


def create_lambda_function(client, function_name, role, memory_size, image_uri):
    status = 'SUCCESS'
    # Create lambda function using the provided values
    print(f'\nCreating lambda function ({function_name}) with {memory_size} MB of memory')
    try:
        client.create_function(
            FunctionName=function_name,
            Role=role,
            PackageType='Image',
            Code={'ImageUri':image_uri},
            Publish=True,
            Timeout=900,
            MemorySize=memory_size
        )
    except Exception as e:
        status = f'Failed to create function: {function_name}, error: {e}'
        return status

    print(f'Verifying lambda function creation ({function_name})...')
    while True:
        status = client.get_function_configuration(FunctionName=function_name)['State']
        if status == "Failed":
            status = f'\tFailed to create function ({function_name}). Try again'
            return status
        elif status == 'Active':
            print(f'\tFunction created successfully')
            break
        time.sleep(2)
    
    return status


def invoke_lambda(lambda_client, job_logs, time_steps, dict_key_args, product_generation_config, aws_config_metadata, current_job, function_name_prefix, dimension, field_files, credentials, debug_mode):
    (grouping_to_process, product_type, output_freq_code, time_steps_to_process) = current_job
    num_jobs = 0
    
    fields = list(field_files.keys())

    if dict_key_args['include_all_timesteps']:
        job_logs['All timesteps'].extend(time_steps)
    
    time_2D_latlon = aws_config_metadata['latlon_2D_time']
    time_2D_native = aws_config_metadata['native_2D_time']
    time_3D_latlon = aws_config_metadata['latlon_3D_time']
    time_3D_native = aws_config_metadata['native_3D_time']

    number_of_batches_to_process = aws_config_metadata['number_of_batches_to_process']

    use_lambda = dict_key_args['use_lambda']


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
        # continue
        return num_jobs

    # If override_max_execs is given, use the lower value between the calculated max_execs
    # and the provided override_max_execs.
    if aws_config_metadata['override_max_execs'] != 0:
        max_execs = min([max_execs, aws_config_metadata['override_max_execs']])

    # Split time_steps into groups with length=max_execs (the final batch will have any left over)
    # ex. time_steps = [1, 2, 3, 4, 5, 6, 7], max_execs=2
    #     time_steps_by_batch = [[1, 2], [3, 4], [5, 6], [7]]
    time_steps_by_batch = [time_steps[x:x+max_execs] for x in range(0,len(time_steps), max_execs)]
    number_of_batches = len(time_steps_by_batch)

    # For each field, split the field_files into groups with length=max_execs (in the same fashion as the time steps)
    # Due to there being multiple fields per batch, the batches are assigned a number which is used as the key to the 
    # dictionary containing the fields and their files for the corresponding batch number
    # ex. field_files = {'SSH':['file1', 'file2', 'file3'], 'SSHIBC':['file1', 'file2', 'file3']}, max_execs = 2
    #     field_files_by_batch = {1:{['SSH':['file1', 'file2']], 'SSHIBC':['file1', 'file2']}, 2:{['SSH':['file3']], 'SSHIBC':['file3']}}
    field_files_by_batch = {}
    for field in fields:
        batched_field_files = [field_files[field][x:x+max_execs] for x in range(0, len(time_steps), max_execs)]
        for batch_number, batch_field_files in enumerate(batched_field_files):
            if batch_number not in field_files_by_batch.keys():
                field_files_by_batch[batch_number] = {}
            field_files_by_batch[batch_number][field] = batch_field_files

    number_of_batches = min([number_of_batches_to_process, number_of_batches])
    print(f'Job information -- {grouping_to_process}, {product_type}, {output_freq_code}, {time_steps_to_process}')
    print(f'Number of batches: {number_of_batches}')
    print(f'Number of time steps per batch: {len(field_files_by_batch[0][fields[0]])}')
    print(f'Length of final batch: {len(field_files_by_batch[number_of_batches-1][fields[0]])}')
    print(f'Total number of time steps to process: {len(time_steps)}')

    if dict_key_args['require_input']:
        create_lambdas = input(f'Would like to start executing the lambda jobs (y/n)?\t').lower()
        if create_lambdas != 'y':
            print('Skipping job')
            return num_jobs
        print()

    if aws_config_metadata['use_workers_to_invoke']:
        num_workers = 10
        times_and_fields_all = list(zip(time_steps_by_batch, field_files_by_batch.values()))
        times_and_fields_all = times_and_fields_all[:number_of_batches]
        print(f'Using {num_workers} workers to invoke {number_of_batches} lambda jobs')

        # invoke lambda function
        def fetch(times_and_fields):
            time_steps, field_files = times_and_fields
            # create payload for current lambda job
            payload = {
                'grouping_to_process': grouping_to_process,
                'product_type': product_type,
                'output_freq_code': output_freq_code,
                'time_steps_to_process': time_steps,
                'field_files': field_files,
                'product_generation_config': product_generation_config,
                'aws_metadata': aws_config_metadata,
                'debug_mode': debug_mode,
                'local': False,
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
                'number_of_time_steps': len(time_steps),
                'time_steps_to_process': time_steps
            }

            # invoke lambda job
            try:
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
                    'timeout': False,
                    'success': False,
                    'any_failed': False,
                    'timesteps_failed': [],
                    'timesteps_succeeded': {},
                    'error': {}
                }

                if dict_key_args['include_all_timesteps']:
                    job_logs['Timesteps submitted'].extend(time_steps_by_batch[i])
            except Exception as e:
                print(f'Lambda invoke error: {e}')
                print(f'\tTime Steps: {time_steps}')
                return (times_and_fields, 0)
            return (times_and_fields, 1)

        # create workers and assign each one a field to look for times and files for
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_key = {executor.submit(fetch, times_and_fields) for times_and_fields in times_and_fields_all}

            for future in futures.as_completed(future_to_key):
                job = future.result()
                exception = future.exception()
                num_jobs += job[1]
                print(f'Lambda Job requested: {num_jobs:4}', end='\r')
                if exception:
                    print(f'ERROR invoking lambda job: {job[0]}, ({exception})')
    else:   
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
                'local': False,
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
                'number_of_time_steps': len(time_steps_by_batch[i]),
                'time_steps_to_process': time_steps_by_batch[i]
            }

            # invoke lambda job
            try:
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
                    'timeout': False,
                    'success': False,
                    'any_failed': False,
                    'timesteps_failed': [],
                    'timesteps_succeeded': {},
                    'error': {}
                }

                if dict_key_args['include_all_timesteps']:
                    job_logs['Timesteps submitted'].extend(time_steps_by_batch[i])
        
                num_jobs += 1
            except Exception as e:
                print(f'Lambda invoke error: {e}')
                print(f'\tTime Steps: {time_steps}')
    
    return num_jobs


# ==========================================================================================================================
# CREDENTIALS
# ==========================================================================================================================
def get_credentials_helper():
    # Get credentials for AWS from "~/.aws/credentials" file
    cred_path = Path.home() / '.aws/credentials'
    credentials = {}
    if cred_path.exists():
        with open(cred_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line == '':
                    break
                elif line[0] == '#':
                    credentials['expiration_date'] = line.split(' = ')[-1]
                elif line[0] == '[':
                    credentials['profile_name'] = line[1:-1]
                else:
                    name, value = line.split(' = ')
                    credentials[name] = value
    return credentials


def get_aws_credentials(credential_method): 
    # credential method is a dict. with the 'region' and 'type'
    # type is one of binary or bash 
    # if binary then aws_credential_path needs to point to the binary file
    # if bash then aws_credential_path needs to point to the bash script
    aws_region = credential_method['region'] 
    aws_credential_path = credential_method['aws_credential_path']
    try:
        if credential_method['type'] == 'binary':
           subprocess.run([aws_credential_path, '-r', f'{aws_region}'], check=True)
        elif credential_method['type'] == 'bash':
           subprocess.run([aws_credential_path], check=True)

        credentials = get_credentials_helper()
    except:
        print(f'Unable to run script to get credentials ("{aws_credential_path}"). Exiting')
        sys.exit()

    return credentials


# ==========================================================================================================================
# LOGGING UTILS
# ==========================================================================================================================
def get_logs(log_client, log_group_names, log_stream_names, start_time=0, end_time=0, filter_pattern='', type=''):
    try:
        if type == 'event':
            ret_logs = defaultdict(list)
            for log_group_name in log_group_names:
                log_stream_ctr = 0
                total_logs_checked = 0
                if len(log_stream_names[log_group_name]) == 0:
                    ret_logs[log_group_name] = []
                    continue
                mod_log_stream_names = log_stream_names[log_group_name]
                while True:
                    if len(log_stream_names[log_group_name]) > 100:
                        mod_log_stream_names = log_stream_names[log_group_name][log_stream_ctr*100:log_stream_ctr*100 + 100]
                    total_logs_checked += len(mod_log_stream_names)
                    events_current = log_client.filter_log_events(logGroupName=log_group_name, logStreamNames=mod_log_stream_names, filterPattern=filter_pattern, startTime=start_time, endTime=end_time)
                    time.sleep(0.11) # AWS limits FilterLogEvents to 10 requests per second in US West. This ensures 10 requests dont occur in a second.
                    ret_logs[log_group_name].extend(events_current['events'])
                    while True:
                        if 'nextToken' in events_current.keys():
                            events_current = log_client.filter_log_events(logGroupName=log_group_name, logStreamNames=mod_log_stream_names, filterPattern=filter_pattern, nextToken=events_current['nextToken'])
                            time.sleep(0.11) # AWS limits FilterLogEvents to 10 requests per second in US West. This ensures 10 requests dont occur in a second.
                            if events_current['events'] != []:
                                ret_logs[log_group_name].extend(events_current['events'])
                        else:
                            break

                    if total_logs_checked == len(log_stream_names[log_group_name]):
                        break 
                    else:
                        log_stream_ctr += 1
        elif type == 'logStream':
            ret_logs = defaultdict(list)
            for log_group_name in log_group_names:
                log_streams_current = log_client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime')
                time.sleep(0.21) # AWS limits DescribeLogStreams to 5 requests per second in US West. This ensures 5 requests dont occur in a second.
                ret_logs[log_group_name] = log_streams_current['logStreams']
                while True:
                    if 'nextToken' in log_streams_current.keys():
                        log_streams_current = log_client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', nextToken=log_streams_current['nextToken'])
                        time.sleep(0.21) # AWS limits DescribeLogStreams to 5 requests per second in US West. This ensures 5 requests dont occur in a second.
                        if log_streams_current['logStreams'] != []:
                            ret_logs[log_group_name].extend(log_streams_current['logStreams'])
                    else:
                        break
    except Exception as e:
        print('Error accessing logs: ')
        print(e)
    return ret_logs


def save_logs(job_logs, MB_to_GB, estimated_jobs, start_time, ctr, log_name, fn_extra=''):
    try:
        for job_id, job_content in job_logs['Jobs'].items():
            if job_id not in estimated_jobs:
                if ('INITIAL' not in fn_extra) and (job_content['end']):
                    estimated_jobs.append(job_id)
                
                job_report = job_content['report']
                if job_report != {}:
                    request_duration_time = job_report["Duration (s)"]
                    request_time = job_report["Billed Duration (s)"]
                    request_memory = job_report["Memory Size (MB)"]
                    cost_estimate = job_report["Cost Estimate (USD)"]
                    job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Time (s)'] += request_duration_time
                    job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Billed Time (s)'] += request_time
                    job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total GB*s'] += (request_memory * MB_to_GB * request_time)
                    job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Cost (USD)'] += cost_estimate
                    job_logs['Cost Information']['Total Cost'] += cost_estimate
                if 'timesteps_failed' not in job_content:
                    job_content['timesteps_failed'] = []
                for ts_fail in job_content['timesteps_failed']:
                    grouping = job_content["data"]["grouping_to_process"]
                    product_type = job_content["data"]["product_type"]
                    freq = job_content["data"]["output_freq_code"]
                    dimension = job_content["data"]["dimension"]
                    ts_fail_info = f'{ts_fail} {grouping} {product_type} {freq} {dimension}'
                    job_logs['Timesteps failed'].append(ts_fail_info)
        job_logs['Timesteps failed'] = sorted(job_logs['Timesteps failed'])

        if fn_extra != '' and fn_extra[0] != '_':
            fn_extra = f'_{fn_extra}'
        if log_name == '':
            log_name = time.strftime('%Y%m%d:%H%M%S', time.localtime())
        with open(f'./logs/job_logs_{start_time}_{ctr}_{log_name}{fn_extra}.json', 'w') as f:
            json.dump(job_logs, f, indent=4)
    except Exception as e:
        print('Error saving logs: ')
        print(e)
    
    return job_logs, estimated_jobs


def lambda_logging(job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method, log_name, retry=-1):
    print(f'\n=== LOGGING START ===')
    delete_logs = True
    # key_filter = '?"START " ?"END " ?"REPORT " ?Task'
    # extra_filter = '?PARTIAL ?SUCCESS ?DURATION ?FILES ?ERROR ?FAILED ?TIMEOUT'
    log_filter = '?"[INFO]" ?"REPORT "'
    duration_keys = ['ALL', 'IMPORT', 'RUN', 'TOTAL', 'SCRIPT', 'IO', 'DOWNLOAD', 'NETCDF', 'UPLOAD']
    log_client = boto3.client('logs')
    num_jobs_ended = 0
    estimated_jobs = []
    ctr = -1
    
    # intital log
    if ctr == -1:
        ctr += 1
        total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
        job_logs['Master Script Total Time (s)'] = total_time
        job_logs['Number of Lambda Jobs'] = num_jobs
        if retry != -1:
            fn_extra = f'INITIAL_RETRY_{retry}'
        else:
            fn_extra = 'INITIAL'
        job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, log_name, fn_extra=fn_extra)
        previous_time = time.time()

    completed_request_ids = []
    deleted_stream_names = defaultdict(list)
    completed_stream_names = defaultdict(list)
    log_stream_request_ids = defaultdict(list)

    previous_num_complete = 0
    credentials_time = time.time() 
    logging_time = time.time()
    while True:
        try:
            log_group_names = [lg['logGroupName'] for lg in log_client.describe_log_groups()['logGroups'] if 'ecco_processing' in lg['logGroupName']]
            # every 10 minutes, refresh the credentials
            if (time.time() - credentials_time) > 600.0:
                print(f'... time elapsed {time.time() - credentials_time}s')
                print(f'... getting new credentials!') 
                _ = get_aws_credentials(credential_method)
                credentials_time = time.time()

            print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
            time.sleep(30)
            end_time = int(time.time()/ms_to_sec)
            request_id_count = defaultdict(int)
            
            log_stream_names = defaultdict(list)
            total_num_log_streams = 0
            print(f'\tGetting log streams')
            log_streams = get_logs(log_client, log_group_names, [], type='logStream')
            for log_group_name, group_streams in log_streams.items():
                for ls in group_streams:
                    log_stream_name = ls['logStreamName']
                    if log_stream_name not in deleted_stream_names[log_group_name]:
                        log_stream_names[log_group_name].append(log_stream_name)
                        total_num_log_streams += 1

            if total_num_log_streams > 0:
                # Get all events in the logs that have "[INFO]" or "REPORT " in them. Only look at log streams
                # from the list "log_stream_names" and between "start_time" and "end_time"
                info_log_events = get_logs(log_client, log_group_names, log_stream_names, start_time, end_time, log_filter, 'event')

                # Loop through all [INFO] and REPORT events
                for log_group_name, info_logs in info_log_events.items():
                    for log in info_logs:
                        log_stream_name = log['logStreamName']
                        log_message = log['message'].strip().split('\t')

                        if 'REPORT' in log_message[0]:
                            log_time = log['timestamp']
                            request_id = log_message[0].split(' ')[-1]
                        else:
                            log_time = log_message[1]
                            request_id = log_message[2]

                        if request_id in completed_request_ids:
                            continue


                        if request_id not in log_stream_request_ids[log_stream_name]:
                            log_stream_request_ids[log_stream_name].append(request_id)


                        if request_id not in job_logs['Jobs'].keys():
                            job_logs['Jobs'][request_id] = {}
                        if 'extra' not in job_logs['Jobs'][request_id].keys():
                            job_logs['Jobs'][request_id]['extra'] = {}
                        if 'Duration (s)' not in job_logs['Jobs'][request_id]['extra'].keys():
                            job_logs['Jobs'][request_id]['extra']['Duration (s)'] = defaultdict(float)
                            for dur_key in duration_keys:
                                job_logs['Jobs'][request_id]['extra']['Duration (s)'][dur_key] = 0.0
                        if 'timesteps_failed' not in job_logs['Jobs'][request_id].keys():
                            job_logs['Jobs'][request_id]['timesteps_failed'] = []
                        if 'Files (#)' not in job_logs['Jobs'][request_id]['extra'].keys():
                            job_logs['Jobs'][request_id]['extra']['Files (#)'] = defaultdict(int)
                        if 'error' not in job_logs['Jobs'][request_id].keys():
                            job_logs['Jobs'][request_id]['error'] = {}
                        if 'timesteps_failed' not in job_logs['Jobs'][request_id].keys():
                            job_logs['Jobs'][request_id]['timesteps_failed'] = []


                        if log_message[3] == 'START':
                            job_logs['Jobs'][request_id]['start'] = log_time
                            request_id_count[request_id] += 1
                        elif log_message[3] == 'END':
                            job_logs['Jobs'][request_id]['end'] = log_time
                            request_id_count[request_id] += 1
                        elif 'REPORT' in log_message[0]:
                            request_id_count[request_id] += 1
                            report = {'logStreamName':log_stream_name, 'logGroupName':log_group_name}
                            for report_msg in log_message[1:]:
                                report_msg = report_msg.split(': ')
                                report_type = report_msg[0]
                                if 'Duration' in report_type:
                                    report_type += ' (s)'
                                    report_value = float(report_msg[1].replace(' ms', '').strip()) * ms_to_sec
                                elif 'Memory' in report_type:
                                    report_type += ' (MB)'
                                    report_value = int(report_msg[1].replace(' MB', '').strip())
                                report[report_type] = report_value
                            request_time = report['Billed Duration (s)']
                            request_memory = report['Memory Size (MB)'] * MB_to_GB
                            cost_estimate = request_memory * request_time * USD_per_GBsec
                            report['Cost Estimate (USD)'] = cost_estimate
                            job_logs['Jobs'][request_id]['report'] = report
                        elif log_message[3] == 'DURATION':
                            duration_type = log_message[4]
                            duration = log_message[5]
                            job_logs['Jobs'][request_id]['extra']['Duration (s)'][duration_type] = float(duration)
                        elif log_message[3] == 'SUCCESS':
                            job_logs['Jobs'][request_id]['success'] = True
                        elif log_message[3] == 'PARTIAL':
                            job_logs['Jobs'][request_id]['any_failed'] = True
                        elif log_message[3] == 'TIMEOUT':
                            job_logs['Jobs'][request_id]['timeout'] = True
                        elif log_message[3] == 'FAILED':
                            failed_logs_list_raw = log_message[4]
                            failed_timesteps = sorted(ast.literal_eval(failed_logs_list_raw))
                            job_logs['Jobs'][request_id]['timesteps_failed'].extend(failed_timesteps)
                        elif log_message[3] == 'ERROR':
                            failed_timestep = log_message[4]
                            error_msg = ast.literal_eval(log_message[5])
                            job_logs['Jobs'][request_id]['error'][failed_timestep] = error_msg
                        elif log_message[3] == 'FILES':
                            file_type = log_message[4]
                            file_count = log_message[5]
                            job_logs['Jobs'][request_id]['extra']['Files (#)'][file_type] += int(file_count)
                        elif log_message[3] == 'SUCCEEDED':
                            succeeded_timesteps = ast.literal_eval(log_message[4])
                            job_logs['Jobs'][request_id]['timesteps_succeeded'] = succeeded_timesteps


                        if request_id_count[request_id] == 3:
                            if request_id not in completed_request_ids:
                                completed_request_ids.append(request_id)
                                num_jobs_ended += 1
                            if log_stream_name not in completed_stream_names[log_group_name]:
                                completed_stream_names[log_group_name].append(log_stream_name)

                # Loop through completed_stream_names, delete log streams if all requests within it are completed
                print(f'\tGoing through completed log streams')
                for log_group_name in log_group_names:
                    delete_ctr = 0
                    delete_start = time.time()
                    for log_stream_name in completed_stream_names[log_group_name]:
                        if log_stream_name not in deleted_stream_names[log_group_name]:
                            delete_current_log = True
                            for request_id in log_stream_request_ids[log_stream_name]:
                                if request_id not in completed_request_ids:
                                    delete_current_log = False
                                    break
                            if delete_current_log and delete_logs:
                                # delete current log stream (all requests contained within log stream are complete)
                                print(f'Deleting log stream: {log_stream_name}')
                                log_client.delete_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
                                # AWS limits DeleteLogStream to 5 requests per second (at a minimum). This checks to see if 5 calls have been
                                # made within a second, and waits the amount necessary to exceed 1 second before continuing to delete streams.
                                delete_ctr += 1
                                if delete_ctr == 5 and (time.time() - delete_start) < 1.0:
                                    time.sleep(1.1 - (time.time() - delete_start))
                                    delete_ctr = 0
                            deleted_stream_names[log_group_name].append(log_stream_name)


            if (num_jobs_ended == num_jobs):
                ctr += 1
                print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                # write final job_logs to file
                if retry != -1:
                    fn_extra = f'FINAL_RETRY_{retry}'
                else:
                    fn_extra = 'FINAL'
                job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, log_name, fn_extra=fn_extra)
                print(f'=== LOGGING COMPLETE ===')
                break
            
            if (previous_num_complete != num_jobs_ended) and (time.time() - previous_time > 2):
                ctr += 1
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                if retry != -1:
                    fn_extra = f'RETRY_{retry}'
                else:
                    fn_extra = ''
                job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, log_name, fn_extra=fn_extra)
                previous_num_complete = copy.deepcopy(num_jobs_ended)
                previous_time = time.time()

            if time.time()- logging_time > 600.:
                enter_debug = input(f'It has been 10 minutes, would you like to enter a debugger? (y/n)\t').strip().lower() == 'y'
                if enter_debug:
                    print('Entering python debugger. Enter "c" to continue execution')
                    import pdb; pdb.set_trace()
                logging_time = time.time()
 
        except Exception as e:
            # If the error was not a ThrottlingException, save the final log and return
            # Otherwise, continue processing the logs. Logic has been implemented to prevent a ThrottlingException to occur,
            # but this exists as a backup in case it does.
            if 'ThrottlingException' not in str(e):
                print(f'Error processing logs for lambda jobs')
                print(e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                try:
                    total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                    job_logs['Master Script Total Time (s)'] = total_time
                    # write final job_logs to file
                    job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, log_name, fn_extra='FINAL')
                except Exception as e:
                    print(f'Failed saving final log too: {e}')
                return job_logs

    return job_logs