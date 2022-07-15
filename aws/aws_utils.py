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


def get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, source_bucket, num_time_steps_to_process):
    """Create lists of files and timesteps for each field from files present on S3

    Args:
        s3 (botocore.client.S3): Boto3 S3 client initalized with necessary credentials
        fields (list): List of field names
        s3_dir_prefix (str): Prefix of files stored on S3 (i.e. 'V4r4/diags_monthly)
        period_suffix (str): Period suffix of files (i.e. 'mon_mean')
        source_bucket (str): Name of S3 bucket
        num_time_steps_to_process (str/int): String 'all' or an integer specifing the number of time
                                             steps to process

    Returns:
        ()
    """


    # num_time_steps_to_process must either be the string 'all' or a number, corresponding to the total number
    # of time steps to process over all jobs (if using lambda), or overall (when local)
    if num_time_steps_to_process != 'all' and not isinstance(num_time_steps_to_process, int):
        print(f'Bad time steps provided ("{num_time_steps_to_process}"). Skipping job.')
        return -1

    print(f'\nGetting timesteps and files for fields: {fields} for {num_time_steps_to_process} {period_suffix} timesteps')

    # ORIGINAL TECHNIQUE (More complicated and slightly slower (~10%))
    # # Construct the list of field paths
    # # i.e. ['aws/temp_model_output/SSH', 'aws/temp_model_output/SSHIBC', ...]
    # s3_field_paths = []
    # for field in fields:
    #     s3_field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')

    # # Get all the files present in the provided S3 bucket and organize them by field
    # field_files = defaultdict(list)
    # field_time_steps = defaultdict(list)
    # all_time_steps_all_vars = []
    # for i, s3_field_path in enumerate(s3_field_paths):
    #     start_after = ''
    #     field = fields[i]
    #     total_field_files = 0
    #     while True:
    #         # List the files present in the source_bucket, that have a prefix corresponding to the current field path
    #         # There is a limit on the number of objects this can return, so start_after is the last object returned
    #         # in the previous call of the function.
    #         source_objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=s3_field_path, StartAfter=start_after)

    #         # If no files are returned
    #         if 'Contents' not in source_objects:
    #             break

    #         # only look at .data files for timesteps. Could also look at .meta files, but not both (duplicate time steps would be found)
    #         file_type = '.data'

    #         file_keys = [key['Key'] for key in source_objects['Contents'] if file_type in key['Key'] and key['Key'] not in field_files[field]]
            
    #         # If files were returned (i.e. files were found in the bucket for the field), then add them to the list of files for this field
    #         if len(file_keys) > 0:
    #             # if a specific number of time steps is provided, only collect that many for each field
    #             # since multiple calls to list_objects_v3 is likely required, keeping track of the total number of files
    #             # and the number of files left is required
    #             if num_time_steps_to_process != 'all':
    #                 if total_field_files < num_time_steps_to_process:
    #                     num_files_left = num_time_steps_to_process - total_field_files
    #                     if len(file_keys) > num_files_left:
    #                         file_keys = file_keys[:num_files_left]
    #                 else:
    #                     break
    #             # add all files present in file_keys to the field's list in field_files
    #             total_field_files += len(file_keys)
    #             field_files[field].extend(file_keys)
    #             time_steps = [key.split('.')[-2] for key in file_keys]
    #             field_time_steps[field].extend(time_steps)
    #             start_after = file_keys[-1]
    #             all_time_steps_all_vars.extend(time_steps)
    #         else:
    #             break
        
    #     # sort lists of field files and timesteps, and all timesteps
    #     field_files[field] = sorted(field_files[field])
    #     field_time_steps[field] = sorted(field_time_steps[field])
    #     all_time_steps_all_vars = sorted(all_time_steps_all_vars)

    # NEWEST TECHNIQUE (simpler and slightly quicker (~10%))
    # Construct the list of field paths
    # i.e. ['aws/temp_model_output/SSH', 'aws/temp_model_output/SSHIBC', ...]
    s3_field_paths = []
    for field in fields:
        s3_field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(source_bucket)
    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    all_time_steps_all_vars = []

    # OLD ONE AT A TIME TECHNIQUE (MUCH SLOWER THAN THREADED TECHNIQUE)
    # # loop through each s3_field_path (eg. V4r4/diags_daily/SSH_day_mean, etc.)
    # for i, s3_field_path in enumerate(s3_field_paths):
    #     field = fields[i]
    #     num_time_steps = 0
    #     # loop through all the objects in the source_bucket with a prefix matching the s3_field_path
    #     for obj in bucket.objects.filter(Prefix=s3_field_path):
    #         if num_time_steps == num_time_steps_to_process:
    #             break
    #         if '.meta' in obj.key:
    #             continue
    #         field_files[field].append(obj.key)
    #         field_time_steps[field].append(obj.key.split('.')[-2])
    #         all_time_steps_all_vars.append(obj.key.split('.')[-2])
    #         num_time_steps += 1
    #     field_files[field] = sorted(field_files[field])
    #     field_time_steps[field] = sorted(field_time_steps[field])
    #     all_time_steps_all_vars = sorted(all_time_steps_all_vars)

    # NEW THREADED TECHNIQUE
    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    all_time_steps_all_vars = []
    if True:
        num_workers = len(s3_field_paths)
        print(f'Using {num_workers} workers to get time steps and files for {len(fields)} fields')

        # get files function
        def fetch(s3_field_path):
            field = s3_field_path.split('/')[-1].split(period_suffix)[0][:-1]
            # field = fields[i]
            num_time_steps = 0
            # loop through all the objects in the source_bucket with a prefix matching the s3_field_path
            for obj in bucket.objects.filter(Prefix=s3_field_path):
                if num_time_steps == num_time_steps_to_process:
                    break
                if '.meta' in obj.key:
                    continue
                field_files[field].append(obj.key)
                field_time_steps[field].append(obj.key.split('.')[-2])
                all_time_steps_all_vars.append(obj.key.split('.')[-2])
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

        all_time_steps_all_vars = sorted(all_time_steps_all_vars)

    return (field_files, field_time_steps, all_time_steps_all_vars)


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


def create_lambda_function(client, function_name, role, memory_size, image_uri):
    # Create lambda function using the provided values

    # Create function
    try:
        print(f'\nCreating lambda function ({function_name}) with {memory_size} MB of memory')
        client.create_function(
            FunctionName=function_name,
            Role=role,
            PackageType='Image',
            Code={'ImageUri':image_uri},
            Publish=True,
            Timeout=900,
            MemorySize=memory_size
        )
    except:
        print(f'Failed to create function: {function_name}')
        return

    print(f'Verifying lambda function creation ({function_name})...')
    while True:
        status = client.get_function_configuration(FunctionName=function_name)['State']
        if status == "Failed":
            print(f'\tFailed to create function ({function_name}). Try again\n')
            sys.exit()
        elif status == 'Active':
            print(f'\tFunction created successfully\n')
            break
        time.sleep(2)
    
    return


# ======
# CREDENTIALS
# ======
# Get credentials for AWS from "~/.aws/credentials" file
def get_credentials_helper():
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


# def get_credentials_helper():
#     # Get credentials for AWS from "~/.aws/credentials" file
#     cred_path = Path.home() / '.aws/credentials'
#     credentials = {}
#     if cred_path.exists():
#         with open(cred_path, 'r') as f:
#             for line in f:
#                 line = line.strip()
#                 if line == '':
#                     break
#                 elif line[0] == '#':
#                     credentials['expiration_date'] = line.split(' = ')[-1]
#                 elif line[0] == '[':
#                     credentials['profile_name'] = line[1:-1]
#                 else:
#                     name, value = line.split(' = ')
#                     credentials[name] = value
#     return credentials


# def get_aws_credentials(aws_login_file, aws_region):
#     try:
#         subprocess.run([aws_login_file, '-r', f'{aws_region}'], check=True)
#         credentials = get_credentials_helper()
#     except:
#         print(f'Unable to run script to get credentials ("{aws_login_file}"). Exiting')
#         sys.exit()

#     return credentials


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
                    ret_logs[log_group_name].extend(events_current['events'])
                    while True:
                        if 'nextToken' in events_current.keys():
                            events_current = log_client.filter_log_events(logGroupName=log_group_name, logStreamNames=mod_log_stream_names, filterPattern=filter_pattern, nextToken=events_current['nextToken'])
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
                ret_logs[log_group_name] = log_streams_current['logStreams']
                while True:
                    if 'nextToken' in log_streams_current.keys():
                        log_streams_current = log_client.describe_log_streams(logGroupName=log_group_name, orderBy='LastEventTime', nextToken=log_streams_current['nextToken'])
                        if log_streams_current['logStreams'] != []:
                            ret_logs[log_group_name].extend(log_streams_current['logStreams'])
                    else:
                        break
    except Exception as e:
        print('Error accessing logs: ')
        print(e)
    return ret_logs


def save_logs(job_logs, MB_to_GB, estimated_jobs, start_time, ctr, fn_extra=''):
    try:
        for job_id, job_content in job_logs['Jobs'].items():
            if job_id not in estimated_jobs:
                if (fn_extra != 'INITIAL') and (job_content['end']):
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
                job_logs['Timesteps failed'].extend(job_content['timesteps_failed'])
        job_logs['Timesteps failed'] = sorted(job_logs['Timesteps failed'])

        if fn_extra != '' and fn_extra[0] != '_':
            fn_extra = f'{fn_extra}'
        time_str = time.strftime('%Y%m%d:%H%M%S', time.localtime())
        # if fn_extra != 'INITIAL':
        with open(f'./logs/job_logs_{start_time}_{ctr}_{time_str}_{fn_extra}.json', 'w') as f:
            json.dump(job_logs, f, indent=4)
    except Exception as e:
        print('Error saving logs: ')
        print(e)
    
    return job_logs, estimated_jobs


def lambda_logging(job_logs, start_time, ms_to_sec, MB_to_GB, USD_per_GBsec, lambda_start_time, num_jobs, credential_method):
    delete_logs = True
    key_filter = '?START ?END ?REPORT'
    extra_filter = '?PARTIAL ?SUCCESS ?DURATION ?FILES ?ERROR ?FAILED'
    duration_keys = ['ALL', 'IMPORT', 'RUN', 'TOTAL', 'SCRIPT', 'IO', 'DOWNLOAD', 'NETCDF', 'UPLOAD']
    log_client = boto3.client('logs')
    log_group_names = [lg['logGroupName'] for lg in log_client.describe_log_groups()['logGroups'] if 'ecco_processing' in lg['logGroupName']]
    num_jobs_ended = 0
    estimated_jobs = []
    ctr = -1


    completed_request_ids = []
    deleted_stream_names = defaultdict(list)
    completed_stream_names = defaultdict(list)
    log_stream_request_ids = defaultdict(list)

    try:
        previous_num_complete = 0
        ticking_time_bomb = time.time() 
        logging_time = time.time()
        while True:
            tick_tick_tick = time.time() - ticking_time_bomb
            # every 10 minutes, refresh the credentials
            if tick_tick_tick > 600.0:
                print(f'... time elapsed {tick_tick_tick}s')
                print(f'... getting new credentials!') 
                get_aws_credentials(credential_method)
                credentials = get_credentials_helper()
                ticking_time_bomb = time.time()
            request_id_count = defaultdict(int)
            # intital log
            if ctr == -1:
                ctr += 1
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                job_logs['Number of Lambda Jobs'] = num_jobs
                job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, fn_extra='INITIAL')
                previous_time = time.time()

            print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
            time.sleep(120)
            end_time = int(time.time()/ms_to_sec)
            
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
                print(f'\tGetting key logs')
                group_key_logs = get_logs(log_client, log_group_names, log_stream_names, start_time=start_time, end_time=end_time, filter_pattern=key_filter, type='event')

                # Loop through all START, END, and REPORT events
                for log_group_name, key_logs in group_key_logs.items():
                    for log in key_logs:
                        log_stream_name = log['logStreamName']
                        if 'START' == log['message'][:5]:
                            request_id = log['message'].split(' ')[2].strip()
                            if request_id not in log_stream_request_ids[log_stream_name]:
                                log_stream_request_ids[log_stream_name].append(request_id)
                            if request_id not in job_logs['Jobs'].keys():
                                job_logs['Jobs'][request_id] = {}
                            job_logs['Jobs'][request_id]['start'] = log['timestamp']
                            request_id_count[request_id] += 1
                        elif 'END' == log['message'][:3]:
                            request_id = log['message'].split(' ')[2].strip()
                            job_logs['Jobs'][request_id]['end'] = log['timestamp']
                            request_id_count[request_id] += 1
                        elif 'REPORT' == log['message'][:6]: #'REPORT' in log['message']
                            request_id = ''
                            report = {'logStreamName':log_stream_name, 'logGroupName':log_group_name}
                            report_message = log['message'].split('\t')[:-1]
                            for rm in report_message:
                                if 'REPORT' in rm:
                                    rm = rm[7:]
                                rm = rm.split(': ')
                                if ' ms' in rm[-1]:
                                    rm[-1] = float(rm[-1].replace(' ms', '').strip()) * ms_to_sec
                                    rm[0] = f'{rm[0].strip()} (s)'
                                elif ' MB' in rm[-1]:
                                    rm[-1] = int(rm[-1].replace(' MB', '').strip())
                                    rm[0] = f'{rm[0].strip()} (MB)'
                                elif 'RequestId' in rm[0]:
                                    request_id = rm[-1].strip()
                                    continue
                                report[rm[0]] = rm[-1]

                            # estimate cost
                            request_time = report['Billed Duration (s)']
                            request_memory = report['Memory Size (MB)'] * MB_to_GB
                            cost_estimate = request_memory * request_time * USD_per_GBsec
                            report['Cost Estimate (USD)'] = cost_estimate
                            job_logs['Jobs'][request_id]['report'] = report

                            request_id_count[request_id] += 1

                # Loop through all requests that have a START, END, and REPORT event
                print(f'\tGetting important info for each completed job')
                for request_id, request_fields in job_logs['Jobs'].items():
                    # request_fields_keys = list(request_fields.keys())
                    # if 'start' in request_fields_keys and 'end' in request_fields_keys and 'report' in request_fields_keys:
                    if request_id_count[request_id] == 3:
                        req_start = request_fields['start']
                        req_end = request_fields['end']
                        log_stream_name = request_fields['report']['logStreamName']
                        log_group_name = request_fields['report']['logGroupName']
                        req_extra_logs = get_logs(log_client, [log_group_name], {log_group_name:[log_stream_name]}, start_time=req_start, end_time=req_end, filter_pattern=extra_filter, type='event')
                        for log_group_name, extra_logs in req_extra_logs.items():
                            for log in extra_logs:
                                # check that the log is for the current request time range
                                # some jobs are executed within the same log stream, and specifying
                                # the start_time and end_time in filter_log_events() doesnt seem to work
                                timestamp = log['timestamp']
                                if timestamp < req_start or timestamp > req_end:
                                    continue

                                # get FAILED logs
                                if 'FAILED' == log['message'][:6]:
                                    failed_logs_list_raw = log['message'].split('\t')[-1].strip()
                                    failed_timesteps = sorted(ast.literal_eval(failed_logs_list_raw))
                                    if 'timesteps_failed' not in job_logs['Jobs'][request_id].keys():
                                        job_logs['Jobs'][request_id]['timesteps_failed'] = []
                                    job_logs['Jobs'][request_id]['timesteps_failed'].extend(failed_timesteps)

                                # get SUCCESS logs
                                if 'SUCCESS' == log['message'][:7]:
                                    job_logs['Jobs'][request_id]['success'] = True

                                # get PARTIAL logs
                                if 'PARTIAL' == log['message'][:7]:
                                    job_logs['Jobs'][request_id]['any_failed'] = True

                                # get TIME logs
                                if 'DURATION' == log['message'][:8]:
                                    if 'extra' not in job_logs['Jobs'][request_id].keys():
                                        job_logs['Jobs'][request_id]['extra'] = {}
                                    if 'Duration (s)' not in job_logs['Jobs'][request_id]['extra'].keys():
                                        job_logs['Jobs'][request_id]['extra']['Duration (s)'] = defaultdict(float)
                                        for dur_key in duration_keys:
                                            job_logs['Jobs'][request_id]['extra']['Duration (s)'][dur_key] = 0.0
                                    _, duration_type, duration, _ = log['message'].split('\t')
                                    job_logs['Jobs'][request_id]['extra']['Duration (s)'][duration_type] = float(duration)
                                
                                # get COUNT logs
                                if 'FILES' == log['message'][:5]:
                                    if 'extra' not in job_logs['Jobs'][request_id].keys():
                                        job_logs['Jobs'][request_id]['extra'] = {}
                                    if 'Files (#)' not in job_logs['Jobs'][request_id]['extra'].keys():
                                        job_logs['Jobs'][request_id]['extra']['Files (#)'] = defaultdict(int)
                                    _, file_type, file_count = log['message'].split('\t')
                                    job_logs['Jobs'][request_id]['extra']['Files (#)'][file_type] += int(file_count)

                                # get ERROR logs
                                if 'ERROR' == log['message'][:5]:
                                    error_msg_split = log['message'].split('\t')
                                    if 'Exception' not in error_msg_split[0]:
                                        failed_timestep = error_msg_split[1]
                                        error_msg = error_msg_split[2]
                                        if 'error' not in job_logs['Jobs'][request_id].keys():
                                            job_logs['Jobs'][request_id]['error'] = {}
                                        job_logs['Jobs'][request_id]['error'][failed_timestep] = error_msg

                        if request_id not in completed_request_ids:
                            completed_request_ids.append(request_id)
                            num_jobs_ended += 1
                        if log_stream_name not in completed_stream_names[log_group_name]:
                            completed_stream_names[log_group_name].append(log_stream_name)

            # Loop through completed_stream_names, delete log streams if all requests within it are completed
            print(f'\tGoing through completed log streams')
            for log_group_name in log_group_names:
                for log_stream_name in completed_stream_names[log_group_name]:
                    if log_stream_name not in deleted_stream_names[log_group_name]:
                        num_req_to_complete = len(log_stream_request_ids[log_stream_name])
                        num_completed = 0
                        for request_id in log_stream_request_ids[log_stream_name]:
                            if request_id in completed_request_ids:
                                num_completed += 1
                        if num_req_to_complete == num_completed:
                            if delete_logs:
                                # delete current log stream (all requests contained within log stream are complete)
                                print(f'Deleting log stream: {log_stream_name}')
                                log_client.delete_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
                            deleted_stream_names[log_group_name].append(log_stream_name)

            if (num_jobs_ended == num_jobs):
                ctr += 1
                print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                # write final job_logs to file
                job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, fn_extra='FINAL')
                break
            
            if (previous_num_complete != num_jobs_ended) and (time.time() - previous_time > 2):
                ctr += 1
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr)
                previous_num_complete = copy.deepcopy(num_jobs_ended)
                previous_time = time.time()


            if time.time()- logging_time > 600.:
                enter_debug = input(f'It has been 10 minutes, would you like to enter a debugger? (y/n)\t').strip().lower() == 'y'
                if enter_debug:
                    print('Entering python debugger. Enter "c" to continue execution')
                    import pdb; pdb.set_trace()
                logging_time = time.time()

    except Exception as e:
        print(f'Error processing logs for lambda jobs')
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        try:
            total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
            job_logs['Master Script Total Time (s)'] = total_time
            # write final job_logs to file
            job_logs, estimated_jobs = save_logs(job_logs, MB_to_GB, estimated_jobs, lambda_start_time, ctr, fn_extra='FINAL')
        except:
            print('Failed saving final log')