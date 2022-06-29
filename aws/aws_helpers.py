import sys
import glob
import time
import json
import boto3
import subprocess
from pathlib import Path
from collections import defaultdict


def get_files_time_steps(s3, fields, s3_dir_prefix, period_suffix, source_bucket, product_type, time_steps_to_process):
    if time_steps_to_process != 'all' and not isinstance(time_steps_to_process, int):
        print(f'Bad time steps provided ("{time_steps_to_process}"). Skipping job.')
        return -1

    s3_field_paths = []
    for field in fields:
        s3_field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')

    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    all_time_steps_all_vars = []
    for i, s3_field_path in enumerate(s3_field_paths):
        start_after = ''
        field = fields[i]
        total_field_files = 0
        while True:
            source_objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=s3_field_path, StartAfter=start_after)

            if 'Contents' not in source_objects:
                break

            file_type = '.data'

            file_keys = [key['Key'] for key in source_objects['Contents'] if file_type in key['Key'] and key['Key'] not in field_files[field]]
            
            if time_steps_to_process != 'all':
                if total_field_files < time_steps_to_process:
                    num_files_left = time_steps_to_process - total_field_files
                    if len(file_keys) > num_files_left:
                        file_keys = file_keys[:num_files_left]
                else:
                    break
            # else:
            #     file_keys = []
            #     for ts in time_steps_to_process:
            #         ts = str(ts).zfill(10)
            #         temp_keys = [key['Key'] for key in source_objects['Contents'] if file_type in key['Key'] and key['Key'] not in field_files[field] and ts in key['Key']]
            #         if temp_keys == []:
            #             print(f'Invalid time step provided ("{ts}"). Skipping job.')
            #             return -1
            #         file_keys.extend(temp_keys)
            total_field_files += len(file_keys)
            field_files[field].extend(file_keys)
            time_steps = [key.split('.')[-2] for key in file_keys]
            field_time_steps[field].extend(time_steps)
            start_after = file_keys[-1]
        
        field_files[field] = sorted(field_files[field])
        field_time_steps[field] = sorted(field_time_steps[field])
        all_time_steps_all_vars.extend(time_steps)

    return (field_files, field_time_steps, all_time_steps_all_vars)


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
                    if len(log_stream_names) > 100:
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
        for job in job_logs.keys():
            if job != 'Cost Information' and job != 'Master Script Total Time (s)':
                if job not in estimated_jobs:
                    if (fn_extra != 'INITIAL') and (job_logs[job]['end']):
                        estimated_jobs.append(job)
                    if job_logs[job]['report'] != []:
                        job_reports = job_logs[job]['report']
                        for job_report in job_reports:
                            request_duration_time = job_report["Duration (s)"]
                            request_time = job_report["Billed Duration (s)"]
                            request_memory = job_report["Memory Size (MB)"]
                            cost_estimate = job_report["Cost Estimate (USD)"]
                            job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Time (s)'] += request_duration_time
                            job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Billed Time (s)'] += request_time
                            job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total GB*s'] += (request_memory * MB_to_GB * request_time)
                            job_logs['Cost Information'][f'{job_report["Memory Size (MB)"]} MB Total Cost (USD)'] += cost_estimate
                            job_logs['Cost Information']['Total Cost'] += cost_estimate

        if fn_extra != '' and fn_extra[0] != '_':
            fn_extra = f'{fn_extra}'
        time_str = time.strftime('%Y%m%d:%H%M%S', time.localtime())
        with open(f'./logs/job_logs_{start_time}_{ctr}_{time_str}_{fn_extra}.json', 'w') as f:
            json.dump(job_logs, f, indent=4)
    except Exception as e:
        print('Error saving logs: ')
        print(e)
    
    return job_logs, estimated_jobs


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


def get_aws_credentials(aws_login_file, aws_region):
    try:
        subprocess.run([aws_login_file, '-r', f'{aws_region}'], check=True)
        credentials = get_credentials_helper()
    except:
        print(f'Unable to run script to get credentials ("{aws_login_file}"). Exiting')
        sys.exit()

    return credentials


















