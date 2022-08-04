"""
ECCO Dataset Production logging utilities

Author: Duncan Bark

Contains functions for getting, processing, and saving AWS Lambda logs

"""

import os
import ast
import sys
import copy
import time
import json
import boto3
from pathlib import Path
from collections import defaultdict

# Local imports
main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path / "src" / "utils"}')
import credentials_utils as credentials_utils


# ==========================================================================================================================
# PROCESS LOGS
# ==========================================================================================================================
def lambda_logging(job_logs, 
                   start_time, 
                   ms_to_sec, 
                   MB_to_GB, 
                   USD_per_GBsec, 
                   lambda_start_time, 
                   num_jobs, 
                   credential_method, 
                   log_name, 
                   aws_path, 
                   retry=-1):
    """
    Collect, process, and save AWS Lambda logs from AWS CloudWatch

    Args:
        job_logs (dict): Dictionary containing information of each job and processing overall
        start_time (int): Time of master script start in ms since 1970
        ms_to_sec (float): Constant for converting ms to sec and vice versa
        MB_to_GB (float): Constant for converting MB to GB and vice versa
        USD_per_GBsec (float): Constant cost ing USD for 1 GBsec of Lambda compute
        lambda_start_time (str): String of date and time representation of start of Lambda execution
        num_jobs (int): Number of total jobs submitted to AWS Lambda
        credential_method (dict): Dictionary containing values for getting AWS credentials (region, path, etc.)
        log_name (str): Name of log (appened to log filename(s), and within logs)
        aws_path (PosixPath): Path to /ECCO-Dataset-Product/aws
        retry (optional, int): Number indicating current retry number. This is appended to log filename if not -1

    Returns:
        job_logs (dict): Dictionary containing information of each job and processing overall
    """
    print(f'\n=== LOGGING START ===')

    delete_logs = True
    log_filter = '?"[INFO]" ?"REPORT "'
    duration_keys = ['ALL', 'IMPORT', 'RUN', 'TOTAL', 'SCRIPT', 'IO', 'DOWNLOAD', 'NETCDF', 'UPLOAD']
    log_client = boto3.client('logs')
    num_jobs_ended = 0
    estimated_jobs = []
    log_ctr = -1

    completed_request_ids = []
    deleted_stream_names = defaultdict(list)
    completed_stream_names = defaultdict(list)
    log_stream_request_ids = defaultdict(list)

    previous_num_complete = 0
    credentials_time = time.time() 
    logging_time = time.time()


    # ========== <Save inital log> ================================================================
    # intital log
    if log_ctr == -1:
        log_ctr += 1
        total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
        job_logs['Master Script Total Time (s)'] = total_time
        job_logs['Number of Lambda Jobs'] = num_jobs
        if retry != -1:
            fn_extra = f'INITIAL_RETRY_{retry}'
        else:
            fn_extra = 'INITIAL'
        job_logs, estimated_jobs = __save_logs_helper(job_logs, 
                                                      MB_to_GB, 
                                                      estimated_jobs, 
                                                      lambda_start_time, 
                                                      log_ctr, 
                                                      log_name, 
                                                      aws_path, 
                                                      fn_extra=fn_extra)
        previous_time = time.time()
    # ========== </Save inital log> ===============================================================


    # ========== <Main logging loop> ==============================================================
    while True:
        try:
            print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
            # Sleep for X seconds to allow lambda jobs to make progress and to not bombard AWS API with log requests
            time.sleep(30)
            end_time = int(time.time()/ms_to_sec)
            request_id_count = defaultdict(int)

            # Get current log group names, in case they havent been created yet, or got deleted
            log_group_names = [lg['logGroupName'] for lg in log_client.describe_log_groups()['logGroups'] if 'ecco_processing' in lg['logGroupName']]
            

            # ========== <Credentials> ============================================================
            # every 10 minutes, refresh the credentials
            if (time.time() - credentials_time) > 600.0:
                print(f'... time elapsed {time.time() - credentials_time}s')
                print(f'... getting new credentials!') 
                _ = credentials_utils.get_aws_credentials(credential_method)
                credentials_time = time.time()
            # ========== </Credentials> ===========================================================
            

            # ========== <Get log streams> ========================================================
            log_stream_names = defaultdict(list)
            total_num_log_streams = 0
            print(f'\tGetting log streams')
            log_streams = __get_logs_helper(log_client, log_group_names, [], type='logStream')
            for log_group_name, group_streams in log_streams.items():
                for ls in group_streams:
                    log_stream_name = ls['logStreamName']
                    if log_stream_name not in deleted_stream_names[log_group_name]:
                        log_stream_names[log_group_name].append(log_stream_name)
                        total_num_log_streams += 1
            # ========== </Get log streams> =======================================================


            # ========== <Log stream processing> ==================================================
            if total_num_log_streams > 0:
                # Get all events in the logs that have "[INFO]" or "REPORT " in them. Only look at log streams
                # from the list "log_stream_names" and between "start_time" and "end_time"
                info_log_events = __get_logs_helper(log_client, 
                                                    log_group_names, 
                                                    log_stream_names, 
                                                    start_time, 
                                                    end_time, 
                                                    log_filter, 
                                                    'event')

                # ========== <Log event loop> =====================================================
                # Loop through all [INFO] and REPORT events
                for log_group_name, info_logs in info_log_events.items():
                    for log in info_logs:
                        # Get log stream name, and log event message
                        log_stream_name = log['logStreamName']
                        log_message = log['message'].strip().split('\t')

                        # Get the log time and Lambda job request ID
                        if 'REPORT' in log_message[0]:
                            log_time = log['timestamp']
                            request_id = log_message[0].split(' ')[-1]
                        else:
                            log_time = log_message[1]
                            request_id = log_message[2]

                        # If the lambda job has been compelted, skip looking at this event
                        if request_id in completed_request_ids:
                            continue

                        # If this lambda job hasnt been assigned to it's parent log stream, add it
                        # log_stream_request_ids is a dictionary, where the keys are log stream names
                        # the values are lists of Lambda job request IDs that are within that log stream
                        if request_id not in log_stream_request_ids[log_stream_name]:
                            log_stream_request_ids[log_stream_name].append(request_id)

                        # Make sure the dictionary entry for the current Lambda job request ID has all necessary
                        # base fields and values
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

                        # Check log_message for it's type and update the corresponding field in the
                        # Lambda job request ID's entry in job_logs
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

                        # If a Lambda job request ID has a recorded START, REPORT, and END log event,
                        # then add it to the completed request_ids. Also add it's parent log stream
                        # to a list of completed log streams to go through to delete later
                        if request_id_count[request_id] == 3:
                            if request_id not in completed_request_ids:
                                completed_request_ids.append(request_id)
                                num_jobs_ended += 1
                            if log_stream_name not in completed_stream_names[log_group_name]:
                                completed_stream_names[log_group_name].append(log_stream_name)
                # ========== </Log event loop> ====================================================


                # ========== <Delete log streams> =================================================
                # Loop through completed_stream_names, delete log streams if all requests within it are completed
                print(f'\tGoing through completed log streams')
                for log_group_name in log_group_names:
                    delete_ctr = 0
                    delete_start = time.time()
                    # For each log stream that has at least one completed Lambda request within it
                    for log_stream_name in completed_stream_names[log_group_name]:
                        # If the log stream has not been deleted
                        if log_stream_name not in deleted_stream_names[log_group_name]:
                            delete_current_log = True
                            # If all requests within the log stream have been completed
                            for request_id in log_stream_request_ids[log_stream_name]:
                                if request_id not in completed_request_ids:
                                    delete_current_log = False
                                    break
                            # delete the log stream
                            if delete_current_log and delete_logs:
                                # delete current log stream (all requests contained within log stream are complete)
                                print(f'Deleting log stream: {log_stream_name}')
                                log_client.delete_log_stream(logGroupName=log_group_name, 
                                                             logStreamName=log_stream_name)
                                # AWS limits DeleteLogStream to 5 requests per second (at a minimum). This checks to see if 5 calls have been
                                # made within a second, and waits the amount necessary to exceed 1 second before continuing to delete streams.
                                delete_ctr += 1
                                if delete_ctr == 5 and (time.time() - delete_start) < 1.0:
                                    time.sleep(1.1 - (time.time() - delete_start))
                                    delete_ctr = 0
                                deleted_stream_names[log_group_name].append(log_stream_name)
                # ========== </Delete log streams> ================================================
            # ========== </Log stream processing> =================================================


            # ========== <Final log> ==============================================================
            if (num_jobs_ended == num_jobs):
                log_ctr += 1
                print(f'Processing job logs -- {num_jobs_ended}/{num_jobs}')
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                # write final job_logs to file
                if retry != -1:
                    fn_extra = f'FINAL_RETRY_{retry}'
                else:
                    fn_extra = 'FINAL'
                job_logs, estimated_jobs = __save_logs_helper(job_logs, 
                                                              MB_to_GB, 
                                                              estimated_jobs, 
                                                              lambda_start_time, 
                                                              log_ctr, 
                                                              log_name, 
                                                              aws_path, 
                                                              fn_extra=fn_extra)
                print(f'\n=== LOGGING COMPLETE ===')
                break
            # ========== </Final log> =============================================================


            # ========== <Partial log> ============================================================
            # Save a log if it has been longer than 60 seconds since the last log, and new jobs have ended
            if (previous_num_complete != num_jobs_ended) and (time.time() - previous_time > 60):
                log_ctr += 1
                total_time = (int(time.time()/ms_to_sec)-start_time) * ms_to_sec
                job_logs['Master Script Total Time (s)'] = total_time
                if retry != -1:
                    fn_extra = f'RETRY_{retry}'
                else:
                    fn_extra = ''
                job_logs, estimated_jobs = __save_logs_helper(job_logs, 
                                                              MB_to_GB, 
                                                              estimated_jobs, 
                                                              lambda_start_time, 
                                                              log_ctr, 
                                                              log_name, 
                                                              aws_path, 
                                                              fn_extra=fn_extra)
                previous_num_complete = copy.deepcopy(num_jobs_ended)
                previous_time = time.time()
            # ========== </Partial log> ===========================================================


            # ========== <Debug> ==================================================================
            # Enter the python debugger if it has been longer than 15 minutes, and the user wants to
            if time.time()- logging_time > 900.:
                enter_debug = input(f'It has been 10 minutes, would you like to enter a debugger? (y/n)\t').strip().lower() == 'y'
                if enter_debug:
                    print('Entering python debugger. Enter "c" to continue execution')
                    import pdb; pdb.set_trace()
                logging_time = time.time()
            # ========== </Debug> =================================================================

            print()
 
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
                    job_logs, estimated_jobs = __save_logs_helper(job_logs, 
                                                                  MB_to_GB, 
                                                                  estimated_jobs, 
                                                                  lambda_start_time, 
                                                                  log_ctr, 
                                                                  log_name, 
                                                                  aws_path, 
                                                                  fn_extra='FINAL_error')
                except Exception as e:
                    print(f'Failed saving final log too: {e}')
                return job_logs
    # ========== </Main logging loop> =============================================================

    return job_logs


def __get_logs_helper(log_client, 
                      log_group_names, 
                      log_stream_names, 
                      start_time=0, 
                      end_time=0, 
                      filter_pattern='', 
                      type=''):
    """
    Get the logs from AWS CloudWatch for the provided log groups, and log streams, within start and end time, using provided filter

    Args:
        log_client (botocore.client.CloudWatchLogs): boto3 client object for AWS CloudWatch
        log_group_names (list): List of AWS CloudWatch log groups (i.e. ['/aws/lambda/ecco_processing_2D_latlon', ...])
        log_stream_names (dict): Dictionary with keys = log group names, and values is a list of log streams in the group
        start_time (optional, int): Time of master script start in ms since 1970
        end_time (optional, int): Time of last processing log loop in ms since 1970
        filter_pattern (optional, str): String used to match events (i.e. '?[INFO] ?"REPORT"')
        type (optional, str): Type of logs to get (either 'logStream', or 'event')

    Returns:
        ret_logs (defaultdict(list)):
            if type == 'logStream': ret_logs is a dictionary with keys = log group names, and values is a list of log streams and info
            if type == 'event': ret_logs is a dictionary with keys = log group names, and values is a list of log event dictionaries
    """
    try:
        # if type is event, then filter all the log events in the provided log streams that occured between start and end time, that
        # match the provided filter
        if type == 'event':
            ret_logs = defaultdict(list)
            for log_group_name in log_group_names:
                log_stream_ctr = 0
                total_logs_checked = 0
                # if there are no log streams in the current log group, continue
                if len(log_stream_names[log_group_name]) == 0:
                    ret_logs[log_group_name] = []
                    continue
                
                # get log stream names for the current log group
                mod_log_stream_names = log_stream_names[log_group_name]
                while True:
                    # if there are more than 100 log streams for the current log group, only look at 100 at a time
                    # this is a moving window, that increases by 100 each time this loop happens. This ensures all logs are looked at.
                    if len(log_stream_names[log_group_name]) > 100:
                        mod_log_stream_names = log_stream_names[log_group_name][log_stream_ctr*100:log_stream_ctr*100 + 100]
                    total_logs_checked += len(mod_log_stream_names)

                    # filter the (max) 100 log streams to get the events between start and end time, and that match the filter pattern
                    events_current = log_client.filter_log_events(logGroupName=log_group_name, 
                                                                  logStreamNames=mod_log_stream_names, 
                                                                  filterPattern=filter_pattern, 
                                                                  startTime=start_time, 
                                                                  endTime=end_time)

                    time.sleep(0.11) # AWS limits FilterLogEvents to 10 requests per second in US West. This ensures 10 requests dont occur in a second.
                    ret_logs[log_group_name].extend(events_current['events'])
                    # if there are more events left, get those as well (filter_log_events puts a limit to the total number of events returned)
                    while True:
                        if 'nextToken' in events_current.keys():
                            events_current = log_client.filter_log_events(logGroupName=log_group_name, 
                                                                          logStreamNames=mod_log_stream_names, 
                                                                          filterPattern=filter_pattern, 
                                                                          nextToken=events_current['nextToken'])
                            time.sleep(0.11) # AWS limits FilterLogEvents to 10 requests per second in US West. This ensures 10 requests dont occur in a second.
                            if events_current['events'] != []:
                                ret_logs[log_group_name].extend(events_current['events'])
                        else:
                            break

                    # if the number of log streams checked equals the number of log streams in the curernt log group, exit
                    if total_logs_checked == len(log_stream_names[log_group_name]):
                        break
                    # otherwise, increase the counter to get the next (max) 100 log streams
                    else:
                        log_stream_ctr += 1
        elif type == 'logStream':
            ret_logs = defaultdict(list)
            # Get all the log streams in each log group
            for log_group_name in log_group_names:
                log_streams_current = log_client.describe_log_streams(logGroupName=log_group_name, 
                                                                      orderBy='LastEventTime')

                time.sleep(0.21) # AWS limits DescribeLogStreams to 5 requests per second in US West. This ensures 5 requests dont occur in a second.
                ret_logs[log_group_name] = log_streams_current['logStreams']

                # if there are more log streams left, get those as well (describe_log_streams puts a limit to the total number of log streams returned)
                while True:
                    if 'nextToken' in log_streams_current.keys():
                        log_streams_current = log_client.describe_log_streams(logGroupName=log_group_name, 
                                                                              orderBy='LastEventTime', 
                                                                              nextToken=log_streams_current['nextToken'])
                        time.sleep(0.21) # AWS limits DescribeLogStreams to 5 requests per second in US West. This ensures 5 requests dont occur in a second.
                        if log_streams_current['logStreams'] != []:
                            ret_logs[log_group_name].extend(log_streams_current['logStreams'])
                    else:
                        break
    except Exception as e:
        print('Error accessing logs: ')
        print(e)

    return ret_logs


def __save_logs_helper(job_logs, 
                       MB_to_GB, 
                       estimated_jobs, 
                       start_time, 
                       log_ctr, 
                       log_name, 
                       aws_path, 
                       fn_extra=''):
    """
    Save job_logs

    Args:
        job_logs (dict): Dictionary containing information of each job and processing overall
        MB_to_GB (float): Constant for converting MB to GB and vice versa
        estimated_jobs (list): List of jobs, whos information has been added to job_logs
        start_time (int): Time of master script start in ms since 1970
        log_ctr (int): Counter for number of logs saved
        log_name (str): Name of log (appened to log filename(s), and within logs)
        aws_path (PosixPath): Path to /ECCO-Dataset-Product/aws
        fn_extra (optional, str): Extra string to add on end of log file name

    Returns:
        (job_logs, estimated_jobs) (tuple): 
            job_logs (dict): Dictionary containing information of each job and processing overall
            estimated_jobs (list): List of jobs, whos information has been added to job_logs
    """
    # ========== <Save job logs> ==================================================================
    try:
        # Loop through each job in job_logs, and update overall time/cost information IF
        # the job has been been included yet, and the job has ended
        for job_id, job_content in job_logs['Jobs'].items():
            if job_id not in estimated_jobs:
                if ('INITIAL' not in fn_extra) and (job_content['end']):
                    estimated_jobs.append(job_id)
                
                job_report = job_content['report']
                if job_report != {}:
                    # Update overall time and cost values
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
                    # Update timesteps_failed for any time step that has failed. Include additional information for
                    # automatic job submission
                    grouping = job_content["data"]["grouping_to_process"]
                    product_type = job_content["data"]["product_type"]
                    freq = job_content["data"]["output_freq_code"]
                    dimension = job_content["data"]["dimension"]
                    ts_fail_info = f'{ts_fail} {grouping} {product_type} {freq} {dimension}'
                    job_logs['Timesteps failed'].append(ts_fail_info)
        job_logs['Timesteps failed'] = sorted(job_logs['Timesteps failed'])

        # Prepare fn_extra
        if fn_extra != '' and fn_extra[0] != '_':
            fn_extra = f'_{fn_extra}'
        # if no log name is provided, use current time
        if log_name == '':
            log_name = time.strftime('%Y%m%d:%H%M%S', time.localtime())

        # Make directory(ies) for log file
        log_folder = start_time.split(':')[0]
        log_path = f'{aws_path}/logs/{log_folder}'
        if not os.path.exists(log_path):
            os.makedirs(log_path, exist_ok=True)

        # Save job_logs
        with open(f'{log_path}/job_logs_{start_time}_{log_ctr}_{log_name}{fn_extra}.json', 'w') as f:
            json.dump(job_logs, f, indent=4)
    except Exception as e:
        print('Error saving logs: ')
        print(e)
    # ========== </Save job logs> =================================================================
    
    return job_logs, estimated_jobs
