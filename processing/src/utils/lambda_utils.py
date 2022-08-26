"""
ECCO Dataset Production AWS Lambda utilities

Author: Duncan Bark

Contains functions for updating, creating, and invoking AWS Lambda functions

"""

import json
import time
from concurrent import futures

from print_utils import printc

# ==========================================================================================================================
# CREATE LAMBDA FUNCTION
# ==========================================================================================================================
def create_lambda_function(lambda_client, 
                           function_name, 
                           role, 
                           memory_size, 
                           image_uri):
    """
    Create AWS Lambda function with AWS ECR Docker image and specified memory

    Args:
        lambda_client (botocore.client.Lambda): boto3 client object for AWS Lambda
        function_name (str): Name of function to create
        role (str): Name of AWS role to use for the lambda function (i.e. 'lambda-role')
        memory_size (int): Amount of memory (in MB) to assign to this function
        image_uri (str): URI of Docker image on AWS ECR to use for the function

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """
    status = 'SUCCESS'

    # Create lambda function using the provided values
    print(f'\n\tCreating lambda function ({function_name}) with {memory_size} MB of memory')
    try:
        lambda_client.create_function(FunctionName=function_name,
                                      Role=role,
                                      PackageType='Image',
                                      Code={'ImageUri':image_uri},
                                      Publish=True,
                                      Timeout=900,
                                      MemorySize=memory_size)
    except Exception as e:
        status = f'\tFailed to create function: {function_name}, error: {e}'
        return status

    # Get function info for the current function until the State is "Active"
    print(f'\tVerifying lambda function creation ({function_name})...')
    while True:
        status = lambda_client.get_function_configuration(FunctionName=function_name)['State']
        if status == "Failed":
            status = f'\t\tFailed to create function ({function_name}). Try again'
            return status
        elif status == 'Active':
            print(f'\t\tFunction created successfully')
            break
        # Sleep for 2 seconds to not bombard the AWS API and to give time for the function to finish being created
        time.sleep(2)
    return status


# ==========================================================================================================================
# UPDATE LAMBDA FUNCTION
# ==========================================================================================================================
def update_lambda_function(lambda_client, 
                           function_name, 
                           image_uri):
    """
    Update AWS Lambda function with AWS ECR Docker image

    Args:
        lambda_client (botocore.client.Lambda): boto3 client object for AWS Lambda
        function_name (str): Name of function to update
        image_uri (str): URI of Docker image on AWS ECR to use to update function

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """
    status = 'SUCCESS'

    # Update lambda_function with current image_uri
    print(f'\n\tUpdating lambda function ({function_name})')
    try:
        lambda_client.update_function_code(FunctionName=function_name,
                                           ImageUri=image_uri)
    except Exception as e:
        status = f'ERROR updating lambda function ({function_name})\n\terror: {e}'
        return status

    # Get function info for the current function until the LastUpdateStatus is "Successful"
    print(f'\tVerifying lambda function update ({function_name})...')
    while True:
        last_update_status = lambda_client.get_function_configuration(FunctionName=function_name)['LastUpdateStatus']
        if last_update_status == "Failed":
            status = f'\t\tFailed to update function ({function_name}). Try again'
            return status
        elif last_update_status == 'Successful':
            print(f'\t\tFunction updated successfully')
            break
        # Sleep for 2 seconds to not bombard the AWS API and to give time for the function to finish updating
        time.sleep(2)
    return status


# ==========================================================================================================================
# LAMBDA INVOCATION
# ==========================================================================================================================
def invoke_lambda(lambda_client, 
                  job_logs, 
                  time_steps, 
                  dict_key_args, 
                  product_generation_config, 
                  aws_config, 
                  job, 
                  function_name_prefix, 
                  dimension, 
                  field_files, 
                  credentials):
    """
    Invoke the lambda function for the current job

    Args:
        lambda_client (botocore.client.Lambda): boto3 client object for AWS Lambda
        job_logs (dict): Dictionary containing information of each job and processing overall
        time_steps (list): List of raw time steps to process (i.e. ['0000000732', ...])
        dict_key_args (dict): Dictionary of command line arguments to master_scipt.py
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        aws_config (dict): Dictionary of aws_config.yaml config
        job (list): List (grouping_num, product_type, output_frequency, time_steps_to_process) for the current job
        function_name_prefix (str): String of the prefix for the function (i.e. 'ecco_processing')
        dimension (str): Dimension of current job dataset
        field_files (dict): Field files for current job and timesteps
        credentials (dict): Dictionary containaing credentials information for AWS

    Returns:
        num_jobs (int): Number of Lambda jobs submitted
    """
    (grouping_to_process, product_type, output_freq_code, time_steps_to_process) = job
    num_jobs = 0
    
    fields = list(field_files.keys())
    
    time_1D = aws_config['1D_time']
    time_2D_latlon = aws_config['latlon_2D_time']
    time_2D_native = aws_config['native_2D_time']
    time_3D_latlon = aws_config['latlon_3D_time']
    time_3D_native = aws_config['native_3D_time']

    number_of_batches_to_process = aws_config['number_of_batches_to_process']

    use_lambda = dict_key_args['use_lambda']

    # group number of time steps and files to process based on time to execute
    if product_type == '1D':
        exec_time_per_vl = time_1D
        function_name = f'{function_name_prefix}_1D'
    elif product_type == 'latlon':
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


    # ========== <Create job batches> =============================================================
    # Organize files and timesteps into batches with lengths calculated based on 
    # execution times provided in product_generation_config.yaml config file
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
        printc(f'Max execs is 0, this means you cannot process a single vertical level in less than 15 minutes. Skipping job', 'red')
        return num_jobs

    # If override_max_execs is given, use the lower value between the calculated max_execs
    # and the provided override_max_execs.
    if aws_config['override_max_execs'] != 0:
        max_execs = min([max_execs, aws_config['override_max_execs']])

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

    # Prompt user to approve job
    if dict_key_args['require_input']:
        create_lambdas = input(f'Would like to start executing the lambda jobs (y/n)?\t').lower()
        if create_lambdas != 'y':
            print('Skipping job')
            return num_jobs
        print()
    # ========== </Create job batches> =============================================================


    # ========== <Invoke Lambda job> ==============================================================
    if aws_config['use_workers_to_invoke']:
        # Invoke Lambda job batches using groups of 10 workers, invoking in parallel
        num_workers = 10
        times_and_fields_all = list(zip(time_steps_by_batch, field_files_by_batch.values()))
        times_and_fields_all = times_and_fields_all[:number_of_batches]
        print(f'Using {num_workers} workers to invoke {number_of_batches} lambda jobs')

        # invoke lambda function for workers (returns the passed argument and a number indicating 
        # if the worker successfully invoke the Lambda job or not)
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
                'aws_config': aws_config,
                'local': False,
                'use_lambda': use_lambda,
                'credentials': credentials
            }

            # define data to process for the current lambda job
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
                invoke_response = lambda_client.invoke(FunctionName=function_name,
                                                       InvocationType='Event',
                                                       Payload=json.dumps(payload))

                request_id = invoke_response['ResponseMetadata']['RequestId'].strip()

                # create job entry for request job in job_logs
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
            except Exception as e:
                printc(f'Lambda invoke error: {e}', 'red')
                printc(f'\tTime Steps: {time_steps}', 'red')
                return (times_and_fields, 0)
            return (times_and_fields, 1)

        # create workers and assign each one a batch of time_steps and 
        # field_files to invoke a Lambda job with
        with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_key = {executor.submit(fetch, times_and_fields) for times_and_fields in times_and_fields_all}

            # Go through return values for each completed worker
            for future in futures.as_completed(future_to_key):
                job = future.result()
                exception = future.exception()
                num_jobs += job[1]
                print(f'Lambda Job requested: {num_jobs:0>3}', end='\r')
                if exception:
                    printc(f'ERROR invoking lambda job: {job[0]}, ({exception})', 'red')
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
                'aws_config': aws_config,
                'local': False,
                'use_lambda': use_lambda,
                'credentials': credentials,
                'processing_code_filename': product_generation_config['processing_code_filename'],
                'use_workers_to_download': product_generation_config['use_workers_to_download']
            }

            # define data to process for the current lambda job
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
                print(f'Lambda Job requested: {num_jobs+1:0>4}', end='\r')
                invoke_response = lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',
                    Payload=json.dumps(payload),   
                )

                request_id = invoke_response['ResponseMetadata']['RequestId'].strip()

                # create job entry for request job in job_logs
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
        
                num_jobs += 1
            except Exception as e:
                printc(f'Lambda invoke error: {e}', 'red')
                printc(f'\tTime Steps: {time_steps}', 'red')
    # ========== </Invoke Lambda job> =============================================================

    return num_jobs

