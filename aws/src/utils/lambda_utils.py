import time
import json
from concurrent import futures

# ==========================================================================================================================
# LAMBDA FUNCTION CREATION and UPDATING
# ==========================================================================================================================
def update_lambda_function(client, function_name, image_uri):
    status = 'SUCCESS'

    # Update lambda_function with current image_uri
    print(f'\n\tUpdating lambda function ({function_name})')
    try:
        client.update_function_code(
            FunctionName=function_name,
            ImageUri=image_uri
        )
    except Exception as e:
        status = f'ERROR updating lambda function ({function_name})\n\terror: {e}'
        return status

    print(f'\tVerifying lambda function update ({function_name})...')
    while True:
        last_update_status = client.get_function_configuration(FunctionName=function_name)['LastUpdateStatus']
        if last_update_status == "Failed":
            status = f'\t\tFailed to update function ({function_name}). Try again'
            return status
        elif last_update_status == 'Successful':
            print(f'\t\tFunction updated successfully')
            break
        time.sleep(2)
    return status


def create_lambda_function(client, function_name, role, memory_size, image_uri):
    status = 'SUCCESS'
    # Create lambda function using the provided values
    print(f'\n\tCreating lambda function ({function_name}) with {memory_size} MB of memory')
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
        status = f'\tFailed to create function: {function_name}, error: {e}'
        return status

    print(f'\tVerifying lambda function creation ({function_name})...')
    while True:
        status = client.get_function_configuration(FunctionName=function_name)['State']
        if status == "Failed":
            status = f'\t\tFailed to create function ({function_name}). Try again'
            return status
        elif status == 'Active':
            print(f'\t\tFunction created successfully')
            break
        time.sleep(2)
    
    return status


# ==========================================================================================================================
# LAMBDA INVOKATION
# ==========================================================================================================================
def invoke_lambda(lambda_client, job_logs, time_steps, dict_key_args, product_generation_config, aws_config_metadata, current_job, function_name_prefix, dimension, field_files, credentials, total_num_jobs, debug_mode):
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
                total_num_jobs += job[1]
                print(f'Lambda Job requested: {num_jobs:4} ({total_num_jobs:4})', end='\r')
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

