import glob
import boto3
from concurrent import futures
from collections import defaultdict


# ==========================================================================================================================
# S3 FILES
# ==========================================================================================================================
def get_files_time_steps_s3(s3, fields, s3_dir_prefix, period_suffix, source_bucket, time_steps_to_process):
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

    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    time_steps_all_vars = []
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(source_bucket)
        field_files = defaultdict(list)
        field_time_steps = defaultdict(list)
        time_steps_all_vars = []

        # THREADED TECHNIQUE
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
                        status = 'ERROR getting field times/files'
                    else:
                        print(f'Got times/files: {field}')
                    
            if status != 'SUCCESS':
                return ((field_files, field_time_steps, time_steps_all_vars), status)

            time_steps_all_vars = sorted(time_steps_all_vars)

        # check that each field has the same number of times
        time_steps_all_vars = sorted(list(set(time_steps_all_vars)))
        skip_job = False
        for field in fields:
            if time_steps_all_vars == field_time_steps[field]:
                continue
            else:
                print(f'Unequal time steps for field "{field}". Skipping job')
                skip_job = True
        if skip_job:
            status = 'SKIP'
    except Exception as e:
        status = f'ERROR getting S3 files and timesteps for fields {fields}. Error: {e}'

    return ((field_files, field_time_steps, time_steps_all_vars), status)


def get_files_time_steps_local(fields, grouping_to_process, product_generation_config, freq_folder, period_suffix, num_time_steps_to_process, product_type, output_freq_code):
    status = 'SUCCESS'

    field_files = {}
    field_time_steps = {}
    all_time_steps_all_vars = []
    try:
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
        all_time_steps_all_vars = sorted(list(set(all_time_steps_all_vars)))
        skip_job = False
        for field in fields:
            if all_time_steps_all_vars == field_time_steps[field]:
                continue
            else:
                print(f'Unequal time steps for field "{field}". Skipping job')
                skip_job = True
        if skip_job:
            status = 'SKIP'
    except Exception as e:
        status = f'ERROR getting local files and timesteps for fields {fields}. Error: {e}'
    
    return ((field_files, field_time_steps, all_time_steps_all_vars), status)