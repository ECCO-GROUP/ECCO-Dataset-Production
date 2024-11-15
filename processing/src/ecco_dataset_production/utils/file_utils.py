"""
ECCO Dataset Production file utilities

Author: Duncan Bark

Contains functions for getting the available files either from S3 or from a local directory.

"""

import ast
import glob
import boto3
from concurrent import futures
from collections import defaultdict


# ==========================================================================================================================
# GET FILES and TIME STEPS
# ==========================================================================================================================
def get_files_time_steps(fields, 
                         period_suffix, 
                         time_steps_to_process, 
                         freq_folder, 
                         use_S3, 
                         curr_grouping,
                         model_output_dir=None, 
                         s3_dir_prefix=None, 
                         source_bucket=None,
                         derived_bucket=None):
    """
    Create lists of files and timesteps for each field from files present on S3 in source_bucket or local in model_output_dir

    Args:
        fields (list): List of field names
        period_suffix (str): Period suffix of files (i.e. 'mon_mean')
        time_steps_to_process (str/int/list): String 'all', an integer specifing the number of time
                                                steps, or a list of time steps to process
        freq_folder (str): Subfolder name relating to frequency (i.e. 'diags_monthly')
        use_S3 (bool): True/False if processing using AWS S3
        curr_grouping (dict): Dictionary containing the grouping information for the current dataset from its goupings.json metadata file
        model_output_dir (optional, str): String directory to model output (for local processing)
        s3_dir_prefix (optional, str): Prefix of files stored on S3 (i.e. 'V4r4/diags_monthly)
        source_bucket (optional, str): Name of S3 bucket
        derived_bucket (optional, str): Name of S3 bucket for derived products

    Returns:
        ((field_files,, time_steps_all_vars), status) (tuple):
            field_files (defaultdict(list)): Dictionary with field names as keys, and S3/local file paths for each timestep as values
            time_steps_all_vars (list): List of all unique timesteps to process
            status (str): String that is either "SUCCESS", "ERROR {error message}" or 'SKIP'
    """
    status = 'SUCCESS'
    field_files = {}
    field_time_steps = {}
    time_steps_all_vars = []

    # time_steps_to_process must either be the string 'all', a number corresponding to the total number
    # of time steps to process over all jobs (if using lambda) or overall (when local), or a list of timesteps indices
    if time_steps_to_process != 'all' and not isinstance(time_steps_to_process, int) and not isinstance(time_steps_to_process, list):
        status = f'ERROR Bad time steps provided ("{time_steps_to_process}")'
        return ((field_files, time_steps_all_vars), status)

    print(f'Getting timesteps and files for fields: {fields} for {time_steps_to_process} {period_suffix} timesteps')

    # get the fields to rotate, and the rotated fields from the curr_gouping if present
    # create a list of fields to source from source_bucket, and a list of all fields.
    # Those fields not in "fields" to source from source_bucket, will instead be sourced
    # from derived_bucket if present. If they are not present, they will be created during the job.
    fields_to_rotate = []
    rotated_fields = []
    all_fields = fields
    vector_rotate = False
    if 'vector_inputs' in curr_grouping:
        vector_rotate = True
        vector_inputs = curr_grouping['vector_inputs']

        # Get list of fields that are used in a rotation (fields_to_rotate)
        # Get list of fields that need to be created from a rotation (rotated_fields)
        for field, vector_fields in vector_inputs.items():
            fields_to_rotate.append(field)
            for vfield in vector_fields:
                if vfield not in rotated_fields:
                    rotated_fields.append(vfield)

        # Get list of fields that dont need to be made from a rotation
        # and add it to the list of fields to use when processing (new_fields)
        all_fields = []
        new_fields = []
        for field in fields:
            if field not in rotated_fields:
                new_fields.append(field)
            all_fields.append(field)

        # Get list of fields from fields_to_rotate to use when processing (new_fields)
        for field in fields_to_rotate:
            if field not in new_fields:
                new_fields.append(field)
            all_fields.append(field)
        fields = sorted(new_fields)
        all_fields = sorted(all_fields)

    field_paths = []
    field_paths_derived = []
    if use_S3:
        # Construct the list of field paths in S3
        # i.e. ['V4r4/diags_monthly/SSH_mon_mean', 'V4r4/diags_monthly/SSHIBC_mon_mean', ...]
        for field in all_fields:
            # field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')
            if field in fields:
                field_paths.append(f'{s3_dir_prefix}/{field}_{period_suffix}')
            elif field in rotated_fields:
                field_paths_derived.append(f'{s3_dir_prefix}/{field}_{period_suffix}')
    else:
        # Construct the list of field paths for local
        # i.e. ['aws/tmp/tmp_model_output/V4r4/diags_mothly/SSH_mon_mean', 'aws/tmp/tmp_model_output/V4r4/diags_mothly/SSHIBC_mon_mean', ...]
        for field in fields:
            field_paths.append(f'{model_output_dir}/{freq_folder}/{field}_{period_suffix}')

    # ========== <Get files> ==============================================================
    try:
        # setup AWS clients
        if use_S3:
            s3 = boto3.resource('s3')
            source_bucket = s3.Bucket(source_bucket)
            derived_bucket = s3.Bucket(derived_bucket)
        else:
            s3 = None
            source_bucket = None
            derived_bucket = None

        # get files for roated_fields if present in the "derived_bucket" on S3
        if vector_rotate:
            all_files, status = __get_files_from_field_path(fields, derived_bucket, field_paths_derived, time_steps_to_process)
            field_files = all_files['field_files']
            field_time_steps = all_files['field_time_steps']
            time_steps_all_vars.extend(all_files['time_steps'])
            if status != 'SUCCESS':
                return ((field_files, time_steps_all_vars), status)

        # get files for fields if present either locally or via the "source_bucket" in S3 
        all_files, status = __get_files_from_field_path(fields, source_bucket, field_paths, time_steps_to_process)
        field_files = all_files['field_files']
        field_time_steps = all_files['field_time_steps']
        time_steps_all_vars.extend(all_files['time_steps'])
        if status != 'SUCCESS':
            return ((field_files, time_steps_all_vars), status)

        # sort list of all timesteps
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

        # Compare number of found time steps and requested time steps. Error out if they are not equal
        if type(time_steps_to_process) == list:
            if len(time_steps_all_vars) < len(time_steps_to_process):
                status = f'Found unequal number of files for requested jobs {time_steps_all_vars} vs {time_steps_to_process} requested'
        elif len(time_steps_all_vars) < time_steps_to_process:
                status = f'Found unequal number of files for requested jobs {time_steps_all_vars} vs {time_steps_to_process} requested'


    except Exception as e:
        status = f'ERROR getting files and timesteps for fields {fields}. Error: {e}'
    # ========== </Get files> =============================================================

    return ((field_files, time_steps_all_vars), status)


def __get_files_from_field_path(fields, bucket, field_paths, time_steps_to_process):
    status = 'SUCCESS'

    # Collect files from S3/local for each field, where each field is collected in parallel by a separate worker
    num_workers = len(fields)
    print(f'Using {num_workers} workers to get time steps and files for {len(fields)} fields')

    # Define dictionary for field files and timesteps
    field_files = {}
    field_time_steps = {}
    time_steps_all_vars = []

    # ========== <Workers fetch function> =====================================================
    # get files function
    def fetch(field_info):
        # get the field name and field path from field_info
        field, field_path = field_info

        if bucket:
            # loop through all the objects in the source_bucket with a prefix matching the s3_field_path
            # and append those with .data to the list of field files (along with the timestep)
            curr_field_files = []
            curr_field_time_steps = []
            for obj in bucket.objects.filter(Prefix=field_path):
                filename = obj.key
                if '.meta' in filename:
                    continue
                curr_field_files.append(filename)
                curr_field_time_steps.append(filename.split('.')[-2])
        else:
            # Glob all .data files in field_path directory
            # and get the timesteps from those files
            curr_field_files = glob.glob(f'{field_path}/*.data')
            curr_field_time_steps = [key.split('.')[-2] for key in curr_field_files]

        # sort field files and timesteps
        curr_field_files = sorted(curr_field_files)
        curr_field_time_steps = sorted(curr_field_time_steps)

        # collect the requested field files and time steps per time_steps_to_process
        all_files = __get_files_helper(field, 
                                        time_steps_to_process, 
                                        curr_field_files, 
                                        curr_field_time_steps)

        field_files[field], field_time_steps[field], curr_time_steps_all_vars, status = all_files
        if status != 'SUCCESS':
            raise Exception(status)
        
        time_steps_all_vars.extend(curr_time_steps_all_vars)

        return field, status
    # ========== </Workers fetch function> ====================================================

    # create workers and assign each one a field to look for times and files for.
    # Each worker is assigned a field name, and an S3 prefix pointing to a specific field (i.e. "V4r4/diags_monthly/SSH_mon_mean")
    # or (if local) a local directory (i.e. "aws/tmp/tmp_model_output/V4r4/diags_mothly/SSH_mon_mean")
    # field info = (field name, field_path)
    with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_key = {executor.submit(fetch, field_info) for field_info in zip(fields, field_paths)}

        # Go through return values for each completed worker
        for future in futures.as_completed(future_to_key):
            (field, status) = future.result()
            exception = future.exception()
            if exception:
                status = f'ERROR getting field times/files: {field} ({status})'
            else:
                print(f'Got times/files: {field}')
                # field_files[field] = all_fields['field_files']
                # field_time_steps[field] = all_fields['field_time_steps']
                # time_steps_all_vars.extend(all_fields['time_steps'])
    
    all_files = {'field_files': field_files,
                 'field_time_steps': field_time_steps,
                 'time_steps': time_steps_all_vars}
    
    return all_files, status
                

def __get_files_helper(field, 
                       time_steps_to_process, 
                       curr_field_files, 
                       curr_field_time_steps):
    """
    Helps get_files_time_steps function. Takes list of files and time steps, and returns
        dictionaries with fields as keys and files/timesteps as values for time steps specified
        by time_steps_to_process

    Args:
        field (str): Field name
        time_steps_to_process (str/int/list): String 'all', an integer specifing the number of time
                                                steps, or a list of time steps to process
        curr_field_files (list): List of all files for the current field available in S3 or locally
        curr_field_time_steps (list): Timesteps of all files for the current field available in S3 or locally

    Returns:
        ((field_files[field], field_time_steps[field], time_steps, status) (tuple):
            field_files[field] (list): List of field files to process according to time_steps_to_process
            field_time_steps[field] (list): List of field timesteps to process according to time_steps_to_process
            time_steps (list): List of all unique timesteps to process
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """
    status = 'SUCCESS'
    field_files = defaultdict(list)
    field_time_steps = defaultdict(list)
    time_steps = []

    # if 'all' timesteps are wanted, add all of the file timesteps to field_files and time_steps
    if time_steps_to_process == 'all':
        field_files[field] = curr_field_files
        field_time_steps[field] = curr_field_time_steps
        time_steps.extend(curr_field_time_steps)
    # else if time_steps_to_process is an int, add that many files to field_files and time_steps
    elif isinstance(time_steps_to_process, int):
        field_files[field] = curr_field_files[:time_steps_to_process]
        curr_field_time_steps = curr_field_time_steps[:time_steps_to_process]
        field_time_steps[field] = curr_field_time_steps
        time_steps.extend(curr_field_time_steps)
    # else if time_steps_to_process is a list, add the files corresponding to those indices/values
    elif isinstance(time_steps_to_process, list):
        for ts_val in time_steps_to_process:
            # if the list is a list of raw time values, get the index of the time step
            if isinstance(ts_val, str):
                if ts_val in curr_field_time_steps:
                    ts_ind = curr_field_time_steps.index(ts_val)
                else:
                    status = f'ERROR Unable to find timestep {ts_val} in list of time steps'
                    break
            else:
                ts_ind = ts_val
            if ts_ind >= len(curr_field_files):
                break
            field_files[field].append(curr_field_files[ts_ind])
            field_time_steps[field].append(curr_field_time_steps[ts_ind])
            time_steps.append(curr_field_time_steps[ts_ind])

    # sort names and timesteps
    field_files[field] = sorted(field_files[field])
    field_time_steps[field] = sorted(field_time_steps[field])

    # all_files = {'field_files': field_files[field],
    #              'field_time_steps': field_time_steps[field],
    #              'time_steps': time_steps,
    #              'status': status}

    return (field_files[field], field_time_steps[field], time_steps, status)