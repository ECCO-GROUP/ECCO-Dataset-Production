"""
Created May 18, 2022

Author: Duncan Bark
Adapted from ifenty's "eccov4r4_gen_for_podaac.py"

Main code file for processing. This file takes a payload specifing timesteps and files to process (among other information), and processes said files to produce output datasets

"""

import os
import sys
import json
import time
import boto3
import shutil
import hashlib
import logging
import traceback
import numpy as np
import pandas as pd
import xarray as xr
import netCDF4 as nc4
from pathlib import Path

# Local imports
main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
import ecco_v4_py as ecco
import gen_netcdf_utils as gen_netcdf_utils

# =================================================================================================
# PRINT LOGGING INFO
# =================================================================================================
def logging_info(time_steps_to_process, 
                 successful_time_steps, 
                 start_time, 
                 total_download_time, 
                 num_downloaded, 
                 total_netcdf_time, 
                 total_upload_time, 
                 num_uploaded, 
                 succeeded_checksums, 
                 total_checksum_time, 
                 logger, 
                 timeout):
    """
    Prints all the logging info (time, number of files, successes, fails, etc.)

    Args:
        time_steps_to_process (list): List of all timesteps sent to the job to process 
        successful_time_steps (list): List of timesteps successfully processed
        start_time (int): Time of master script start in ms since 1970
        total_download_time (float): Total time spent downloading files (sec)
        num_downloaded (int): Total number of downloaded files
        total_netcdf_time (float): Total time spent creating netCDF files (sec)
        total_upload_time (float): Total time spent uploading files to S3 (sec)
        num_uploaded (int): Total number of files uploaded to S3 
        succeeded_checksums (dict): Dictionary of key=timestep and value=Dictionary of 's3_fname', 'checksum' and 'uuid'
        total_checksum_time (float): Total time spent creating and checking checksums (sec) 
        logger (bool): Boolean for whether or not to use the python logger.info() for the log prints
        timeout (bool): Boolean for whether or not the current job reached the timeout time

    Returns:
        None
    """

    # Calculate total time values
    script_time = time.time() - start_time
    script_time -= (total_download_time + total_netcdf_time + total_upload_time)
    IO_time = total_download_time + total_netcdf_time + total_upload_time
    total_time = script_time + IO_time
    print()
    print('='*25 + ' EXECUTION COMPLETE ' + '='*25)

    # Append each print statement to a list, to be printed out as a batch
    print_messages = []
    print_messages.append(f'DURATION\tTOTAL\t{total_time}\tseconds')
    print_messages.append(f'DURATION\tSCRIPT\t{script_time}\tseconds')
    print_messages.append(f'DURATION\tIO\t{IO_time}\tseconds')
    print_messages.append(f'DURATION\tDOWNLOAD\t{total_download_time}\tseconds')
    print_messages.append(f'DURATION\tNETCDF\t{total_netcdf_time}\tseconds')
    print_messages.append(f'DURATION\tUPLOAD\t{total_upload_time}\tseconds')
    print_messages.append(f'DURATION\tCHECKSUM\t{total_checksum_time}\tseconds')
    print_messages.append(f'FILES\tDOWNLOAD\t{num_downloaded}')
    print_messages.append(f'FILES\tUPLOAD\t{num_uploaded}')

    # Include a TIMEOUT print if the job timeout
    if timeout:
        print_messages.append('TIMEOUT')

    # Include a SUCCEEDED print out with the checksums
    print_messages.append(f'SUCCEEDED\t{succeeded_checksums}')

    # If there are timesteps that failed, include a print out for those,
    # otherwise include a SUCCESS print out
    if not time_steps_to_process == successful_time_steps:
        failed_time_steps = list(set(time_steps_to_process) ^ set(successful_time_steps))
        print_messages.append(f'FAILED\t{failed_time_steps}')
        print_messages.append('PARTIAL')
    else:
        print_messages.append('SUCCESS')

    # Include an END printout
    print_messages.append('END')

    # Print the logging messages with the python logger.info() or just to screen
    for msg in print_messages:
        if logger != False:
            logger.info(msg)
        else:
            print(msg)

    return


def generate_netcdfs(event):
    """
    Primary processing function. Gathers the metadata, files, transforms granules, and applies metadata
    for the passed job

    Args:
        event (dict): Contains all the information required to process the passed job:
            grouping_to_process (int): Grouping number from groupings json file for current dataset
            product_type (str): String product type (i.e. 'latlon', 'native')
            output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
            time_steps_to_process (list): List of timesteps to process for the current job
            field_files (defaultdict(list)): Dictionary with field names as keys, and S3/local file paths for each timestep as values
            product_generation_config (dict): Dictionary of product_generation_config.yaml config file
            aws_config (dict): Dictionary of aws_config.yaml config file
            local (bool): Boolean for whether or not processing is to occur locally (no S3, no Lambda)
            use_lambda (bool): Boolean for whether or not processing is to occur on Lambda
            credentials (dict): Dictionary containaing credentials information for AWS
            processing_code_filename (only for lambda, str): Name of this file, used to call it from the lambda_code app.py file

    Returns:
        None
    """
    job_start_time = time.time()
    timeout = False

    # Logging values
    start_time = time.time()
    total_netcdf_time = 0
    total_download_time = 0
    num_downloaded = 0
    total_upload_time = 0
    num_uploaded = 0
    total_checksum_time = 0
    succeeded_checksums = {}

    # Pull variables from "event" argument
    grouping_to_process = event['grouping_to_process']
    product_type = event['product_type']
    output_freq_code = event['output_freq_code']
    time_steps_to_process = event['time_steps_to_process']
    field_files = event['field_files']
    product_generation_config = event['product_generation_config']
    aws_config = event['aws_config']
    local = event['local']
    use_lambda = event['use_lambda']
    credentials = event['credentials']

    # get list of fields to process
    fields_to_load = list(field_files.keys())

    try:
        # Fix paths. Serializing JSON doesnt allow for PosixPath variables, so they need to be remade
        if use_lambda:
            # Lambda paths are based off of the parent folder
            product_generation_config['mapping_factors_dir'] = Path(__file__).parent / 'mapping_factors'
            product_generation_config['metadata_dir'] = Path(__file__).parent / 'metadata'
            product_generation_config['ecco_grid_dir'] = Path(__file__).parent / 'ecco_grids'
            product_generation_config['ecco_grid_dir_mds'] = Path(__file__).parent / 'ecco_grids'
            product_generation_config['model_output_dir'] = Path('/tmp') / 'diags_all'
            product_generation_config['processed_output_dir_base'] = Path('/tmp') / 'temp_output'
        else:
            # Local paths are based off of the passed directories (or defaults)
            product_generation_config['mapping_factors_dir'] = Path(product_generation_config['mapping_factors_dir'])
            product_generation_config['metadata_dir'] = Path(product_generation_config['metadata_dir'])
            product_generation_config['ecco_grid_dir'] = Path(product_generation_config['ecco_grid_dir'])
            product_generation_config['ecco_grid_dir_mds'] = Path(product_generation_config['ecco_grid_dir_mds'])
            product_generation_config['model_output_dir'] = Path(product_generation_config['model_output_dir'])
            product_generation_config['processed_output_dir_base'] = Path(product_generation_config['processed_output_dir_base'])
        
        # Prepare variables with values from product_generation_config that are used more than once
        extra_prints = product_generation_config['extra_prints']
        create_checksum = product_generation_config['create_checksum']
        mapping_factors_dir = product_generation_config['mapping_factors_dir']
        processed_output_dir_base = product_generation_config['processed_output_dir_base']
        download_all_fields = product_generation_config['download_all_fields']
        ecco_grid_dir_mds = product_generation_config['ecco_grid_dir_mds']
        read_ecco_grid = product_generation_config['read_ecco_grid_for_native_load']

        print('\nBEGIN generate_netcdfs')
        print('OFC', output_freq_code)
        print('PDT', product_type)
        print('GTP', grouping_to_process)
        print('TSP', time_steps_to_process)
        print('')

        # Setup S3
        if 'source_bucket' in aws_config and 'output_bucket' in aws_config:
            buckets = (aws_config['source_bucket'], aws_config['output_bucket'])
            if not local and buckets != None and credentials != None:
                # boto3.setup_default_session(profile_name=aws_config['profile_name'])
                s3 = boto3.client('s3')
                model_granule_bucket, processed_data_bucket = buckets
        elif not local:
            status = f'ERROR No bucket names in aws_config:\n{aws_config}'
            raise Exception(status)

        # Create processed_output_dir_base directory if using S3 (and not Lambda)
        if not local and not use_lambda:
            if not os.path.exists(processed_output_dir_base):
                os.makedirs(processed_output_dir_base, exist_ok=True)

        # Define fill values for binary and netcdf
        # ECCO always uses -9999 for missing data.
        binary_fill_value = product_generation_config['binary_fill_value']
        if product_generation_config['array_precision'] == 'float32':
            # binary_output_dtype = '>f4'
            array_precision = np.float32
            netcdf_fill_value = nc4.default_fillvals['f4']
        else:
            # binary_output_dtype = '>f8'
            array_precision = np.float64
            netcdf_fill_value = nc4.default_fillvals['f8']

        # num of depth levels
        nk = product_generation_config['num_vertical_levels']

        ecco_start_time = np.datetime64(product_generation_config['model_start_time'])
        ecco_end_time   = np.datetime64(product_generation_config['model_end_time'])

        # list of successful timesteps, used for logging and job resubmission for lambdas
        successful_time_steps = []

        # if using lambda, setup logger
        logger = False
        if use_lambda:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)


        # ========== <Metadata setup> =============================================================
        # Define tail for dataset description (summary)
        dataset_description_tail_native = product_generation_config['dataset_description_tail_native']
        dataset_description_tail_latlon = product_generation_config['dataset_description_tail_latlon']

        # Get .json metadata file names from metadata directory for current ecco_version
        metadata_fields = [f[:-5] for f in os.listdir(product_generation_config['metadata_dir']) if '.json' in f]

        # load METADATA
        if extra_prints: print('\nLOADING METADATA')
        metadata = {}

        # Open and load each metadata file into a dictionary where it's key is the filename
        for mf in metadata_fields:
            mf_e = mf + '.json'
            if extra_prints: print(mf_e)
            with open(str(Path(product_generation_config['metadata_dir']) / mf_e), 'r') as fp:
                metadata[mf] = json.load(fp)

        # collect all metadata files into a single dictionary with simple names as keys
        variable_metadata_native = metadata['variable_metadata'] + metadata['geometry_metadata_for_native_datasets']

        all_metadata = {'var_native': variable_metadata_native, 
                        'var_latlon': metadata['variable_metadata_for_latlon_datasets'], 
                        'coord_1D': metadata['coordinate_metadata_for_1D_datasets'],
                        'coord_native': metadata['coordinate_metadata_for_native_datasets'], 
                        'coord_latlon': metadata['coordinate_metadata_for_latlon_datasets'], 
                        'global_all': metadata['global_metadata_for_all_datasets'], 
                        'global_native': metadata['global_metadata_for_native_datasets'], 
                        'global_latlon': metadata['global_metadata_for_latlon_datasets'],
                        'geometry_native': metadata['geometry_metadata_for_native_datasets'],
                        'geometry_latlon': metadata['geometry_metadata_for_latlon_datasets'],
                        'groupings_1D': metadata['groupings_for_1D_datasets'],
                        'groupings_native': metadata['groupings_for_native_datasets'],
                        'groupings_latlon': metadata['groupings_for_latlon_datasets']}
        # ========== </Metadata setup> ============================================================


        # ========== <Native/Latlon setup> ========================================================
        if extra_prints: print('\nproduct type', product_type)
        if product_type == 'native':
            dataset_description_tail = dataset_description_tail_native
            groupings = all_metadata['groupings_native']
            output_dir_type = processed_output_dir_base / 'native'
            status, latlon_grid = gen_netcdf_utils.get_latlon_grid(mapping_factors_dir)
        elif product_type == 'latlon':
            dataset_description_tail = dataset_description_tail_latlon
            groupings = all_metadata['groupings_latlon']
            output_dir_type = processed_output_dir_base / 'lat-lon'
            status, latlon_grid = gen_netcdf_utils.get_latlon_grid(mapping_factors_dir)
        if status != 'SUCCESS':
            raise Exception(status)
        # ========== </Native/Latlon setup> =======================================================


        # ========== <Groupings> ==================================================================
        # determine which grouping to process
        if extra_prints: print('\nDetermining grouping to process')
        grouping = []
        if extra_prints: print('... using provided grouping ', grouping_to_process)
        grouping_num = grouping_to_process

        grouping = groupings[grouping_num]
        if extra_prints: print('... grouping to use ', grouping['name'])
        if extra_prints: print('... fields in grouping ', grouping['fields'])

        # dimension of dataset
        dataset_dim = grouping['dimension']
        if extra_prints: print('... grouping dimension', dataset_dim)
        # ========== </Groupings> =================================================================


        # ========== <Directories and File Paths> =================================================
        if extra_prints: print('\nGetting directories for group fields')
        if output_freq_code == 'AVG_DAY':
            period_suffix = 'day_mean'
            dataset_description_head = 'This dataset contains daily-averaged '

        elif output_freq_code == 'AVG_MON':
            period_suffix = 'mon_mean'
            dataset_description_head = 'This dataset contains monthly-averaged '

        elif output_freq_code == 'SNAPSHOT':
            period_suffix = 'day_inst'
            dataset_description_head = 'This dataset contains instantaneous '
        else:
            status = f'ERROR Invalid output_freq_code provided ("{output_freq_code}")'
            print(f'FAIL ALL')
            raise Exception(status)

        if extra_prints: print('...output_freq_code ', output_freq_code)

        output_dir_freq = output_dir_type / period_suffix
        if extra_prints: print('...making output_dir freq ', output_dir_freq)
        
        # make output directory
        if not output_dir_freq.exists():
            try:
                output_dir_freq.mkdir(parents=True)
            except:
                status = f'ERROR Cannot make output directory "{output_dir_freq}"'
                print(f'FAIL ALL')
                raise Exception(status)

        # create dataset description head
        dataset_description = dataset_description_head + grouping['name'] + dataset_description_tail
        # ========== </Directories and File Paths> ================================================


        # ========== <Process each time level> ====================================================
        # load ECCO grid and land masks
        ecco_grid = xr.open_dataset(Path(product_generation_config['ecco_grid_dir']) / product_generation_config['ecco_grid_filename'])
        ecco_land_mask_c  = ecco_grid.maskC.copy(deep=True)
        ecco_land_mask_c.values = np.where(ecco_land_mask_c==True, 1, np.nan)
        ecco_land_mask_w  = ecco_grid.maskW.copy(deep=True)
        ecco_land_mask_w.values = np.where(ecco_land_mask_w==True, 1, np.nan)
        ecco_land_mask_s  = ecco_grid.maskS.copy(deep=True)
        ecco_land_mask_s.values = np.where(ecco_land_mask_s==True, 1, np.nan)
        ecco_land_masks = (ecco_land_mask_c, ecco_land_mask_w, ecco_land_mask_s)
        if extra_prints: print(ecco_grid)

        if extra_prints: print('\nLooping through time levels')
        for cur_ts_i, cur_ts in enumerate(time_steps_to_process):
            data_file_paths = {}
            meta_file_paths = {}
            try:
                # Check if the current Lambda job execution as reached the user defined timeout time
                if use_lambda and time.time() - job_start_time >= aws_config['job_timeout']:
                    timeout = True
                    raise Exception('TIMEOUT')

                # ========== <Calculate times> ====================================================
                print('\n\n=== TIME LEVEL ===', str(cur_ts_i).zfill(5), cur_ts)
                if extra_prints: print('\n')
                time_delta = np.timedelta64(int(cur_ts), 'h')
                cur_time = ecco_start_time + time_delta
                times = [pd.to_datetime(str(cur_time))]

                if 'AVG' in output_freq_code:
                    tb, record_center_time = ecco.make_time_bounds_from_ds64(np.datetime64(times[0]), 
                                                                             output_freq_code)
                    if extra_prints: print('ORIG  tb, ct ', tb, record_center_time)

                    # fix beginning of last record
                    if tb[1].astype('datetime64[D]') == ecco_end_time.astype('datetime64[D]'):
                        if extra_prints: print('end time match ')
                        time_delta = np.timedelta64(12,'h')
                        rec_avg_start = tb[0] + time_delta
                        rec_avg_end = tb[1]
                        rec_avg_delta = rec_avg_end - rec_avg_start
                        rec_avg_middle = rec_avg_start + rec_avg_delta/2

                        tb[0] = rec_avg_start
                        record_center_time = rec_avg_middle

                    # truncate to ecco_start_time
                    if tb[0].astype('datetime64[D]') == ecco_start_time.astype('datetime64[D]'):
                        if extra_prints: print('start time match ')
                        rec_avg_start = ecco_start_time
                        rec_avg_end = tb[1]
                        rec_avg_delta = tb[1] - ecco_start_time
                        rec_avg_middle = rec_avg_start + rec_avg_delta/2

                        tb[0] = ecco_start_time
                        record_center_time = rec_avg_middle

                    record_start_time = tb[0]
                    record_end_time = tb[1]
                    if extra_prints: print('FINAL tb, ct ', tb, record_center_time)

                else:
                    #snapshot, all times are the same
                    if extra_prints: print(times)
                    if extra_prints: print(type(times[0]))

                    record_start_time = np.datetime64(times[0])
                    record_end_time = np.datetime64(times[0])
                    record_center_time = np.datetime64(times[0])

                record_times = {'start':record_start_time, 'center':record_center_time, 'end':record_end_time}
                # ========== </Calculate times> ===================================================


                # ========== <Download files> ==================================================
                F_DS_vars = []

                # Download field file(s)
                # If 'download_all_fields' in product_generation_config.yaml is True, then all field files
                # for the current time step will be downloaded, otherwise each field file is downloaded 
                # and processed one at a time
                if not local and download_all_fields:
                    print(f'Downloading all files for current timestep')
                    s3_download_start_time = time.time()
                    (status, (all_files)) = gen_netcdf_utils.download_all_files(s3, 
                                                                                fields_to_load, 
                                                                                field_files, 
                                                                                cur_ts, 
                                                                                data_file_paths, 
                                                                                meta_file_paths, 
                                                                                product_generation_config, 
                                                                                product_type, 
                                                                                model_granule_bucket)

                    data_file_paths, meta_file_paths, curr_num_downloaded = all_files
                    num_downloaded += curr_num_downloaded

                    if status != 'SUCCESS':
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)
                    total_download_time += (time.time() - s3_download_start_time)
                else:
                    print(f'Downloading and processing fields one at a time for current timestep')

                # Get data_file_paths for local files when processing locally
                if local:
                    for field in fields_to_load:
                        curr_field_files = field_files[field]
                        for field_file in curr_field_files:
                            if cur_ts in field_file:
                                if '.data' in field_file:
                                    data_file_paths[field] = field_file
                                break
                # ========== </Download files> ====================================================


                # ============================== TODO =============================================
                # ========== <Vector rotation> ====================================================
                # PERFORM VECTOR ROTATION AS NECESSARY
                if 'vector_inputs' in grouping:
                    grouping['vector_inputs'] = ['UVEL', 'VVEL']
                    # load specified field files from the provided directory
                    # This loads them into the native tile grid
                    F_DSs = []
                    for vec_field in grouping['vector_inputs']:
                        status, F_DS = ecco.load_ecco_vars_from_mds(
                                            Path(data_file_paths[vec_field]).parent,
                                            mds_grid_dir = download_all_fields,
                                            mds_files = Path(data_file_paths[vec_field]).name.split('.')[0],
                                            vars_to_load = vec_field,
                                            drop_unused_coords = True,
                                            grid_vars_to_coords = False,
                                            output_freq_code = output_freq_code,
                                            model_time_steps_to_load = int(cur_ts),
                                            less_output = True,
                                            read_grid = read_ecco_grid)
                        print(status)
                        F_DSs.append(F_DS)
                # ========== </Vector rotation> ===================================================
                # ============================== TODO =============================================


                # ========== <Field transformations> ==============================================
                # Load fields and place them in the dataset
                for i, field in enumerate(sorted(fields_to_load)):
                    if not local and not download_all_fields:
                        s3_download_start_time = time.time()
                        (status, (all_files)) = gen_netcdf_utils.download_all_files(s3, 
                                                                                    [field], 
                                                                                    field_files, 
                                                                                    cur_ts, 
                                                                                    data_file_paths, 
                                                                                    meta_file_paths, 
                                                                                    product_generation_config, 
                                                                                    product_type, 
                                                                                    model_granule_bucket)
                                                                                  
                        data_file_paths, meta_file_paths, curr_num_downloaded = all_files
                        num_downloaded += curr_num_downloaded

                        if status != 'SUCCESS':
                            print(f'FAIL {cur_ts}')
                            raise Exception(status)
                        total_download_time += (time.time() - s3_download_start_time)

                    # Check if the current Lambda job execution as reached the user defined timeout time
                    if use_lambda and time.time() - job_start_time >= aws_config['job_timeout']:
                        timeout = True
                        raise Exception('TIMEOUT')
                        
                    data_file_path = Path(data_file_paths[field])
                        
                    # Transform latlon vs native variable
                    if product_type == 'latlon':
                        status, F_DS = gen_netcdf_utils.transform_latlon(ecco, 
                                                                         ecco_grid.Z.values, 
                                                                         latlon_grid, 
                                                                         data_file_path, 
                                                                         record_end_time, 
                                                                         nk, 
                                                                         dataset_dim, 
                                                                         field, 
                                                                         output_freq_code, 
                                                                         mapping_factors_dir, 
                                                                         extra_prints=extra_prints)
                    elif product_type == 'native':
                        status, F_DS = gen_netcdf_utils.transform_native(ecco, 
                                                                         field, 
                                                                         ecco_land_masks, 
                                                                         ecco_grid_dir_mds, 
                                                                         data_file_path, 
                                                                         output_freq_code, 
                                                                         cur_ts, 
                                                                         read_ecco_grid, 
                                                                         extra_prints=extra_prints)
                    
                    # If latlon/native transformation failed, FAIL the current timestep
                    if status != 'SUCCESS':
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)

                    # Apply global DS changes (coords, values, etc.)
                    status, F_DS = gen_netcdf_utils.global_DS_changes(F_DS, 
                                                                      output_freq_code, 
                                                                      grouping, 
                                                                      array_precision, 
                                                                      ecco_grid, 
                                                                      latlon_grid, 
                                                                      netcdf_fill_value, 
                                                                      record_times, 
                                                                      extra_prints=extra_prints)
                    if status != 'SUCCESS':
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)

                    # add this dataset to F_DS_vars and repeat for next variable
                    F_DS_vars.append(F_DS)
                # ========== </Field transformations> =============================================
                

                # ========== <Create output dataset> ==============================================
                # merge the data arrays to make one DATASET
                print('\n... merging F_DS_vars')
                G = xr.merge((F_DS_vars))

                # delete F_DS_vars from memory
                del(F_DS_vars)

                # Set metadata for output dataset
                status, G, netcdf_filename, encoding = gen_netcdf_utils.set_metadata(ecco, 
                                                                                     G, 
                                                                                     all_metadata, 
                                                                                     output_freq_code, 
                                                                                     netcdf_fill_value, 
                                                                                     grouping, 
                                                                                     output_dir_freq, 
                                                                                     dataset_description, 
                                                                                     product_generation_config, 
                                                                                     extra_prints=extra_prints)
                if status != 'SUCCESS':
                    print(f'FAIL {cur_ts}')
                    raise Exception(status)

                # Save dataset
                netcdf_start_time = time.time()
                print('\n... saving to netcdf ', netcdf_filename)
                G.load()
                G.to_netcdf(netcdf_filename, 
                            encoding=encoding)
                orig_uuid = G.attrs['uuid']
                G.close()
                total_netcdf_time += (time.time() - netcdf_start_time)
                if extra_prints: print('\n... checking existence of new file: ', netcdf_filename.exists())

                # create checksum of processed netcdf file
                if create_checksum:
                    checksum_time = time.time()
                    hash_md5 = hashlib.md5()
                    with open(netcdf_filename, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            hash_md5.update(chunk)
                    orig_checksum = hash_md5.hexdigest()
                    total_checksum_time += (time.time() - checksum_time)
                # ========== </Create output dataset> =============================================


                # ========== <Upload to S3> =======================================================
                # Upload output netcdf to s3
                if not local:
                    s3_upload_start_time = time.time()
                    print('\n... uploading new file to S3 bucket')
                    name = str(netcdf_filename).replace(f'{str(processed_output_dir_base)}/', 
                                                        f'{aws_config["bucket_subfolder"]}/')
                    try:
                        # upload "netcdf_filename" to AWS S3 bucket "processed_data_bucket" with name "name"
                        response = s3.upload_file(str(netcdf_filename), 
                                                  processed_data_bucket, 
                                                  name)
                        if extra_prints: print(f'\n... uploaded {netcdf_filename} to bucket {processed_data_bucket}')
                    except:
                        # delete file if it failed to upload, and FAIL the current timestep
                        os.remove(netcdf_filename)
                        status = f'ERROR Unable to upload file {netcdf_filename} to bucket {processed_data_bucket} ({response})'
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)

                    # delete file from local Lambda environment after uploading
                    if use_lambda:
                        os.remove(netcdf_filename)

                    total_upload_time += (time.time() - s3_upload_start_time)
                    num_uploaded += 1

                    # ========== <Compare checksums> ==============================================
                    # create checksum of downloaded dataset from S3 and compare to the checksum created of the
                    # dataset file uploaded to S3 (in ensure there was no issue uploading the file)
                    if product_generation_config['compare_checksums'] and create_checksum:
                        checksum_time = time.time()
                        new_netcdf = netcdf_filename.parent / f'new_{netcdf_filename.name}'

                        # download "name" file from AWS S3 bucket "processed_data_bucket" to local "new_netcdf" file
                        s3.download_file(processed_data_bucket, 
                                         str(name), 
                                         str(new_netcdf))

                        # create checksum of downloaded netcdf file
                        hash_md5 = hashlib.md5()
                        with open(new_netcdf, 'rb') as f:
                            for chunk in iter(lambda: f.read(4096), b""):
                                hash_md5.update(chunk)
                        downloaded_checksum = hash_md5.hexdigest()
                        total_checksum_time += (time.time() - checksum_time)

                        # Delete downloaded netcdf file
                        os.remove(new_netcdf)

                        # compare checksum of dataset from pre-upload, and the dataset downloaded from S3
                        if orig_checksum != downloaded_checksum:
                            # if the checksums dont match, delete the file on S3 (e.g. something is wrong with the uploading or downloading, unknown)
                            print(f'Deleting {name} from S3 bucket {processed_data_bucket}')
                            response = s3.delete_object(Bucket=processed_data_bucket,  
                                                        Key=str(name))
                            status = f'ERROR uploaded and downloaded netcdf file checksums dont match ({orig_checksum} vs {downloaded_checksum})'
                            print(f'FAIL {cur_ts}')
                            raise Exception(status)
                        else:
                            print(f'\n... uploaded and downloaded netcdf checksums match')
                    # ========== </Compare checksums> =============================================
                # ========== </Upload to S3> ======================================================
                
                # Create succeeded checksum entry for current timestep with s3_fname (if not local),
                # along with the dataset checksum and uuid
                if not local:
                    if create_checksum:
                        succeeded_checksums[cur_ts] = {'s3_fname':name, 'checksum':orig_checksum, 'uuid':orig_uuid}
                    else:
                        succeeded_checksums[cur_ts] = {'s3_fname':name, 'checksum':'None created', 'uuid':orig_uuid}
                else:
                    succeeded_checksums[cur_ts] = {'checksum':orig_checksum, 'uuid':orig_uuid}

                # Made it to the end of processing, therefore it was successful
                successful_time_steps.append(cur_ts)
            except Exception as e:
                delete_log = 'No deletetions'
                # if the code failed for a reason other than a timeout, delete all the files in preparation of the next timestep
                if not timeout:
                    status = gen_netcdf_utils.delete_files(data_file_paths, 
                                                           product_generation_config, 
                                                           fields_to_load, 
                                                           all=True)
                    delete_log = status
                exception_type, exception_value, exception_traceback = sys.exc_info()
                traceback_string = traceback.format_exception(exception_type, 
                                                              exception_value, 
                                                              exception_traceback)
                err_msg = json.dumps({
                    "errorType": exception_type.__name__,
                    "errorMessage": str(exception_value),
                    "stackTrace": traceback_string,
                    "deleteFilesStatus": delete_log
                })

                # log current timestep and error message
                error_log = f'ERROR\t{cur_ts}\t{err_msg}'
                if use_lambda:
                    logger.info(error_log)
                else:
                    print(error_log)
        # ========== </Process each time level> ===================================================

        # Remove processed_output_dir_base directory
        # if not local and not use_lambda:
        #     if os.path.exists(processed_output_dir_base):
        #         shutil.rmtree(processed_output_dir_base)

        # Remove model output directory
        # if os.path.exists(product_generation_config['model_output_dir']):
        #     shutil.rmtree(product_generation_config['model_output_dir'])
    
    except Exception as e:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, 
                                                      exception_value, 
                                                      exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
        })

        # log error message (ALL means an error occured independent of any one timestep)
        error_log = f'ERROR\tALL\t{err_msg}'
        if use_lambda:
            logger.info(error_log)
        else:
            print(error_log)

    # Print logging information
    logging_info(time_steps_to_process, 
                 successful_time_steps, 
                 start_time, 
                 total_download_time, 
                 num_downloaded, 
                 total_netcdf_time, 
                 total_upload_time, 
                 num_uploaded, 
                 succeeded_checksums, 
                 total_checksum_time, 
                 logger, 
                 timeout)

    return


#  ================= TESTING FOR VECTOR ROTATION FOR VEL FIELDS ===============================
import xgcm


def get_llc_grid(ds,domain='global'):
    """
    Define xgcm Grid object for the LLC grid
    See example usage in the xgcm documentation:
    https://xgcm.readthedocs.io/en/latest/example_eccov4.html#Spatially-Integrated-Heat-Content-Anomaly
    Parameters
    ----------
    ds : xarray Dataset
        formed from LLC90 grid, must have the basic coordinates:
        i,j,i_g,j_g,k,k_l,k_u,k_p1
    Returns
    -------
    grid : xgcm Grid object
        defines horizontal connections between LLC tiles
    """

    if 'domain' in ds.attrs:
        domain = ds.attrs['domain']

    if domain == 'global':
        # Establish grid topology
        tile_connections = {'tile':  {
                0: {'X': ((12, 'Y', False), (3, 'X', False)),
                    'Y': (None, (1, 'Y', False))},
                1: {'X': ((11, 'Y', False), (4, 'X', False)),
                    'Y': ((0, 'Y', False), (2, 'Y', False))},
                2: {'X': ((10, 'Y', False), (5, 'X', False)),
                    'Y': ((1, 'Y', False), (6, 'X', False))},
                3: {'X': ((0, 'X', False), (9, 'Y', False)),
                    'Y': (None, (4, 'Y', False))},
                4: {'X': ((1, 'X', False), (8, 'Y', False)),
                    'Y': ((3, 'Y', False), (5, 'Y', False))},
                5: {'X': ((2, 'X', False), (7, 'Y', False)),
                    'Y': ((4, 'Y', False), (6, 'Y', False))},
                6: {'X': ((2, 'Y', False), (7, 'X', False)),
                    'Y': ((5, 'Y', False), (10, 'X', False))},
                7: {'X': ((6, 'X', False), (8, 'X', False)),
                    'Y': ((5, 'X', False), (10, 'Y', False))},
                8: {'X': ((7, 'X', False), (9, 'X', False)),
                    'Y': ((4, 'X', False), (11, 'Y', False))},
                9: {'X': ((8, 'X', False), None),
                    'Y': ((3, 'X', False), (12, 'Y', False))},
                10: {'X': ((6, 'Y', False), (11, 'X', False)),
                     'Y': ((7, 'Y', False), (2, 'X', False))},
                11: {'X': ((10, 'X', False), (12, 'X', False)),
                     'Y': ((8, 'Y', False), (1, 'X', False))},
                12: {'X': ((11, 'X', False), None),
                     'Y': ((9, 'Y', False), (0, 'X', False))}
        }}

        grid = xgcm.Grid(ds,
                periodic=False,
                face_connections=tile_connections
        )
    elif domain == 'aste':
        tile_connections = {'tile':{
                    0:{'X':((5,'Y',False),None),
                       'Y':(None,(1,'Y',False))},
                    1:{'X':((4,'Y',False),None),
                       'Y':((0,'Y',False),(2,'X',False))},
                    2:{'X':((1,'Y',False),(3,'X',False)),
                       'Y':(None,(4,'X',False))},
                    3:{'X':((2,'X',False),None),
                       'Y':(None,None)},
                    4:{'X':((2,'Y',False),(5,'X',False)),
                       'Y':(None,(1,'X',False))},
                    5:{'X':((4,'X',False),None),
                       'Y':(None,(0,'X',False))}
                   }}
        grid = xgcm.Grid(ds,periodic=False,face_connections=tile_connections)
    else:
        raise TypeError(f'Domain {domain} not recognized')


    return grid