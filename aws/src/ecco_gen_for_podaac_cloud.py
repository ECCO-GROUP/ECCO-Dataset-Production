"""
Created May 18, 2022

Author: Duncan Bark
Adapted from ifenty's "eccov4r4_gen_for_podaac.py"

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


main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path / "src"}')
sys.path.append(f'{main_path / "src" / "utils"}')
import gen_netcdf_utils as gen_netcdf_utils

import ecco_v4_py as ecco


# ==========================================================================================================================
def logging_info(time_steps_to_process, successful_time_steps, start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded, succeeded_checksums, total_checksum_time, logger, timeout):
    script_time = time.time() - start_time
    script_time -= (total_download_time + total_netcdf_time + total_upload_time)
    IO_time = total_download_time + total_netcdf_time + total_upload_time
    total_time = script_time + IO_time
    print()
    print('='*25 + ' EXECUTION COMPLETE ' + '='*25)
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

    if timeout:
        print_messages.append('TIMEOUT')

    print_messages.append(f'SUCCEEDED\t{succeeded_checksums}')

    if not time_steps_to_process == successful_time_steps:
        failed_time_steps = list(set(time_steps_to_process) ^ set(successful_time_steps))
        print_messages.append(f'FAILED\t{failed_time_steps}')
        print_messages.append('PARTIAL')
    else:
        print_messages.append('SUCCESS')

    print_messages.append('END')

    for msg in print_messages:
        if logger != False:
            logger.info(msg)
        else:
            print(msg)


    return


def generate_netcdfs(event):
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
    output_freq_code = event['output_freq_code']
    product_type = event['product_type']
    grouping_to_process = event['grouping_to_process']
    time_steps_to_process = event['time_steps_to_process']
    field_files = event['field_files']
    product_generation_config = event['product_generation_config']
    aws_metadata = event['aws_metadata']
    debug_mode = event['debug_mode']
    local = event['local']
    use_lambda = event['use_lambda']
    credentials = event['credentials']
    use_workers_to_download = event['use_workers_to_download']

    extra_prints = product_generation_config['extra_prints']

    # get list of fields to process
    fields_to_load = list(field_files.keys())

    try:
        # Fix paths
        if use_lambda:
            product_generation_config['mapping_factors_dir'] = Path(__file__).parent / 'mapping_factors'
            product_generation_config['metadata_dir'] = Path(__file__).parent / 'metadata'
            product_generation_config['ecco_grid_dir'] = Path(__file__).parent / 'ecco_grids'
            product_generation_config['ecco_grid_dir_mds'] = Path(__file__).parent / 'ecco_grids'
            product_generation_config['model_output_dir'] = Path('/tmp') / 'diags_all'
            product_generation_config['processed_output_dir_base'] = Path('/tmp') / 'temp_output'
        else:
            product_generation_config['mapping_factors_dir'] = Path(product_generation_config['mapping_factors_dir'])
            product_generation_config['metadata_dir'] = Path(product_generation_config['metadata_dir'])
            product_generation_config['ecco_grid_dir'] = Path(product_generation_config['ecco_grid_dir'])
            product_generation_config['ecco_grid_dir_mds'] = Path(product_generation_config['ecco_grid_dir_mds'])
            product_generation_config['model_output_dir'] = Path(product_generation_config['model_output_dir'])
            product_generation_config['processed_output_dir_base'] = Path(product_generation_config['processed_output_dir_base'])

        print('\nBEGIN generate_netcdfs')
        print('OFC', output_freq_code)
        print('PDT', product_type)
        print('GTP', grouping_to_process)
        print('TSP', time_steps_to_process)
        print('DBG', debug_mode)
        print('')

        # Setup S3
        if 'source_bucket' in aws_metadata and 'output_bucket' in aws_metadata:
            buckets = (aws_metadata['source_bucket'], aws_metadata['output_bucket'])
            if not local and buckets != None and credentials != None:
                # boto3.setup_default_session(profile_name=aws_metadata['profile_name'])
                s3 = boto3.client('s3')
                model_granule_bucket, processed_data_bucket = buckets
        elif not local:
            status = f'ERROR No bucket names in aws_metadata:\n{aws_metadata}'
            raise Exception(status)

        # Create processed_output_dir_base directory if using S3 (and not Lambda)
        if not local and not use_lambda:
            if not os.path.exists(product_generation_config['processed_output_dir_base']):
                os.makedirs(product_generation_config['processed_output_dir_base'], exist_ok=True)

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

        # ======================================================================================================================
        # METADATA SETUP
        # ======================================================================================================================
        # Define tail for dataset description (summary)
        dataset_description_tail_native = product_generation_config['dataset_description_tail_native']
        dataset_description_tail_latlon = product_generation_config['dataset_description_tail_latlon']
        
        filename_tail_native = f'_ECCO_{product_generation_config["ecco_version"]}_native_{product_generation_config["filename_tail_native"]}'
        filename_tail_latlon = f'_ECCO_{product_generation_config["ecco_version"]}_latlon_{product_generation_config["filename_tail_latlon"]}'

        metadata_fields = ['ECCOv4r4_global_metadata_for_all_datasets',
                        'ECCOv4r4_global_metadata_for_latlon_datasets',
                        'ECCOv4r4_global_metadata_for_native_datasets',
                        'ECCOv4r4_coordinate_metadata_for_1D_datasets',
                        'ECCOv4r4_coordinate_metadata_for_latlon_datasets',
                        'ECCOv4r4_coordinate_metadata_for_native_datasets',
                        'ECCOv4r4_geometry_metadata_for_latlon_datasets',
                        'ECCOv4r4_geometry_metadata_for_native_datasets',
                        'ECCOv4r4_groupings_for_1D_datasets',
                        'ECCOv4r4_groupings_for_latlon_datasets',
                        'ECCOv4r4_groupings_for_native_datasets',
                        'ECCOv4r4_variable_metadata',
                        'ECCOv4r4_variable_metadata_for_latlon_datasets']

        # load METADATA
        if extra_prints: print('\nLOADING METADATA')
        metadata = {}

        for mf in metadata_fields:
            mf_e = mf + '.json'
            if extra_prints: print(mf_e)
            with open(str(Path(product_generation_config['metadata_dir']) / mf_e), 'r') as fp:
                metadata[mf] = json.load(fp)

        # metadata for different variables
        global_metadata_for_all_datasets = metadata['ECCOv4r4_global_metadata_for_all_datasets']
        global_metadata_for_latlon_datasets = metadata['ECCOv4r4_global_metadata_for_latlon_datasets']
        global_metadata_for_native_datasets = metadata['ECCOv4r4_global_metadata_for_native_datasets']

        coordinate_metadata_for_1D_datasets = metadata['ECCOv4r4_coordinate_metadata_for_1D_datasets']
        coordinate_metadata_for_latlon_datasets = metadata['ECCOv4r4_coordinate_metadata_for_latlon_datasets']
        coordinate_metadata_for_native_datasets = metadata['ECCOv4r4_coordinate_metadata_for_native_datasets']

        geometry_metadata_for_latlon_datasets = metadata['ECCOv4r4_geometry_metadata_for_latlon_datasets']
        geometry_metadata_for_native_datasets = metadata['ECCOv4r4_geometry_metadata_for_native_datasets']

        groupings_for_1D_datasets = metadata['ECCOv4r4_groupings_for_1D_datasets']
        groupings_for_latlon_datasets = metadata['ECCOv4r4_groupings_for_latlon_datasets']
        groupings_for_native_datasets = metadata['ECCOv4r4_groupings_for_native_datasets']

        variable_metadata_latlon = metadata['ECCOv4r4_variable_metadata_for_latlon_datasets']
        variable_metadata_default = metadata['ECCOv4r4_variable_metadata']

        variable_metadata_native = variable_metadata_default + geometry_metadata_for_native_datasets

        all_metadata = {'var_native':variable_metadata_native, 
                        'var_latlon':variable_metadata_latlon, 
                        'coord_native':coordinate_metadata_for_native_datasets, 
                        'coord_latlon':coordinate_metadata_for_latlon_datasets, 
                        'global_all':global_metadata_for_all_datasets, 
                        'global_native':global_metadata_for_native_datasets, 
                        'global_latlon':global_metadata_for_latlon_datasets}
        # ======================================================================================================================


        # ======================================================================================================================
        # NATIVE vs LATLON SETUP
        # ======================================================================================================================
        if extra_prints: print('\nproduct type', product_type)
        if product_type == 'native':
            dataset_description_tail = dataset_description_tail_native
            filename_tail = filename_tail_native
            groupings = groupings_for_native_datasets
            output_dir_type = product_generation_config['processed_output_dir_base'] / 'native'
            status, latlon_grid = gen_netcdf_utils.get_latlon_grid(Path(product_generation_config['mapping_factors_dir']), debug_mode)
        elif product_type == 'latlon':
            dataset_description_tail = dataset_description_tail_latlon
            filename_tail = filename_tail_latlon
            groupings = groupings_for_latlon_datasets
            output_dir_type = product_generation_config['processed_output_dir_base'] / 'lat-lon'
            status, latlon_grid = gen_netcdf_utils.get_latlon_grid(Path(product_generation_config['mapping_factors_dir']), debug_mode)
        if status != 'SUCCESS':
            raise Exception(status)

        if product_type == 'native':
            (latlon_bounds, depth_bounds, _, _) = latlon_grid
        elif product_type == 'latlon':
            (latlon_bounds, depth_bounds, target_grid, wet_pts_k) = latlon_grid
        # ======================================================================================================================


        # ======================================================================================================================
        # GROUPINGS
        # ======================================================================================================================
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
        # ======================================================================================================================


        # ======================================================================================================================
        # DIRECTORIES & FILE PATHS
        # ======================================================================================================================
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
        # ======================================================================================================================


        # ======================================================================================================================
        # PROCESS EACH TIME LEVEL
        # ======================================================================================================================
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
                if use_lambda and time.time() - job_start_time >= aws_metadata['job_timeout']:
                    timeout = True
                    raise Exception('TIMEOUT')
                # ==================================================================================================================
                # CALCULATE TIMES
                # ==================================================================================================================
                print('\n\n=== TIME LEVEL ===', str(cur_ts_i).zfill(5), cur_ts)
                if extra_prints: print('\n')
                time_delta = np.timedelta64(int(cur_ts), 'h')
                cur_time = ecco_start_time + time_delta
                times = [pd.to_datetime(str(cur_time))]

                if 'AVG' in output_freq_code:
                    tb, record_center_time = ecco.make_time_bounds_from_ds64(np.datetime64(times[0]), output_freq_code)
                    if extra_prints: print('ORIG  tb, ct ', tb, record_center_time)

                    # fix beginning of last record
                    if tb[1].astype('datetime64[D]') == ecco_end_time.astype('datetime64[D]'):
                        if extra_prints: print('end time match ')
                        time_delta = np.timedelta64(12,'h')
                        rec_avg_start = tb[0] + time_delta
                        rec_avg_end   = tb[1]
                        rec_avg_delta = rec_avg_end - rec_avg_start
                        rec_avg_middle = rec_avg_start + rec_avg_delta/2

                        tb[0] = rec_avg_start
                        record_center_time = rec_avg_middle

                    # truncate to ecco_start_time
                    if tb[0].astype('datetime64[D]') == ecco_start_time.astype('datetime64[D]'):
                        if extra_prints: print('start time match ')
                        rec_avg_start = ecco_start_time
                        rec_avg_end   = tb[1]
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
                # ==================================================================================================================


                # ==================================================================================================================
                # LOOP THROUGH VARIABLES & CREATE DATASET
                # ==================================================================================================================
                F_DS_vars = []

                if not debug_mode:
                    
                    # RANDOM FAILURE (FOR TESTING)
                    # if np.random.random() < 0.5:
                    #     print(f'FAIL {cur_ts}')
                    #     raise Exception('Random failure')

                    # Download field file(s)
                    # If 'download_all_fields' in product_generation_config.yaml is True, then all field files
                    # for the current time step will be downloaded, otherwise each field file is downloaded 
                    # and processed one at a time
                    if not local and product_generation_config['download_all_fields']:
                        print(f'Downloading all files for current timestep')
                        s3_download_start_time = time.time()
                        (status, data_file_paths, meta_file_paths, curr_num_downloaded) = gen_netcdf_utils.download_all_files(s3, fields_to_load, field_files, cur_ts, 
                                                                                                        use_lambda, data_file_paths, meta_file_paths, product_generation_config, product_type, 
                                                                                                        use_workers_to_download, model_granule_bucket)
                        num_downloaded += curr_num_downloaded
                        if status != 'SUCCESS':
                            print(f'FAIL {cur_ts}')
                            raise Exception(status)
                        total_download_time += (time.time() - s3_download_start_time)
                    else:
                        print(f'Downloading and processing fields one at a time for current timestep')



                    # ============================== TODO ==============================
                    # PERFORM VECTOR ROTATION AS NECESSARY
                    if 'vector_inputs' in grouping:
                        grouping['vector_inputs'] = ['UVEL', 'VVEL']
                        # load specified field files from the provided directory
                        # This loads them into the native tile grid
                        F_DSs = []
                        for vec_field in grouping['vector_inputs']:
                            status, F_DS = ecco.load_ecco_vars_from_mds(Path(data_file_paths[vec_field]).parent,
                                                                mds_grid_dir = product_generation_config['ecco_grid_dir_mds'],
                                                                mds_files = Path(data_file_paths[vec_field]).name.split('.')[0],
                                                                vars_to_load = vec_field,
                                                                drop_unused_coords = True,
                                                                grid_vars_to_coords = False,
                                                                output_freq_code=output_freq_code,
                                                                model_time_steps_to_load=int(cur_ts),
                                                                less_output = True,
                                                                read_grid=product_generation_config['read_ecco_grid_for_native_load'])
                            print(status)
                            F_DSs.append(F_DS)
                    # ============================== TODO ==============================






                    # Load fields and place them in the dataset
                    for i, field in enumerate(sorted(fields_to_load)):
                        if not product_generation_config['download_all_fields']:
                            s3_download_start_time = time.time()
                            (status, data_file_paths, meta_file_paths, curr_num_downloaded) = gen_netcdf_utils.download_all_files(s3, [field], field_files, cur_ts, 
                                                                                                            use_lambda, data_file_paths, meta_file_paths, product_generation_config, product_type, 
                                                                                                            use_workers_to_download, model_granule_bucket)
                            num_downloaded += curr_num_downloaded
                            if status != 'SUCCESS':
                                print(f'FAIL {cur_ts}')
                                raise Exception(status)
                            total_download_time += (time.time() - s3_download_start_time)

                        if use_lambda and time.time() - job_start_time >= aws_metadata['job_timeout']:
                            timeout = True
                            raise Exception('TIMEOUT')
                            
                        data_file_path = Path(data_file_paths[field])
                        meta_file_path = Path(meta_file_paths[field])

                            
                        # Load latlon vs native variable
                        if product_type == 'latlon':
                            status, F_DS = gen_netcdf_utils.transform_latlon(ecco, ecco_grid.Z.values, wet_pts_k, target_grid, 
                                                                data_file_path, record_end_time, nk, 
                                                                dataset_dim, field, output_freq_code, 
                                                                Path(product_generation_config['mapping_factors_dir']), extra_prints=extra_prints)
                            if status != 'SUCCESS':
                                print(f'FAIL {cur_ts}')
                                raise Exception(status)
                            
                        elif product_type == 'native':
                            status, F_DS = gen_netcdf_utils.transform_native(ecco, field, ecco_land_masks, product_generation_config['ecco_grid_dir_mds'], 
                                                                data_file_path, output_freq_code, cur_ts, product_generation_config['read_ecco_grid_for_native_load'],
                                                                extra_prints=extra_prints)
                            if status != 'SUCCESS':
                                print(f'FAIL {cur_ts}')
                                raise Exception(status)

                        # delete files
                        # gen_netcdf_utils.delete_files(data_file_path, meta_file_path, product_generation_config)
                        if os.path.exists(data_file_path):
                            os.remove(data_file_path)
                        if os.path.exists(meta_file_path):
                            os.remove(meta_file_path)

                        F_DS = gen_netcdf_utils.global_DS_changes(F_DS, output_freq_code, grouping, field,
                                                    array_precision, ecco_grid, depth_bounds, product_type, 
                                                    latlon_bounds, netcdf_fill_value, dataset_dim, record_times, extra_prints=extra_prints)

                        # add this dataset to F_DS_vars and repeat for next variable
                        F_DS_vars.append(F_DS)

                    # merge the data arrays to make one DATASET
                    print('\n... merging F_DS_vars')
                    G = xr.merge((F_DS_vars))

                    # delete F_DS_vars from memory
                    del(F_DS_vars)

                    podaac_dir = Path(product_generation_config['metadata_dir']) / product_generation_config['podaac_metadata_filename']
                    status, G, netcdf_output_filename, encoding = gen_netcdf_utils.set_metadata(ecco, G, product_type, all_metadata, dataset_dim, 
                                                                                    output_freq_code, netcdf_fill_value, 
                                                                                    grouping, filename_tail, output_dir_freq, 
                                                                                    dataset_description, podaac_dir, 
                                                                                    product_generation_config,
                                                                                    extra_prints=extra_prints)
                    if status != 'SUCCESS':
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)

                    # SAVE DATASET
                    netcdf_start_time = time.time()
                    print('\n... saving to netcdf ', netcdf_output_filename)
                    G.load()
                    G.to_netcdf(netcdf_output_filename, encoding=encoding)
                    orig_uuid = G.attrs['uuid']
                    G.close()
                    total_netcdf_time += (time.time() - netcdf_start_time)
                    if extra_prints: print('\n... checking existence of new file: ', netcdf_output_filename.exists())

                    # Upload output netcdf to s3
                    if not local:
                        # create checksum of netcdf file
                        if product_generation_config['create_checksum']:
                            checksum_time = time.time()
                            hash_md5 = hashlib.md5()
                            with open(netcdf_output_filename, 'rb') as f:
                                for chunk in iter(lambda: f.read(4096), b""):
                                    hash_md5.update(chunk)
                            orig_checksum = hash_md5.hexdigest()
                            total_checksum_time += (time.time() - checksum_time)

                        s3_upload_start_time = time.time()
                        print('\n... uploading new file to S3 bucket')
                        name = str(netcdf_output_filename).replace(f'{str(product_generation_config["processed_output_dir_base"])}/', f'{aws_metadata["bucket_subfolder"]}/')
                        try:
                            response = s3.upload_file(str(netcdf_output_filename), processed_data_bucket, name)
                            if extra_prints: print(f'\n... uploaded {netcdf_output_filename} to bucket {processed_data_bucket}')
                        except:
                            os.remove(netcdf_output_filename)
                            status = f'ERROR Unable to upload file {netcdf_output_filename} to bucket {processed_data_bucket}'
                            print(f'FAIL {cur_ts}')
                            raise Exception(status)
                        if use_lambda:
                            os.remove(netcdf_output_filename)
                        total_upload_time += (time.time() - s3_upload_start_time)
                        num_uploaded += 1

                        # create checksum of downloaded dataset from S3 and compare to the checksum created of the
                        # dataset file uploaded to S3 (in ensure there was no issue uploading the file)
                        if product_generation_config['compare_checksums'] and product_generation_config['create_checksum']:
                            checksum_time = time.time()
                            new_netcdf = netcdf_output_filename.parent / f'new_{netcdf_output_filename.name}'
                            s3.download_file(processed_data_bucket, str(name), str(new_netcdf))
                            hash_md5 = hashlib.md5()
                            with open(new_netcdf, 'rb') as f:
                                for chunk in iter(lambda: f.read(4096), b""):
                                    hash_md5.update(chunk)
                            downloaded_checksum = hash_md5.hexdigest()
                            total_checksum_time += (time.time() - checksum_time)

                            os.remove(new_netcdf)

                            if orig_checksum != downloaded_checksum:
                                print(f'Deleting {name} from S3 bucket {processed_data_bucket}')
                                response = s3.delete_object(Bucket=processed_data_bucket, Key=str(name))
                                status = f'ERROR uploaded and downloaded netcdf file checksums dont match ({orig_checksum} vs {downloaded_checksum})'
                                print(f'FAIL {cur_ts}')
                                raise Exception(status)
                            else:
                                print(f'\n... uploaded and downloaded netcdf checksums match')
                
                if product_generation_config['create_checksum']:
                    succeeded_checksums[cur_ts] = {'s3_fname':name, 'checksum':orig_checksum, 'uuid':orig_uuid}
                else:
                    succeeded_checksums[cur_ts] = {'s3_fname':name, 'checksum':'None created', 'uuid':orig_uuid}
                successful_time_steps.append(cur_ts)
            except Exception as e:
                if not timeout:
                    gen_netcdf_utils.delete_files(data_file_paths, meta_file_paths, product_generation_config, fields_to_load, all=True)
                exception_type, exception_value, exception_traceback = sys.exc_info()
                traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
                err_msg = json.dumps({
                    "errorType": exception_type.__name__,
                    "errorMessage": str(exception_value),
                    "stackTrace": traceback_string
                })
                error_log = f'ERROR\t{cur_ts}\t{err_msg}'
                if use_lambda:
                    logger.info(error_log)
                else:
                    print(error_log)
            # ==================================================================================================================
        # =============================================================================================

        # Remove processed_output_dir_base directory
        # if not local and not use_lambda:
        #     if os.path.exists(product_generation_config['processed_output_dir_base']):
        #         shutil.rmtree(product_generation_config['processed_output_dir_base'])

        # Remove model output directory
        if os.path.exists(product_generation_config['model_output_dir']):
            shutil.rmtree(product_generation_config['model_output_dir'])
    
    except Exception as e:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
        })
        error_log = f'ERROR\tALL\t{err_msg}'
        if use_lambda:
            logger.info(error_log)
        else:
            print(error_log)

    # LOGGING
    logging_info(time_steps_to_process, successful_time_steps, start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded, succeeded_checksums, total_checksum_time, logger, timeout)

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