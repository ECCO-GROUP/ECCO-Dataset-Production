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
import traceback
import numpy as np
import pandas as pd
import xarray as xr
import netCDF4 as nc4
from pathlib import Path

sys.path.append(f'{Path(__file__).parent.resolve()}')
import ecco_v4_py as ecco
import gen_netcdf_utils as gen_netcdf_utils


# ==========================================================================================================================
def logging_info(time_steps_to_process, successful_time_steps, start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded):
    script_time = time.time() - start_time
    script_time -= (total_download_time + total_netcdf_time + total_upload_time)
    IO_time = total_download_time + total_netcdf_time + total_upload_time
    total_time = script_time + IO_time
    print()
    print('='*25 + ' EXECUTION COMPLETE ' + '='*25)
    print(f'DURATION\tTOTAL\t{total_time}\tseconds')
    print(f'DURATION\tSCRIPT\t{script_time}\tseconds')
    print(f'DURATION\tIO\t{IO_time}\tseconds')
    print(f'DURATION\tDOWNLOAD\t{total_download_time}\tseconds')
    print(f'DURATION\tNETCDF\t{total_netcdf_time}\tseconds')
    print(f'DURATION\tUPLOAD\t{total_upload_time}\tseconds')
    print(f'FILES\tDOWNLOAD\t{num_downloaded}')
    print(f'FILES\tUPLOAD\t{num_uploaded}')

    if not time_steps_to_process == successful_time_steps:
        failed_time_steps = list(set(time_steps_to_process) ^ set(successful_time_steps))
        print(f'FAILED\t{failed_time_steps}')
        print('PARTIAL')
    else:
        print('SUCCESS')
    return


def generate_netcdfs(event):
    # Logging values
    start_time = time.time()
    total_netcdf_time = 0
    total_download_time = 0
    num_downloaded = 0
    total_upload_time = 0
    num_uploaded = 0

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

    extra_prints = product_generation_config['extra_prints']

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
            raise status

        # Create processed_output_dir_base directory if using S3 (and not Lambda)
        if not local and not use_lambda:
            if not os.path.exists(product_generation_config['processed_output_dir_base']):
                os.mkdir(product_generation_config['processed_output_dir_base'])

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
        
        # get list of fields to process
        fields_to_load = list(field_files.keys())

        # list of successful timesteps, used for logging and job resubmission for lambdas
        successful_time_steps = []

        # ======================================================================================================================
        # METADATA SETUP
        # ======================================================================================================================
        # Define tail for dataset description (summary)
        dataset_description_tail_native = product_generation_config['dataset_description_tail_native']
        dataset_description_tail_latlon = product_generation_config['dataset_description_tail_latlon']

        filename_tail_native = product_generation_config['filename_tail_native']
        filename_tail_latlon = product_generation_config['filename_tail_latlon']

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
            status, (latlon_bounds, depth_bounds, _, _) = gen_netcdf_utils.get_latlon_grid(Path(product_generation_config['mapping_factors_dir']), debug_mode)
        elif product_type == 'latlon':
            dataset_description_tail = dataset_description_tail_latlon
            filename_tail = filename_tail_latlon
            groupings = groupings_for_latlon_datasets
            output_dir_type = product_generation_config['processed_output_dir_base'] / 'lat-lon'
            status, (latlon_bounds, depth_bounds, target_grid, wet_pts_k) = gen_netcdf_utils.get_latlon_grid(Path(product_generation_config['mapping_factors_dir']), debug_mode)
        if status != 'SUCCESS':
            raise status
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
            raise status

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
                raise status

        # create dataset description head
        dataset_description = dataset_description_head + grouping['name'] + dataset_description_tail
        # ======================================================================================================================


        # ======================================================================================================================
        # PROCESS EACH TIME LEVEL
        # ======================================================================================================================
        # load ECCO grid
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
            try:
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
                    # Load fields and place them in the dataset
                    for field in fields_to_load:
                        source_data_file_path = []
                        data_files = field_files[field]
                        for df in data_files:
                            if cur_ts in df:
                                source_data_file_path.append(df)

                        if len(source_data_file_path) != 1:
                            status = f'ERROR Invalid # of data files. Data files: ({source_data_file_path})'
                            print(f'FAIL {cur_ts}')
                            raise Exception(status)
                        else:
                            source_data_file_path = Path(source_data_file_path[0])

                        if not use_lambda:
                            data_file_path = product_generation_config['model_output_dir'] / source_data_file_path
                        else:
                            data_file_path = Path(f'/tmp/{source_data_file_path}')

                        meta_file_path = ''
                        if product_type == 'native':
                            source_meta_file_path = f'{str(source_data_file_path)[:-5]}.meta'
                            meta_file_path = Path(f'{str(data_file_path)[:-5]}.meta')

                        # Download data_file from S3
                        if not local:
                            s3_download_start_time = time.time()
                            if not data_file_path.parent.exists():
                                try:
                                    data_file_path.parent.mkdir(parents=True, exist_ok=True)
                                except:
                                    status = f'ERROR Cannot make {data_file_path}'
                                    print(f'FAIL {cur_ts}')
                                    raise Exception(status)
                            if not data_file_path.exists():
                                # print(f'S3: {source_data_file_path}\nLOCAL: {data_file_path}')
                                s3.download_file(model_granule_bucket, str(source_data_file_path), str(data_file_path))
                            if product_type == 'native' and not meta_file_path.exists():
                                # print(f'S3: {source_meta_file_path}\nLOCAL: {meta_file_path}')
                                s3.download_file(model_granule_bucket, str(source_meta_file_path), str(meta_file_path))
                            s3_download_end_time = time.time()
                            total_download_time += s3_download_end_time-s3_download_start_time
                            num_downloaded += 1

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
                                                                data_file_path, output_freq_code, cur_ts, extra_prints=extra_prints)
                            if status != 'SUCCESS':
                                print(f'FAIL {cur_ts}')
                                raise Exception(status)


                        # delete downloaded file from cloud disks
                        if not local:
                            if os.path.exists(data_file_path):
                                os.remove(data_file_path)
                            if product_type == 'native' and os.path.exists(meta_file_path):
                                os.remove(meta_file_path)
                        
                        F_DS = gen_netcdf_utils.global_DS_changes(F_DS, output_freq_code, grouping, field,
                                                    array_precision, ecco_grid, depth_bounds, product_type, 
                                                    latlon_bounds, netcdf_fill_value, dataset_dim, record_times, extra_prints=extra_prints)

                        # TODO: Figure out way to not need to append each field DS to a list and merge via xarray. New way should
                        # do it in less memory

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
                                                                                    product_generation_config['doi'], 
                                                                                    product_generation_config['ecco_version'],
                                                                                    extra_prints=extra_prints)
                    if status != 'SUCCESS':
                        print(f'FAIL {cur_ts}')
                        raise Exception(status)

                    # SAVE DATASET
                    netcdf_start_time = time.time()
                    print('\n... saving to netcdf ', netcdf_output_filename)
                    G.load()

                    G.to_netcdf(netcdf_output_filename, encoding=encoding)
                    G.close()

                    netcdf_end_time = time.time()
                    total_netcdf_time += netcdf_end_time-netcdf_start_time
                    if extra_prints: print('\n... checking existence of new file: ', netcdf_output_filename.exists())

                    # Upload output netcdf to s3
                    if not local:
                        s3_upload_start_time = time.time()
                        print('\n... uploading new file to S3 bucket')
                        name = str(netcdf_output_filename).replace(f'{str(product_generation_config["processed_output_dir_base"])}/', '')
                        try:
                            response = s3.upload_file(str(netcdf_output_filename), processed_data_bucket, name)
                            if extra_prints: print(f'\n... uploaded {netcdf_output_filename} to bucket {processed_data_bucket}')
                        except:
                            status = f'ERROR Unable to upload file {netcdf_output_filename} to bucket {processed_data_bucket}'
                            print(f'FAIL {cur_ts}')
                            raise status
                        os.remove(netcdf_output_filename)
                        s3_upload_end_time = time.time()
                        total_upload_time += s3_upload_end_time-s3_upload_start_time
                        num_uploaded += 1
                
                successful_time_steps.append(cur_ts)
            except Exception as e:
                exception_type, exception_value, exception_traceback = sys.exc_info()
                traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
                err_msg = json.dumps({
                    "errorType": exception_type.__name__,
                    "errorMessage": str(exception_value),
                    "stackTrace": traceback_string
                })
                print(f'ERROR\t{cur_ts}\t{err_msg}')
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
        print(f'ERROR\tALL\t{err_msg}')

    # LOGGING
    logging_info(time_steps_to_process, successful_time_steps, start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)

    return