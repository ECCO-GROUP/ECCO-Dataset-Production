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
import ecco_cloud_utils as ea
import gen_netcdf_utils as ut


# ==========================================================================================================================
def logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded):
    total_time = time.time() - start_time
    print(f'DURATION\tSCRIPT\t{total_time}\tseconds')
    print(f'DURATION\tDOWNLOAD\t{total_download_time}\tseconds')
    print(f'FILES\tDOWNLOAD\t{num_downloaded}')
    print(f'DURATION\tNETCDF\t{total_netcdf_time}\tseconds')
    print(f'DURATION\tUPLOAD\t{total_upload_time}\tseconds')
    print(f'FILES\tUPLOAD\t{num_uploaded}')
    return


def generate_netcdfs(event):
    output_freq_code = event['output_freq_code']
    product_type = event['product_type']
    grouping_to_process = event['grouping_to_process']
    time_steps_to_process = event['time_steps_to_process']
    field_files = event['field_files']
    config_metadata = event['config_metadata']
    aws_metadata = event['aws_metadata']
    debug_mode = event['debug_mode']
    local = event['local']
    use_lambda = event['use_lambda']
    credentials = event['credentials']

    # Logging values
    start_time = time.time()
    total_netcdf_time = 0
    total_download_time = 0
    num_downloaded = 0
    total_upload_time = 0
    num_uploaded = 0

    try:
        # Fix paths
        config_metadata['mapping_factors_dir'] = Path(__file__).parent / 'mapping_factors'
        config_metadata['metadata_dir'] = Path(__file__).parent / 'metadata/ECCov4r4_metadata_json'
        config_metadata['ecco_grid_dir'] = Path(__file__).parent / 'ecco_grids'
        config_metadata['ecco_grid_dir_mds'] = Path(__file__).parent / 'ecco_grids'
        if use_lambda:
            config_metadata['model_data_dir'] = Path('/tmp') / 'diags_all'
            config_metadata['output_dir_base'] = Path('/tmp') / 'temp_output'
        else:
            config_metadata['model_data_dir'] = Path(config_metadata['model_data_dir'])
            config_metadata['output_dir_base'] = Path(config_metadata['output_dir_base'])

        print('\nBEGIN: generate_netcdfs')
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
            print(f'ERROR No bucket names in aws_metadata:\n{aws_metadata}')
            logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
            print(f'FAILURE')
            sys.exit()

        # Create tmp_data directory if using S3 (and not Lambda)
        if not local and not use_lambda:
            tmp_data_path = Path(__file__).parent / 'tmp_data'
            if not os.path.exists(tmp_data_path):
                os.mkdir(tmp_data_path)

        # Define fill values for binary and netcdf
        # ECCO always uses -9999 for missing data.
        binary_fill_value = config_metadata['binary_fill_value']
        if config_metadata['array_precision'] == 'float32':
            # binary_output_dtype = '>f4'
            array_precision = np.float32
            netcdf_fill_value = nc4.default_fillvals['f4']
        else:
            # binary_output_dtype = '>f8'
            array_precision = np.float64
            netcdf_fill_value = nc4.default_fillvals['f8']

        # num of depth levels
        nk = config_metadata['num_vertical_levels']

        ecco_start_time = np.datetime64(config_metadata['model_start_time'])
        ecco_end_time   = np.datetime64(config_metadata['model_end_time'])
        
        # get list of fields to process
        fields_to_load = list(field_files.keys())

        # ======================================================================================================================
        # METADATA SETUP
        # ======================================================================================================================
        # Define tail for dataset description (summary)
        dataset_description_tail_native = config_metadata['dataset_description_tail_native']
        dataset_description_tail_latlon = config_metadata['dataset_description_tail_latlon']

        filename_tail_native = config_metadata['filename_tail_native']
        filename_tail_latlon = config_metadata['filename_tail_latlon']

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
        # print('\nLOADING METADATA')
        metadata = {}

        for mf in metadata_fields:
            mf_e = mf + '.json'
            # print(mf_e)
            with open(str(Path(config_metadata['metadata_dir']) / mf_e), 'r') as fp:
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
        # print('\nproduct type', product_type)
        if product_type == 'native':
            dataset_description_tail = dataset_description_tail_native
            filename_tail = filename_tail_native
            groupings = groupings_for_native_datasets
            output_dir_type = config_metadata['output_dir_base'] / 'native'
            status, (latlon_bounds, depth_bounds, _, _) = ut.get_latlon_grid(Path(config_metadata['mapping_factors_dir']), debug_mode)
        elif product_type == 'latlon':
            dataset_description_tail = dataset_description_tail_latlon
            filename_tail = filename_tail_latlon
            groupings = groupings_for_latlon_datasets
            output_dir_type = config_metadata['output_dir_base'] / 'lat-lon'
            status, (latlon_bounds, depth_bounds, target_grid, wet_pts_k) = ut.get_latlon_grid(Path(config_metadata['mapping_factors_dir']), debug_mode)
        if status == -1:
            logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
            print('FAILURE')
            sys.exit()
        # ======================================================================================================================


        # ======================================================================================================================
        # GROUPINGS
        # ======================================================================================================================
        # determine which grouping to process
        # print('\nDetermining grouping to process')
        grouping = []
        # print('... using provided grouping ', grouping_to_process)
        grouping_num = grouping_to_process

        grouping = groupings[grouping_num]
        # print('... grouping to use ', grouping['name'])
        # print('... fields in grouping ', grouping['fields'])

        # dimension of dataset
        dataset_dim = grouping['dimension']
        # print('... grouping dimension', dataset_dim)
        # ======================================================================================================================


        # ======================================================================================================================
        # DIRECTORIES & FILE PATHS
        # ======================================================================================================================
        # diags_root = Path(config_metadata['model_data_dir'])

        # print('\nGetting directories for group fields')
        if output_freq_code == 'AVG_DAY':
            # mds_diags_root_dir = diags_root / 'diags_daily'
            period_suffix = 'day_mean'
            dataset_description_head = 'This dataset contains daily-averaged '

        elif output_freq_code == 'AVG_MON':
            # mds_diags_root_dir = diags_root / 'diags_monthly'
            period_suffix = 'mon_mean'
            dataset_description_head = 'This dataset contains monthly-averaged '

        elif output_freq_code == 'SNAPSHOT':
            # mds_diags_root_dir = diags_root / 'diags_inst'
            period_suffix = 'day_inst'
            dataset_description_head = 'This dataset contains instantaneous '
        else:
            print(f'ERROR Invalid output_freq_code provided ("{output_freq_code}")')
            logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
            print(f'FAILURE')
            sys.exit()

        # print('...output_freq_code ', output_freq_code)

        output_dir_freq = output_dir_type / period_suffix
        # print('...making output_dir freq ', output_dir_freq)
        # make output directory
        if not output_dir_freq.exists():
            try:
                output_dir_freq.mkdir(parents=True)
            except:
                print(f'ERROR Cannot make output directory "{output_dir_freq}"')
                logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                print('FAILURE')
                sys.exit()

        # create dataset description head
        dataset_description = dataset_description_head + grouping['name'] + dataset_description_tail
        # ======================================================================================================================


        # ======================================================================================================================
        # PROCESS EACH TIME LEVEL
        # ======================================================================================================================
        # load ECCO grid
        ecco_grid = xr.open_dataset(Path(config_metadata['ecco_grid_dir']) / config_metadata['ecco_grid_filename'])
        # print(ecco_grid)

        # print('\nLooping through time levels')
        for cur_ts_i, cur_ts in enumerate(time_steps_to_process):
            # ==================================================================================================================
            # CALCULATE TIMES
            # ==================================================================================================================
            print('\n\n=== TIME LEVEL ===', str(cur_ts_i).zfill(5), cur_ts)
            # print('\n')
            time_delta = np.timedelta64(int(cur_ts), 'h')
            cur_time = ecco_start_time + time_delta
            times = [pd.to_datetime(str(cur_time))]

            if 'AVG' in output_freq_code:
                tb, record_center_time = ecco.make_time_bounds_from_ds64(np.datetime64(times[0]), output_freq_code)
                # print('ORIG  tb, ct ', tb, record_center_time)

                # fix beginning of last record
                if tb[1].astype('datetime64[D]') == ecco_end_time.astype('datetime64[D]'):
                    # print('end time match ')
                    time_delta = np.timedelta64(12,'h')
                    rec_avg_start = tb[0] + time_delta
                    rec_avg_end   = tb[1]
                    rec_avg_delta = rec_avg_end - rec_avg_start
                    rec_avg_middle = rec_avg_start + rec_avg_delta/2

                    tb[0] = rec_avg_start
                    record_center_time = rec_avg_middle

                # truncate to ecco_start_time
                if tb[0].astype('datetime64[D]') == ecco_start_time.astype('datetime64[D]'):
                    # print('start time match ')
                    rec_avg_start = ecco_start_time
                    rec_avg_end   = tb[1]
                    rec_avg_delta = tb[1] - ecco_start_time
                    rec_avg_middle = rec_avg_start + rec_avg_delta/2

                    tb[0] = ecco_start_time
                    record_center_time = rec_avg_middle

                record_start_time = tb[0]
                record_end_time = tb[1]
                # print('FINAL tb, ct ', tb, record_center_time)

            else:
                #snapshot, all times are the same
                # print(times)
                # print(type(times[0]))

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
                        print(f'ERROR Invalid # of data files. Data files: ({source_data_file_path})')
                        logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                        print(f'FAILURE')
                        sys.exit()
                    else:
                        source_data_file_path = Path(source_data_file_path[0])

                    if local:
                        data_file_path = source_data_file_path
                    elif not use_lambda:
                        data_file_path = Path(__file__).parent / 'tmp_data' / source_data_file_path
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
                                print(f'ERROR Cannot make {data_file_path}')
                                logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                                print(f'FAILURE')
                                sys.exit()
                        if not data_file_path.exists():
                            print(f'S3: {source_data_file_path}\nLOCAL: {data_file_path}')
                            s3.download_file(model_granule_bucket, str(source_data_file_path), str(data_file_path))
                        if product_type == 'native' and not meta_file_path.exists():
                            print(f'S3: {source_meta_file_path}\nLOCAL: {meta_file_path}')
                            s3.download_file(model_granule_bucket, str(source_meta_file_path), str(meta_file_path))
                        s3_download_end_time = time.time()
                        total_download_time += s3_download_end_time-s3_download_start_time
                        num_downloaded += 1

                    # Load latlon vs native variable
                    if product_type == 'latlon':
                        status, F_DS = ut.latlon_load(ea, ecco, ecco_grid.Z.values, wet_pts_k, target_grid, 
                                                    data_file_path, record_end_time, nk, 
                                                    dataset_dim, field, output_freq_code, Path(config_metadata['mapping_factors_dir']))
                        if status == -1:
                            logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                            print('FAILURE')
                            sys.exit()
                        
                    elif product_type == 'native':
                        status, F_DS = ut.native_load(ecco, field, ecco_grid, config_metadata['ecco_grid_dir_mds'], 
                                                data_file_path, output_freq_code, cur_ts)
                        if status == -1:
                            logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                            print('FAILURE')
                            sys.exit()


                    # delete downloaded file from cloud disks
                    if not local:
                        if os.path.exists(data_file_path):
                            os.remove(data_file_path)
                        if product_type == 'native' and os.path.exists(meta_file_path):
                            os.remove(meta_file_path)
                    
                    F_DS = ut.global_DS_changes(F_DS, output_freq_code, grouping, field,
                                                array_precision, ecco_grid, depth_bounds, product_type, 
                                                latlon_bounds, netcdf_fill_value, dataset_dim, record_times)

                    # TODO: Figure out way to not need to append each field DS to a list and merge via xarray. New way should
                    # do it in less memory

                    # add this dataset to F_DS_vars and repeat for next variable
                    F_DS_vars.append(F_DS)

                # merge the data arrays to make one DATASET
                print('\n... merging F_DS_vars')
                G = xr.merge((F_DS_vars))

                # delete F_DS_vars from memory
                del(F_DS_vars)

                podaac_dir = Path(config_metadata['metadata_dir']) / config_metadata['podaac_metadata_filename']
                status, G, netcdf_output_filename, encoding = ut.set_metadata(ecco, G, product_type, all_metadata, dataset_dim, 
                                                                                output_freq_code, netcdf_fill_value, 
                                                                                grouping, filename_tail, output_dir_freq, 
                                                                                dataset_description, podaac_dir)
                if status == -1:
                    logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                    print('FAILURE')
                    sys.exit()

                # SAVE DATASET
                netcdf_start_time = time.time()
                print('\n... saving to netcdf ', netcdf_output_filename)
                G.load()

                G.to_netcdf(netcdf_output_filename, encoding=encoding)
                G.close()

                netcdf_end_time = time.time()
                total_netcdf_time += netcdf_end_time-netcdf_start_time
                # print('\n... checking existence of new file: ', netcdf_output_filename.exists())

                # Upload output netcdf to s3
                if not local:
                    s3_upload_start_time = time.time()
                    print('\n... uploading new file to S3 bucket')
                    name = str(netcdf_output_filename).replace(f'{str(config_metadata["output_dir_base"])}/', '')
                    try:
                        response = s3.upload_file(str(netcdf_output_filename), processed_data_bucket, name)
                        # print(f'\n... uploaded {netcdf_output_filename} to bucket {processed_data_bucket}')
                    except:
                        print(f'ERROR Unable to upload file {netcdf_output_filename} to bucket {processed_data_bucket}')
                        logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
                        print(f'FAILURE')
                        sys.exit()
                    os.remove(netcdf_output_filename)
                    s3_upload_end_time = time.time()
                    total_upload_time += s3_upload_end_time-s3_upload_start_time
                    num_uploaded += 1
            # ==================================================================================================================
        # =============================================================================================

        # Remove tmp_data directory
        if not local and not use_lambda:
            if os.path.exists(tmp_data_path):
                shutil.rmtree(tmp_data_path)

        # LOGGING
        logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
        print(f'SUCCESS')
    
    except Exception as e:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
        })
        print(f'ERROR {err_msg}\n{repr(e)}')
        logging_info(start_time, total_download_time, num_downloaded, total_netcdf_time, total_upload_time, num_uploaded)
        print(f'FAILURE')

    return