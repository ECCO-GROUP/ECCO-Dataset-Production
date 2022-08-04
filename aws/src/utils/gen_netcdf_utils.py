"""
ECCO Dataset Production generate netCDF utilities

Author: Duncan Bark

Contains functions used while processing ECCO granules into netCDFs for PODAAC

"""

import os
import glob
import lzma
import uuid
import pickle
import datetime
import numpy as np
import xarray as xr
from pathlib import Path
from scipy import sparse
from pandas import read_csv
from concurrent import futures
from collections import OrderedDict


# =================================================================================================
# DOWNLOAD FILES FROM S3
# =================================================================================================
def download_all_files(s3, 
                       fields_to_load, 
                       field_files, 
                       cur_ts, 
                       data_file_paths, 
                       meta_file_paths, 
                       product_generation_config, 
                       product_type, 
                       model_granule_bucket):
    """
    Download the field files for the cur_ts from S3

    Args:
        s3 (botocore.client.S3): boto3 client object for AWS S3
        fields_to_load (list): List of field names
        field_files (defaultdict(list)): Dictionary with field names as keys, and S3/local file paths for each timestep as values
        cur_ts (str): String of the current timestep (i.e. '0000000732')
        data_file_paths (dict): Dictionary where key=field name, value=downloaded data file path at cur_ts for field
        meta_file_paths (dict): Dictionary where key=field name, value=downloaded meta file path at cur_ts for field
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        product_type (str): String product type (i.e. 'latlon', 'native')
        model_granule_bucket (str): String name of the AWS S3 bucket for model granules

    Returns:
        (status, (data_file_paths, meta_file_paths, num_downloaded)) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            data_file_paths (dict): Dictionary where key=field name, value=downloaded data file path at cur_ts for field
            meta_file_paths (dict): Dictionary where key=field name, value=downloaded meta file path at cur_ts for field
            num_downloaded (int): Total number of downloaded files
    """
    status = 'SUCCESS'
    try:
        source_data_file_paths = {}
        source_meta_file_paths = {}
        num_downloaded = 0

        # Get the source data/meta file path for each field, for cur_ts, as well as
        # create the data/meta file path to download it to locally
        for field in fields_to_load:
            for df in field_files[field]:
                if cur_ts in df:
                    # update source data/meta file paths for current field
                    source_data_file_paths[field] = df
                    source_meta_file_paths[field] = f'{str(df)[:-5]}.meta'

                    # create local path to download the file to
                    df_local = df.replace(f'{product_generation_config["ecco_version"]}/', '')
                    file_path = product_generation_config['model_output_dir'] / df_local
                    if not file_path.parent.exists():
                        try:
                            file_path.parent.mkdir(parents=True, exist_ok=True)
                        except:
                            status = f'ERROR Cannot make {file_path}'
                            return (status, data_file_paths, meta_file_paths, num_downloaded)

                    # update data/meta file paths for current field
                    data_file_paths[field] = str(file_path)
                    meta_file_paths[field] = f'{str(file_path)[:-5]}.meta'

        # create a zipped list of source data file paths and local data file paths
        # eg. [(S3 bucket path, Local download path), ...]
        source_local_paths = {}
        for field in fields_to_load:
            source_local_paths[field] = [(source_data_file_paths[field], data_file_paths[field])]

        # if the product is native, include the meta paths to the source_local_paths var
        if product_type == 'native':
            source_local_meta = {}
            for field in fields_to_load:
                source_local_meta[field] = [(source_meta_file_paths[field], meta_file_paths[field])]
                source_local_paths[field].extend(source_local_meta[field])

        # create a list of all (source, local) paths for all fields
        all_files = []
        for field in fields_to_load:
            all_files.extend(source_local_paths[field])

        # download all the files in parallel using workers, where each worker downloads a different file
        # where there are as many workers as there are files to download
        if product_generation_config['use_workers_to_download']:
            num_workers = len(all_files)
            print(f'Using {num_workers} workers to download {len(all_files)} files')

            # download function
            def fetch(paths):
                key, file_path = paths
                s3.download_file(model_granule_bucket, str(key), str(file_path))
                return str(key)

            # create workers and assign each one a file to download. This paths object is a tuple with the
            # first value the path on S3, and the second the local path to download to
            with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_key = {executor.submit(fetch, paths) for paths in all_files}

                # Go through return values for each completed worker
                for future in futures.as_completed(future_to_key):
                    key = future.result()
                    exception = future.exception()
                    if exception:
                        status = f'ERROR downloading file: {key} ({exception})'
                        return (status, data_file_paths, meta_file_paths, num_downloaded)
                    else:
                        print(f'Downloaded: {key}')
                    num_downloaded += 1
        else:
            # download files one at a time from the source_local_paths list
            # always download .data files, and download .meta files if the product type is 'native'
            for key, df in all_files:
                df = Path(df)
                if (('.data' in key) or (product_type == 'native' and '.meta' in key)) and not df.exists():
                    print(f'S3: {key}\tLOCAL: {df}')
                    s3.download_file(model_granule_bucket, str(key), str(df))
                    num_downloaded += 1
    except Exception as e:
        status = f'ERROR Failed to download files "{e}"'

    return (status, (data_file_paths, meta_file_paths, num_downloaded))


# =================================================================================================
# DELETE LOCAL FILES
# =================================================================================================
def delete_files(data_file_paths, 
                 product_generation_config, 
                 fields, 
                 all=False):
    """
    Download the field files for the cur_ts from S3

    Args:
        data_file_paths (dict): Dictionary where key=field name, value=downloaded data file path at cur_ts for field
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        fields (list): List of field names
        all (optional, bool): Boolean for deleting all files or not. All files includes all .nc files in processed_output_dir

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """
    status = 'SUCCESS'
    try:
        # Create a single list with all the files to delete
        files_to_delete = []
        for field in fields:
            files_to_delete.append(data_file_paths[field])
            files_to_delete.append(f'{data_file_paths[field][:-5]}.meta')

        # if all is True, then include all the .nc files in processed_output_dir
        if all:
            # if all, then include the processed output files as well
            processed_output_files = glob.glob(f'{product_generation_config["processed_output_dir_base"]}/**/*.nc', recursive=True)
            files_to_delete.extend(processed_output_files)

        # Go through each file and delete it if it exists
        print(f'Deleting files: {files_to_delete}')
        for file in files_to_delete:
            if os.path.exists(file):
                os.remove(file)
    except Exception as e:
        status = f'ERROR deleting files: {data_file_paths}. Error: {e}'
    
    return status
    

# =================================================================================================
# GET LAND MASK
# =================================================================================================
def get_land_mask(mapping_factors_dir, 
                  k=0, 
                  extra_prints=False):
    """
    Get land mask from mapping_factors_dir for level k

    Args:
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        k (optional, int): Integer vertical level index to retrieve land mask for (0-{num_vertical_levels})
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, land_mask_ll) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            land_mask_ll (list): Flat list mask where nan indicates a dry point, and a 1 indicates a wet point
    """
    if extra_prints: print('\nGetting Land Mask')

    status = 'SUCCESS'
    land_mask_ll = []

    # check to see if you have already calculated the land mask
    land_mask_fname = Path(mapping_factors_dir) / 'land_mask' / f'ecco_latlon_land_mask_{k}.xz'

    # if so, load
    if land_mask_fname.is_file():
        if extra_prints: print('.... loading land_mask_ll')

        try:
            land_mask_ll = pickle.load(lzma.open(land_mask_fname, 'rb'))
        except:
            status = f'ERROR Unable to load land mask "{land_mask_fname}"'
            return (status, land_mask_ll)
    else:
        status = f'ERROR Land mask has not been created or cannot be found "{land_mask_fname}"'
        return (status, land_mask_ll)

    return (status, land_mask_ll)


# =================================================================================================
# GET LATLON GRID
# =================================================================================================
def get_latlon_grid(mapping_factors_dir, 
                    extra_prints=False):
    """
    Get latlon grid values from mapping_factors_dir

    Args:
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, latlon_grid) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            latlon_grid (list): List containing other lists/dictionaries describing the latlon grid:
                latlon_bounds: Contains the lat and lon bounds for each grid cell
                depth_bounds: Contains the vertical bounds for each vertical level
                target_grid_dict: Contains latlon grid 'shape', and lists of all the 'lats_1D' and 'lons_1D'
                wet_pts_k: Dictionary with key=vertical level index, and value=tuple of numpy.arrays of source grid wet points
    """
    if extra_prints: print('\nGetting latlon grid')

    status = 'SUCCESS'
    latlon_grid = {}

    # check to see if you have already calculated the latlon_grid
    latlon_grid_name = Path(mapping_factors_dir) / 'latlon_grid' / f'latlon_grid.xz'

    # if so, load
    if latlon_grid_name.is_file():
        if extra_prints: print('.... loading latlon_grid')

        try:
            latlon_grid = pickle.load(lzma.open(latlon_grid_name, 'rb'))
        except:
            status = f'ERROR Unable to load latlon_grid "{latlon_grid_name}"'
            return (status, latlon_grid)
    else:
        status = f'ERROR latlon_grid has not been created or cannot be found "{latlon_grid_name}"'
        return (status, latlon_grid)

    return (status, latlon_grid)


# =================================================================================================
# TRANSFORM LATLON
# =================================================================================================
def transform_latlon(ecco, 
                     Z, 
                     latlon_grid, 
                     data_file_path, 
                     record_end_time, 
                     nk, 
                     dataset_dim, 
                     var, 
                     output_freq_code, 
                     mapping_factors_dir, 
                     extra_prints=False):
    """
    Transform source grid to latlon grid

    Args:
        ecco (module 'ecco_v4_py'): ecco_v4_py imported module
        Z (numpy.array): Array of ecco_grid Z values
        latlon_grid (list): list of values describing the latlon grid (latlon_bounds, depth_bounds, target_grid_dict, wet_pts_k)
        data_file_path (PosixPath): Path to data file to load
        record_end_time (numpy.datetime64): End date of the current data file record
        nk (int): Integer number of total vertical levels
        dataset_dim (str): Dimension of the dataset to create factors for
        var (str): String name to assign to created DataArray
        output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, F_DS) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            F_DS (xarray.Dataset): xarray Dataset of the ECCO granule on a latlon grid
    """
    status = 'SUCCESS'

    # get target_grid_dict and wet_pts_k from latlon_grid
    (_, _, target_grid_dict, wet_pts_k) = latlon_grid

    # Make sure nk (number of vertical levels) is 1 for 2D datasets
    if dataset_dim == '2D':
        nk = 1

    # Read in model output mds
    if extra_prints: print('... loading mds', data_file_path)
    F = ecco.read_llc_to_tiles(data_file_path,
                                llc=90, skip=0, 
                                nk=nk, nl=1,
                                filetype='>f',
                                less_output=True,
                                use_xmitgcm=False)

    # Initialize blank transformed grid with nk vertical levels
    # For 2D, this has shape (1, X, Y)
    # For 3D, this has shape (nk, X, Y), where nk is the number of vertical levels
    latlon_shape = target_grid_dict['shape']
    F_ll = np.zeros((nk,latlon_shape[0],latlon_shape[1]))

    for k in range(nk):
        # Select corresponding vertical level from loaded model output file
        if dataset_dim == '2D':
            F_wet_native = F[wet_pts_k[k]]
        else:
            F_wet_native = F[k][wet_pts_k[k]]

        # Get land mask for the corresponding vertical level
        status, ll_land_mask = get_land_mask(mapping_factors_dir, 
                                             k=k, 
                                             extra_prints=extra_prints)
        if status != 'SUCCESS':
            return (status, [])

        # Load sparse matrix for level k from disk
        sm_path = mapping_factors_dir / 'sparse' / f'sparse_matrix_{k}.npz'
        B = sparse.load_npz(sm_path)

        # Dot product the sparse matrix and the wet source points
        # This performs a weighted average of the points within the target point radius,
        # or includes the nearest neighbor if applicable.
        A = B.T.dot(F_wet_native)

        # Set land values to nan
        A = np.where(np.isnan(ll_land_mask), np.nan, A)

        # Reshape transformed model output to latlon grid
        # Place into blank transformed grid at corresponding vertical level
        F_ll[k,:] = A.reshape(target_grid_dict['shape'])

    # Delete F and A from memory, not needed anymore
    del(F)
    del(A)

    # Remove single vertical level from 2D datasets
    if dataset_dim == '2D':
        F_ll = F_ll[0]

    # expand F_ll with time dimension
    F_ll = np.expand_dims(F_ll, 0)

    # Create DataArray's for transformed grid, with coords and dims.
    if extra_prints: print('... creating DataArray', F_ll)
    if dataset_dim == '2D':
        F_DA = xr.DataArray(F_ll,
                            coords=[[record_end_time],
                                    target_grid_dict['lats_1D'],
                                    target_grid_dict['lons_1D']],
                            dims=["time", "latitude","longitude"])
    elif dataset_dim == '3D':
        F_DA = xr.DataArray(F_ll,
                            coords=[[record_end_time],
                                    Z,
                                    target_grid_dict['lats_1D'],
                                    target_grid_dict['lons_1D']],
                            dims=["time", "Z", "latitude","longitude"])

    # assign name to data array
    if extra_prints: print('... assigning name', var)
    F_DA.name = var

    F_DS = F_DA.to_dataset()

    # add time bounds object
    if 'AVG' in output_freq_code:
        tb_ds, _ = \
            ecco.make_time_bounds_and_center_times_from_ecco_dataset(F_DS,
                                                                     output_freq_code)
        F_DS = xr.merge((F_DS, tb_ds))
        F_DS = F_DS.set_coords('time_bnds')

    return (status, F_DS)


# =================================================================================================
# TRANSFORM NATIVE
# =================================================================================================
def transform_native(ecco, 
                     var, 
                     ecco_land_masks, 
                     ecco_grid_dir_mds, 
                     mds_var_dir, 
                     output_freq_code, 
                     cur_ts, 
                     read_grid, 
                     extra_prints=False):
    """
    Transform source grid to native grid

    Args:
        ecco (module 'ecco_v4_py'): ecco_v4_py imported module
        var (str): String name to assign to created DataArray
        ecco_land_masks (tuple): Tuple of ecco_grid xarray DataArray land_masks (maskC, maskW, maskS)
        ecco_grid_dir_mds (PosixPath): Path to /ECCO-Dataset-Production/aws/ecco_grids/{ecco_version}
        mds_var_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/tmp/tmp_model_output/{ecco_version}/{output_freq}/{field}
        output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
        cur_ts (str): String of the current timestep (i.e. '0000000732')
        read_grid (bool): Boolean option from product_generation_config.yaml config file controls xmitgcm reading of grid file
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, F_DS) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            F_DS (xarray.Dataset): xarray Dataset of the ECCO granule on the native grid
    """
    status = 'SUCCESS'

    # land masks
    ecco_land_mask_c, ecco_land_mask_w, ecco_land_mask_s = ecco_land_masks

    short_mds_name = mds_var_dir.name.split('.')[0]
    mds_var_dir = mds_var_dir.parent

    # load specified field files from the provided directory
    # This loads them into the native tile grid
    status, F_DS = ecco.load_ecco_vars_from_mds(mds_var_dir,
                                                mds_grid_dir = ecco_grid_dir_mds,
                                                mds_files = short_mds_name,
                                                vars_to_load = var,
                                                drop_unused_coords = True,
                                                grid_vars_to_coords = False,
                                                output_freq_code=output_freq_code,
                                                model_time_steps_to_load=int(cur_ts),
                                                less_output = True,
                                                read_grid=read_grid)

    if status != 'SUCCESS':
        return (status, F_DS)

    # drop vars that are not the passed var
    vars_to_drop = set(F_DS.data_vars).difference(set([var]))
    F_DS.drop_vars(vars_to_drop)

    # determine all of the dimensions used by data variables
    all_var_dims = set([])
    for ecco_var in F_DS.data_vars:
        all_var_dims = set.union(all_var_dims, set(F_DS[ecco_var].dims))

    # mask the data variables
    for data_var in F_DS.data_vars:
        data_var_dims = set(F_DS[data_var].dims)
        if len(set.intersection(data_var_dims, set(['k','k_l','k_u','k_p1']))) > 0:
            data_var_3D = True
        else:
            data_var_3D = False

        # 'i, j = 'c' point
        if len(set.intersection(data_var_dims, set(['i','j']))) == 2 :
            if data_var_3D:
                F_DS[data_var].values = F_DS[data_var].values * ecco_land_mask_c.values
                if extra_prints: print('... masking with 3D maskC ', data_var)
            else:
                if extra_prints: print('... masking with 2D maskC ', data_var)
                F_DS[data_var].values= F_DS[data_var].values * ecco_land_mask_c[0,:].values

        # i_g, j = 'u' point
        elif len(set.intersection(data_var_dims, set(['i_g','j']))) == 2 :
            if data_var_3D:
                if extra_prints: print('... masking with 3D maskW ', data_var)
                F_DS[data_var].values = F_DS[data_var].values * ecco_land_mask_w.values
            else:
                if extra_prints: print('... masking with 2D maskW ', data_var)
                F_DS[data_var].values = F_DS[data_var].values * ecco_land_mask_w[0,:].values

        # i, j_g = 's' point
        elif len(set.intersection(data_var_dims, set(['i','j_g']))) == 2 :
            if data_var_3D:
                if extra_prints: print('... masking with 3D maskS ', data_var)
                F_DS[data_var].values = F_DS[data_var].values * ecco_land_mask_s.values
            else:
                if extra_prints: print('... masking with 2D maskS ', data_var)
                F_DS[data_var].values = F_DS[data_var].values * ecco_land_mask_s[0,:].values

        else:
            status = f'ERROR: Cannot determine dimension of data variable "{data_var}"'
            return (status, F_DS)

    return (status, F_DS)


# =================================================================================================
# GLOBAL DS CHANGES
# =================================================================================================
def global_DS_changes(F_DS, 
                      output_freq_code, 
                      grouping, 
                      array_precision, 
                      ecco_grid, 
                      latlon_grid, 
                      netcdf_fill_value, 
                      record_times, 
                      extra_prints=False):
    """
    Apply changes to every transformed Dataset (coords, values, etc.)

    Args:
        F_DS (xarray.Dataset): xarray Dataset of the transformed ECCO granule
        output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
        grouping (dict): Dictionary containing information about the current grouping (name, fields, etc.)
        array_precision (type): Type, for setting array precision
        ecco_grid (xarray.Dataset): Dataset object of the ECCO grid
        latlon_grid (list): list of values describing the latlon grid (latlon_bounds, depth_bounds, target_grid_dict, wet_pts_k)
        netcdf_fill_value (float): Fill value to replace nans with
        record_times (dict): Dictionary of 'start', 'center', and 'end' numpy.datetime64 times
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, F_DS) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            F_DS (xarray.Dataset): xarray Dataset of the transformed ECCO granule
    """
    status = 'SUCCESS'

    # get data variable name from F_DS
    var = list(F_DS.keys())[0]

    (latlon_bounds, depth_bounds, _, _) = latlon_grid

    try:
        # Specify time bounds values for the dataset
        if 'AVG' in output_freq_code:
            F_DS.time_bnds.values[0][0] = record_times['start']
            F_DS.time_bnds.values[0][1] = record_times['end']
            F_DS.time.values[0] = record_times['center']

        # Possibly rename variable if indicated
        if 'variable_rename' in grouping.keys():
            rename_pairs = grouping['variable_rename'].split(',')

            for rename_pair in rename_pairs:
                orig_var_name, new_var_name = rename_pair.split(':')

                if var == orig_var_name:
                    F_DS = F_DS.rename({orig_var_name:new_var_name})
                    if extra_prints: print('renaming from ', orig_var_name, new_var_name)
                    if extra_prints: print(F_DS.data_vars)

        # cast data variable to desired precision
        for data_var in F_DS.data_vars:
            if F_DS[data_var].values.dtype != array_precision:
                F_DS[data_var].values = F_DS[data_var].astype(array_precision)


        # set valid min and max, and replace nan with fill values
        for data_var in F_DS.data_vars:
            F_DS[data_var].attrs['valid_min'] = np.nanmin(F_DS[data_var].values)
            F_DS[data_var].attrs['valid_max'] = np.nanmax(F_DS[data_var].values)

            # replace nan with fill value
            F_DS[data_var].values = np.where(np.isnan(F_DS[data_var].values),
                                                netcdf_fill_value, F_DS[data_var].values)

        # add bounds to spatial coordinates
        if grouping['product'] == 'latlon':
            # assign lat and lon bounds
            F_DS=F_DS.assign_coords({"latitude_bnds": (("latitude","nv"), latlon_bounds['lat'])})
            F_DS=F_DS.assign_coords({"longitude_bnds": (("longitude","nv"), latlon_bounds['lon'])})

            # if 3D assign depth bounds, use Z as index
            if grouping['dimension'] == '3D' and 'Z' in list(F_DS.coords):
                F_DS = F_DS.assign_coords({"Z_bnds": (("Z","nv"), depth_bounds)})

        elif grouping['product'] == 'native':
            # Assign XC bounds coords to the dataset if it is in the grid
            if 'XC_bnds' in ecco_grid.coords:
                F_DS = F_DS.assign_coords({"XC_bnds": (("tile","j","i","nb"), ecco_grid['XC_bnds'].data)})
            # Assign YC bounds coords to the dataset if it is in the grid
            if 'YC_bnds' in ecco_grid.coords:
                F_DS = F_DS.assign_coords({"YC_bnds": (("tile","j","i","nb"), ecco_grid['YC_bnds'].data)})

            #   if 3D assign depth bounds, use k as index
            if grouping['dimension'] == '3D' and 'Z' in list(F_DS.coords):
                F_DS = F_DS.assign_coords({"Z_bnds": (("k","nv"), depth_bounds)})
    except Exception as e:
        status = f'ERROR unable to apply global DS changes. Error: {e}'
    
    return (status, F_DS)


# =================================================================================================
# SET METADATA
# =================================================================================================
def set_metadata(ecco, 
                 G, 
                 all_metadata, 
                 output_freq_code, 
                 netcdf_fill_value, 
                 grouping, 
                 output_dir_freq, 
                 dataset_description, 
                 product_generation_config, 
                 extra_prints=False):
    """
    Set metadata of the final output Dataset for the current timestep

    Args:
        ecco (module 'ecco_v4_py'): ecco_v4_py imported module
        G (xarray.Dataset): xarray Dataset for grouping (all fields) for current timestep
        all_metadata (dict): Dictionary with all metadata dictionaries (var, coord, global for native/latlon)
        output_freq_code (str): String output frequency code (i.e. 'AVG_MON', 'AVG_DAY', 'SNAP')
        netcdf_fill_value (float): Fill value to replace nans with
        grouping (dict): Dictionary containing information about the current grouping (name, fields, etc.)
        output_dir_freq (PosixPath): Path to /ECCO-Dataset-Production/aws/tmp/tmp_output/{ecco_version}/{product_type}/{freq}/
        dataset_description (str): String to set as the output dataset description attribute
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        (status, G, netcdf_output_filename, encoding) (tuple):
            status (str): String that is either "SUCCESS" or "ERROR {error message}"
            G (xarray.Dataset): xarray Dataset for grouping (all fields) for current timestep
            netcdf_output_filename (PosixPath): Path to save the Dataset to (includes nc filename)
            encoding (dict): Dictionary to use as the encoding when saving to netcdf
    """
    status = 'SUCCESS'

    product_type = grouping['product']
    dataset_dim = grouping['dimension']
    doi = product_generation_config['doi']
    ecco_version = product_generation_config['ecco_version']
    podaac_dir = Path(product_generation_config['metadata_dir']) / product_generation_config['podaac_metadata_filename']
    curr_filename_tail = product_generation_config[f'filename_tail_{product_type}']
    filename_tail = f'_ECCO_{ecco_version}_{product_type}_{curr_filename_tail}'

    # ADD VARIABLE SPECIFIC METADATA TO VARIABLE ATTRIBUTES (DATA ARRAYS)
    if extra_prints: print('\n... adding metadata specific to the variable')
    G, grouping_gcmd_keywords = \
        ecco.add_variable_metadata(all_metadata['var_native'], G)

    if product_type == 'latlon':
        if extra_prints: print('\n... using latlon dataseta metadata specific to the variable')
        G, grouping_gcmd_keywords = \
            ecco.add_variable_metadata(all_metadata['var_latlon'], G)

    # ADD COORDINATE METADATA
    if product_type == 'latlon':
        if extra_prints: print('\n... adding coordinate metadata for latlon dataset')
        G = ecco.add_coordinate_metadata(all_metadata['coord_latlon'],G)

    elif product_type == 'native':
        if extra_prints: print('\n... adding coordinate metadata for native dataset')
        G = ecco.add_coordinate_metadata(all_metadata['coord_native'],G)

    # ADD GLOBAL METADATA
    if extra_prints: print("\n... adding global metadata for all datasets")
    G = ecco.add_global_metadata(all_metadata['global_all'], G, dataset_dim)

    if product_type == 'latlon':
        if extra_prints: print('\n... adding global meta for latlon dataset')
        G = ecco.add_global_metadata(all_metadata['global_latlon'], G, dataset_dim)
    elif product_type == 'native':
        if extra_prints: print('\n... adding global metadata for native dataset')
        G = ecco.add_global_metadata(all_metadata['global_native'], G, dataset_dim)

    # Add time metadata if dataset has a time coordinate
    # if so -> add_coordinate_metadata(all_metadata['coord_time'],G) (TO BE MADE)
    # if not -> keep going
    if 'time' in G.coords:
        if 'coord_time' in all_metadata:
            G = ecco.add_coordinate_metadata(all_metadata['coord_time'], G)
        else:
            print(f'No "coord_time" metadata found')

    # ADD GLOBAL METADATA ASSOCIATED WITH TIME AND DATE
    if extra_prints: print('\n... adding time / data global attrs')
    if 'AVG' in output_freq_code:
        G.attrs['time_coverage_start'] = str(G.time_bnds.values[0][0])[0:19]
        G.attrs['time_coverage_end'] = str(G.time_bnds.values[0][1])[0:19]

    else:
        G.attrs['time_coverage_start'] = str(G.time.values[0])[0:19]
        G.attrs['time_coverage_end'] = str(G.time.values[0])[0:19]

    # current time and date
    current_time = datetime.datetime.now().isoformat()[0:19]
    G.attrs['date_created'] = current_time
    G.attrs['date_modified'] = current_time
    G.attrs['date_metadata_modified'] = current_time
    G.attrs['date_issued'] = current_time

    # Update G attrs with values from product_generation_config.yaml
    G.attrs['history'] = product_generation_config['history']
    G.attrs['geospatial_vertical_min'] = product_generation_config['geospatial_vertical_min']
    G.attrs['product_time_coverage_start'] = product_generation_config['model_start_time']
    G.attrs['product_time_coverage_end'] = product_generation_config['model_end_time']
    G.attrs['product_version'] = product_generation_config['product_version']
    G.attrs['references'] = product_generation_config['references']
    G.attrs['source'] = product_generation_config['source']
    G.attrs['summary'] = product_generation_config['summary']

    # add coordinate attributes to the variables
    dv_coordinate_attrs = {}

    for dv in list(G.data_vars):
        dv_coords_orig = set(list(G[dv].coords))

        # REMOVE TIME STEP FROM LIST OF COORDINATES (PODAAC REQUEST)
        set_intersect = dv_coords_orig.intersection(set(['XC','YC','XG','YG','Z','Zp1','Zu','Zl','time']))

        dv_coordinate_attrs[dv] = " ".join(set_intersect)


    if extra_prints: print('\n... creating variable encodings')
    # PROVIDE SPECIFIC ENCODING DIRECTIVES FOR EACH DATA VAR
    dv_encoding = {}
    for dv in G.data_vars:
        dv_encoding[dv] = {'zlib':True,
                           'complevel':5,
                           'shuffle':True,
                           '_FillValue':netcdf_fill_value}

        # overwrite default coordinats attribute (PODAAC REQUEST)
        G[dv].encoding['coordinates'] = dv_coordinate_attrs[dv]

    # PROVIDE SPECIFIC ENCODING DIRECTIVES FOR EACH COORDINATE
    if extra_prints: print('\n... creating coordinate encodings')
    coord_encoding = {}

    for coord in G.coords:
        # default encoding: no fill value, float32
        coord_encoding[coord] = {'_FillValue':None, 'dtype':'float32'}

        if (G[coord].values.dtype == np.int32) or (G[coord].values.dtype == np.int64):
            coord_encoding[coord]['dtype'] ='int32'

        if coord == 'time' or coord == 'time_bnds':
            coord_encoding[coord]['dtype'] ='int32'

            if 'units' in G[coord].attrs:
                # apply units as encoding for time
                coord_encoding[coord]['units'] = G[coord].attrs['units']
                # delete from the attributes list
                del G[coord].attrs['units']

        elif coord == 'time_step':
            coord_encoding[coord]['dtype'] ='int32'

    # MERGE ENCODINGS for coordinates and variables
    encoding = {**dv_encoding, **coord_encoding}

    # MERGE GCMD KEYWORDS
    if extra_prints: print('\n... merging GCMD keywords')
    common_gcmd_keywords = G.keywords.split(',')
    gcmd_keywords_list = list(set(grouping_gcmd_keywords + common_gcmd_keywords))

    gcmd_keywords_list = sorted(gcmd_keywords_list)

    # Takes a list of strings and combines them into a single comma separated string
    gcmd_keyword_str = ''
    for gcmd_keyword in gcmd_keywords_list:
        if len(gcmd_keyword_str) == 0:
            gcmd_keyword_str = gcmd_keyword
        else:
            gcmd_keyword_str += ', ' + gcmd_keyword

    #print(gcmd_keyword_str)
    G.attrs['keywords'] = gcmd_keyword_str

    ## ADD FINISHING TOUCHES

    # uuid
    if extra_prints: print('\n... adding uuid')
    G.attrs['uuid'] = str(uuid.uuid1())

    # add any dataset grouping specific comments.
    if 'comment' in grouping:
        G.attrs['comment'] = G.attrs['comment'] + ' ' + grouping['comment']

    # set the long name of the time attribute
    if 'AVG' in output_freq_code:
        G.time.attrs['long_name'] = 'center time of averaging period'
    else:
        G.time.attrs['long_name'] = 'snapshot time'

    # set averaging period duration and resolution
    if extra_prints: print('\n... setting time coverage resolution')
    # --- AVG MON
    if output_freq_code == 'AVG_MON':
        G.attrs['time_coverage_duration'] = 'P1M'
        G.attrs['time_coverage_resolution'] = 'P1M'

        date_str = str(np.datetime64(G.time.values[0],'M'))
        ppp_tttt = 'mon_mean'

    # --- AVG DAY
    elif output_freq_code == 'AVG_DAY':
        G.attrs['time_coverage_duration'] = 'P1D'
        G.attrs['time_coverage_resolution'] = 'P1D'

        date_str = str(np.datetime64(G.time.values[0],'D'))
        ppp_tttt = 'day_mean'

    # --- SNAPSHOT
    elif output_freq_code == 'SNAPSHOT':
        G.attrs['time_coverage_duration'] = 'P0S'
        G.attrs['time_coverage_resolution'] = 'P0S'

        G.time.attrs.pop('bounds')

        # convert from original
        #   '1992-01-16T12:00:00.000000000'
        # to new format
        # '1992-01-16T120000'
        date_str = str(G.time.values[0])[0:19].replace(':','')
        ppp_tttt = 'snap'

    ## construct filename
    if extra_prints: print('\n... creating filename')

    filename = grouping['filename'] + '_' + ppp_tttt + '_' + date_str + filename_tail

    # make subdirectory for the grouping
    output_dir = Path(output_dir_freq) / grouping['filename']
    if extra_prints: print('\n... creating output_dir', output_dir)

    if not output_dir.exists():
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except:
            status = f'ERROR Cannot make output directory "{output_dir}"'
            return (status, G, '', encoding)


    # create full pathname for netcdf file
    netcdf_filename = Path(output_dir) / filename

    # add product name attribute = filename
    G.attrs['product_name'] = filename

    # add summary attribute = description of dataset
    G.attrs['summary'] = dataset_description + ' ' + G.attrs['summary']

    # get podaac metadata based on filename
    if extra_prints: print('\n... getting PODAAC metadata')
    # podaac_dataset_table = read_csv(podaac_dir / 'datasets.csv')
    podaac_dataset_table = read_csv(podaac_dir)
    podaac_metadata = __find_podaac_metadata(podaac_dataset_table, 
                                             filename, 
                                             doi, 
                                             ecco_version, 
                                             extra_prints)

    # apply podaac metadata based on filename
    if extra_prints: print('\n... applying PODAAC metadata')
    G = __apply_podaac_metadata(G, podaac_metadata)

    # sort comments alphabetically
    if extra_prints: print('\n... sorting global attributes')
    G.attrs = __sort_attrs(G.attrs)

    # add one final comment (PODAAC request)
    G.attrs["coordinates_comment"] = \
        "Note: the global 'coordinates' attribute describes auxillary coordinates."

    return (status, G, netcdf_filename, encoding)


def __find_podaac_metadata(podaac_dataset_table, 
                           filename, 
                           doi, 
                           ecco_version,
                           extra_prints=False):
    """
    Return revised file metadata based on an input ECCO `filename`.

    This should consistently parse a filename that conforms to the
    ECCO filename conventions and match it to a row in my metadata
    table.

    Args:
        podaac_dataset_table (pandas.Dataframe): pandas Dataframe containing contents of PODAAC metadata file
        filename (str): String filename of the output netCDF dataset
        doi (str): String DOI of the dataset from product_generation_config.yaml config file
        ecco_version (str): String current ECCO version, from product_generation_config.yaml config file

    Returns:
        podaac_metadata (dict): Dictionary containing 'id', 'metadata_link', and 'title'

    """

    # Use filename components to find metadata row from podaac_dataset_table.
    if "_snap_" in filename:
        head, tail = filename.split("_snap_")
        head = f"{head}_snap"
    elif "_day_mean_" in filename:
        head, tail = filename.split("_day_mean_")
        head = f"{head}_day_mean"
    elif "_mon_mean_" in filename:
        head, tail = filename.split("_mon_mean_")
        head = f"{head}_mon_mean"
    else:
        raise Exception(f"Error: filename may not conform to ECCO {ecco_version} convention.")

    if extra_prints: print('split filename into ', head, tail)

    tail = tail.split(f"_ECCO_{ecco_version}_")[1]

    if extra_prints: print('further split tail into ',  tail)

    # Get the filenames column from my table as a list of strings.
    names = podaac_dataset_table['DATASET.FILENAME']

    # Find the index of the row with a filename with matching head, tail chars.
    index = names.apply(lambda x: all([x.startswith(head), x.endswith(tail)]))

    # Select that row from podaac_dataset_table table and make a copy of it.
    metadata = podaac_dataset_table[index].iloc[0].to_dict()

    podaac_metadata = {
        'id': metadata['DATASET.PERSISTENT_ID'].replace("PODAAC-",f"{doi}/"),
        'metadata_link': f"https://cmr.earthdata.nasa.gov/search/collections.umm_json?ShortName={metadata['DATASET.SHORT_NAME']}",
        'title': metadata['DATASET.LONG_NAME'],
    }

    if extra_prints:
        print('\n... podaac metadata:')
        print(podaac_metadata)

    return podaac_metadata


def __apply_podaac_metadata(G, 
                            podaac_metadata):
    """
    Apply attributes podaac_metadata to ECCO dataset and its variables.

    Attributes that are commented `#` are retained with no modifications.
    Attributes that are assigned `None` are dropped.
    New attributes added to dataset.

    Args:
        G (xarray.Dataset): xarray Dataset for grouping (all fields) for current timestep
        podaac_metadata (dict): Dictionary containing 'id', 'metadata_link', and 'title'

    Returns:
        G (xarray.Dataset): xarray Dataset for grouping (all fields) for current timestep

    """
    # REPLACE GLOBAL ATTRIBUTES WITH NEW/UPDATED DICTIONARY.
    atts = G.attrs
    for name, modifier in podaac_metadata.items():
        if callable(modifier):
            atts.update({name: modifier(x=atts[name])})

        elif modifier is None:
            if name in atts:
                del atts[name]
        else:
            atts.update({name: modifier})

    G.attrs = atts

    # MODIFY VARIABLE ATTRIBUTES IN A LOOP.
    for v in G.variables:
        if "gmcd_keywords" in G.variables[v].attrs:
            del G.variables[v].attrs['gmcd_keywords']

    return G  # Return the updated xarray Dataset.


def __sort_attrs(attrs):
    """
    Sort attribute dictionary

    Args:
        attrs (dict): Attribute dictionary

    Returns:
        od (collections.OrderedDict): Sorted attribute dictionary

    """
    # Sort attributes of a givent attribute dictionary
    od = OrderedDict()

    keys = sorted(list(attrs.keys()),key=str.casefold)

    for k in keys:
        od[k] = attrs[k]

    return od