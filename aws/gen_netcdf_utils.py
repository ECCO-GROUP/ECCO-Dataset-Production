import lzma
import uuid
import pickle
import datetime
import numpy as np
import xarray as xr
from pathlib import Path
from scipy import sparse
from pandas import read_csv
from collections import OrderedDict


# ==========================================================================================================================
# GET LAND MASK, and GET LATLON GRID
# ==========================================================================================================================
def get_land_mask(mapping_factors_dir, k=0, debug_mode=False, extra_prints=False):
    if extra_prints: print('\nGetting Land Mask')

    status = 'SUCCESS'
    land_mask_ll = []

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING LAND MASK')
        land_mask_ll = []
    else:
        # check to see if you have already calculated the land mask
        land_mask_fname = Path(mapping_factors_dir) / 'land_mask' / f'ecco_latlon_land_mask_{k}.xz'

        if land_mask_fname.is_file():
            # if so, load
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


def get_latlon_grid(mapping_factors_dir, debug_mode=False, extra_prints=False):
    if extra_prints: print('\nGetting latlon grid')

    status = 'SUCCESS'
    latlon_grid = {}

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING latlon grid')
        latlon_grid = {}
    else:
        # check to see if you have already calculated the latlon_grid
        latlon_grid_name = Path(mapping_factors_dir) / 'latlon_grid' / f'latlon_grid.xz'

        if latlon_grid_name.is_file():
            # if so, load
            try:
                latlon_grid = pickle.load(lzma.open(latlon_grid_name, 'rb'))
            except:
                status = f'ERROR Unable to load land mask "{latlon_grid_name}"'
                return (status, latlon_grid)
        else:
            status = f'ERROR Land mask has not been created or cannot be found "{latlon_grid_name}"'
            return (status, latlon_grid)

    return (status, latlon_grid)


# ==========================================================================================================================
# VARIABLE TRANSFORMATION UTILS
# ==========================================================================================================================
def transform_latlon(ecco, Z, wet_pts_k, target_grid, data_file_path, record_end_time, 
                    nk, dataset_dim, var, output_freq_code, mapping_factors_dir, extra_prints=False):
    status = 'SUCCESS'

    # Make sure nk (number of vertical levels) is 1 for 2D datasets
    if dataset_dim == '2D':
        nk = 1

    # # if str(data_file_path) == '/Users/bark/Documents/ECCO_GROUP/ECCO-Dataset-Production/aws/temp_model_output/V4r4/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000001428.data' or str(data_file_path) == '/Users/bark/Documents/ECCO_GROUP/ECCO-Dataset-Production/aws/temp_model_output/V4r4/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000002172.data':
    # if str(data_file_path) == '/tmp/V4r4/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000001428.data' or str(data_file_path) == '/tmp/V4r4/diags_monthly/SSH_mon_mean/SSH_mon_mean.0000002172.data':
    #     status = f'ERROR test error'
    # if status != 'SUCCESS':
    #     return (status, [])

    # Read in model output mds
    if extra_prints: print('... loading mds', data_file_path)
    F = ecco.read_llc_to_tiles(data_file_path,
                                llc=90, skip=0, 
                                nk=nk, nl=1,
                                filetype='>f',
                                less_output=True,
                                use_xmitgcm=False)

    # Initialize blank transformed grid with nk vertical levels
    # For 2D, this has shape (1, 360, 720)
    # For 3D, this has shape (nk, 360, 720), where nk is the number of vertical levels
    F_ll = np.zeros((nk,360,720))

    for k in range(nk):
        # Select corresponding vertical level from loaded model output file
        if dataset_dim == '2D':
            F_wet_native = F[wet_pts_k[k]]
        else:
            F_wet_native = F[k][wet_pts_k[k]]

        # Get land mask for the corresponding vertical level
        status, ll_land_mask = get_land_mask(mapping_factors_dir, k=k, extra_prints=extra_prints)
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
        F_ll[k,:] = A.reshape(target_grid['shape'])

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
                                    target_grid['lats_1D'],
                                    target_grid['lons_1D']],
                            dims=["time", "latitude","longitude"])
    elif dataset_dim == '3D':
        F_DA = xr.DataArray(F_ll,
                            coords=[[record_end_time],
                                    Z,
                                    target_grid['lats_1D'],
                                    target_grid['lons_1D']],
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


def transform_native(ecco, var, ecco_land_masks, ecco_grid_dir_mds, mds_var_dir, output_freq_code, cur_ts, extra_prints=False):
    status = 'SUCCESS'
    import time
    native_trans_time = time.time()

    # land masks
    ll_time = time.time()
    ecco_land_mask_c, ecco_land_mask_w, ecco_land_mask_s = ecco_land_masks
    ll_time = time.time() - ll_time

    short_mds_name = mds_var_dir.name.split('.')[0]
    mds_var_dir = mds_var_dir.parent

    load_time = time.time()
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
                                        less_output = True)
    load_time = time.time() - load_time

    if status != 'SUCCESS':
        return (status, F_DS)

    extra_time = time.time()
    vars_to_drop = set(F_DS.data_vars).difference(set([var]))
    F_DS.drop_vars(vars_to_drop)

    # determine all of the dimensions used by data variables
    all_var_dims = set([])
    for ecco_var in F_DS.data_vars:
        all_var_dims = set.union(all_var_dims, set(F_DS[ecco_var].dims))
    extra_time = time.time() - extra_time

    mask_time = time.time()
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
    mask_time = time.time() - mask_time

    native_trans_time = time.time() - native_trans_time
    print(f'\nNative trans time: {native_trans_time}')
    print(f'Native load time: {load_time}')
    print(f'Native mask time: {mask_time}')
    print(f'Native land mask time: {ll_time}')
    print(f'Native extra time: {extra_time}\n')
    
    return (status, F_DS)


# ==========================================================================================================================
# METADATA UTILS
# ==========================================================================================================================
def global_DS_changes(F_DS, output_freq_code, grouping, var, array_precision, ecco_grid,
                        depth_bounds, product_type, latlon_bounds, netcdf_fill_value, dataset_dim, record_times, extra_prints=False):
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
    if product_type == 'latlon':
        # assign lat and lon bounds
        F_DS=F_DS.assign_coords({"latitude_bnds": (("latitude","nv"), latlon_bounds['lat'])})
        F_DS=F_DS.assign_coords({"longitude_bnds": (("longitude","nv"), latlon_bounds['lon'])})

        # if 3D assign depth bounds, use Z as index
        if dataset_dim == '3D' and 'Z' in list(F_DS.coords):
            F_DS = F_DS.assign_coords({"Z_bnds": (("Z","nv"), depth_bounds)})

    elif product_type == 'native':
        # Assign XC bounds coords to the dataset if it is in the grid
        if 'XC_bnds' in ecco_grid.coords:
            F_DS = F_DS.assign_coords({"XC_bnds": (("tile","j","i","nb"), ecco_grid['XC_bnds'].data)})
        # Assign YC bounds coords to the dataset if it is in the grid
        if 'YC_bnds' in ecco_grid.coords:
            F_DS = F_DS.assign_coords({"YC_bnds": (("tile","j","i","nb"), ecco_grid['YC_bnds'].data)})

        #   if 3D assign depth bounds, use k as index
        if dataset_dim == '3D' and 'Z' in list(F_DS.coords):
            F_DS = F_DS.assign_coords({"Z_bnds": (("k","nv"), depth_bounds)})
    
    return F_DS


def sort_attrs(attrs):
    # Sort attributes of a givent attribute dictionary
    od = OrderedDict()

    keys = sorted(list(attrs.keys()),key=str.casefold)

    for k in keys:
        od[k] = attrs[k]

    return od


def find_podaac_metadata(podaac_dataset_table, filename, doi, ecco_version, debug=False):
    """Return revised file metadata based on an input ECCO `filename`.

    This should consistently parse a filename that conforms to the
    ECCO filename conventions and match it to a row in my metadata
    table.

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

    if debug:
        print('split filename into ', head, tail)

    tail = tail.split(f"_ECCO_{ecco_version}_")[1]

    if debug:
        print('further split tail into ',  tail)

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
    if debug:
        print('\n... podaac metadata:')
        print(podaac_metadata)

    return podaac_metadata


def apply_podaac_metadata(xrds, podaac_metadata):
    """Apply attributes podaac_metadata to ECCO dataset and its variables.

    Attributes that are commented `#` are retained with no modifications.
    Attributes that are assigned `None` are dropped.
    New attributes added to dataset.

    """
    # REPLACE GLOBAL ATTRIBUTES WITH NEW/UPDATED DICTIONARY.
    atts = xrds.attrs
    for name, modifier in podaac_metadata.items():
        if callable(modifier):
            atts.update({name: modifier(x=atts[name])})

        elif modifier is None:
            if name in atts:
                del atts[name]
        else:
            atts.update({name: modifier})

    xrds.attrs = atts

    # MODIFY VARIABLE ATTRIBUTES IN A LOOP.
    for v in xrds.variables:
        if "gmcd_keywords" in xrds.variables[v].attrs:
            del xrds.variables[v].attrs['gmcd_keywords']

    return xrds  # Return the updated xarray Dataset.


def set_metadata(ecco, G, product_type, all_metadata, dataset_dim, output_freq_code, netcdf_fill_value,
                    grouping, filename_tail, output_dir_freq, dataset_description, podaac_dir, doi, ecco_version, extra_prints=False):
    status = 'SUCCESS'

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
    gcmd_keywords_list = set(grouping_gcmd_keywords + common_gcmd_keywords)

    gcmd_keyword_str = ''
    for gcmd_keyword in gcmd_keywords_list:
        if len(gcmd_keyword_str) == 0:
            gcmd_keyword_str = gcmd_keyword
        else:
            gcmd_keyword_str += ', ' + gcmd_keyword

    #print(gcmd_keyword_str)
    G.attrs['keywords'] = gcmd_keyword_str

    ## ADD FINISHING TOUCHES

    # uuic
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
    netcdf_output_filename = Path(output_dir) / filename

    # add product name attribute = filename
    G.attrs['product_name'] = filename

    # add summary attribute = description of dataset
    G.attrs['summary'] = dataset_description + ' ' + G.attrs['summary']

    # get podaac metadata based on filename
    if extra_prints: print('\n... getting PODAAC metadata')
    # podaac_dataset_table = read_csv(podaac_dir / 'datasets.csv')
    podaac_dataset_table = read_csv(podaac_dir)
    podaac_metadata = find_podaac_metadata(podaac_dataset_table, filename, doi, ecco_version)

    # apply podaac metadata based on filename
    if extra_prints: print('\n... applying PODAAC metadata')
    G = apply_podaac_metadata(G, podaac_metadata)

    # sort comments alphabetically
    if extra_prints: print('\n... sorting global attributes')
    G.attrs = sort_attrs(G.attrs)

    # add one final comment (PODAAC request)
    G.attrs["coordinates_comment"] = \
        "Note: the global 'coordinates' attribute describes auxillary coordinates."

    return (status, G, netcdf_output_filename, encoding)