import os
import sys
import lzma
import pickle
import numpy as np
import xarray as xr
import pyresample as pr
from pathlib import Path
from scipy import sparse

import gen_netcdf_utils as gen_netcdf_utils

# =================================================================================================
# GET MAPPING FACTORS
# =================================================================================================
def get_mapping_factors(dataset_dim, mapping_factors_dir, factors_to_get, debug_mode=False, extra_prints=False, k=0):
    status = 1
    
    # factors_to_get : factors to load in from the mapping_factors_dir
    # can be 'all', 'k', or 'both'
    grid_mappings_all = []
    grid_mappings_k = []

    if extra_prints: print('\nGetting Grid Mappings')
    grid_mapping_fname_all = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_all.xz'
    grid_mapping_fname_2D = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_2D.xz'
    grid_mapping_fname_3D = Path(mapping_factors_dir) / '3D' / f'ecco_latlon_grid_mappings_3D_{k}.xz'

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING GRID MAPPINGS')
        grid_mappings_all = []
        grid_mappings_k = []
    else:
        # Check to see that the mapping factors have been made
        if (dataset_dim == '2D' and grid_mapping_fname_2D.is_file()) or (dataset_dim == '3D' and grid_mapping_fname_3D.is_file()):
            # if so, load
            try:
                if factors_to_get == 'all' or factors_to_get == 'both':
                    if extra_prints: print(f'... loading ecco_latlon_grid_mappings_all.xz')
                    grid_mappings_all = pickle.load(lzma.open(grid_mapping_fname_all, 'rb'))

                if factors_to_get == 'k' or factors_to_get == 'both':
                    if dataset_dim == '2D':
                        if extra_prints: print(f'... loading ecco_latlon_grid_mappings_{dataset_dim}.xz')
                        grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_2D, 'rb'))
                    elif dataset_dim == '3D':
                        if extra_prints: print(f'... loading ecco_latlon_grid_mappings_{dataset_dim}_{k}.xz')
                        grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_3D, 'rb'))
            except:
                print(f'ERROR Unable to load grid mapping factors: {mapping_factors_dir}')
                return (-1, grid_mappings_all, grid_mappings_k)
        else:
            print(f'ERROR Grid mapping factors have not been created or cannot be found: {mapping_factors_dir}')
            return (-1, grid_mappings_all, grid_mappings_k)

    return (status, grid_mappings_all, grid_mappings_k)


# =================================================================================================
# CREATE MAPPING FACTORS
# =================================================================================================
def create_mapping_factors(ea, dataset_dim, mapping_factors_dir, debug_mode, source_grid_all, target_grid, target_grid_radius, source_grid_min_L, source_grid_max_L, source_grid_k, nk):
    print(f'\nCreating Grid Mappings ({dataset_dim})')

    status = 1
    grid_mapping_fname_all = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_all.xz'
    grid_mapping_fname_2D = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_2D.xz'
    grid_mapping_fname_3D = Path(mapping_factors_dir) / '3D'

    if not grid_mapping_fname_3D.exists():
        try:
            grid_mapping_fname_3D.mkdir()
        except:
            print(f'ERROR Cannot make grid mappings 3D directory "{grid_mapping_fname_3D}"')
            return -1

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING GRID MAPPINGS')
        grid_mappings_all = []
        grid_mappings_k = []
    else:
        # first check to see if you have already calculated the grid mapping factors
        if dataset_dim == '3D':
            all_3D = True
            all_3D_fnames = [f'ecco_latlon_grid_mappings_3D_{i}.xz' for i in range(nk)]
            curr_3D_fnames = os.listdir(grid_mapping_fname_3D)
            for fname in all_3D_fnames:
                if fname not in curr_3D_fnames:  
                    all_3D = False
                    break

        if (dataset_dim == '2D' and grid_mapping_fname_2D.is_file()) or (dataset_dim == '3D' and all_3D):
            # Factors already made, continuing
            print('... mapping factors already created')
        else:
            # if not, make new grid mapping factors
            print('... no mapping factors found, recalculating')

            if ~(grid_mapping_fname_all.is_file()):
                # find the mapping between all points of the ECCO grid and the target grid.
                grid_mappings_all = \
                    ea.find_mappings_from_source_to_target(source_grid_all,
                                                            target_grid,
                                                            target_grid_radius,
                                                            source_grid_min_L,
                                                            source_grid_max_L)

                # Save grid_mappings_all
                try:
                    pickle.dump(grid_mappings_all, lzma.open(grid_mapping_fname_all, 'wb'))
                except:
                    print(f'ERROR Cannot save grid_mappings_all file "{grid_mapping_fname_all}"')
                    return -1

            # If the dataset is 2D, only compute one level of the mapping factors
            if dataset_dim == '2D':
                nk = 1

            # Find the mapping factors between all wet points of the ECCO grid
            # at each vertical level and the target grid
            for k_i in range(nk):
                print(k_i)
                grid_mappings_k = \
                    ea.find_mappings_from_source_to_target(source_grid_k[k_i],
                                                            target_grid,
                                                            target_grid_radius,
                                                            source_grid_min_L,
                                                            source_grid_max_L)

                try:
                    if dataset_dim == '2D':
                        pickle.dump(grid_mappings_k, lzma.open(grid_mapping_fname_2D, 'wb'))
                    elif dataset_dim == '3D':
                        fname_3D = Path(grid_mapping_fname_3D) / f'ecco_latlon_grid_mappings_3D_{k_i}.xz'
                        pickle.dump(grid_mappings_k, lzma.open(fname_3D, 'wb'))
                except:
                    print(f'ERROR Cannot save grid_mappings_k file(s) "{mapping_factors_dir}"')
                    return -1
    return status


# =================================================================================================
# CREATE LAND MASK
# =================================================================================================
def create_land_mask(ea, mapping_factors_dir, debug_mode, nk, target_grid_shape, ecco_grid, dataset_dim):
    print(f'\nCreating Land Mask ({dataset_dim})')

    status = 1
    ecco_land_mask_c = ecco_grid.maskC.copy(deep=True)
    ecco_land_mask_c.values = np.where(ecco_land_mask_c==True, 1, np.nan)

    land_mask_fname = Path(mapping_factors_dir) / 'land_mask'

    if not land_mask_fname.exists():
        try:
            land_mask_fname.mkdir()
        except:
            print(f'ERROR Cannot make land_mask directory "{land_mask_fname}"')
            return -1

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING LAND MASK')
        land_mask_ll = []

    else:
        # first check to see if you have already calculated the landmask
        all_mask = True
        all_mask_fnames = [f'ecco_latlon_land_mask_{i}.xz' for i in range(nk)]
        curr_mask_fnames = os.listdir(land_mask_fname)
        for fname in all_mask_fnames:
            if fname not in curr_mask_fnames:  
                all_mask = False
                break

        if all_mask:
            # Land mask already made, continuing
            print('... land mask already created')
        else:
            # if not, recalculate.

            (status, source_indices_within_target_radius_i, \
            nearest_source_index_to_target_index_i), _ = get_mapping_factors(dataset_dim, mapping_factors_dir,
                                                                            'all', debug_mode)
            if status == -1:
                return status

            for k in range(nk):
                print(k)

                source_field = ecco_land_mask_c.values[k,:].ravel()

                land_mask_ll = ea.transform_to_target_grid(source_indices_within_target_radius_i,
                                                            nearest_source_index_to_target_index_i,
                                                            source_field, target_grid_shape,
                                                            operation='nearest', 
                                                            allow_nearest_neighbor=True)

                try:
                    fname_mask = Path(land_mask_fname) / f'ecco_latlon_land_mask_{k}.xz'
                    pickle.dump(land_mask_ll.ravel(), lzma.open(fname_mask, 'wb'))
                except:
                    print(f'ERROR Cannot save land_mask file "{land_mask_fname}"')
                    return -1
    return status


# =================================================================================================
# CREATE ALL FACTORS (MAPPING FACTORS, LAND MASK, LATLON GRID, and SPARSE MATRICES)
# =================================================================================================
def create_all_factors(ea, product_generation_config, dataset_dim, debug_mode, extra_prints=False):
    status = 1
    mapping_factors_dir = Path(product_generation_config['mapping_factors_dir'])
    nk = product_generation_config['num_vertical_levels']
    ecco_grid = xr.open_dataset(Path(product_generation_config['ecco_grid_dir']) / product_generation_config['ecco_grid_filename'])

    if not mapping_factors_dir.exists():
        try:
            mapping_factors_dir.mkdir()
        except:
            print(f'ERROR Cannot make mapping factors directory "{mapping_factors_dir}"')
            return -1

    if debug_mode:
        print('...DEBUG MODE -- SKIPPING LATLON GRID')
    else:
        wet_pts_k = {}
        xc_wet_k = {}
        yc_wet_k = {}

        # Dictionary of pyresample 'grids' for each level of the ECCO grid where
        # there are wet points.  Used for the bin-averaging.  We don't want to bin
        # average dry points.
        source_grid_k = {}
        if extra_prints: print('\nSwath Definitions')
        if extra_prints: print('... making swath definitions for latlon grid levels 1..nk')
        for k in range(nk):
            wet_pts_k[k] = np.where(ecco_grid.hFacC[k,:] > 0)
            xc_wet_k[k] = ecco_grid.XC.values[wet_pts_k[k]]
            yc_wet_k[k] = ecco_grid.YC.values[wet_pts_k[k]]

            source_grid_k[k] = pr.geometry.SwathDefinition(lons=xc_wet_k[k], lats=yc_wet_k[k])


        # The pyresample 'grid' information for the 'source' (ECCO grid) defined using
        # all XC and YC points, even land.  Used to create the land mask
        source_grid_all =  pr.geometry.SwathDefinition(lons=ecco_grid.XC.values.ravel(),
                                                        lats=ecco_grid.YC.values.ravel())

        # the largest and smallest length of grid cell size in the ECCO grid.  Used
        # to determine how big of a lookup table we need to do the bin-average interp.
        source_grid_min_L = np.min([float(ecco_grid.dyG.min().values), float(ecco_grid.dxG.min().values)])
        source_grid_max_L = np.max([float(ecco_grid.dyG.max().values), float(ecco_grid.dxG.max().values)])


        # Define the TARGET GRID -- a lat lon grid
        ## create target grid.
        product_name = ''

        latlon_grid_resolution = product_generation_config['latlon_grid_resolution']
        latlon_max_lat = product_generation_config['latlon_max_lat']
        latlon_grid_area_extent = product_generation_config['latlon_grid_area_extent']
        latlon_grid_dims = [int(d/latlon_grid_resolution) for d in product_generation_config['latlon_grid_dims']]

        # Grid projection information
        proj_info = {'area_id':'longlat',
                        'area_name':'Plate Carree',
                        'proj_id':'EPSG:4326',
                        'proj4_args':'+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'}

        _, _, target_grid, target_grid_lons, target_grid_lats = ea.generalized_grid_product(product_name,
                                                                                            latlon_grid_resolution,
                                                                                            latlon_max_lat,
                                                                                            latlon_grid_area_extent,
                                                                                            latlon_grid_dims,
                                                                                            proj_info)

        # pull out just the lats and lons (1D arrays)
        target_grid_lons_1D = target_grid_lons[0,:]
        target_grid_lats_1D = target_grid_lats[:,0]

        # calculate the areas of the lat-lon grid
        ea_area = ea.area_of_latlon_grid(-180, 180, -90, 90, latlon_grid_resolution, latlon_grid_resolution, less_output=True)
        lat_lon_grid_area = ea_area['area']
        target_grid_shape = lat_lon_grid_area.shape


        # calculate effective radius of each target grid cell.  required for the bin
        # averaging
        target_grid_radius = np.sqrt(lat_lon_grid_area / np.pi).ravel()


        # CALCULATE GRID-TO-GRID MAPPING FACTORS
        if not isinstance(dataset_dim, list):
            dataset_dim = [dataset_dim]
        for dim in dataset_dim:
            status = create_mapping_factors(ea, dim, mapping_factors_dir, debug_mode, 
                                            source_grid_all, target_grid, target_grid_radius, 
                                            source_grid_min_L, source_grid_max_L, source_grid_k, nk)
            if status == -1:
                return status

            # make a land mask in lat-lon using hfacC
            status = create_land_mask(ea, mapping_factors_dir, debug_mode, nk, 
                                        target_grid_shape, ecco_grid, dim)
            if status == -1:
                return status

        ## MAKE LAT AND LON BOUNDS FOR NEW DATA ARRAYS
        lat_bounds = np.zeros((latlon_grid_dims[1],2))
        for i in range(latlon_grid_dims[1]):
            lat_bounds[i,0] = target_grid_lats[i,0] - latlon_grid_resolution/2
            lat_bounds[i,1] = target_grid_lats[i,0] + latlon_grid_resolution/2

        lon_bounds = np.zeros((latlon_grid_dims[0],2))
        for i in range(latlon_grid_dims[0]):
            lon_bounds[i,0] = target_grid_lons[0,i] - latlon_grid_resolution/2
            lon_bounds[i,1] = target_grid_lons[0,i] + latlon_grid_resolution/2

        # Make depth bounds
        depth_bounds = np.zeros((nk,2))
        tmp = np.cumsum(ecco_grid.drF.values)

        for k in range(nk):
            if k == 0:
                depth_bounds[k,0] = 0.0
            else:
                depth_bounds[k,0] = -tmp[k-1]
            depth_bounds[k,1] = -tmp[k]


        latlon_bounds = {'lat':lat_bounds, 'lon':lon_bounds}
        target_grid_dict = {'shape':target_grid_shape, 'lats_1D':target_grid_lats_1D, 'lons_1D':target_grid_lons_1D}
        latlon_grid = [latlon_bounds, depth_bounds, target_grid_dict, wet_pts_k]

        print('\nCreating latlon grid')
        latlon_grid_name = Path(mapping_factors_dir) / 'latlon_grid' / f'latlon_grid.xz'
        if latlon_grid_name.is_file():
            # latlon grid already made, continuing
            print('... latlon grid already created')
        else:
            # if not, recalculate.
            print('.... making new latlon_grid')
            try:
                pickle.dump(latlon_grid, lzma.open(latlon_grid_name, 'wb'))
            except:
                print(f'ERROR Cannot save latlon_grid file "{latlon_grid_name}"')
                return -1

        # create sparse matrices
        create_sparse_matrix(product_generation_config, debug_mode=debug_mode, extra_prints=extra_prints)

    return status


# ====================================================================================================
# SPARSE MATRIX CREATION
# ====================================================================================================
def create_sparse_matrix(product_generation_config, debug_mode=False, extra_prints=False):
    print(f'\nCreating sparse matrices')

    mapping_factors_dir = product_generation_config['mapping_factors_dir']

    # get the land mask of the latlon grid
    nk = product_generation_config['num_vertical_levels']

    # Check if all sparse matrices are already present
    present_sm_files = list(sorted(os.listdir(f'{mapping_factors_dir}/sparse')))
    expected_sm_files = list(sorted([f'sparse_matrix_{k}.npz' for k in range(nk)]))
    if present_sm_files == expected_sm_files:
        # sparse matrices already made, continuing
        print('... sparse matrices already created')
    else:
        for k in range(nk):
            sm_path = f'./mapping_factors/sparse/sparse_matrix_{k}.npz'
            print(f'Level: {k}')
            
            status, ll_land_mask = gen_netcdf_utils.get_land_mask(product_generation_config['mapping_factors_dir'], k, extra_prints=extra_prints)
            if status == -1:
                print(f'Error getting land mask for level k={k}')
                sys.exit()

            # Create sparse matrix representation of mapping factors
            for dataset_dim in ['2D', '3D']:
                if dataset_dim == '2D' and k > 0:
                    continue
                status, _, (source_indices_within_target_radius_i, \
                nearest_source_index_to_target_index_i) = get_mapping_factors(dataset_dim, product_generation_config['mapping_factors_dir'], 'k', k=k)
                if status == -1:
                    print(f'Error getting mapping factors for level k={k}')
                    sys.exit()

                status, (_, _, _, wet_pts_k) = gen_netcdf_utils.get_latlon_grid(Path(product_generation_config['mapping_factors_dir']), debug_mode)

                n = len(wet_pts_k[k][0])
                m = 2*180 * 2*360
                
                target_ind_raw = np.where(source_indices_within_target_radius_i != -1)[0]
                nearest_ind_raw = np.where((nearest_source_index_to_target_index_i != -1) & (source_indices_within_target_radius_i == -1))[0]
                target_ind = []
                source_ind = []
                source_to_target_weights = []
                for wet_ind in np.where(~np.isnan(ll_land_mask))[0]:
                    if wet_ind in target_ind_raw:
                        si_list = source_indices_within_target_radius_i[wet_ind]
                        for si in si_list:
                            target_ind.append(wet_ind)
                            source_ind.append(si)
                            source_to_target_weights.append(1/len(si_list))
                    elif wet_ind in nearest_ind_raw:
                        ni = nearest_source_index_to_target_index_i[wet_ind]
                        target_ind.append(wet_ind)
                        source_ind.append(ni)
                        source_to_target_weights.append(1)

                # create sparse matrix
                B = sparse.csr_matrix((source_to_target_weights, (source_ind, target_ind)), shape=(n,m))

                # save sparse matrix
                try:
                    sparse.save_npz(sm_path, B)
                except:
                    print(f'ERROR Cannot save sparse matrix file "{sm_path}"')
                    return -1