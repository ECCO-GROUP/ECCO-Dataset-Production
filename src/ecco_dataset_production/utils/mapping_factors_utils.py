"""
ECCO Dataset Production AWS mapping factors utilities

Author: Duncan Bark

Contains functions for creating and getting the mapping factors (factors, land masks, sparse matrices, etc.)

"""

import ast
import inspect
import logging
import lzma
import numpy as np
import os
from pathlib import Path
import pickle
import pyresample as pr
from scipy import sparse
import sys
import xarray as xr

#import ecco_v4_py

import ecco_cloud_utils
from . import gen_netcdf_utils

log = logging.getLogger('ecco_dataset_production')


def _print_progress_bar(current, total, prefix='Progress', bar_length=40):
    """Print a fun ASCII progress bar that updates in place.

    Args:
        current (int): Current iteration (0-indexed or 1-indexed, will be normalized)
        total (int): Total number of iterations
        prefix (str): Text to display before the progress bar
        bar_length (int): Length of the progress bar in characters
    """
    progress = (current + 1) / total
    filled = int(bar_length * progress)
    bar = '█' * filled + '░' * (bar_length - filled)
    print(f'\r{prefix}: [{bar}] {progress*100:.1f}% ({current+1}/{total})', end='', flush=True)
    if current + 1 == total:
        print()  # Newline when complete


## Local imports
#main_path = Path(__file__).parent.resolve().parent.resolve()
#sys.path.append(f'{main_path / "src"}')
#sys.path.append(f'{main_path / "src" / "utils"}')
#sys.path.append(f'{main_path / "src" / "utils" / "ecco_utils"}')
#import gen_netcdf_utils as gen_netcdf_utils
#from ecco_utils.ecco_code import read_bin_gen
#from ecco_utils import ecco_cloud_utils as ea


# =================================================================================================
# GET MAPPING FACTORS
# =================================================================================================
def get_mapping_factors(dataset_dim, 
                        mapping_factors_dir, 
                        factors_to_get,
                        k=0):
    """
    Get mapping factors from mapping_factors_dir for level k and the factors
    requested (factors_to_get).

    Args:
        dataset_dim (str): Dimension of the dataset to get factors for.
        mapping_factors_dir (PosixPath): Path to
            /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}.
        factors_to_get (str): 'all'=grid_mappings_all, 'k'=grid_mappings_{k}, or
            'both'=grid_mappings_all and grid_mappings_{k} (grid_mappings_all
            includes dry points factors).
        k (int, optional): Integer vertical level index to retrieve mapping
            factors for (0-{num_vertical_levels}).

    Returns:
        tuple: A tuple (status, grid_mappings_all, grid_mappings_k) where
        ``status`` (str) is either "SUCCESS" or "ERROR {error message}",
        ``grid_mappings_all`` (tuple) contains two lists
        (source_indices_within_target_radius_i and
        nearest_source_index_to_target_index_i), and ``grid_mappings_k``
        (tuple) contains two lists (source_indices_within_target_radius_i
        where index is target index and value is -1 if no source indices in
        target radius or a list of source indices within target radius, and
        nearest_source_index_to_target_index_i where index is the target
        index and value is the nearest source index to target index).
    """
    log.info('-'*80)
    log.info(f'SUBROUTINE: get_mapping_factors.   k = {k}, dataset_dim = {dataset_dim}')
    log.info('-'*80 )

    status = 'SUCCESS'
    
    # factors_to_get : factors to load in from the mapping_factors_dir
    # can be 'all', 'k', or 'both'
    grid_mappings_all = []
    grid_mappings_k = []

    log.info('Getting Grid Mappings')
    grid_mapping_fname_all = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_all.xz'
    grid_mapping_fname_2D = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_2D.xz'
    grid_mapping_fname_3D = Path(mapping_factors_dir) / '3D' / f'ecco_latlon_grid_mappings_3D_{k}.xz'

    get_all=False;
    get_k=False;
    get_2D=False;
    get_3D=False;

    if factors_to_get == 'all' or factors_to_get == 'both':
        get_all = True
    elif factors_to_get == 'k':
        get_k = True
    else:
        status = f'ERROR Invalid input for "factors_to_get": {factors_to_get}. Must be "all", "k", or "both".'
        return (status, grid_mappings_all, grid_mappings_k)
            
    if dataset_dim == '2D':
        get_2D = True
    elif dataset_dim == '3D':
        get_3D = True
        get_k = True    
    else:
        status = f'ERROR Invalid input for "dataset_dim": {dataset_dim}. Must be "2D" or "3D".'
        return (status, grid_mappings_all, grid_mappings_k)
    

    grid_mappings_all = None
    grid_mappings_k = None


    # try to load grid_mappings_all if factors_to_get is 'all' or 'both'. 
    # If you cannot load it, return an error status and empty grid_mappings_all and grid_mappings_k
    try:
        # if factors_to_get is just 'all' or 'both' then load grid_mappings_all
        if get_all:
            log.info(f'... loading {grid_mapping_fname_all.name} ...')
            grid_mappings_all = pickle.load(lzma.open(grid_mapping_fname_all, 'rb'))
    except:
        status = f'ERROR Unable to load {grid_mapping_fname_all} mapping factors file: {mapping_factors_dir}'
        log.exception('%s', status)
        return (status, grid_mappings_all, grid_mappings_k)
    
    # try to load grid_mappings_k if factors_to_get is 'k' or 'both'. 
    # If you cannot load it, return an error status and empty grid_mappings_all and grid_mappings_k
    try:
        if get_k:
            if get_2D:
                log.info(f'... loading {grid_mapping_fname_2D.name} ...')
                grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_2D, 'rb'))
            elif get_3D:
                log.info(f'... loading {grid_mapping_fname_3D.name} ...')
                grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_3D, 'rb'))
    except:
        status = f'ERROR Unable to load grid mapping factors: {mapping_factors_dir}'
        log.exception('%s', status)
        return (status, grid_mappings_all, grid_mappings_k)
    

    
    # # Check to see that the mapping factors have been made
    # if  (dataset_dim == '2D' and grid_mapping_fname_2D.is_file()) \
    #     or \
    #     (dataset_dim == '3D' and grid_mapping_fname_3D.is_file()):
    #     # if so, load
    #     try:
    #         # if factors_to_get is just 'all' or 'both' then load grid_mappings_all
    #         if factors_to_get == 'all' or factors_to_get == 'both':
    #             log.info(f'... loading {grid_mapping_fname_all.name} ...')
    #             grid_mappings_all = pickle.load(lzma.open(grid_mapping_fname_all, 'rb'))

    #         # if factors_to_get is just 'k' or 'both' then load the grid_mappings_k file for the corresponding
    #         # value of k passed to the function, as well as for the dataset_dim passed. If dataset_dim is '2D'
    #         # it doesnt matter what value k is since 2D only has 1 vertical level.
    #         if factors_to_get == 'k' or factors_to_get == 'both':
    #             if dataset_dim == '2D':
    #                 log.info(f'... loading {grid_mapping_fname_2D.name} ...')
    #                 grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_2D, 'rb'))
    #             elif dataset_dim == '3D':
    #                 log.info(f'... loading {grid_mapping_fname_3D.name} ...')
    #                 grid_mappings_k = pickle.load(lzma.open(grid_mapping_fname_3D, 'rb'))
    #     except:
    #         status = f'ERROR Unable to load grid mapping factors: {mapping_factors_dir}'
    #         return (status, grid_mappings_all, grid_mappings_k)
    # else:
    #     status = f'ERROR Grid mapping factors have not been created or cannot be found: {mapping_factors_dir}'
    #     return (status, grid_mappings_all, grid_mappings_k)

    return (status, grid_mappings_all, grid_mappings_k)


# =================================================================================================
# CREATE MAPPING FACTORS
# =================================================================================================
def create_mapping_factors(dataset_dim,
                           mapping_factors_dir,
                           source_grid_all,
                           target_grid,
                           target_grid_radius,
                           source_grid_min_L,
                           source_grid_max_L,
                           source_grid_k,
                           nk,
                           force_recalc=False):
    """
    Create mapping factors for dataset_dim for nk many vertical levels

    Args:
        dataset_dim (str): Dimension of the dataset to create factors for
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        source_grid_all (pyresample.geometry.SwathDefinition): Swath definition of source grid over all points
        target_grid (pyresample.geometry.SwathDefinition): Swath definition of target grid
        target_grid_radius (list): Target grid radius (float) for each cell index
        source_grid_min_L (float): Minimum ECCO grid cell length
        source_grid_max_L (float): Maximum ECCO grid cell length
        source_grid_k (list): List of nk many pyresample.geometry.SwathDefinition
        nk (int): Integer number of total vertical levels
        force_recalc (bool): If True, recalculate mapping factors even if they exist (default: False)

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """

    log.info('-'*80)
    log.info('SUBROUTINE: create_mapping_factors')
    log.info('-'*80 )


    status = 'SUCCESS'
    grid_mapping_fname_all = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_all.xz'
    grid_mapping_fname_2D = Path(mapping_factors_dir) / 'ecco_latlon_grid_mappings_2D.xz'
    grid_mapping_fname_3D = Path(mapping_factors_dir) / '3D'

    # check that the 3D directory exists
    if not grid_mapping_fname_3D.exists():
        try:
            grid_mapping_fname_3D.mkdir(parents=True, exist_ok=True)
        except:
            status = f'ERROR Cannot make grid mappings 3D directory "{grid_mapping_fname_3D}"'
            return status

    # first check to see if you have already calculated the grid mapping factors
    # if the dataset is 3D, check to see that all the nk vertical levels have a matching grid_mappings file
    if dataset_dim == '3D':
        all_3D = True
        all_3D_fnames = [f'ecco_latlon_grid_mappings_3D_{i}.xz' for i in range(nk)]
        curr_3D_fnames = os.listdir(grid_mapping_fname_3D)
        for fname in all_3D_fnames:
            if fname not in curr_3D_fnames:  
                all_3D = False
                break

    # if dataset dim is 2D and the 2D mapping factors file exists, or if dataset dim is 3D and all
    # 3D mapping factors files exist, dont remake the factors (unless force=True)
    files_exist = (dataset_dim == '2D' and grid_mapping_fname_2D.is_file()) or (dataset_dim == '3D' and all_3D)

    if files_exist and not force_recalc:
        # Factors already made, continuing
        log.info('... found grid_mappings_all [all points] and grid_mappings_k [only wet points], not recalculating')
    else:
        # if not, make new grid mapping factors
        if force_recalc and files_exist:
            log.info('... forcing recalculation of grid_mappings_all and grid_mappings_k')
        else:
            log.info('... no existing grid mapping factors found, calculating new ones')

        # Calculate optimal neighbours value to avoid "using more neighbours than upper bound" message
        # Both target_grid_radius and source_grid_min_L are in meters
        log.info('-'*80)
        log.info('Geometric parameters for mapping:')
        log.info(f'  Target grid radius (min):  {np.nanmin(target_grid_radius)/1000:.2f} km ({np.nanmin(target_grid_radius):.1f} m)')
        # print where the grid radius is max, and where the grid radius is min
        min_idx = np.unravel_index(np.nanargmin(target_grid_radius), target_grid_radius.shape)
        max_idx = np.unravel_index(np.nanargmax(target_grid_radius), target_grid_radius.shape)
        log.info(f'    ... min radius target grid cell found at index idx={min_idx[0]} ')
        log.info(f'    ... max radius target grid cell found at index idx={max_idx[0]} ')

        log.info(f'  Target grid radius (max):  {np.nanmax(target_grid_radius)/1000:.2f} km ({np.nanmax(target_grid_radius):.1f} m)')
        log.info(f'  Target grid radius (mean): {np.nanmean(target_grid_radius)/1000:.2f} km ({np.nanmean(target_grid_radius):.1f} m)')
        log.info(f'  Source grid min length:    {source_grid_min_L/1000:.2f} km ({source_grid_min_L:.1f} m)')
        log.info(f'  Source grid max length:    {source_grid_max_L/1000:.2f} km ({source_grid_max_L:.1f} m)')

        max_target_grid_radius = np.nanmax(target_grid_radius)

        neighbours_upper_bound = int(np.ceil((max_target_grid_radius * 2 / source_grid_min_L) ** 2))

        log.info(f'  Maximum # of neighbours for bin-averaging calculation:')
        log.info(f'    Formula: ceil((max_target_radius * 2 / source_min_L)^2)')
        log.info(f'    = ceil(({max_target_grid_radius:.1f} * 2 / {source_grid_min_L:.1f})^2)')
        log.info(f'    = ceil({(max_target_grid_radius * 2 / source_grid_min_L):.3f}^2)')
        log.info(f'    = {neighbours_upper_bound}')
        log.info('-'*80)

        if neighbours_upper_bound > 100:
            log.warning(f'Calculated neighbours_upper_bound = {neighbours_upper_bound} is > 100, limiting to 100')
        neighbours_upper_bound = min(neighbours_upper_bound, 100)  # set a maximum upper bound of 100 to avoid too high of a neighbours value
        log.info('-'*80)

        # find the mapping between *all* 2D ECCO grid model grid points and the target grid.
        grid_mappings_all = \
            ecco_cloud_utils.mapping.find_mappings_from_source_to_target_for_processing(
                source_grid_all,
                target_grid,
                target_grid_radius,
                source_grid_min_L,
                source_grid_max_L,
                neighbours=neighbours_upper_bound)

        # Save grid_mappings_all
        try:
            pickle.dump(grid_mappings_all, lzma.open(grid_mapping_fname_all, 'wb'))
        except:
            status = f'ERROR Cannot save grid_mappings_all file "{grid_mapping_fname_all}"'
            return status

        log.info(f'... done creating {grid_mapping_fname_all} saving to disk')

        # If the dataset is 2D, only compute one level of the mapping factors
        if dataset_dim == '2D':
            nk = 1

        # Find the mapping factors between the *wet points* of the ECCO grid
        # at each vertical level and the target grid (create mapping factors)
        log.info(f'... calculating grid_mappings_k for wet points at each vertical level, nk={nk}...')

        for k_i in range(nk):
            _print_progress_bar(k_i, nk, prefix='Creating grid mappings')
            grid_mappings_k = \
                ecco_cloud_utils.mapping.find_mappings_from_source_to_target_for_processing(
                    source_grid_k[k_i],
                    target_grid,
                    target_grid_radius,
                    source_grid_min_L,
                    source_grid_max_L,
                    neighbours=neighbours_upper_bound)
            try:
                # if the dataset dim is 2D, save the factors using the 2D name, otherwise
                # save it with the 3D name, and with level {k}
                if dataset_dim == '2D':
                    pickle.dump(grid_mappings_k, lzma.open(grid_mapping_fname_2D, 'wb'))
                elif dataset_dim == '3D':
                    fname_3D = Path(grid_mapping_fname_3D) / f'ecco_latlon_grid_mappings_3D_{k_i}.xz'
                    pickle.dump(grid_mappings_k, lzma.open(fname_3D, 'wb'))
            except:
                status = f'ERROR Cannot save grid_mappings_k file(s) "{mapping_factors_dir}"'
                return status

        log.info(f'... done creating grid_mappings_k for nk={nk} vertical levels ...')
        
    return status


# =================================================================================================
# CREATE LAND MASK
# =================================================================================================
def create_land_mask(mapping_factors_dir,
                     nk,
                     target_grid_shape,
                     ecco_grid,
                     dataset_dim,
                     force_recalc=False):
    """
    Create land mask file(s) for dataset_dim for nk many vertical levels

    Args:
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        nk (int): Integer number of total vertical levels
        target_grid_shape (tuple): Tuple of the shape of the target grid (i.e. (360, 720))
        ecco_grid (xarray.Dataset): ECCO grid xarray dataset
        dataset_dim (str): Dimension of the dataset to create factors for
        force_recalc (bool): If True, recalculate land mask even if it exists (default: False)

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """       

    log.info('-'*80)
    log.info('SUBROUTINE: create_land_mask')
    log.info('-'*80 )

    status = 'SUCCESS'
    ecco_land_mask_c = ecco_grid.maskC.copy(deep=True)
    ecco_land_mask_c.values = np.where(ecco_land_mask_c==True, 1, np.nan)

    land_mask_fname = Path(mapping_factors_dir) / 'land_mask'

    # check that the land mask directory exists
    if not land_mask_fname.exists():
        try:
            land_mask_fname.mkdir(parents=True, exist_ok=True)
        except:
            status = f'ERROR Cannot make land_mask directory "{land_mask_fname}"'
            log.exception('%s', status)
            return status

    # first check to see if you have already calculated all the land mask files for each vertical level
    all_mask = True
    all_mask_fnames = [f'ecco_latlon_land_mask_{i}.xz' for i in range(nk)]

    # make list of all the land mask files that are currently in the land mask directory
    curr_mask_fnames = os.listdir(land_mask_fname)
    # loop through all the expected land mask filenames and check if they are in the current land mask directory.  If any one is not, set all_mask = False
    for fname in all_mask_fnames:
        if fname not in curr_mask_fnames:  
            all_mask = False
            log.exception(f'... land mask file "{fname}" not found in land_mask directory')
            log.exception(f'... expected land mask files: {all_mask_fnames}')
            break

    if all_mask and not force_recalc:
        # Land masks are already made, continuing
        log.info('... land masks are already present in land_mask directory, not recalculating')
    else:
        # if not, recalculate.
        if force_recalc and all_mask:
            log.info('... forcing recalculation of existing land mask files')
        else:
            log.info('... no land mask files found, recalculating')


        # land mask needs the "grid_mappings_all" mapping factors
        log.info(f'... loading grid_mappings_all for creating land masks ...')
        (status, grid_mappings_all, _) = get_mapping_factors(dataset_dim, 
                                                             mapping_factors_dir, 
                                                             'all')

        if status != 'SUCCESS':
            raise RuntimeError(status)

        source_indices_within_target_radius_i, nearest_source_index_to_target_index_i = grid_mappings_all

        if status != 'SUCCESS':
            return status

        for k in range(nk):
            _print_progress_bar(k, nk, prefix=f'Creating land masks. k={k}/{nk}')

            # create source field for level k
            source_field = ecco_land_mask_c.values[k,:].ravel()

            # create land mask for level k
            land_mask_ll = \
                ecco_cloud_utils.mapping.transform_to_target_grid_for_processing(
                    source_indices_within_target_radius_i,
                    nearest_source_index_to_target_index_i,
                    source_field, target_grid_shape,
                    operation='nearest', 
                    allow_nearest_neighbor=True)
            try:
                # save land mask with level {k}
                fname_mask = Path(land_mask_fname) / f'ecco_latlon_land_mask_{k}.xz'
                pickle.dump(land_mask_ll.ravel(), lzma.open(fname_mask, 'wb'))
            except:
                status = f'ERROR Cannot save land_mask file "{land_mask_fname}"'
                log.exception('%s', status)
                return status
        log.info(f'... done creating land masks for nk={nk} vertical levels!! big yay')
        log.info('-'*80)
    return status


# ====================================================================================================
# SPARSE MATRIX CREATION
# ====================================================================================================
def create_sparse_matrix(
    mapping_factors_dir, 
    product_generation_config, 
    target_grid_shape, 
    wet_pts_k,
    dataset_dim, 
    force_recalc=False):
    """
    Create sparse matrix file(s)

    Args:
        mapping_factors_dir (PosixPath): Path to /ECCO-Dataset-Production/aws/mapping_factors/{ecco_version}
        product_generation_config (dict): Dictionary of product_generation_config.yaml config file
        target_grid_shape (tuple): Tuple of the shape of the target grid (i.e. (360, 720))
        wet_pts_k (optional, dict): Dictionary of wet point indices where keys are vertical levels
        extra_prints (optional, bool): Boolean to enable more print statements

    Returns:
        status (str): String that is either "SUCCESS" or "ERROR {error message}"
    """       

    log.info('-'*80)
    log.info('SUBROUTINE: create_sparse_matrix')
    log.info('-'*80 )

    status = 'SUCCESS'


    if dataset_dim == '2D':
        nk=1
        k=0
        log.info(f'Creating sparse matrices for 2D dataset with nk={nk} vertical levels (only 1 level will be used for 2D)')
    elif dataset_dim == '3D':
        nk = product_generation_config['num_vertical_levels']
        log.info(f'Creating sparse matrices for 3D dataset with nk={nk} vertical levels')
    else:
        status = f'ERROR Invalid input for "dataset_dim": {dataset_dim}. Must be "2D" or "3D".'
        return status

    sm_path = Path(mapping_factors_dir) / 'sparse'
    # check that the sparse matrix directory exists
    if not sm_path.exists():
        try:
            sm_path.mkdir(parents=True, exist_ok=True)
        except:
            status = f'ERROR Cannot make sparse matrix directory "{sm_path}"'
            return status

    # Check if all sparse matrices are already present
    present_sm_files = list(sorted(os.listdir(f'{mapping_factors_dir}/sparse')))

    # list of expected sparse matrix files based on nk and dataset_dim
    expected_sm_files = list(sorted([f'sparse_matrix_{k}.npz' for k in range(nk)]))
    
    # check to see if all the expected sparse matrix files are in the present sparse matrix files list. If any one is not, set all_sm_files = False
    all_sm_files = all([sm_file in present_sm_files for sm_file in expected_sm_files])

    if all_sm_files and not force_recalc:
        # sparse matrices already made, continuing
        log.info('... sparse matrix files already exist, not recalculating')
    else:
        # if force_recalc is True, or not all sparse matrix files are present, recalculate.
        if force_recalc and all_sm_files:
            log.info('... forcing recalculation of existing sparse matrix files')
        else:
            log.info('... no sparse matrix files found, recalculating')

        log.info('... creating sparse matrix files for each vertical level k ... nk=%d' % nk)
        for k in range(nk):
            sm_path_fname = Path(sm_path) / f'sparse_matrix_{k}.npz'

            # get the land mask for level k (should exist for all k)
            status, land_mask = gen_netcdf_utils.get_land_mask(
                mapping_factors_dir,
                product_generation_config,
                k=k
            )
            if status != 'SUCCESS':
                # stick with this for now (TODO: Pythonic mods to get_land_mask):
                log.error(status)
                sys.exit(status)

            # Create sparse matrix representation of mapping factors
#            for dataset_dim in ['2D', '3D']:
                # only do 2D sparse matrix for vertical level k=0
            if dataset_dim == '2D' and k > 0:
                continue

            # get the mapping_factors_k factors for vertical level k
            status, _, (source_indices_within_target_radius_i, \
            nearest_source_index_to_target_index_i) = get_mapping_factors(dataset_dim, 
                                                                            mapping_factors_dir, 
                                                                            'k', 
                                                                            k=k)
            if status != 'SUCCESS':
                return status

            # # If not using a custom grid and factors, then get the wet_pts_k form the latlon_grid object
            # if not product_generation_config['custom_grid_and_factors']:
            #     # get the latlon grid object, and only use the wet_pts_k list
            #     status, (_, _, _, wet_pts_k) = gen_netcdf_utils.get_latlon_grid(Path(mapping_factors_dir))
            #     if status != 'SUCCESS':
            #         return status

            # get the length of the first dimension of wet_pts_k at vertical level k
            n = len(wet_pts_k[k][0])

            # get the total number of target grid cells
            m = target_grid_shape[0] * target_grid_shape[1]
        
            # get the target indices where source indices exist within it's target radius
            target_ind_raw = np.where(source_indices_within_target_radius_i != -1)[0]

            # get the nearest source index for each target index IF there are no source indices within the target index radius
            nearest_ind_raw = np.where((nearest_source_index_to_target_index_i != -1) & (source_indices_within_target_radius_i == -1))[0]

            # loop through all the wet points indices, and if that index has source indices within it's target radius then
            # append that the target index to target_ind, the source index to source_ind, and append the weighting 
            # (calculated as 1/number of source indices) to source_to_target_weights.
            # Otherwise, use the nearest source index, and append 1 (1/1) to source_to_target_weights
            target_ind = []
            source_ind = []
            source_to_target_weights = []
            for wet_ind in np.where(~np.isnan(land_mask))[0]:
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

            # create sparse matrix using the list of weights, the source indices, and target indices
            # B is a matrix that has a row of length equal to the number of source indices, for each
            # target grid index. (source_ind, target_ind) are the coordinates in the sparse matrix
            # that point to the corresponding value in source_to_target_weights.
            # i.e. (source_ind[0], target_ind[0]) = (0, 5), and source_to_target_weights[0] = 10,
            # then the value of 10 will be placed at (0, 5) in B.
            B = sparse.csr_matrix((source_to_target_weights, (source_ind, target_ind)), shape=(n,m))

            # save sparse matrix
            try:
                sparse.save_npz(sm_path_fname, B)
            except:
                status = f'ERROR Cannot save sparse matrix file "{sm_path_fname}"'
                return status
                
        print('\n')  # add a newline after the progress bar is done
    return status


# =================================================================================================
# PREPARE CUSTOM GRID VALUES
# =================================================================================================
def create_custom_grid_values(product_generation_config,
                               mapping_factors_dir):

    log.info('-'*80)
    log.info('SUBROUTINE: create_custom_grid_values')
    log.info('-'*80 )

    force_recalc = product_generation_config.get('force_recalculation', False)

    # Create relevant paths
    source_grid_path = f'{Path(product_generation_config["grid_files_dir"]) / "source_grids"}'
    target_grid_path = f'{Path(product_generation_config["grid_files_dir"]) / "target_grids"}'

    # get number of vertical levels of the grid from product_generation_config
    nk = product_generation_config['num_vertical_levels']

    # ========== <Prepare source grid information> ============================================
    # Get the source grid meta and data
    log.info('-'*80)
    log.info('Preparing Source Grid')
    _, source_grid_data = __get_meta_data(source_grid_path)
    
    # Create source swath defintion using the XC and YC values from the .data files in source_grids
    source_grid = pr.geometry.SwathDefinition(lons=source_grid_data['XC'].ravel(),
                                                lats=source_grid_data['YC'].ravel())

    # Get source_grid_min_L and source_grid_max_L
    # If they are not provided in the config, calculate it from source_grid_data['RAC']
    source_grid_radius = np.sqrt(source_grid_data['RAC'] / np.pi) * np.sqrt(2) * 1.1
    if product_generation_config['source_grid_min_L'] == -1:
        source_grid_min_L = np.nanmin(source_grid_radius[np.where(source_grid_radius > 0)])
    else:
        source_grid_min_L = float(product_generation_config['source_grid_min_L'])
    if product_generation_config['source_grid_max_L'] == -1:
        source_grid_max_L = np.nanmax(source_grid_radius[np.where(source_grid_radius > 0)])
    else:
        source_grid_max_L = float(product_generation_config['source_grid_max_L'])

    log.info('Source grid information:')
    log.info(f'... source_grid_min_L: {source_grid_min_L/1000:.2f} km ({source_grid_min_L:.1f} m)')
    log.info(f'... source_grid_max_L: {source_grid_max_L/1000:.2f} km ({source_grid_max_L:.1f} m)')

    # create land mask from source
    wet_pts_k = {}
    xc_wet_k = {}
    yc_wet_k = {}
    source_grid_k = {}
    for k in range(nk):
        wet_pts_k[k] = np.where(source_grid_data['hFacC'][k,:] > 0)
        xc_wet_k[k] = source_grid_data['XC'][wet_pts_k[k]]
        yc_wet_k[k] = source_grid_data['YC'][wet_pts_k[k]]

        source_grid_k[k] = pr.geometry.SwathDefinition(lons=xc_wet_k[k],
                                                        lats=yc_wet_k[k])
    # ========== </Prepare source grid information> ============================================


    # ========== <Prepare target grid information> ============================================
    # Get the target grid meta and data
    log.info('Preparing Target Grid')
    _, target_grid_data = __get_meta_data(target_grid_path)

    # Create target swath defintion using the XC and YC values from the .data files in target_grids
    target_grid = pr.geometry.SwathDefinition(lons=target_grid_data['XC'].ravel(),
                                                lats=target_grid_data['YC'].ravel())

    # Get target_grid_radius from provided eff_grid_radius.data file
    if 'effective_grid_radius' in target_grid_data:
        target_grid_radius = target_grid_data['effective_grid_radius'].ravel()
    else:
        target_grid_radius = (np.sqrt(target_grid_data['RAC'] / np.pi) * np.sqrt(2) * 1.1).ravel()

    log.info('Target grid radius statistics:')
    log.info(f'... min:  {np.nanmin(target_grid_radius)/1000:.2f} km ({np.nanmin(target_grid_radius):.1f} m)')
    log.info(f'... max:  {np.nanmax(target_grid_radius)/1000:.2f} km ({np.nanmax(target_grid_radius):.1f} m)')
    log.info(f'... mean: {np.nanmean(target_grid_radius)/1000:.2f} km ({np.nanmean(target_grid_radius):.1f} m)')

    # Create target_grid_shape from target XC shape
    target_grid_shape = target_grid_data['XC'].shape

    land_mask_fname = Path(product_generation_config['land_mask_dir'])
    #land_mask_fname = Path(mapping_factors_dir) / 'land_mask'
    # check that the land mask directory exists
    if not land_mask_fname.exists():
        try:
            land_mask_fname.mkdir(parents=True, exist_ok=True)
        except:
            status = f'ERROR Cannot make land_mask directory "{land_mask_fname}"'
            return status

    # If hFacC is not provided, make the mask all 1s
    if 'hFacC' not in target_grid_data:
        target_grid_data['hFacC'] = []
        for k in range(nk):
            target_grid_data['hFacC'].append(np.ones(target_grid_shape))
        target_grid_data['hFacC'] = np.array(target_grid_data['hFacC'])

    log.info('Creating Land Mask (2D, 3D)')

    # Create a list of all land mask filenames from k=0 to nk-1 from target_grid hFacC
    land_mask_fnames = [Path(land_mask_fname) / f'land_mask_{k}.xz' for k in range(nk)]

    # Check if land masks have already been made
    #   defaults set all_masks= True (all of the land masks are present)
    # #  if any one land mask file is not found, set all_mask =False
    all_mask = True
    for fname_mask in land_mask_fnames:
        if not os.path.exists(fname_mask):
            all_mask = False
            break

    if all_mask and not force_recalc:
        # Land mask already made, continuing
        log.info('... land mask already created')
    else:
        if force_recalc and all_mask:
            log.info('... forcing recalculation of existing land mask files')
        else:
            log.info('... no land mask files found, recalculating')

        # if not, recalculate.
        for i, fname_mask in enumerate(land_mask_fnames):
            # Create name to save the land mask as, check if it exists and continue if it does
            if os.path.exists(fname_mask):
                continue
            land_mask = np.where(target_grid_data['hFacC'][i] == 1, 1, np.nan)
            try:
                # save land mask with level {i}
                pickle.dump(land_mask.ravel(), lzma.open(fname_mask, 'wb'))
            except:
                status = f'ERROR Cannot save land_mask file "{land_mask_fname}"'
                return status
    # ========== </Prepare target grid information> ============================================

    custom_grid_values = {'source_grid': source_grid, 
                          'target_grid': target_grid, 
                          'target_grid_radius': target_grid_radius, 
                          'target_grid_shape': target_grid_shape, 
                          'source_grid_min_L': source_grid_min_L, 
                          'source_grid_max_L': source_grid_max_L, 
                          'source_grid_k': source_grid_k, 
                          'wet_pts_k': wet_pts_k, 
                          'nk': nk}

    return custom_grid_values


def __get_meta_data(grid_path):
    """
    Get the information from the .meta and .data files for the provided directory.
    This is used for custom grids to create the grid factors.

    Args:
        grid_path (str): Path to the directory for source/target grid files

    Returns:
        grid_meta (dict): Dictionary with keys corresponding to each .meta file in the directory,
                          and the information contained within. (i.e. {'hFaaC': {'nDims': [3]}})
        grid_data (dict): Dictionary with keys corresponding to each .data file in the directory,
                          and the information contained within. (i.e. {'XC': [data]})
    """           
    # Read the .meta files in the grid_file_path dir
    # and make a dictionary where the key is the name of the grid file
    # and the value is all the values contained within the .meta file organized by name
    # ex. grid_meta = {'XC': {'nDims': [2], 'dimList': array([[ 270, 1, 270], [3510, 1, 3510]])}}
    grid_meta = {}
    for meta_file in os.listdir(grid_path):
        if '.meta' in meta_file:
            meta_name = meta_file.split('.meta')[0]
            grid_meta[meta_name] = {}

            # Create a single string with all the values from .meta
            fstring = ''
            with open(f'{grid_path}/{meta_file}', 'r') as mf:
                for line in mf:
                    fstring += line
            
            # Loop through each value in the .meta file, parse it, and add it to the grid_meta dict
            meta_values = fstring.split(';')
            for meta_value in meta_values:
                if '=' not in meta_value:
                    continue
                meta_data = ast.literal_eval(meta_value.split('=')[-1].replace(' ', '').replace(';', ''))
                if 'nDims' in meta_value:
                    nDims = meta_data
                    grid_meta[meta_name]['nDims'] = nDims
                if 'dimList' in meta_value:
                    dimList = np.reshape(np.array(meta_data), (nDims[0], len(meta_data)//nDims[0]))
                    grid_meta[meta_name]['dimList'] = dimList
                if 'dataprec' in meta_value:
                    dataprec = meta_data
                    grid_meta[meta_name]['dataprec'] = dataprec
                if 'nrecords' in meta_value:
                    nrecords = meta_data
                    grid_meta[meta_name]['nrecords'] = nrecords
                if 'timeStepNumber' in meta_value:
                    timeStepNumber = meta_data
                    grid_meta[meta_name]['timeStepNumber'] = timeStepNumber

    # Read the binary data from each grid file and save it, by name,
    # into the grid_data dictionary
    grid_data = {}
    for data_file in os.listdir(grid_path):
        # read via load_binary_array in read_bin_gen.py
        if '.data' in data_file:
            data_name = data_file.split('.data')[0]

            # if the file is 2D (nDims = 2), nk = 1
            # otherwise, get the nk from the last dim in dimList
            if grid_meta[data_name]['nDims'][0] == 2:
                nk = 1
            else:
                nk = grid_meta[data_name]['dimList'][-1][0]

            # Load in ni and nj dims from meta
            # ni, nj are the lengths of the array dimensions
            ni = grid_meta[data_name]['dimList'][0][0]
            nj = grid_meta[data_name]['dimList'][1][0]

            # read binary data array and save it in the grid_data dict
            print(f'\tLoading {data_file}', end='\r')
            data = ecco_v4_py.read_bin_gen.load_binary_array(grid_path, 
            #data = read_bin_gen.load_binary_array(grid_path, 
                                                  data_file, 
                                                  ni, 
                                                  nj, 
                                                  nk, 
                                                  less_output=True)
            grid_data[data_name] = data
            print(f'\tLoading {data_file} -- DONE')
    
    return grid_meta, grid_data


# =================================================================================================
# PREPARE ECCO GRID VALUES
# =================================================================================================
def create_ecco_grid_values(
    product_generation_config, mapping_factors_dir):
    """
    Create ECCO source and target grid data used in subsequent mapping
    coefficient routines.

    Args:
        product_generation_config (dict): Configuration data, generally
            originating from product_generation_config.yaml, with defaults
            applied.

    Returns:
        Dictionary of ECCO source and target grid data.

    Note:
        ECCO grid data are implicit input, and are located via reference to the
        'ecco_grid_dir' and 'ecco_grid_filename' fields in product_generation_config.

    """

    log.info('-'*80)
    log.info('SUBROUTINE: create_ecco_grid_values')
    log.info('-'*80 )

    try:
        ecco_grid_path = Path(product_generation_config['ecco_grid_dir']) / Path(product_generation_config['ecco_grid_filename'])
        log.info(f'Attempting to open ECCO grid dataset at path: {ecco_grid_path}')
        ecco_grid = xr.open_dataset(ecco_grid_path)
    except Exception as e:
        log.exception(f'Exception in %s: %s', inspect.currentframe().f_code.co_name, e)
        #log.error(f'error in %s: %s', inspect.currentframe().f_code.co_name, e)

    nk = product_generation_config['num_vertical_levels']

    # check that the mapping_factors/ dir exists
    if not mapping_factors_dir.exists():
        try:
            mapping_factors_dir.mkdir(parents=True, exist_ok=True)
        except:
            errstr = f'Cannot make mapping factors directory "{mapping_factors_dir}"'
            log.exception(errstr)
            sys.exit(errstr)

    wet_pts_k = {}
    xc_wet_k = {}
    yc_wet_k = {}

    force_recalc = product_generation_config.get('force_recalculation', False)

    # ========== <Prepare grid values> ========================================================
    # Dictionary of pyresample 'grids' for each level of the ECCO grid where
    # there are wet points.  Used for the bin-averaging.  We don't want to bin
    # average dry points.
    source_grid_k = {}
    log.info('-'*80)
    log.info(f'Creating Swath Definition Objects for ECCO grid wet points at each vertical level nk={nk}')
    for k in range(nk):
        wet_pts_k[k] = np.where(ecco_grid.hFacC[k,:] > 0)
        xc_wet_k[k] = ecco_grid.XC.values[wet_pts_k[k]]
        yc_wet_k[k] = ecco_grid.YC.values[wet_pts_k[k]]
        source_grid_k[k] = pr.geometry.SwathDefinition(lons=xc_wet_k[k], lats=yc_wet_k[k])

    # The pyresample 'grid' information for the 'source' (ECCO grid) defined using
    # all XC and YC points, even land.  Used to create the land mask
    source_grid = pr.geometry.SwathDefinition(
        lons=ecco_grid.XC.values.ravel(),
        lats=ecco_grid.YC.values.ravel())

    # the largest and smallest length of grid cell size in the ECCO grid.  Used
    # to determine how big of a lookup table we need to do the bin-average interp.
    source_grid_min_L = np.min([float(ecco_grid.dyG.min().values), float(ecco_grid.dxG.min().values)])
    source_grid_max_L = np.max([float(ecco_grid.dyG.max().values), float(ecco_grid.dxG.max().values)])

    # print out the source grid information
    log.info('-'*80)
    log.info('Source grid information:')
    log.info(f'... source_grid_min_L: {source_grid_min_L/1000:.2f} km ({source_grid_min_L:.1f} m)')
    log.info(f'... source_grid_max_L: {source_grid_max_L/1000:.2f} km ({source_grid_max_L:.1f} m)')
    log.info(f'... dyG min/max: {float(ecco_grid.dyG.min().values):.1f} / {float(ecco_grid.dyG.max().values):.1f} m')
    log.info(f'... dxG min/max: {float(ecco_grid.dxG.min().values):.1f} / {float(ecco_grid.dxG.max().values):.1f} m')


    log.info('-'*80)
    log.info('Creating target latlon grid')

    # Define the TARGET GRID -- a lat lon grid
    ## create target grid.
    product_name = ''

    latlon_grid_resolution = product_generation_config['latlon_grid_resolution']
    #latlon_max_lat = product_generation_config['latlon_max_lat']
    latlon_grid_area_extent = product_generation_config['latlon_grid_area_extent']

    # latlon_grid_area_extent is # [lon_min, lat_max, lon_max, lat_min]
    latlon_max_lat = latlon_grid_area_extent[1]

    latlon_grid_dims = [int(np.abs(latlon_grid_area_extent[2] - latlon_grid_area_extent[0]) / latlon_grid_resolution),
                        int(np.abs(latlon_grid_area_extent[3] - latlon_grid_area_extent[1]) / latlon_grid_resolution)]
    # latlon_grid_dims = [int(d/latlon_grid_resolution) for d in product_generation_config['latlon_grid_dims']]

    # Grid projection information
    proj_info = {
        'area_id':'longlat',
        'area_name':'Plate Carree',
        'proj_id':'EPSG:4326',
        'proj4_args':'+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'}

    log.info('calling ecco_cloud_utils.generalized_functions.generalized_grid_product')
    log.info('.   to create target latlon grid and get the lats, lons, and grid cell areas of the target latlon grid')
    target_grid_min_L, target_grid_max_L, target_grid, target_grid_lons, target_grid_lats = \
        ecco_cloud_utils.generalized_functions.generalized_grid_product(
            product_name,
            latlon_grid_resolution,
            latlon_max_lat,
            latlon_grid_area_extent,
            latlon_grid_dims,
            proj_info)

    # pull out just the lats and lons (1D arrays)
    target_grid_lons_1D = target_grid_lons[0,:]
    target_grid_lats_1D = target_grid_lats[:,0]

    # calculate the areas of the lat-lon grid
    ea_area = ecco_cloud_utils.geometry.area_of_latlon_grid(
        -180, 180, -90, 90, latlon_grid_resolution, latlon_grid_resolution,
        less_output=True)
    lat_lon_grid_area = ea_area['area']
    target_grid_shape = lat_lon_grid_area.shape


    #   print a long line of -----
    log.info('-'*80)
    # print out the target grid information
    log.info('Target latlon grid information:')
    log.info(f'... latlon_grid_resolution: {latlon_grid_resolution}')
    log.info(f'... latlon_grid_dims: {latlon_grid_dims}')
    log.info(f'... latlon_grid_area_extent: {latlon_grid_area_extent}')
    # print shape
    # print out the lat and lon values of the target grid
    log.info(f'... target_grid_lons_1D: {target_grid_lons_1D[0:3]} ... {target_grid_lons_1D[-3:]}')
    log.info(f'... target_grid_lats_1D: {target_grid_lats_1D[0:3]} ... {target_grid_lats_1D[-3:]}')
    # print the min/max of the target grid lats and lons
    log.info(f'... target_grid_min_L :{target_grid_min_L}')
    log.info(f'... target_grid_max_L :{target_grid_max_L}')
    

    #if product_generation_config['ecco_version'] == 'V4r4':
    #    target_grid_radius = np.sqrt(lat_lon_grid_area / np.pi).ravel()
    #else:
    #    # use 111 km per degree everywhere, even at high latitudes
    #    # as a consequence, at higher latitudes the effective grid area
    #    # will be the sameas latitudes
    #    target_grid_radius = ((latlon_grid_resolution*111.)/2.)*np.sqrt(2)*1.1

    # get the effective radius of each target grid cell.  required for the bin averaging
    if product_generation_config['latlon_effective_grid_radius'] != None:
        target_grid_radius = product_generation_config['latlon_effective_grid_radius']
        # config files specification is in km, but ecco_cloud_utils expects meters, so convert here
        target_grid_radius = np.ones(lat_lon_grid_area.shape) * target_grid_radius / 1000.0 
    else:
        
        # alternative methods for calculating the effective radii of the lat-lon grid cells based on the lat-lon grid resolution
        grid_radius_method = product_generation_config.get('latlon_grid_radius_method')
        
        log.info('-'*80)
        log.info('Calculating lat-lon grid effective radius for bin-averaging')
        log.info('-'*80)
        if grid_radius_method is None:
            log.info('No method provided in the config file for calculating the lat-lon grid effective radius, using default method: method_1')
            grid_radius_method = 'method_1'
        
        elif grid_radius_method not in ['method_1', 'method_2']:
            # fail the program at this point, provide error logging
            errstr = f'Invalid method provided for calculating lat-lon grid effective radius: {grid_radius_method}. Valid options are "method_1" and "method_2".'
            log.error(errstr)
            sys.exit(errstr)

        if grid_radius_method == 'method_1':
            # Method 1: (used in V4r4) calculate the effective radius as the radius of a circle with the same area as the lat-lon grid cell.
            log.info('... using method_1: spatially-varying effective radius = radius of a circle with the same area as the lat-lon grid cell.')
            log.info('                                  effective radius = sqrt(lat_lon_grid_area/pi)')
            target_grid_radius = np.sqrt(lat_lon_grid_area/np.pi).ravel()
        
        elif grid_radius_method == 'method_2':
            # Method 2:  use a constant radius everywhere based on the distance between lat-lon grid points at the equator.
            #
            tgr = 111*latlon_grid_resolution*np.sqrt(2)/2 * 1000
            log.info(f'... using method 2: spatially-invariant effective radius = {tgr/1000:.2f} km ({tgr:.1f} m), the distance from the center of a square grid cell of {latlon_grid_resolution} degree resolution at the equator to a corner.')
            log.info(f'using method 2: spatially-invariant effective radius = 111,000 m *latlon_grid_resolution*np.sqrt(2)/2 = {tgr/1000:.2f} km ({tgr:.1f} m)')
            
            # ...calculate in km, then convert to meters (ecco_cloud_utils expects meters)
            target_grid_radius = (np.ones(lat_lon_grid_area.shape)*(111*latlon_grid_resolution*np.sqrt(2)/2) * 1000).ravel()
        else:
            # fail the program at this point, provide error logging
            errstr = f'Invalid method provided for calculating lat-lon grid effective radius: {grid_radius_method}. Valid options are "method_1" and "method_2".'
            log.error(errstr)
            sys.exit(errstr)

    # reshape to the shape of the target grid
    target_grid_radius_2D = target_grid_radius.reshape(target_grid_shape)

    # save the 2D target grid radius as a netcdf using xarray
    tgr_da = xr.DataArray(target_grid_radius_2D, dims=['lat', 'lon'], coords={'lat': target_grid_lats_1D, 'lon': target_grid_lons_1D}, name='target_grid_radius')
    tga_da = xr.DataArray(lat_lon_grid_area, dims=['lat', 'lon'], coords={'lat': target_grid_lats_1D, 'lon': target_grid_lons_1D}, name='target_grid_area')
    
    tgr_ds = xr.Dataset(xr.merge([tgr_da, tga_da])) 

    lon_target_info_fname = Path(mapping_factors_dir) / 'lat-lon_target_grid_info.nc'
    try:
        log.info(f'... saving target info file to {lon_target_info_fname}')   
        tgr_ds.to_netcdf(lon_target_info_fname)
    except:
        status = f'ERROR Cannot save target_grid_info file "{lon_target_info_fname}"'
        return status

    # Log target grid radius statistics
    log.info('-'*80)
    log.info(f'Target grid radius statistics:')
    log.info(f'... min:  {np.nanmin(target_grid_radius)/1000:.2f} km ({np.nanmin(target_grid_radius):.1f} m)')
    log.info(f'... max:  {np.nanmax(target_grid_radius)/1000:.2f} km ({np.nanmax(target_grid_radius):.1f} m)')
    log.info(f'... mean: {np.nanmean(target_grid_radius)/1000:.2f} km ({np.nanmean(target_grid_radius):.1f} m)')
    log.info('-'*80)



    # ========== <Create latlon grid> =============================================================
    # latlon grid is a list of values describing the latlon grid which includes:
    #   - latlon_bounds: Contains the lat and lon bounds for each grid cell
    #   - depth_bounds: Contains the vertical bounds for each vertical level
    #   - target_grid_dict: Contains latlon grid 'shape', and lists of all the 'lats_1D' and 'lons_1D'
    #   - wet_pts_k: Dictionary with key=vertical level index, and value=tuple of numpy.arrays of source grid wet points

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


    latlon_grid_dir = Path(mapping_factors_dir) / 'latlon_grid'
    if not os.path.exists(latlon_grid_dir):
        os.makedirs(latlon_grid_dir, exist_ok=True)
    latlon_grid_name = latlon_grid_dir / 'latlon_grid.xz'
    
    if latlon_grid_name.is_file() and not force_recalc:
        # latlon grid already made, continuing
        log.info('... latlon_grid.xz already exists, not overwriting')
    else:
        if force_recalc and latlon_grid_name.is_file():
            log.info('... overwriting existing latlon_grid.xz file')
        else:
            log.info('... latlon_grid.xz file does not exist, creating new one')
        try:
            pickle.dump(latlon_grid, lzma.open(latlon_grid_name, 'wb'))
        except:
            status = f'ERROR Cannot save latlon_grid file "{latlon_grid_name}"'
            return status

    # ========== </Create latlon grid> ============================================================

    ecco_grid_values = {
        'source_grid': source_grid, 
        'target_grid': target_grid, 
        'target_grid_radius': target_grid_radius, 
        'target_grid_shape': target_grid_shape, 
        'ecco_grid': ecco_grid, 
        'source_grid_min_L': source_grid_min_L, 
        'source_grid_max_L': source_grid_max_L, 
        'source_grid_k': source_grid_k, 
        'wet_pts_k': wet_pts_k, 
        'nk': nk}

    return ecco_grid_values


# =================================================================================================
# CREATE ALL FACTORS (MAPPING FACTORS, LAND MASK, LATLON GRID, and SPARSE MATRICES)
# =================================================================================================
def create_all_factors(product_generation_config, dataset_dim):
    """
    Generates 2- and/or 3-D mapping factors including land masks, and lon/lat
    grid files.

    Args:
        product_generation_config (dict): Configuration data, generally
            originating from product_generation_config.yaml, with defaults
            applied.
        dataset_dim (list of str): Dimension(s) of desired mapping factor
            datasets (e.g., ['2D','3D'] for both 2- and 3-D factors)

    Returns:
        ECCO grid mapping factors written to 'mapping_factors_dir' specified in
            product_generation_config.
    """           
    mapping_factors_dir = Path(product_generation_config['mapping_factors_dir'])

    log.info('-'*80)
    log.info('SUBROUTINE: create_all_factors')
    log.info('-'*80 )

    # ========== <Prepare grid values> ===========================================================
    # Create custom or ecco grid values

    if product_generation_config['custom_grid_and_factors']:
        grid_values = create_custom_grid_values(
            product_generation_config, mapping_factors_dir)
    else:
        grid_values = create_ecco_grid_values(
            product_generation_config, mapping_factors_dir
#           extra_prints)
            )

    # Check if grid_values creation failed (returns error string instead of dict)
    if isinstance(grid_values, str):
        log.error(grid_values)
        raise RuntimeError(grid_values)
    # ========== </Prepare grid values> ===========================================================


    # ========== <Create mapping factors and land mask> ===========================================
    # CALCULATE GRID-TO-GRID MAPPING FACTORS
    if not isinstance(dataset_dim, list):
        dataset_dim = [dataset_dim]

    # Get force flag from config (defaults to False if not set)
    force_recalc = product_generation_config.get('force_recalculation', False)

    for dim in dataset_dim:
        create_mapping_factors(
            dim,
            mapping_factors_dir,
            grid_values['source_grid'],
            grid_values['target_grid'],
            grid_values['target_grid_radius'],
            grid_values['source_grid_min_L'],
            grid_values['source_grid_max_L'],
            grid_values['source_grid_k'],
            grid_values['nk'],
            force_recalc=force_recalc)
        
        if not product_generation_config['custom_grid_and_factors']:
            # make a land mask in lat-lon using hfacC
            create_land_mask(
                mapping_factors_dir, 
                grid_values['nk'], 
                grid_values['target_grid_shape'], 
                grid_values['ecco_grid'], 
                dim,
                force_recalc=force_recalc)
    # ========== </Create mapping factors and land mask> ==========================================


    # ========== <Create sparse matrices> =========================================================
    # create sparse matrices
    for dim in dataset_dim:
        log.info('-'*80)
        log.info(f'Creating sparse matrix for {dim} dataset')

        create_sparse_matrix(
            mapping_factors_dir, 
            product_generation_config, 
            grid_values['target_grid_shape'], 
            grid_values['wet_pts_k'],
            dim,
            force_recalc=force_recalc)
    # ========== </Create sparse matrices> ========================================================

