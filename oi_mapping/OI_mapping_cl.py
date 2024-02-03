#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 18:02:06 2024

@author: owang
"""

import numpy as np
import sys
import xarray as xr
import time
from pathlib import Path
import pickle
import os
import json
import argparse

from multiprocessing import shared_memory
import multiprocessing
import concurrent
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Manager
from collections import namedtuple

#sys.path.append('/home/owang/CODE/Python/projects/modules/ECCOv4-py/')
import ecco_v4_py as ecco

#%% 
# create parser
def create_parser():
    """Set up list of command-line arguments to the objective mapping script.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""A Python script to do objective mapping from one 
        grid (soruce) to another grid (destination).""",
        epilog="""Note: Currently, the following mapping is implemented: 
        merra2 to llc90, merra2 to llc270, and llc90 to llc270.""")       
    parser.add_argument('--json_dir', default='./', help="""
        Directory of input json file (default: "%(default)s")""")        
    parser.add_argument('--src', default='merra2', help="""
        Source grid (default: "%(default)s")""")
    parser.add_argument('--dest', default='llc90', help="""
        Destination grid (default: "%(default)s")""")
    parser.add_argument('--NUM_WORKERS', type=int, default=1, help="""
        Number of processes (default: %(default)s)""")
    parser.add_argument('--variable', default='TAUX', help="""
        Variable name (default: %(default)s)""")        
    parser.add_argument('--year', type=int, default=1992, help="""
        Year (default: %(default)s)""")
    parser.add_argument('--rec0', type=int, default=1, help="""
        Start record number (default: %(default)s)""")
    parser.add_argument('--rec1', type=int, default=4, help="""
        End record number (default: %(default)s)""")
    parser.add_argument('--nchunk', type=int, default=1, help="""
        Number of chunnks to split and process the record range 
        from rec0 to rec1 into (default: %(default)s)""")  
    parser.add_argument('--band0', type=int, default=1, help="""
        Start band index (default: %(default)s)""")
    parser.add_argument('--band1', type=int, default=1, help="""
        End band index (default: %(default)s)""")                
    parser.add_argument('--src_fn', default='', help="""
        Source filename (default: %(default)s)""")         
    parser.add_argument('--iternum', type=int, default=0, help="""
        Iternation number (default: %(default)s)""")        
# =============================================================================
#     # Python >= 3.9
#     parser.add_argument('--fill_dry_points', 
#                         action=argparse.BooleanOptionalAction, help="""
#                         Fill dry points with the nearest wet point""")    
# =============================================================================
    # Python < 3.9
    parser.add_argument('--fill_dry_points', action='store_true', help="""
                        Fill dry points with the nearest wet point""")
    parser.add_argument('--no-fill_dry_points', dest='fill_dry_points', 
                        action='store_false', help="""
                        Do not fill dry points with the nearest wet point""")
    parser.set_defaults(fill_dry_points=False)            
    return parser

#%% shared memory
def create_shared_memory_nparray(data, NP_DATA_TYPE=np.float32,
                                 ARRAY_SHAPE=(0), 
                                 NP_SHARED_NAME='src_field_shm'):

    # destory shared memory NP_SHARED_NAME if it exists 
    # (likely beacuse a previous run didn't stop properly).
    try:
        _ = shared_memory.SharedMemory(name=NP_SHARED_NAME)
        # If it exists, unlink it
        release_shared(NP_SHARED_NAME)
    except FileNotFoundError:
        # This means the shared memory does not exist
        pass
    
    d_size = np.dtype(NP_DATA_TYPE).itemsize * np.prod(ARRAY_SHAPE)
    shm = shared_memory.SharedMemory(create=True, size=d_size, 
                                     name=NP_SHARED_NAME)
    # numpy array on shared memory buffer
    dst = np.ndarray(shape=ARRAY_SHAPE, dtype=NP_DATA_TYPE, buffer=shm.buf)
    dst[:] = data[:]
    return shm

def release_shared(name):
    shm = shared_memory.SharedMemory(name=name)
    shm.close()
    shm.unlink()  # Free and release the shared memory block
    
#%% load source field (by chunk)
def load_src_field_chunk(src, src_dir, src_fn, sgrid_size, sgrid_shape,
                         chunk_start, nrec_chunk,
                         src_precision = 4,
                         use_shared_mem=False, NP_DATA_TYPE=np.float32):
    if(src=='merra2'):
        src_field_tmp = np.fromfile(src_dir+src_fn,dtype='>f4',
                                    count = nrec_chunk*sgrid_size,
                                  offset=chunk_start*sgrid_size*src_precision)
        src_field = np.reshape(src_field_tmp,(nrec_chunk,)+sgrid_shape)
    elif src=='llc90' or src=='llc270':
        llc =int(src[3:])
        src_field = ecco.read_llc_to_tiles(src_dir, src_fn, 
                                           nk = nrec_chunk,
                                           skip=chunk_start,
                                           llc=llc,
                                           less_output=True)
        src_field_tmp = src_field.ravel()
    src_field_flat_shape = src_field_tmp.shape  
    if use_shared_mem:
        shm = create_shared_memory_nparray(src_field_tmp, NP_DATA_TYPE,
                                           ARRAY_SHAPE=src_field_flat_shape)     
    return src_field, src_field_flat_shape

#%% load json file 
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

#%% find mask for current band
def get_band_mask(grid_y, lat0, lat1):
    return ((grid_y>=lat0) & (grid_y<lat1))

#%% process one latitudinal band
def proc_band(sgrid_y, dgrid_y,
              band_idx, slat0, 
              K, name, src_field_flat_shape, 
              fill_dry_points,
              out_fn, nrec_chunk,
              sgrid_shape,
              NP_DATA_TYPE=np.float32,
              src_field='',
              closest_idx=np.nan,
              sgrid_drypnts=np.nan,              
              use_shared_mem=False,
              verbose=False):
    if(verbose):
        time_bf_matrix_a = time.time() 
    earthrad = grid_params.earthrad
    twodlattap = 2*K_params.dlattap
    dlattap_inmeters = grid_params.earthrad * np.deg2rad(K_params.dlattap)
    if use_shared_mem:
        shm = shared_memory.SharedMemory(name=name)    
        src_field_flat = np.ndarray(src_field_flat_shape, dtype=NP_DATA_TYPE, buffer=shm.buf)
        if (verbose):
            print('shape of src_field_flat: ',src_field_flat.shape)
    
        src_field = src_field_flat.reshape((nrec_chunk,)+sgrid_shape)
            
    if(fill_dry_points==True):
        src_field[:, sgrid_drypnts] = \
            np.copy(src_field.reshape((nrec_chunk,-1))[:,closest_idx][:,sgrid_drypnts])            

    slat1 = slat0 + K_params.dlatcell
    lat_mid = slat0+(slat1-slat0)/2
                    
    if (verbose):
        print('Processing latitudial band: ',slat0,slat1)
    
    tlat0 = slat0-K_params.dlathilo
    tlat1 = slat1+K_params.dlathilo   

    # find grid points in this band
    masksub_src = get_band_mask(sgrid_y, slat0, slat1)
    masksub_dest = get_band_mask(dgrid_y, tlat0, tlat1)
 
    if (verbose):
        print('masksub_src shape: ',src_field.shape,  masksub_src.shape)
    src_tapered = np.copy(src_field[0:nrec_chunk,masksub_src])                 
    
    if(True): # tappering    
        disttmp = earthrad*np.deg2rad(sgrid_y[masksub_src]-lat_mid)
        # taperdist0 is actually 
        # earthrad*np.deg2rad(lat_mid-dlatcell/2+dlattap-lat_mid).
        # Because of the two lat_mids cancel with each other,
        # we have earthrad*np.deg2rad(lat_mid-dlatcell/2+dlattap-lat_mid).
        # Same for taperdist1, taperdist2, and taperdist3.
        taperdist0 = earthrad*\
            np.deg2rad(-K_params.dlatcell/2+K_params.dlattap)
        taperdist1 = earthrad*\
            np.deg2rad(-K_params.dlatcell/2+twodlattap)
        taperdist2 = earthrad\
            *np.deg2rad(+K_params.dlatcell/2-twodlattap)
        taperdist3 = earthrad\
            *np.deg2rad(+K_params.dlatcell/2-K_params.dlattap)
        
        if(slat0>grid_params.y0):
            idxtaperdist0 = disttmp<taperdist0      
            src_tapered[0:nrec_chunk,idxtaperdist0] = 0. #*src_tapered[idxtaperdist0]
            idxtaperdist1 = (taperdist0<=disttmp) & (disttmp<taperdist1)     
            src_tapered[0:nrec_chunk,idxtaperdist1] = src_tapered[0:nrec_chunk,idxtaperdist1] * \
                (disttmp[idxtaperdist1]-taperdist0)/dlattap_inmeters                    
                
        if(slat1<grid_params.ymax): 
            # no tapering (i.e., taper factor is 1) between taperdist1 and taperdist2
            idxtaperdist3 = (taperdist2<=disttmp) & (disttmp<taperdist3)
            src_tapered[0:nrec_chunk,idxtaperdist3] = src_tapered[0:nrec_chunk,idxtaperdist3] * \
                (taperdist3-disttmp[idxtaperdist3])/dlattap_inmeters                    
            idxtaperdist4 = taperdist3<=disttmp
            src_tapered[0:nrec_chunk,idxtaperdist4] = 0. #*src_tapered[idxtaperdist4]    

        dest_field_flat_sub = K @ src_tapered.T

        if (verbose):              
            print(f'exec time (s) for band: ,{band_idx:3d}', 
                  f'{time.time()-time_bf_matrix_a:.4f}')  
        return masksub_dest, dest_field_flat_sub.T, out_fn, nrec_chunk
#%%
def load_grid(grid_params, grid_nm, grid_dir='./', local_or_s3='local'):
    if grid_nm == 'merra2':
        # grid: MERRA2 
        nx = 576
        ny = 361
        dlat = 0.5
        dlon = 5/8
        x=[grid_params.x0 + dlon*i for i in range(nx)]
        y=[grid_params.y0 + dlat*i for i in range(ny)]
        grid_x, grid_y = np.meshgrid(x, y)

    elif grid_nm == 'llc90':
        # grid: llc90
        if local_or_s3 == 'local':
            fgrid = 'ECCO-GRID.nc'
            grid_ds = xr.open_dataset(grid_dir + '/' +fgrid)
        elif local_or_s3 == 's3':
            fgrid = 'GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc'
            ecco_s3_v4r4_grid_path = grid_dir + fgrid
            print(ecco_s3_v4r4_grid_path)
            grid_file = ecco_s3_fs.open(ecco_s3_v4r4_grid_path)
            grid_ds = xr.open_dataset(grid_file).load()

        grid_x = np.copy(grid_ds.XC.values)
        grid_y = np.copy(grid_ds.YC.values)

    elif grid_nm == 'llc270':
        # grid: llc270 
        fgrid = 'ECCO-GRID.nc'
        grid_ds = xr.open_dataset(grid_dir + '/' +fgrid)
        grid_x = np.copy(grid_ds.XC.values)
        grid_y = np.copy(grid_ds.YC.values) 

    else:
        print('Error! Grid can only be merra2, llc90, or llc270: ', grid_nm)
    grid_shape = grid_x.shape
    grid_flat_shape = grid_x.ravel().shape
    grid_size = np.prod(grid_shape)        
      
    return grid_shape, grid_flat_shape, grid_size, grid_y

#%%
def load_K(mapping_factors_dir, fnprefix, band_idx, slat0, slat1):
# =============================================================================
# # May have to use a shared dictionary if memory load is too large.
# # Using shared dictionary reduces performance. 
#     manager = Manager()
#     K_dict = manager.dict()
# =============================================================================

    K_dict = {}

    OI_mapping_fname = mapping_factors_dir / \
        (fnprefix + f"_{band_idx:d}_{slat0:d}_{slat1:d}.p" )    
    K_dict[band_idx] = pickle.load(open(OI_mapping_fname, 'rb'))
    return K_dict

#%%
def write_mapped_field_to_file(out_dir, out_fn, dest_field,
                               OutputGlobalField=False):
    if OutputGlobalField:
        dest_field_tmp= ecco.llc_tiles_to_compact(dest_field, 
                                               less_output=True)
    else:
        # make dest_field_tmp a copy of dest_field to avoid error in writing
        dest_field_tmp = dest_field.copy()
    with open(out_dir+'/'+out_fn, "wb") as file:
        file.write(dest_field_tmp.astype('>f4'))        
    return

#%%
def load_params_from_json(json_dir, json_fn, fill_dry_points=False):
# load some parameters from json file
    json_data = load_json_data(json_dir+'/'+json_fn)    
    mapping_factors_dir = Path(json_data['mapping_factors_dir_str'])
    fnprefix = json_data['fnprefix']
    src_dir = json_data['src_dir']
    dgrid_dir = json_data['dgrid_dir']
    sgrid_dir = json_data['sgrid_dir']
    out_dir = json_data['out_dir']
    if fill_dry_points:
        nearest_wet_points_indices_dir = \
            json_data['nearest_wet_points_indices_dir']
    else:
        nearest_wet_points_indices_dir=''

    if 's3://' in str(mapping_factors_dir): 
        print('processing on s3')
        ecco_s3_fs = s3fs.S3FileSystem(profile='saml-pub')
    else:
        ecco_s3_fs=''
        print('processing on local machine')
    return mapping_factors_dir, fnprefix, src_dir, \
        dgrid_dir, sgrid_dir, out_dir, \
        nearest_wet_points_indices_dir, ecco_s3_fs
        

#%% set global parameters
def glob_params():
# parameters (no need to change them)  
    global grid_params, K_params
    GridParameters = namedtuple('GridParameters',
                                ['earthrad', 'x0', 'xmax', 'y0', 'ymax']) 
    grid_params = GridParameters(6371000, -180, 180, -90, 90)
# =============================================================================
#     nband_max = 19 # maximum number of latitudinal bands
#     dlathilo = 0 # source and destination grids are over the same region    
#     #shift step is dlatcell-3*dlattap so the two neighboring tapering regions 
#     # are the same
#     # latitudinal band width in degrees
#     dlatcell = 18
#     dlattap = 3 # 3 degrees for tapering     
# =============================================================================
    KParameters = namedtuple('KParameters', 
                             ['nband_max', 'dlathilo', 'dlatcell', 'dlattap'])
    K_params = KParameters(19, 0, 18, 3)
    return [grid_params, K_params]

#%% preparation
def prep_proc():
    parser = create_parser()
    args = parser.parse_args()  
    
    # to be put in argument 
    json_dir = args.json_dir
    src = args.src
    dest = args.dest
    
    NUM_WORKERS = args.NUM_WORKERS
    variable = args.variable
    year = args.year
    rec0 = args.rec0
    rec1 = args.rec1
    nchunk = args.nchunk
    band0 = args.band0
    band1 = args.band1    
    src_fn = args.src_fn
    iternum = args.iternum    
    fill_dry_points = args.fill_dry_points
    if(nchunk<=0):
        print('Error!')
        print(f'nchunk has an integer that is larger than 0: {nchunk}')
        sys.exit()
    
    mappingtype=src+'to'+dest
    if(mappingtype!='merra2tollc90' and 
       mappingtype!='merra2tollc270' and
       mappingtype!='llc90tollc270'):
        print('Error!')
        print('Variable mappingtype has to be one of the following:')
        print('merra2tollc90, merra2tollc270, or llc90tollc270')
        sys.exit()
    
    llc = int(dest[3:])

    # output a global field or just one band    
    OutputGlobalField=False

    # used shared memory or not
    use_shared_mem=False
    
    if use_shared_mem:
        print("""ERROR! use_shared_mem has to be set to False, as each worker
              now processes one chunk of data for one latitudinal band.""")
        sys.exit()
        
    if NUM_WORKERS>=6:
        print(f"""WARNING! Using NUM_WORKERS larger than 6 might 
              require much larger memory! NUM_WORKERS = {NUM_WORKERS}""")

    # cap band0 and band1 at nband_max;
    # note that they are counted staring from 1. 
    if(band0>K_params.nband_max):
        band0 = K_params.nband_max        
    if(band1>K_params.nband_max):
        band1 = K_params.nband_max 
        
    if src_fn == '':
        if src == 'merra2':
            src_fn = variable +f'_{year:d}'
        else: 
            src_fn = 'xx_'+variable +f'.{iternum:010d}.data'

#%%    
    json_fn = mappingtype+".json"
    [mapping_factors_dir, fnprefix, src_dir, dgrid_dir,\
     sgrid_dir, out_dir, nearest_wet_points_indices_dir, ecco_s3_fs] = \
        load_params_from_json(json_dir, json_fn, fill_dry_points)

#%%
# load source and destination grid info        
    sgrid_shape, sgrid_flat_shape, sgrid_size, sgrid_y = \
        load_grid(grid_params, src, sgrid_dir)
    dgrid_shape, dgrid_flat_shape, dgrid_size, dgrid_y = \
        load_grid(grid_params, dest, dgrid_dir)    

#%%
    if fill_dry_points:       
        nearest_wet_points_indices_fn = src + '_nearest_wet_points_indices.p'
        [sgrid_oceanpnts, sgrid_drypnts, closest_idx] = \
            pickle.load(open(nearest_wet_points_indices_dir+\
                             nearest_wet_points_indices_fn, 'rb'))
    else:
        closest_idx=np.nan
        sgrid_drypnts=np.nan
        
#%%
# input/source file information       
    src_precision = 4 # 4 bytes for single precision
    file_stats = os.stat(src_dir+src_fn)
    nrec_src = int((file_stats.st_size+1e-5)/sgrid_size/src_precision)

#%% create latitudinal bands
    # slat0_all is the southern boundary of each band.
    # from one band to the next, the latitude is shifted by dlatshift.
    dlatshift = K_params.dlatcell-3*K_params.dlattap
    slat0_all = []
    for slat0_tmp in range(grid_params.y0,
                           grid_params.ymax, 
                           dlatshift):
        slat0_all.append(slat0_tmp)
        if(slat0_tmp+K_params.dlatcell>=grid_params.ymax):
            break
    slat0_all = np.asarray(slat0_all)    

    return [band0, band1, rec0, rec1, nchunk, nrec_src,
            slat0_all, mapping_factors_dir, fnprefix, 
            NUM_WORKERS,
            src, src_dir,
            src_fn, sgrid_shape, sgrid_size, 
            mappingtype,
            sgrid_y, dgrid_y,
            out_dir, llc,
            dgrid_shape,
            fill_dry_points,
            closest_idx,
            sgrid_drypnts,
            OutputGlobalField,
            use_shared_mem]

#%% process all chunks for all bands 
def proc_all(band0, band1, rec0, rec1, nchunk, nrec_src,
             slat0_all,
             mapping_factors_dir, fnprefix,
             NUM_WORKERS,
             src, src_dir,
             src_fn, sgrid_shape, sgrid_size, 
             mappingtype,
             sgrid_y, dgrid_y,
             out_dir, llc,
             dgrid_shape,
             fill_dry_points, closest_idx, sgrid_drypnts               ,
             OutputGlobalField=False,
             use_shared_mem=False,
             NP_SHARED_NAME='src_field_shm',             
             NP_DATA_TYPE = np.float32,
             verbose=False):
    
    for band_idx in range(band0-1, band1):        
        if(band_idx>=K_params.nband_max):
            continue                    
        slat0 = slat0_all[band_idx]
        slat1 = slat0 + K_params.dlatcell
      
        # load mapping matrix for a particular latitudinal band
        K_dict=load_K(mapping_factors_dir, fnprefix, band_idx, slat0, slat1)            
        K = K_dict[band_idx][0]
        if(verbose):
            print('Matrix is now loaded for latitudinal band '+\
                  f'{band_idx+1:d} ({slat0} to {slat1})')        

        chunk_start = rec0-1
        if rec1>nrec_src:
            rec1 = nrec_src
        nrec_chunk = int(((rec1-rec0+1)-1)/nchunk)+1
        
        # do the mapping           
        futures=[]
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:          
            # each job will only do one chunk.
            for ichunk in range(nchunk):   
                if((rec1-chunk_start)<nrec_chunk):
                    nrec_chunk = rec1-chunk_start
                chunk_end = chunk_start + nrec_chunk
    
                # output filename (one file for one band    
                out_fn =src_fn + '_'+mappingtype+'_'+\
                    f'{chunk_start+1:05d}_{chunk_end:05d}_{band_idx+1:02d}_{band_idx+1:02d}'
                    
                src_field, src_field_flat_shape = \
                    load_src_field_chunk(src, src_dir, src_fn, 
                                         sgrid_size, sgrid_shape,
                                         chunk_start, nrec_chunk)        
                  
                if use_shared_mem:                    
                    futures.append(executor.submit(proc_band,
                                                   sgrid_y,
                                                   dgrid_y,
                                                   band_idx, slat0,
                                                   K, NP_SHARED_NAME, 
                                                   src_field_flat_shape,
                                                   fill_dry_points,
                                                   out_fn, nrec_chunk,
                                                   sgrid_shape,
                                                   closest_idx=closest_idx,
                                                   sgrid_drypnts=
                                                   sgrid_drypnts))
                else:
                    futures.append(executor.submit(proc_band,
                                                   sgrid_y,
                                                   dgrid_y,
                                                   band_idx, slat0,
                                                   K, NP_SHARED_NAME, 
                                                   src_field_flat_shape,
                                                   fill_dry_points,
                                                   out_fn, nrec_chunk,
                                                   sgrid_shape,
                                                   closest_idx=closest_idx,
                                                   sgrid_drypnts=
                                                   sgrid_drypnts,
                                                   src_field=src_field))                

                if use_shared_mem:
                    # release shared memory            
                    release_shared(NP_SHARED_NAME)
                # update chunk_start
                chunk_start = chunk_end
        
            # generate the mapped field             
            for fut in concurrent.futures.as_completed(futures):
                fut_res_mask = fut.result()[0]
                fut_res_value = fut.result()[1] 
                fut_res_out_fn = fut.result()[2] 
                fut_res_nrec_chunk = fut.result()[3] 
                
                if OutputGlobalField:                
                    if os.path.isfile(out_dir+fut_res_out_fn):       
                        dest_field = ecco.read_llc_to_tiles(out_dir, fut_res_out_fn, 
                                                            nk = fut_res_nrec_chunk,
                                                            less_output=True,
                                                            llc=llc)
                        if fut_res_nrec_chunk == 1:
                              dest_field = np.expand_dims(dest_field, axis=0)
                    else:
                        print('New dest_field: '+out_dir+fut_res_out_fn)
                        dest_field = np.zeros((fut_res_nrec_chunk,)+dgrid_shape)
    
                    dest_field[0:fut_res_nrec_chunk,fut_res_mask] = + \
                        dest_field[0:fut_res_nrec_chunk,fut_res_mask] + fut_res_value 
                else:
                    dest_field = fut_res_value

                # output
                write_mapped_field_to_file(out_dir, fut_res_out_fn, 
                                           dest_field, 
                                           OutputGlobalField=OutputGlobalField)

#%%    
def main(verbose=False):
    # define parameters
    [grid_params, K_params] = glob_params()

#%%    
    start_time = time.time()
# =============================================================================
#     # Because the size of matrices for high-latitudes bands is 
#     # generally much larger than those for lower-latitudes bands, 
#     # one way to reduce memory load is not to multiple load 
#     # high-latitude matrices at the same time. So, instead of 
#     # loading the matrices sequentially, we would selectively 
#     # load only one high-latitude matrix, along with other 
#     # lower-latitude matrices.
#     nband_inagp = 3
#     ngp = int((nband-1)/nband_inagp)+1
#     if nband_inagp*ngp<nband:
#         print(f"""ERROR! The product of nband_inagp ({nband_inagp}) and 
#               ngp ({ngp}) is smaller than the total number of bands 
#               ({nband})').""")
#         sys.exit()
# =============================================================================
    # preparition
    [band0, band1, rec0, rec1, nchunk, nrec_src,
            slat0_all, mapping_factors_dir, fnprefix, 
            NUM_WORKERS,
            src, src_dir,
            src_fn, sgrid_shape, sgrid_size, 
            mappingtype,
            sgrid_y, dgrid_y,
            out_dir, llc,
            dgrid_shape,
            fill_dry_points,
            closest_idx,
            sgrid_drypnts,
            OutputGlobalField,
            use_shared_mem] = prep_proc()
    
    # processing
    start_map_time = time.time()
    proc_all(band0, band1, rec0, rec1, nchunk, nrec_src,
             slat0_all,
             mapping_factors_dir, fnprefix,
             NUM_WORKERS,
             src, src_dir,
             src_fn, sgrid_shape, sgrid_size, 
             mappingtype,
             sgrid_y, dgrid_y,
             out_dir, llc,
             dgrid_shape,
             fill_dry_points,
             closest_idx,
             sgrid_drypnts,
             OutputGlobalField=OutputGlobalField,
             use_shared_mem=use_shared_mem)
    
    print('exec time (s): ',time.time()-start_time,
          time.time()-start_map_time)

# Main execution
if __name__ == "__main__":
    main()
