#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 15:26:06 2024

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
def create_shared_memory_nparray(data,
                                 ARRAY_SHAPE=(), 
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
    shm = shared_memory.SharedMemory(create=True, size=d_size, name=NP_SHARED_NAME)
    # numpy array on shared memory buffer
    dst = np.ndarray(shape=ARRAY_SHAPE, dtype=NP_DATA_TYPE, buffer=shm.buf)
    dst[:] = data[:]
    return shm

def release_shared(name):
    shm = shared_memory.SharedMemory(name=name)
    shm.close()
    shm.unlink()  # Free and release the shared memory block
    
#%% load source field (by chunk)
def load_src_field_chunk(src, chunk_start, nrec_chunk):
    if(src=='merra2'):
        src_field_tmp = np.fromfile(src_dir+src_fn,dtype='>f4',
                                    count = nrec_chunk*sgrid_size,
                                  offset=chunk_start*sgrid_size*src_precision)
        src_field = np.reshape(src_field_tmp,(nrec_chunk,)+sgrid_shape)
    elif src=='llc90' or src=='llc270':
        src_field = ecco.read_llc_to_tiles(src_dir, src_fn, 
                                           nk = nrec_chunk,
                                           skip=chunk_start,
                                           llc=llc,
                                           less_output=True)  
        src_field_tmp = src_field.ravel()
    src_field_flat_shape = src_field_tmp.shape    
    shm = create_shared_memory_nparray(src_field_tmp,
                                       ARRAY_SHAPE=src_field_flat_shape)     
    return src_field, src_field_flat_shape

#%% load json file 
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

#%% process one latitudinal band  
def proc_band(band_idx, slat0, name, src_field_flat_shape, fill_dry_points,
              out_fn, nrec_chunk,
              src_field='',
              verbose=False):
    if(verbose):
        time_bf_matrix_a = time.time() 

    shm = shared_memory.SharedMemory(name=name)    
    src_field_flat = np.ndarray(src_field_flat_shape, dtype=NP_DATA_TYPE, buffer=shm.buf)
    if (verbose):
        print('shape of src_field_flat: ',src_field_flat.shape)

    src_field = src_field_flat.reshape((nrec_chunk,)+sgrid_shape)

    if(fill_dry_points==True):
        src_field[:, sgrid_drypnts] = \
            np.copy(src_field.reshape((nrec_chunk,-1))[:,closest_idx][:,sgrid_drypnts]) 

    slat1 = slat0 + dlatcell
    lat_mid = slat0+(slat1-slat0)/2
    
    if (verbose):
        print('Processing latitudial band: ',slat0,slat1)
    
    tlat0 = slat0-dlathilo
    tlat1 = slat1+dlathilo   

    # find grid points in this band
    masksub_src = ((sgrid_y>=slat0) & (sgrid_y<slat1))
    masksub_dest = ((dgrid_y>=tlat0) & (dgrid_y<=tlat1))  

    if (verbose):
        print('masksub_src shape: ',src_field.shape,  masksub_src.shape)
    src_tapered = np.copy(src_field[0:nrec_chunk,masksub_src])

    # load mapping matrix for a particular latitudinal band
    K_dict=load_K(band_idx)            
    K = K_dict[band_idx][0] 

    if(True): # tappering    
        disttmp = earthrad*np.deg2rad(sgrid_y[masksub_src]-lat_mid)
        # taperdist0 is actually 
        # earthrad*np.deg2rad(lat_mid-dlatcell/2+dlattap-lat_mid).
        # Because of the two lat_mids cancel with each other,
        # we have earthrad*np.deg2rad(lat_mid-dlatcell/2+dlattap-lat_mid).
        # Same for taperdist1, taperdist2, and taperdist3.
        taperdist0 = earthrad*np.deg2rad(-dlatcell/2+dlattap)
        taperdist1 = earthrad*np.deg2rad(-dlatcell/2+twodlattap)
        taperdist2 = earthrad*np.deg2rad(+dlatcell/2-twodlattap)
        taperdist3 = earthrad*np.deg2rad(+dlatcell/2-dlattap)
        
        if(slat0>y0):
            idxtaperdist0 = disttmp<taperdist0      
            src_tapered[0:nrec_chunk,idxtaperdist0] = 0. #*src_tapered[idxtaperdist0]
            idxtaperdist1 = (taperdist0<=disttmp) & (disttmp<taperdist1)     
            src_tapered[0:nrec_chunk,idxtaperdist1] = src_tapered[0:nrec_chunk,idxtaperdist1] * \
                (disttmp[idxtaperdist1]-taperdist0)/dlattap_inmeters                    
                
        if(slat1<ymax): 
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
def load_grid(grid_nm, grid_dir='./', local_or_s3='local'):
    if grid_nm == 'merra2':
        # grid: MERRA2 
        nx = 576
        ny = 361
        dlat = 0.5
        dlon = 5/8
        x=[x0 + dlon*i for i in range(nx)]
        y=[y0 + dlat*i for i in range(ny)]
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
def load_K(band_idx):
    # have a shared dictionary
    manager = Manager()
    K_dict = manager.dict()
        
    slat0 = slat0_all[band_idx]
    slat1 = slat0 + dlatcell 
    OI_mapping_fname = mapping_factors_dir / \
        (fnprefix + f"_{band_idx:d}_{slat0:d}_{slat1:d}.p" )    
    K_dict[band_idx] = pickle.load(open(OI_mapping_fname, 'rb'))
    print('Matrix is now loaded for latitudinal band '+\
          f'{band_idx+1:d} ({slat0} to {slat1})')

    return K_dict

#%%
def write_mapped_field_to_file(dest_field, out_fn):
    dest_field_c= ecco.llc_tiles_to_compact(dest_field, 
                                           less_output=True)
        
    with open(out_dir+'/'+out_fn, "wb") as file:
        file.write(dest_field_c.astype('>f4'))
    return
#%%    
if __name__ == "__main__":
    """Command-line entry point.
    
    """
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

    if NUM_WORKERS>=6:
        print(f"""WARNING! Using NUM_WORKERS larger than 6 might 
              require much larger memory! NUM_WORKERS = {NUM_WORKERS}""")

    # cap band0 and band1 at nband_max;
    # note that they are counted staring from 1. 
    if(band0>nband_max):
        band0 = nband_max        
    if(band1>nband_max):
        band1 = nband_max
    
    if src_fn == '':
        if src == 'merra2':
            src_fn = variable +f'_{1992:d}'
        else: 
            src_fn = 'xx_'+variable +f'.{iternum:010d}.data'

#%%
# parameters (no need to change them)   
    earthrad = 6371000. # earth radius in m
    # do the mappling for the whole globe
    x0 = -180
    xmax = 180
    y0 = -90
    ymax = 90
    
#%%
# load some parameters from json file
    json_fn = mappingtype+".json"
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

    if 's3://' in str(mapping_factors_dir): 
        print('processing on s3')
        ecco_s3_fs = s3fs.S3FileSystem(profile='saml-pub')
    else:
        print('processing on local machine') 

#%%
# load source and destination grid info        
    sgrid_shape, sgrid_flat_shape, sgrid_size, sgrid_y = \
        load_grid(src, sgrid_dir)
    dgrid_shape, dgrid_flat_shape, dgrid_size, dgrid_y = \
        load_grid(dest, dgrid_dir)    

#%%
    if fill_dry_points:       
        nearest_wet_points_indices_fn = src + '_nearest_wet_points_indices.p'
        [sgrid_oceanpnts, sgrid_drypnts, closest_idx] = \
            pickle.load(open(nearest_wet_points_indices_dir+\
                             nearest_wet_points_indices_fn, 'rb'))

#%%    
    dlathilo = 0 # source and destination grids are over the same region
    
    #shift step is dlatcell-3*dlattap so the two neighboring tapering regions 
    # are the same
    # latitudinal band width in degrees
    dlatcell = 18
    dlattap = 3 # 3 degrees for tapering 
    dlatshift = dlatcell-3*dlattap # the current band is 9 degrees north of the previous band
    
    # twodlattap and twodlattap are needed for tapering
    twodlattap = 2*dlattap
    dlattap_inmeters= earthrad * np.deg2rad(dlattap)

#%%
# input/source file information  
     
    src_precision = 4 # 4 bytes for single precision
    file_stats = os.stat(src_dir+src_fn)
    nrec_src = int((file_stats.st_size+1e-5)/sgrid_size/src_precision)
#%%             
    start_time = time.time()
#%% create latitudinal bands
    # slat0_all is the southern boundary of each band.
    slat0_all = []
    for slat0_tmp in range(y0,ymax, dlatshift):
        slat0_all.append(slat0_tmp)
        if(slat0_tmp+dlatcell>=ymax):
            break
    slat0_all = np.asarray(slat0_all)
    
    nband = len(slat0_all)
    band_ind_all = np.arange(nband)
   
#%% 
    # default shared memory array size and shape
    NP_SHARED_NAME = 'src_field_shm'
    NP_DATA_TYPE = np.float32 
    
    start_map_time = time.time()

    # each job will only do one chunk.    
    chunk_start = rec0-1
    if rec1>nrec_src:
        rec1 = nrec_src
    nrec_chunk = int(((rec1-rec0+1)-1)/nchunk)+1

    # do the mapping           
    futures=[]
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:          
        for ichunk in range(nchunk):   
            if((rec1-chunk_start)<nrec_chunk):
                nrec_chunk = rec1-chunk_start
            chunk_end = chunk_start + nrec_chunk

            # array size and shape of shared memory
            # Put them inside the loop because nrec_chunk may change       
            ARRAY_SIZE = int(nrec_chunk*sgrid_size)
            ARRAY_SHAPE = (ARRAY_SIZE,)
            out_fn =src_fn + '_'+mappingtype+'_'+\
                f'{chunk_start+1:05d}_{chunk_end:05d}_{band0:02d}_{band1:02d}' 
            src_field, src_field_flat_shape = \
                load_src_field_chunk(src, chunk_start, nrec_chunk)

            for band_idx in range(band0-1, band1):        
                if(band_idx>=nband):
                    continue                    
                slat0 = slat0_all[band_idx] 

                futures.append(executor.submit(proc_band, band_idx, slat0, 
                                               NP_SHARED_NAME, 
                                               src_field_flat_shape,
                                               fill_dry_points,
                                               out_fn, nrec_chunk,
                                               src_field=src_field))  
        # generate the mapped field             
        for fut in concurrent.futures.as_completed(futures):
            fut_res_mask = fut.result()[0]
            fut_res_value = fut.result()[1] 
            fut_res_out_fn = fut.result()[2] 
            fut_res_nrec_chunk = fut.result()[3]
       
            if os.path.isfile(out_dir+fut_res_out_fn):
                dest_field = ecco.read_llc_to_tiles(out_dir, fut_res_out_fn,
                                                    nk = fut_res_nrec_chunk,
                                                    less_output=True,
                                                    llc=llc)
            else:
                print('New dest_field: '+out_dir+fut_res_out_fn)
                dest_field = np.zeros((fut_res_nrec_chunk,)+dgrid_shape)
                              
            dest_field[0:fut_res_nrec_chunk,fut_res_mask] = + \
                dest_field[0:fut_res_nrec_chunk,fut_res_mask] + fut_res_value
    
            # output
            write_mapped_field_to_file(dest_field, fut_res_out_fn)

            # release shared memory            
            release_shared(NP_SHARED_NAME)
            # update chunk_start
            chunk_start = chunk_end
          
    print('exec time (s): ',time.time()-start_time,
          time.time()-start_map_time)
