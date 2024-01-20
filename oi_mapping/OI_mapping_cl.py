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
        Directory of input json file (default: "%(default)")""")        
    parser.add_argument('--src', default='merra2', help="""
        Source grid (default: "%(default)")""")
    parser.add_argument('--dest', default='llc90', help="""
        Destination grid (default: "%(default)")""")
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
    return parser

#%% shared memory
def create_shared_memory_nparray(data,
                                 ARRAY_SHAPE=(), 
                                 NP_SHARED_NAME='src_field_shm'):
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
def load_src_field_chunk(chunk_start, nrec_chunk):
    src_field_tmp = np.fromfile(src_dir+src_fn,dtype='>f4',
                                count = nrec_chunk*sgrid_size,
                              offset=chunk_start*sgrid_size*src_precision)
    src_field_flat_shape = src_field_tmp.shape
    src_field = np.reshape(src_field_tmp,(nrec_chunk,)+sgrid_shape)
    shm = create_shared_memory_nparray(src_field_tmp,
                                       ARRAY_SHAPE=src_field_flat_shape)     
    return src_field, src_field_flat_shape

#%% load json file 
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

#%% process one latitudinal band  
def proc_band(band_idx, slat0, name, src_field_flat_shape, verbose=False):
    if(verbose):
        time_bf_matrix_a = time.time() 

    shm = shared_memory.SharedMemory(name=name)    
    src_field_flat = np.ndarray(src_field_flat_shape, dtype=NP_DATA_TYPE, buffer=shm.buf)
    if (verbose):
        print('shape of src_field_flat: ',src_field_flat.shape)

    src_field = src_field_flat.reshape((nrec_chunk,)+sgrid_shape)

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
        return masksub_dest,dest_field_flat_sub.T
#%%
def load_grid(sgrid_nm, grid_dir='./', local_or_s3='local'):
    if sgrid_nm == 'merra2':
        # grid: MERRA2 
        nx = 576
        ny = 361
        dlat = 0.5
        dlon = 5/8
        x=[x0 + dlon*i for i in range(nx)]
        y=[y0 + dlat*i for i in range(ny)]
        sgrid_x, sgrid_y = np.meshgrid(x, y)

    elif sgrid_nm == 'llc90':
        # grid: llc90
        if local_or_s3 == 'local':
            fsgrid = 'ECCO-GRID.nc'
            sgrid_ds = xr.open_dataset(grid_dir + '/' +fsgrid)
        elif local_or_s3 == 's3':
            fsgrid = 'GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc'
            ecco_s3_v4r4_grid_path = grid_dir + fsgrid
            print(ecco_s3_v4r4_grid_path)
            grid_file = ecco_s3_fs.open(ecco_s3_v4r4_grid_path)
            sgrid_ds = xr.open_dataset(grid_file).load()

        sgrid_x = np.copy(sgrid_ds.XC.values)
        sgrid_y = np.copy(sgrid_ds.YC.values)

    elif sgrid_nm == 'llc270':
        # grid: llc270 
        fsgrid = 'ECCO-GRID.nc'
        sgrid_ds = xr.open_dataset(grid_dir + '/' +fsgrid)
        sgrid_x = np.copy(sgrid_ds.XC.values)
        sgrid_y = np.copy(sgrid_ds.YC.values) 

    else:
        print('Error! Grid can only be merra2, llc90, or llc270: ', sgrid_nm)
    sgrid_shape = sgrid_x.shape
    sgrid_flat_shape = sgrid_x.ravel().shape
    sgrid_size = np.prod(sgrid_shape)        
      
    return sgrid_shape, sgrid_flat_shape, sgrid_size, sgrid_y

#%%
def load_K():
    # have a shared dictionary
    manager = Manager()
    K_dict = manager.dict()

    for band_idx, band in enumerate(band_ind_all):
        slat0 = slat0_all[band_idx]
        slat1 = slat0 + dlatcell 
        OI_mapping_fname = mapping_factors_dir / \
            (fnprefix + f"_{band_idx:d}_{slat0:d}_{slat1:d}.p" )    
        K_dict[band_idx] = pickle.load(open(OI_mapping_fname, 'rb'))
    print('Matrices are now loaded.')

    return K_dict

#%%
def write_mapped_field_to_file(dest_field):
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
    
    mappingtype=src+'to'+dest
    if(mappingtype!='merra2tollc90' and 
       mappingtype!='merra2tollc270' and
       mappingtype!='llc90tollc270'):
        print('Error!')
        print('Variable mappingtype has to be one of the following:')
        print('merra2tollc90, merra2tollc270, or llc90tollc270')
        sys.exit()
    
    chunk_start = rec0-1
    nrec_chunk = rec1-rec0+1
    
    src_fn = variable +f'_{1992:d}'

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

    if 's3://' in mapping_factors_dir: 
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
    
    band_ind_all = np.arange(len(slat0_all))  

#%% 
# load matrices   
    K_dict = load_K()
   
#%% 
    # default shared memory array size and shape
    ARRAY_SIZE = int(nrec_chunk*sgrid_size)
    ARRAY_SHAPE = (ARRAY_SIZE,)
    NP_SHARED_NAME = 'src_field_shm'
    NP_DATA_TYPE = np.float32 
    
    start_map_time = time.time()
    band_ind_all = np.arange(len(slat0_all))   

    # each job will only do one chunk.    
    for ichunk in range(1):
    ## for debugging purpose    
        if((nrec_src-chunk_start)<nrec_chunk):
            nrec_chunk = nrec_src-chunk_start
        chunk_end = chunk_start + nrec_chunk
        
        out_fn =src_fn + '_'+mappingtype+'_'+f'{chunk_start:d}_{chunk_end:d}'        
        src_field, src_field_flat_shape = load_src_field_chunk(chunk_start, 
                                                                nrec_chunk)
        dest_field = np.zeros((nrec_chunk,)+dgrid_shape)

        # do the mapping              
        futures=[]        
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            #for i in range(0, NUM_WORKERS):
            for i in range(len(slat0_all)):
            #for i in range(2):
                band_ind = band_ind_all[i]
                slat0 = slat0_all[i]
                futures.append(executor.submit(proc_band, band_ind, slat0, 
                                               NP_SHARED_NAME, 
                                               src_field_flat_shape))
        # generate the mapped field                 
        for fut in concurrent.futures.as_completed(futures):
            fut_res_mask = fut.result()[0]
            fut_res_value = fut.result()[1]        
            dest_field[0:nrec_chunk,fut_res_mask] = + \
                dest_field[0:nrec_chunk,fut_res_mask] + fut_res_value

        # output
        write_mapped_field_to_file(dest_field)

        # release shared memory            
        release_shared(NP_SHARED_NAME)
          
    print('exec time (s): ',time.time()-start_time,
          time.time()-start_map_time)
