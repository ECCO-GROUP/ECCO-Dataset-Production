#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 14:36:28 2024

@author: owang
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import sys
import xarray as xr
import os

from collections import namedtuple
import glob
import argparse

sys.path.append('/home3/owang/CODE/Python/projects/modules/ECCOv4-py/')
import ecco_v4_py as ecco

#%%
font = {'size'   : 16}
matplotlib.rc('font', **font)
earthrad = 6371000.

# create parser
def create_parser():
    """Set up list of command-line arguments to the objective mapping script.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""A Python script to concatenate fields to yearly global
        files (apply floor/cap if needed).""",
        epilog="""Note: Currently, the following mapping is implemented: 
        merra2 to llc90, merra2 to llc270, and llc90 to llc270.""")       
    parser.add_argument('--src', default='merra2', help="""
        Source grid (default: "%(default)s")""")
    parser.add_argument('--dest', default='llc90', help="""
        Destination grid (default: "%(default)s")""")
    parser.add_argument('--src_fn', default='', help="""
        Source filename (default: "%(default)s")""")        
    parser.add_argument('--variable', default='TAUX', help="""
        Variable name (default: %(default)s)""")
    parser.add_argument('--flooring_flag', type=int, default=0, help="""
        Flooring flag: 0, 1, and 2 (no flooring, flooring, and capping enabled) 
        (default: %(default)s)""")
    parser.add_argument('--floor', type=int, default=0, help="""
        Floor values (default: %(default)s)""")
    parser.add_argument('--corr_err_flag', type=int, default=0, help="""
        Flag for correcting mapping error: 0 (no corretion) or 1 (do correction) 
        (default: %(default)s)""")        
    parser.add_argument('--factor', type=float, default=0, help="""
        Scaling factor to be applied to the error field (default: %(default)s)""")
    parser.add_argument('--err_dir', default='', help="""
        Path to error field (default: "%(default)s")""")        
    parser.add_argument('--err_fn', default='', help="""
        Error filename (default: "%(default)s")""")        
    parser.add_argument('--year0', type=int, default=1992, help="""
        Start year (default: %(default)s)""")
    parser.add_argument('--year1', type=int, default=1992, help="""
        End year (default: %(default)s)""")       
    parser.add_argument('--iternum', type=int, default=0, help="""
        Iternation number (default: %(default)s)""")
    parser.add_argument('--dest_dir', default='', help="""
        Directory of input files (default: "%(default)s")""")        
    return parser

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
      
    return grid_shape, grid_flat_shape, grid_size, grid_y, grid_x

#%%
#%%
def read_mapped_field_to_file(out_dir, out_fn,
                               OutputGlobalField=False):
    if OutputGlobalField:
        dest_field_tmp= ecco.read_llc_to_tiles(out_dir, out_fn, 
                                               nk=-1,
                                               less_output=True)
    else:
        dest_field_tmp = np.fromfile(out_dir+'/'+out_fn, dtype='>f4')
      
    return dest_field_tmp

#%% find mask for current band
def get_band_mask(grid_y, lat0, lat1):
    return ((grid_y>=lat0) & (grid_y<lat1))

def concat_to_glb_yearly_file(src, dest, src_fn, variable, year0, year1,
                              dest_dir='', 
                              iternum=0,
                              flooring_flag=0,
                              floor=0,
                              corr_err_flag=0,
                              factor=0,
                              err_dir='',
                              err_fn=''):                                 
    if(dest=='llc90'):
        llc=90
        dgrid_dir = "/nobackup/owang/ECCO/llc90/V4r5/grid/nctiles_grid/"
    elif(dest=='llc270'):
        llc=270
        dgrid_dir = "/nobackup/owang/ECCO/llc270/V5/nctiles_grid/"
    else:
        llc=None
        print('Error! dest has to be llc90 or llc270: ',dest)
        sys.exit()
        
    # Grid Setup    
    
    #shift step is dlatcell-3*dlattap so the two neighboring tapering regions 
    # are the same
    # latitudinal band width in degrees
    dlatcell = 18
    dlattap = 3 # 3 degrees for tapering 
    dlatshift = dlatcell-3*dlattap # the current band is 9 degrees north of the previous band
    
    # Calculate mapping matrices over between 90S and 90N
    y0tmp = -90
    y1tmp = 90
    
    [grid_params, K_params] = glob_params()
    
    # slat0_all is the southern boundary of each band.
    slat0_all = []
    for slat0_tmp in range(y0tmp,y1tmp, dlatshift):
        slat0_all.append(slat0_tmp)
        if(slat0_tmp+dlatcell>=y1tmp):
            break
    slat0_all = np.asarray(slat0_all)
    
    #%%
    # load destination grid info        
    dgrid_shape, dgrid_flat_shape, dgrid_size, dgrid_y, dgrid_x = \
        load_grid(grid_params, dest, dgrid_dir)
    #%% 
    mask_dest_glob_dict = {}           
    for band_idx in range(len(slat0_all)):
        # find grid points in this band
        slat0 = slat0_all[band_idx]
        slat1 = slat0 + K_params.dlatcell
        tlat0 = slat0-K_params.dlathilo
        tlat1 = slat1+K_params.dlathilo     
    
        masksub_dest = get_band_mask(dgrid_y, tlat0, tlat1)
        mask_dest_glob_dict[band_idx] = masksub_dest
    
    if dest_dir=='':
        dest_dir = '/nobackup/owang/v4_release5/MITgcm/verification/'+\
            'full_runs_r4it11/input.extra/forcing/merra2/mappingtrash/'
    #dest = 'llc90'
    #dest_dir = '/nobackup/owang/v4_release5/MITgcm/verification/full_runs_r4it11/input.extra/forcing/merra2/mappingtrash_accuratedist/trash_accuratedist/'
    #dest_dir = '/nobackup/owang/v4_release5/MITgcm/verification/full_runs_r4it11/input.extra/forcing/merra2/mappingtrash_sparse/trash_sparse/'
    output_dir = dest_dir + '/global/'
    nband = 19
    
    for year in range(year0, year1+1):
        if src_fn=='':
            if src == 'merra2':
                file_prefix = variable + f'_{year}_'+\
                    src+'to'+dest            
            else: 
                file_prefix = 'xx_'+variable +f'.{iternum:010d}.data' + \
                    src+'to'+dest
        else:
            file_prefix = src_fn + '_' +\
                src+'to'+dest
               
        os.chdir(dest_dir)
        flist = glob.glob(file_prefix+'_?????_?????_??_??')
        flist.sort()
        print('year ', year, dest_dir, file_prefix, flist[0:2])
        
        output_fn = file_prefix
        if os.path.isfile(output_dir+output_fn):
            os.remove(output_dir+output_fn)

        for fl in flist:
            band1 = int(fl[-2:])
            band0 = int(fl[-5:-3])
            band_idx = band0-1
            rec1  = int(fl[-11:-6])
            rec0  = int(fl[-17:-12])
            if band0!=band1:
                print('Error! Band0 and Band1 are not the same: {band0}, {band1}')
                sys.exit()
            print(band0, band1, rec0, rec1)
            nrec = rec1 - rec0 + 1
            if(band0==1):
                dest_glob = np.zeros((nrec,)+dgrid_shape)
            mask_dest_tmp = mask_dest_glob_dict[band_idx]
            
            dest_field_band_tmp = read_mapped_field_to_file(dest_dir, fl)
            
            # Perform the reshaping and assign to dest_glob[:,mask_dest_tmp]
            dest_glob[:,mask_dest_tmp] = dest_glob[:,mask_dest_tmp] + \
                np.reshape(dest_field_band_tmp, dest_glob[:,mask_dest_tmp].shape)
            
            if(band0==nband):
                dest_glob_c = ecco.llc_tiles_to_compact(dest_glob,
                                                        less_output=True)
                # if corr_err_flag==1, correct the error during mapping
                # by reading in a pre-computed error field
                if(corr_err_flag==1):
                    error_c=ecco.read_llc_to_compact(err_dir, err_fn, nk=-1,
                                                     llc=llc)
                    dest_glob_c = dest_glob_c - factor*error_c
                    
                if(flooring_flag==1):
                    dest_glob_c[dest_glob_c<floor] = floor
                elif(flooring_flag==2):
                    dest_glob_c[dest_glob_c>floor] = floor
                with open(output_dir+output_fn, "ab") as file:
                    file.write(dest_glob_c.astype('>f4'))

#%% preparation
def prep_proc():
    parser = create_parser()
    args = parser.parse_args()  
    
    # to be put in argument 
    src = args.src
    dest = args.dest
    src_fn = args.src_fn
    
    variable = args.variable
    flooring_flag = args.flooring_flag
    floor = args.floor
    year0 = args.year0
    year1 = args.year1
    dest_dir = args.dest_dir
    iternum = args.iternum
    corr_err_flag = args.corr_err_flag
    factor = args.factor
    err_dir = args.err_dir
    err_fn = args.err_fn    

    return [src, dest, src_fn, variable, year0, year1, dest_dir, iternum,
            flooring_flag, floor, corr_err_flag, factor, err_dir, err_fn]    
                
def main():
    [src, dest, src_fn, variable, year0, year1, dest_dir, iternum,
     flooring_flag, floor, corr_err_flag, factor, err_dir, err_fn] \
        = prep_proc()

    concat_to_glb_yearly_file(src, dest, src_fn, variable, year0, year1, 
                              dest_dir=dest_dir, iternum=iternum,
                              flooring_flag=flooring_flag,
                              floor=floor,
                              corr_err_flag=corr_err_flag,
                              factor=factor,
                              err_dir=err_dir,
                              err_fn=err_fn)        
if __name__ == "__main__":
    main()