#!/usr/bin/env python

"""Create ECCO grid mapping factors from native grid geometry.

This tool generates sparse interpolation matrices, land masks, and lat-lon
grid definitions for transforming data from the native LLC90 grid to a
regular lat-lon grid.

Usage:
    # Generate both 2D and 3D mapping factors
    edp_create_factors \
        --cfgfile configs/config_V4r4.yaml \
        --grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
        --output_dir ./mapping_factors \
        2 3 \
        --log INFO

    # Force recalculation of existing factors
    edp_create_factors \
        --cfgfile configs/config_V4r4.yaml \
        --grid_file GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc \
        --output_dir ./mapping_factors \
        2 3 \
        --force \
        --log INFO

The config file contains grid definition parameters (resolution, area extent).
File paths are provided as command-line arguments.
"""

import argparse
import logging
import sys

from .. import configuration
from .. import utils
#import ecco_production.configuration
#import ecco_production.utils

# enable basic logging at all levels:
logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')
log = logging.getLogger(__name__)


def create_parser():
    """Set up list of command-line arguments to create_factors.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""Creates 2- and/or 3-D mapping factors, land mask, and
        lon/lat grid files from a native ECCO grid geometry file.""",
        epilog="""The configuration file contains grid definition parameters
        (resolution, area extent, etc.). File paths for input grid and output
        directory are provided as command-line arguments. Configuration fields
        starting with 'latlon' define the target grid parameters, while
        'custom_grid_and_factors' determines whether to use ECCO grids or
        custom grid definitions.""")

    parser.add_argument('--cfgfile', required=True,
        help="""(Path and) filename of ECCO Dataset Production configuration file
        (e.g., configs/config_V4r4.yaml). Contains grid definition parameters
        like latlon_grid_resolution and latlon_grid_area_extent.""")

    parser.add_argument('--grid_file', required=True,
        help="""Path to native ECCO grid geometry NetCDF file
        (e.g., GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc). This file must
        contain hFacC, XC, YC, and other grid variables.""")

    parser.add_argument('--output_dir', required=True,
        help="""Output directory where mapping factors will be written.
        Will create subdirectories: 3D/, land_mask/, latlon_grid/, and sparse/""")

    parser.add_argument('dims', nargs='+', default=['2', '3'], help="""
        Dimension(s) of mapping factors to be generated (2, 3, or both).
        Example: 2 3 for both two- and three-dimensional mapping factors.""")

    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    parser.add_argument('--force', action='store_true',
        help="""Force recalculation of mapping factors even if they already exist.
        By default, existing mapping factors are not regenerated.""")

    return parser


def create_factors( cfgfile=None, grid_file=None, output_dir=None, dims=None,
                   force=False, log_level=None):
    """Convenience wrapper for call to
    ecco_production.utils.mapping_factors_utils.create_all_factors.

    Args:
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file containing grid definition parameters.
        grid_file (str): Path to native ECCO grid geometry NetCDF file.
        output_dir (str): Directory where mapping factors will be written.
        dims (str): List of dimensions for which mapping factors are to be
            generated (e.g., ['2','3'] for both two- and three-dimensional
            mapping).
        force (bool): If True, recalculate mapping factors even if they exist.
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        Indirectly, 2- and/or 3-D grid mapping factors in output_dir.

    Note:
        Configuration parameters from cfgfile include:
        ``custom_grid_and_factors``, ``latlon_effective_grid_radius``,
        ``latlon_grid_area_extent``, ``latlon_grid_resolution``,
        ``num_vertical_levels``, ``ecco_version``.

        File paths (grid_file, output_dir) are provided as command-line
        arguments and injected into the config for the mapping factors
        generation process.
    """
    log = logging.getLogger(__name__)
    if log_level:
        # Set log level on the parent package logger so all modules inherit it
        logging.getLogger('ecco_dataset_production').setLevel(log_level)
        log.setLevel(log_level)

    log.info('-'*80)
    log.info('Initializing configuration parameters...')
    log.info('-'*80)

    cfg = configuration.ECCODatasetProductionConfig(cfgfile=cfgfile)

    # Inject file paths from command-line arguments into config
    from pathlib import Path
    cfg['ecco_grid_dir'] = str(Path(grid_file).parent)
    cfg['ecco_grid_filename'] = Path(grid_file).name
    cfg['mapping_factors_dir'] = output_dir
    cfg['force_recalculation'] = force

    log.info('Configuration key value pairs:')
    for k,v in cfg.items():
        log.info('%s: %s', k, v)
    log.info('...done initializing configuration parameters.')
    log.info('-'*80)

    # convert input 'dims' into format required by create_all_factors:
    try:
        dims = [d+'D' for d in dims]
    except:
        errstr = f'{sys._getframe().f_code.co_name} "dims" input error'
        log.exception('%s', errstr)

    utils.mapping_factors_utils.create_all_factors(
        cfg, dims)


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    create_factors(
        cfgfile=args.cfgfile,
        grid_file=args.grid_file,
        output_dir=args.output_dir,
        dims=args.dims,
        force=args.force,
        log_level=args.log_level
    )
    
