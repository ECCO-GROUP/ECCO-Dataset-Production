#!/usr/bin/env python

"""A Python wrapper for creating ECCO grid mapping factors.

"""

import argparse
import logging
import sys

import ecco_production.configuration
import ecco_production.utils

log = logging.getLogger('ecco_dataset_production')


def create_parser():
    """Set up list of command-line arguments to create_factors.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""Creates 2- and/or 3-D mapping factors, land mask, and
        lon/lat grid files.""",
        epilog="""The input ECCO grid path and filename and output mapping
        factors directory are implicitly defined via the product generation
        configuration file fields 'ecco_grid_dir', 'ecco_grid_filename', and
        'mapping_factors_dir', respectively. 'ecco_grid_filename' is the only
        one that is required, as path defaults will be assigned if necessary.
        Additionally, configuration fields starting with 'latlon' are referenced
        if lon/lat-based mapping factors are to be generated, while
        'custom_grid_and_factors' is used if custom target grid mappings are to
        instead be generated.""")
    parser.add_argument('--cfgfile', default='./product_generation_config.yaml',
        help="""(Path and) filename of ECCO Production configuration file
        (default: '%(default)s')""")
    parser.add_argument('--workingdir', default='.', help="""
        If any path data are unassigned, --workingdir will be used to set
        default path root values (default: '%(default)s')""")
    parser.add_argument('--dims', nargs='+', help="""
        Dimension(s) of mapping factors to be generated, e.g., --dims 2 3 if
        both two- and three-dimensional mapping factors are to be created.""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='INFO', help="""
        Set logging level (default: %(default)s)""")
    return parser


def create_factors( cfgfile=None, workingdir=None, dims=None, log_level=None):
    """
        mapping_factors_dir
        custom_grid_and_factors
        num_vertical_levels
        grid_files_dir
        source_grid_min_L
        source_grid_max_L
        ecco_grid_dir
        ecco_grid_filename (GRID_GEOMETRY_ECCO_V4r4_native_llc0090.nc)
        latlon_grid_resolution
        latlon_max_lat
        latlon_grid_area_extent
        latlon_grid_dims
        latlon_effective_grid_radius
        ecco_version
    """
    log.info('Initializing configuration parameters...')
    cfg = ecco_production.configuration.ECCOProductionConfig(cfgfile=cfgfile)
    cfg.set_default_paths(workingdir)
    log.info('Configuration key value pairs:')
    for k,v in cfg.items():
        log.info('%s: %s', k, v)
    log.info('...done initializing configuration parameters.')

    # convert input 'dims' into format required by create_all_factors:
    try:
        dims = [d+'D' for d in dims]
    except:
        errstr = f'{sys._getframe().f_code.co_name} "dims" input error'
        log.exception('%s', errstr)

    ecco_production.utils.mapping_factors_utils.create_all_factors(
        cfg, dims)


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    logging.basicConfig(
        format = '%(levelname)-10s %(asctime)s %(message)s',
        level=args.log_level)

    create_factors( args.cfgfile, args.workingdir, args.dims, args.log_level)
    

if __name__=='__main__':
    main()

