#!/usr/bin/env python

"""A Python wrapper for creating ECCO grid mapping factors.

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
        If any configuration path data are unassigned, --workingdir will be used
        to set default path root values (default: '%(default)s')""")
    parser.add_argument('--dims', nargs='+', help="""
        Dimension(s) of mapping factors to be generated, e.g., --dims 2 3 if
        both two- and three-dimensional mapping factors are to be created.""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")
    return parser


def create_factors( cfgfile=None, workingdir=None, dims=None, log_level=None):
    """Convenience wrapper for call to
    ecco_production.utils.mapping_factors_utils.create_all_factors.

    Args:
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        workingdir (str): Working directory path definition default if explicit
            path definitions are otherwise unassigned in cfgfile.
        dims (str): List of dimensions for which mapping factors are to be
            generated (e.g., ['2','3'] for both two- and three-dimensional
            mapping).
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        Indirectly, 2- and/or 3-D grid mapping factors in directory defined by
        configuration variables ['mapping_factors_dir']/['ecco_version'].

    Note:
        Configuration parameters referenced by this, and all called routines,
        include:
            custom_grid_and_factors
            ecco_grid_dir
            ecco_grid_filename
            ecco_version
            grid_files_dir
            latlon_effective_grid_radius
            latlon_grid_area_extent
            latlon_grid_dims
            latlon_grid_resolution
            latlon_max_lat
            mapping_factors_dir
            num_vertical_levels
            source_grid_min_L
            source_grid_max_L
    """
    log = logging.getLogger(__name__)
    if log_level:
        log.setLevel(log_level)

    log.info('Initializing configuration parameters...')
    cfg = configuration.ECCODatasetProductionConfig(cfgfile=cfgfile)
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

    utils.mapping_factors_utils.create_all_factors(
        cfg, dims)


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    create_factors( args.cfgfile, args.workingdir, args.dims, args.log_level)
    

if __name__=='__main__':
    main()

