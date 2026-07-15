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

from ..config import ECCODatasetProductionConfig
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
    # Create parser with config file support
    parser = ECCODatasetProductionConfig.create_parser(
        description="""Creates 2- and/or 3-D mapping factors, land mask, and
        lon/lat grid files."""
    )

    # Add tool-specific arguments
    parser.add_argument('--workingdir', default='.', help="""
        If any configuration path data are unassigned, --workingdir will be used
        to set default path root values (default: '%(default)s')""")
    parser.add_argument('dims', nargs='+', default=['2', '3'], help="""
        Dimension(s) of mapping factors to be generated (2, 3, or both).
        Example: 2 3 for both two- and three-dimensional mapping factors.""")

    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    parser.epilog = """The input ECCO grid path and filename and output mapping
        factors directory are implicitly defined via the product generation
        configuration file fields 'ecco_grid_dir', 'ecco_grid_filename', and
        'mapping_factors_dir', respectively. 'ecco_grid_filename' is the only
        one that is required, as path defaults will be assigned if necessary.
        Additionally, configuration fields starting with 'latlon' are referenced
        if lon/lat-based mapping factors are to be generated, while
        'custom_grid_and_factors' is used if custom target grid mappings are to
        instead be generated."""

    return parser


def create_factors(cfg, workingdir=None, dims=None, log_level=None):
    """Convenience wrapper for call to
    ecco_production.utils.mapping_factors_utils.create_all_factors.

    Args:
        cfg (ECCODatasetProductionConfig): Configuration instance.
        workingdir (str): Working directory path definition default if explicit
            path definitions are otherwise unassigned in cfgfile.
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

    log.info('Configuration key value pairs:')
    for k,v in cfg.items():
        log.info('%s: %s', k, v)

    # convert input 'dims' into format required by create_all_factors:
    try:
        dims = [d+'D' for d in dims]
    except:
        errstr = f'{sys._getframe().f_code.co_name} "dims" input error'
        log.exception('%s', errstr)

    utils.mapping_factors_utils.create_all_factors(cfg, dims)


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    # Load configuration from parsed args
    cfg = ECCODatasetProductionConfig.from_parsed_args(args)

    create_factors(cfg, args.workingdir, args.dims, args.log_level)
    
