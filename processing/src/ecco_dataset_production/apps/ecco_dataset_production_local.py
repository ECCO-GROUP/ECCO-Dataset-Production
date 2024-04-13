#!/usr/bin/env python3
"""
"""

import argparse
import importlib.resources
import json
import logging
import re

from .. import configuration
from .. import metadata
from .. import utils


logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')


def create_parser():
    """Set up list of command-line arguments to ecco_dataset_production_local.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--cfgfile', default='./product_generation_config.yaml',
        help="""(Path and) filename of ECCO Production configuration file
        (default: '%(default)s')""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    return parser


def ecco_dataset_production_local( cfgfile=None, log_level=None):
    """

    Args:
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.

        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:

    """
    log = logging.getLogger(__name__)
    if log_level:
        log.setLevel(log_level)

    # configuration initialization:

    log.info('Initializing configuration parameters...')
    cfg = configuration.ECCODatasetProductionConfig(cfgfile=cfgfile)
    # TODO: cfg.set_default_paths(workingdir)
    log.debug('Configuration key value pairs:')
    for k,v in cfg.items():
        log.debug('%s: %s', k, v)
    log.info('...done initializing configuration parameters.')

    # collect job groupings-related package metadata and organize into a
    # dictionary with keys, '1D', 'latlon', and 'native'

    log.info('collecting %s package resource metadata...' % cfg['ecco_version'])
    dataset_groupings = {}
    traversible = importlib.resources.files(metadata)
    with importlib.resources.as_file(traversible/cfg['ecco_version']) as files:
        for file in files.glob('*groupings*'):
            if re.search(r'_1D_',file.name,re.IGNORECASE):
                log.debug('parsing 1D groupings metadata file %s' % file)
                with open(file) as f:
                    dataset_groupings['1D'] = json.load(f)
            elif re.search(r'_latlon_',file.name,re.IGNORECASE):
                log.debug('parsing latlon groupings metadata file %s' % file)
                with open(file) as f:
                    dataset_groupings['latlon'] = json.load(f)
            elif re.search(r'_native_',file.name,re.IGNORECASE):
                log.debug('parsing native groupings metadata file %s' % file)
                with open(file) as f:
                    dataset_groupings['native'] = json.load(f)
    log.debug('dataset grouping metadata:')
    for key,list_of_dicts in dataset_groupings.items():
        log.debug('%s:' % key)
        for i,dict_i in enumerate(list_of_dicts):
            log.debug(' %d:' % i)
            for k,v in dict_i.items():
                log.debug('  %s: %s' % (k,v))
    log.info('...done collecting %s resource metadata.'  % cfg['ecco_version'])

    # create dataset production job list:

    # with open(jobs_filename) as jf:


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    ecco_dataset_production_local( args.cfgfile, args.log_level)


if __name__=='__main__':
    main()

