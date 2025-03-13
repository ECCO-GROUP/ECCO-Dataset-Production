"""
"""
import argparse
import json
import logging
import os

from fnmatch import fnmatch

logging.basicConfig(
    format = '%(levelname)-10s %(funcName)s %(asctime)s %(message)s')
log = logging.getLogger('edp')


def create_parser():
    """Set up command-line arguments to create_job_files.
    
    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""
            Utility for generating ECCO Dataset Production job files from ECCO
            Configuration groupings files (one job file per item in groupings
            file).""",
        epilog=""" """)
    parser.add_argument('--groupings_file', help="""
        (Path and) filename of ECCO Configurations groupings json file from
        which list of production job files will be generated (e.g.,
        './ECCO-v4-Configurations/ECCOv4 Release 5/metadata/groupings_for_latlon_datasets.json')""")
    parser.add_argument('--output_dir', default="'.'", help="""
        Directory to which list of production job files will be written.
        output_dir will be created if it does not exist (default:
        %(default)s).""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s).""")
    return parser


def create_job_files(
    groupings_file=None, output_dir=None, log_level=None):
    """Generate individual job files, one for each dataset type listed in
    groupings_file.

    Args:
        groupings_file (str): (Path and) filename of ECCO Configurations
            groupings json file from which list of production job files will be
            generated.
        output_dir (str): Directory to which list of production job files will
            be written. output_dir will be created if it does not exist.
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Raises:
        RuntimeException: If groupings_file name does not contain the string
            'native', 'latlon', or '1D'.

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    # for output filenaming purposes, get type of groupings file:
    grid_type=''
    if fnmatch(os.path.basename(groupings_file).lower(),'*native*'):
        grid_type = 'native'
    elif fnmatch(os.path.basename(groupings_file).lower(),'*latlon*'):
        grid_type = 'latlon'
    elif fnmatch(os.path.basename(groupings_file).lower(),'*1d*'):
        grid_type = '1d'
    if not grid_type:
        raise RuntimeException(
            f"Cannot determine grid type ('native', 'latlon', or '1d') from groupings_file, {groupings_file}")

    # for now, just generate job descriptions for 'all' timesteps. In the
    # future, if useful, perhaps add an input argument for individual time
    # step(s) specification.
    timesteps = 'all'

    # json file parses as list of dicts:
    groupings = json.load(open(groupings_file))

    os.makedirs(output_dir,exist_ok=True)

    for id,group in enumerate(groupings):
        for freq in group['frequency'].replace(' ','').split(','):  # remove spaces, 'frequency'
                                                                    # string as iterable
            job_filename = '_'.join([
                group['filename'],
                grid_type,
                freq,
                'jobs.txt'])
            job_description = [id,grid_type,freq,timesteps]
            log.info('creating job file %s...', job_filename)
            fp = open(os.path.join(output_dir,job_filename),'w')
            log.debug('...writing job description %s', job_description)
            fp.write(f"# {group['name']}:\n")
            fp.write(f"{job_description}\n")
            fp.close()
            log.info('...done')


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    create_job_files(
        groupings_file=args.groupings_file, output_dir=args.output_dir,
        log_level=args.log_level)


if __name__=='__main__':
    main()

