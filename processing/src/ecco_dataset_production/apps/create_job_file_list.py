#!/usr/bin/env python3

import argparse
import ast
import collections
import glob
import importlib.resources
import json
import logging
import os
import re

from .. import configuration
from .. import metadata

# TODO: add s3 support

logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')


def create_parser():
    """Set up list of command-line arguments to create_job_file_list.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobfile', help="""
        (Path and) filename of ECCO Dataset Production jobs simple text file""")
    parser.add_argument('--ecco_source_root', help="""
        ECCO results root location, either directory path (e.g.,
        /ecco_nfs_1/shared/ECCOV4r5) or AWS S3 bucket (s3://...)""")
    parser.add_argument('--ecco_destination_root', help="""
        ECCO Dataset Production output root location, either directory path
        (e.g., ECCOV4r5_datasets) or AWS S3 bucket (s3://...)""")
    parser.add_argument('--cfgfile', default='./product_generation_config.yaml',
        help="""(Path and) filename of ECCO Dataset Production configuration
        file (default: '%(default)s')""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    return parser


def create_job_file_list( jobfile=None, ecco_source_root=None, ecco_destination_root=None, cfgfile=None, log_level=None):
    """

    Args:
        jobfile (str): (Path and) filename of ECCO Dataset Production jobs
            simple text file, each line containing a specifier of the form
            "[<metadata_groupings_id>,<product_type>,<frequency>,<time_steps>]"
            where metadata_groupings_id is an integer from 0 through N,
            product_type is one of '1D', 'latlon', or 'native', frequency is one
            of 'SNAP', 'AVG_MON', or 'AVG_DAY', and time_steps is either a list
            of time steps or 'all'.
        ecco_source_root (str): ECCO results root location, either directory
            path (e.g., /ecco_nfs_1/shared/ECCOV4r5) or AWS S3 bucket (s3://...)
        ecco_destination_root (str): ECCO Dataset Production output root
            location, either directory path (e.g., ECCOV4r5_datasets) or AWS S3
            bucket (s3://...)
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:

        List of dictionary elements with keys, 'input', 'dependencies', 'output'
        corresponding to all located input files, and their potential input
        dependencies (i.e., vector-rotated dataproducts) across all job/metadata
        specifications.

    Notes:
        . ECCO version string (e.g., "V4r5" is assumed to be present in AWS S3
        object names and/or directory paths.

    """
    log = logging.getLogger(__name__)
    if log_level:
        log.setLevel(log_level)

    # configuration initialization:

    log.info('initializing configuration parameters...')
    cfg = configuration.ECCODatasetProductionConfig(cfgfile=cfgfile)
    # TODO: cfg.set_default_paths(workingdir)
    log.debug('Configuration key value pairs:')
    for k,v in cfg.items():
        log.debug(' %s: %s', k, v)
    log.info('...done initializing configuration parameters.')

    if not ecco_destination_root:
        ecco_destination_root = ''

    # Example: [0,'latlon','AVG_DAY','all']

    # collect job groupings-related package metadata and organize into a
    # dictionary with primary keys, '1D', 'latlon', and 'native'

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

    #
    # collect (potentially very long) i/o lists for every task implied in the
    # list of jobs, store as a list of dictionaries, for json serialization,
    # i.e.: [{'input':..., 'output':...}, {...}, ...]
    #

    input_files = []
    input_files_dependencies = []
    output_files = []

    Job = collections.namedtuple('Job',['metadata_groupings_id','product_type','frequency','time_steps'])

    with open(jobfile,'r') as fh:

        for line in fh:

            job = Job(*ast.literal_eval(line))
            job_metadata = dataset_groupings[job.product_type][job.metadata_groupings_id]
            log.debug('job metadata for %s, %d: %s' %
                (job.product_type, job.metadata_groupings_id, job_metadata))

            # find all source files referenced by this job/job_metadata combination:

            if job.frequency == 'AVG_DAY':
                path_freq_pat = 'diags_daily'
                file_freq_pat = 'day_mean'
            elif job.frequency == 'AVG_MON':
                path_freq_pat = 'diags_monthly'
                file_freq_pat = 'mon_mean'
            elif job.frequency == 'SNAP':
                # TODO
                pass
            else:
                raise SyntaxError("job frequency must be one of AVG_DAY, AVG_MON, or SNAP")

            for output_field in job_metadata['fields'].replace(' ','').split(','): # fields string as iterable

                # accommodate two basic granule construction schemas: direct
                # (one-to-one), and vector component (output based on many input
                # components) based:

                one_to_one = True
                input_fields_keys = []

                if 'vector_inputs' in job_metadata:

                    # output granule *may* depend on component inputs of
                    # differing types.  if so, collect dependencies as
                    # dictionary keys for subsequent file list accumulation:

                    for k,v in job_metadata['vector_inputs'].items():
                        if output_field in v:
                            input_fields_keys.append(k)
                    if input_fields_keys:
                        one_to_one = False

                if not one_to_one:

                    # output granule *does* depend on component inputs of
                    # differing types; determine availability of input files:

                    all_input_field_files = {}

                    for input_field_key in input_fields_keys:
                        input_field_pat = input_field_key
                        input_field_files = []
                        if 'all' == job.time_steps.lower():
                            # get all possible time matches:
                            time_pat = '.*'
                            file_pat = re.compile(
                                '.*' + input_field_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                            for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                if cfg['ecco_version'] in dirpath:
                                    input_field_files.extend(
                                        [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                            # obsolete glob-based approach:
                            #time_pat = '*'
                            #glob_pat = os.path.join(
                            #    ecco_source_root, path_freq_pat,
                            #    input_field_pat + '_' + file_freq_pat,
                            #    input_field_pat + '_' + file_freq_pat + '.' + time_pat + '.data')
                            #input_field_files.extend(glob.glob(glob_pat))
                        else:
                            # explicit list of time steps; one match per item:
                            time_steps_as_int_list = ast.literal_eval(job.time_steps)
                            for time in time_steps_as_int_list:
                                time_pat = "{0:0>10d}".format(int(time))
                                file_pat = re.compile(
                                    '.*' + input_field_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        input_field_files.append(
                                            [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                                # obsolete glob-based approach:
                                #glob_pat = os.path.join(
                                #    ecco_source_root, path_freq_pat,
                                #    input_field_pat + '_' + file_freq_pat,
                                #    input_field_pat + '_' + file_freq_pat + '.' + time_pat + '.data')
                                #input_field_files.extend(glob.glob(glob_pat))
                        input_field_files.sort()
                        all_input_field_files[input_field_key] = input_field_files

                    # group input field files by time value (i.e., across keys),
                    # collect in time-based input lists:

                    all_input_field_files_by_time = []

                    for _,v in all_input_field_files.items():
                        if not all_input_field_files_by_time:
                            # initialize as list of lists:
                            all_input_field_files_by_time = [[file] for file in v]
                        else:
                            # augment each input list in the list of lists:
                            for i,file in enumerate(v):
                                all_input_field_files_by_time[i].append(file)

                    # for every ECCO results file set located, create
                    # corresponding list of single element lists of output
                    # granule destinations (just use first input field key):

                    field_files_output = [
                        [f.replace(ecco_source_root,ecco_destination_root).replace(
                            input_fields_keys[0],output_field).replace('.data','.nc')]
                            for f in all_input_field_files[input_fields_keys[0]]]

                    # and add to cumulative i/o lists:
                    input_files.extend(all_input_field_files_by_time)
                    output_files.extend(field_files_output)

                else:

                    # output granule is dependent on single input, of same type. for
                    # data organization purposes, arrange as list of lists (a job's
                    # (single) input is contained in a list, and a list of such
                    # lists comprise all jobs):

                    field_pat = output_field
                    field_files = []
                    if 'all' == job.time_steps.lower():
                        # get all possible time matches:
                        time_pat = '.*'
                        file_pat = re.compile('.*' + field_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                        for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                            if cfg['ecco_version'] in dirpath:
                                field_files.extend(
                                    [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                        # obsolete glob-based approach:
                        #time_pat = '*'
                        #glob_pat = os.path.join(
                        #    ecco_source_root, path_freq_pat,
                        #    field_pat + '_' + file_freq_pat,
                        #    field_pat + '_' + file_freq_pat + '.' + time_pat + '.data')
                        #field_files.extend(glob.glob(glob_pat))
                        field_files.sort()
                        # arrange as list of single element lists:
                        field_files = [[f] for f in field_files]
                    else:
                        # explicit list of time steps; one match per item:
                        time_steps_as_int_list = ast.literal_eval(job.time_steps)
                        for time in time_steps_as_int_list:
                            time_pat = "{0:0>10d}".format(int(time))
                            file_pat = re.compile('.*' + field_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                            for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                if cfg['ecco_version'] in dirpath:
                                    field_files.append(
                                        [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                            # obsolete glob-based approach:
                            #glob_pat = os.path.join(
                            #    ecco_source_root, path_freq_pat,
                            #    field_pat + '_' + file_freq_pat,
                            #    field_pat + '_' + file_freq_pat + '.' + time_pat + '.data')
                            #field_files.append([glob.glob(glob_pat)])

                    # for every ECCO results file located, create corresponding
                    # list of single element lists of output granule destinations:

                    renamed_output_field = ''
                    if 'variable_rename' in job_metadata:
                        _,renamed_output_field = job_metadata['variable_rename'].split(':')
                    else:
                        renamed_output_field = output_field

                    field_files_output = [
                        [f[0].replace(ecco_source_root,ecco_destination_root).replace(
                            output_field,renamed_output_field).replace('.data','.nc')]
                        for f in field_files]

                    # and add to cumulative i/o lists:
                    input_files.extend(field_files)
                    output_files.extend(field_files_output)

    return(input_files,output_files)


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    (input_files,output_files) = create_job_file_list(
        jobfile=args.jobfile, ecco_source_root=args.ecco_source_root,
        ecco_destination_root=args.ecco_destination_root,
        cfgfile=args.cfgfile, log_level=args.log_level)

    f = []
    for input_file_list,output_file_list in zip(input_files,output_files):
        f.append({'input':input_file_list,'output':output_file_list})
    fp = open('tmp.json','w')
    json.dump(f,fp)
    fp.close()


if __name__=='__main__':
    main()

