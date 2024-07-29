#!/usr/bin/env python3

import argparse
import ast
import boto3
import collections
import importlib.resources
import json
import logging
import os
import pandas as pd
import re
import subprocess
import sys
import urllib

from .. import aws
from .. import configuration
from .. import ecco_file
from .. import ecco_metadata_store
from .. import ecco_time
from .. import metadata


logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')
log = logging.getLogger('edp')

def create_parser():
    """Set up list of command-line arguments to create_job_task_list.

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
        (e.g., ECCOV4r5_datasets) or AWS S3 bucket (s3://bucket_name)""")
    parser.add_argument('--ecco_grid_loc', help="""
        Directory containing ECCO grid files, e.g., XC.*, YC.*, etc., as well as
        the file available_diagnostics.log, or similar remote location given by
        AWS S3 bucket/prefix.""")
    parser.add_argument('--ecco_mapping_factors_loc', help="""
        Directory containing ECCO mapping factors (3D, land_mask, latlon_grid,
        and sparse subdirectories), or similar remote location given by AWS S3
        bucket/prefix.""")
    parser.add_argument('--ecco_metadata_loc', help="""
        Directory containing ECCO metadata *.json source files, or similar
        remote location given by AWS S3 bucket/prefix.  Three of the source
        files, *_groupings_for_{1D,latlon,native}_datasets.json, are used here
        to create task definitions, while the others, e.g.
        *_global_metadata_for_{all,native,latlon}_datasets.json, etc. are
        referenced during subsequent dataset production task execution.""")
    parser.add_argument('--cfgfile', default='./product_generation_config.yaml',
        help="""(Path and) filename of ECCO Dataset Production configuration
        file (default: '%(default)s')""")
    parser.add_argument('--outfile', help="""
        Resulting job task output file (json format) (default: stdout)""")
    parser.add_argument('--keygen', help="""
        If ecco_source_root references an S3 bucket and if running in JPL
        domain, federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--profile', help="""
        If ecco_source_root references an S3 bucket and if running in JPL
        domain, AWS credential profile name (e.g., 'saml-pub', 'default',
        etc.)""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    return parser


##
## common utilities, at some point:
## 
#
#def is_s3_uri(path_or_uri_str):
#    """Determines whether or not input string is an AWS S3Uri.
#
#    Args:
#        path_or_uri_str (str): Input string.
#
#    Returns:
#        True if string matches 's3://', False otherwise.
#    """ 
#    if re.search( r's3:\/\/', path_or_uri_str, re.IGNORECASE):
#        return True
#    else:
#        return False


def create_job_task_list(
    jobfile=None, ecco_source_root=None, ecco_destination_root=None,
    ecco_grid_loc=None, ecco_mapping_factors_loc=None,
    ecco_metadata_loc=None, cfgfile=None,
    keygen=None, profile=None, log_level=None):
    """Create a list of task inputs and outputs from an ECCO Dataset Production
    job file.

    Args:
        jobfile (str): (Path and) filename of ECCO Dataset Production jobs
            simple text file, each line containing a specifier of the form
            "[<metadata_groupings_id>,<product_type>,<frequency>,<time_steps>]"
            where metadata_groupings_id is an integer from 0 through N,
            product_type is one of '1D', 'latlon', or 'native', frequency is one
            of 'SNAP', 'AVG_MON', or 'AVG_DAY', and time_steps is either a list
            of time steps or 'all'.
        ecco_source_root (str): ECCO results root location, either directory
            path (e.g., /ecco_nfs_1/shared/ECCOV4r5) or AWS S3 bucket
            (s3://...).
        ecco_destination_root (str): ECCO Dataset Production output root
            location, either directory path (e.g., ECCOV4r5_datasets) or AWS S3
            bucket or folder (s3://...).
        ecco_grid_loc (str): Directory containing ECCO grid files, e.g., XC.*,
            YC.*, etc., as well as the file available_diagnostics.log, or
            similar remote location given by AWS S3 bucket/prefix.
        ecco_mapping_factors_loc (str): Directory containing ECCO mapping
            factors (3D, land_mask, latlon_grid, and sparse subdirectories), or
            similar remote location given by AWS S3 bucket/prefix.
        ecco_metadata_loc (str): Directory containing ECCO metadata *.json
            source files (*_groupings_for_{1D,latlon,native}_datasets.json,
            *_global_metadata_for_{all,native,latlon}_datasets.json, etc.), or
            similar remote location given by AWS S3 bucket/prefix.
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        keygen (str): If ecco_source_root If ecco_source_root references an S3
            bucket and if running in JPL domain, federated login key generation
            script (e.g., /usr/local/bin/aws-login-pub.darwin.amd64).
            (default: None)
        profile (str): If ecco_source_root references an AWS S3 bucket and
            if running in JPL domain, AWS credential profile name (e.g.,
            'saml-pub', 'default', etc.)
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        List of resulting job tasks, each as a dictionary with 'granule',
        'variables', 'ecco_grid_loc', 'ecco_mapping_factors', and 'metadata'
        keys.

    Raises:
        RuntimeError: If ecco_source_root or ecco_destination_root are not
            provided.
        ValueError: If ecco_source_root type cannot be determined or if jobfile
            contains format errors.

    Notes:
        - ECCO version string (e.g., "V4r5") is assumed to be contained in
        either directory path or in AWS S3 object names.

    """
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

    if not ecco_source_root or not ecco_destination_root:
        err = None
        if not ecco_source_root:
            err = 'ecco_source_root'
        if not ecco_destination_root:
            if err:
                err += ' and ecco_destination_root'
            else:
                err = 'ecco_destination_root'
        err += ' must be provided'
        raise RuntimeError(err)

    if aws.ecco_aws.is_s3_uri(ecco_source_root):
        # update login credentials:
        log.info('updating credentials...')
        try:
            subprocess.run(keygen,check=True)
        except subprocess.CalledProcessError as e:
            log.error(e)
            sys.exit(1)
        log.info('...done')
        # set session defaults:
        boto3.setup_default_session(profile_name=profile)
        # get bucket object list:
        s3r = boto3.resource('s3')
        s3_parts = urllib.parse.urlparse(ecco_source_root)
        bucket_name = s3_parts.netloc
        log.info("getting contents of bucket '%s'...", bucket_name)
        bucket = s3r.Bucket(bucket_name)
        files_in_bucket = list(bucket.objects.all())
        log.info('...done')
    elif not os.path.exists(ecco_source_root):
        raise ValueError(
            f"Nonexistent ecco_source_root directory location, '{ecco_source_root}'")

    # collect job groupings-related package metadata and organize into a
    # dictionary with primary keys, '1D', 'latlon', and 'native':

    dataset_groupings = ecco_metadata_store.ECCOMetadataStore(
        metadata_loc=ecco_metadata_loc,
        keygen=keygen, profile=profile).dataset_groupings

#    # previous package-based implementation:
#
#    log.info('collecting %s package resource metadata...', cfg['ecco_version'])
#    dataset_groupings = {}
#    traversible = importlib.resources.files(metadata)
#    with importlib.resources.as_file(traversible/cfg['ecco_version']) as files:
#        for file in files.glob('*groupings*'):
#            if re.search(r'_1D_',file.name,re.IGNORECASE):
#                log.debug('parsing 1D groupings metadata file %s', file)
#                with open(file) as f:
#                    dataset_groupings['1D'] = json.load(f)
#            elif re.search(r'_latlon_',file.name,re.IGNORECASE):
#                log.debug('parsing latlon groupings metadata file %s', file)
#                with open(file) as f:
#                    dataset_groupings['latlon'] = json.load(f)
#            elif re.search(r'_native_',file.name,re.IGNORECASE):
#                log.debug('parsing native groupings metadata file %s', file)
#                with open(file) as f:
#                    dataset_groupings['native'] = json.load(f)
#    log.debug('dataset grouping metadata:')
#    for key,list_of_dicts in dataset_groupings.items():
#        log.debug('%s:', key)
#        for i,dict_i in enumerate(list_of_dicts):
#            log.debug(' %d:', i)
#            for k,v in dict_i.items():
#                log.debug('  %s: %s', k, v)
#    log.info('...done collecting %s resource metadata.', cfg['ecco_version'])

    #
    # list of job descriptions to be built in two steps: first, gather list of
    # all available variable (*) input granules, second, step through the
    # collection and organize into list of output directories with 'input',
    # 'output', and 'metadata' keys.
    #
    # (*) - "variable" as used here is meant to imply a NetCDF file variable,
    # i.e., an ECCO dataset production output file component
    #

    task_list = []

    Job = collections.namedtuple(
        'Job',['metadata_groupings_id','product_type','frequency','time_steps'])


    with open(jobfile,'r') as fh:

        for line in fh:

            #
            # Step 1: accumulate and group all variable inputs (ECCO results
            # granules):
            #

            variable_inputs = {}

            job = Job(*ast.literal_eval(line))
            job_metadata = dataset_groupings[job.product_type][job.metadata_groupings_id]
            log.debug('job metadata for %s, %d: %s',
                job.product_type, job.metadata_groupings_id, job_metadata)

            # find all source files referenced by this job/job_metadata combination:

            if job.frequency.lower() == 'avg_day':
                path_freq_pat = 'diags_daily'
                file_freq_pat = 'day_mean'
            elif job.frequency.lower() == 'avg_mon':
                path_freq_pat = 'diags_monthly'
                file_freq_pat = 'mon_mean'
            elif job.frequency.lower() == 'snap':
                # TODO
                pass
            else:
                raise ValueError('job frequency must be one of avg_day, avg_mon, or snap')

            for variable in job_metadata['fields'].replace(' ','').split(','): # 'fields' string as iterable

                # collect list of available input files for the output variable.
                # accommodate two basic schemas: direct (one-to-one), and vector
                # component based (output based on many input components):

                one_to_one = True
                variable_input_components_keys = []
                variable_files = []

                if 'vector_inputs' in job_metadata:

                    # variable *may* depend on component inputs of differing
                    # types. if so, collect dependencies as dictionary keys for
                    # subsequent file list accumulation:

                    for k,v in job_metadata['vector_inputs'].items():
                        if variable in v:
                            variable_input_components_keys.append(k)
                            #input_fields_keys.append(k)
                    if variable_input_components_keys:
                        one_to_one = False

                if not one_to_one:

                    # variable depends on component inputs; determine
                    # availability of input files and gather accordingly:

                    all_variable_input_component_files = {}

                    for variable_input_component_key in variable_input_components_keys:

                        #variable_input_component_pat = variable_input_component_key
                        variable_input_component_files = []

                        if 'all' == job.time_steps.lower():
                            # get all possible time matches:
                            if aws.ecco_aws.is_s3_uri(ecco_source_root):
                                s3_key_pat = re.compile(
                                    s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                    + '.*'                      # allow anything between path and filename
                                    + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component_key,
                                        averaging_period=file_freq_pat).re_filestr)
                                variable_input_component_files.extend(
                                    [os.path.join(
                                        urllib.parse.urlunparse(
                                            (s3_parts.scheme,s3_parts.netloc,'','','','')),
                                        f.key)
                                        for f in files_in_bucket if re.match(s3_key_pat,f.key)])
                                    #[os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                    #    for f in files_in_bucket if re.match(file_pat,f.key)])
                            else:
                                file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                    prefix=variable_input_component_key,
                                    averaging_period=file_freq_pat).re_filestr)
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_input_component_files.extend(
                                            [os.path.join(dirpath,f)
                                                for f in filenames if re.match(file_pat,f)])
                        else:
                            # explicit list of time steps; one match per item:
                            time_steps_as_int_list = ast.literal_eval(job.time_steps)
                            for time in time_steps_as_int_list:
                                s3_key_pat = re.compile(
                                    s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                    + '.*'                      # allow anything between path and filename
                                    + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component_key,
                                        averaging_period=file_freq_pat,
                                        time=time).re_filestr)
                                if aws.ecco_aws.is_s3_uri(ecco_source_root):
                                    variable_input_component_files.append(
                                        [os.path.join(
                                            urllib.parse.urlunparse(
                                                (s3_parts.scheme,s3_parts.netloc,'','','','')),
                                            f.key)
                                            for f in files_in_bucket if re.match(s3_key_pat,f.key)])
                                        #[os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                        #    for f in files_in_bucket if re.match(file_pat,f.key)])
                                else:
                                    file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component_key,
                                        averaging_period=file_freq_pat,
                                        time=time).re_filestr)
                                    for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                        if cfg['ecco_version'] in dirpath:
                                            variable_input_component_files.append(
                                                [os.path.join(dirpath,f)
                                                    for f in filenames if re.match(file_pat,f)])

                        # group .data/.meta pairs for all specified/retrieved time
                        # steps:

                        variable_input_component_files.sort()
                        variable_input_component_files_as_list_of_lists = []
                        for f in variable_input_component_files:
                            if ecco_file.ECCOMDSFilestr(os.path.basename(f)).ext == 'data':
                                tmplist = [f]
                            else:   # '.meta'
                                tmplist.append(f)
                                variable_input_component_files_as_list_of_lists.append(tmplist)
                        variable_input_component_files = variable_input_component_files_as_list_of_lists

                        # and add to dictionary:

                        all_variable_input_component_files[variable_input_component_key] = \
                            variable_input_component_files

                    # group variable input component files by time value (i.e.,
                    # across keys), collect in time-based input lists that
                    # consist of just those variables for which all components
                    # are available:

                    # the following can probably be optimised:

                    # get list of times common across all variable input components:
                    times = None
                    for _,v in all_variable_input_component_files.items():
                        if not times:
                            # init:
                            times = {
                                ecco_file.ECCOMDSFilestr(os.path.basename(file_list[0])).time
                                for file_list in v}   # file_list[0] -> just use first element of .data/.meta pair
                        else:
                            # compare:
                            times = times & {
                                ecco_file.ECCOMDSFilestr(os.path.basename(file_list[0])).time
                                for file_list in v}   # file_list[0] -> just use first element of .data/.meta pair
                    times = list(times)
                    times.sort()
                    # reduce, and group by time:
                    for time in times:
                        tmp = []
                        for _,v in all_variable_input_component_files.items():
                            tmp.append(next(file_list for file_list in v if
                                time==ecco_file.ECCOMDSFilestr(os.path.basename(file_list[0])).time))
                            # as above, file_list[0] -> just use first element of .data/.meta pair
                        variable_files.append(tmp)
                    #dbg:
                    #print(' ')
                    #print(f'variable: {variable}')
                    #print(f'variable_files: {variable_files}')
                    #end.

                else:

                    # variable depends on a single MDS input pair (.data/.meta),
                    # of same type. for data organization purposes, arrange as
                    # list of lists (a variable's single input MDS pair is
                    # contained in a list, and a list of such lists comprises
                    # all selected/retrieved times).

                    variable_files = []
                    if 'all' == job.time_steps.lower():
                        # get all possible time matches:
                        if aws.ecco_aws.is_s3_uri(ecco_source_root):
                            s3_key_pat = re.compile(
                                s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                + '.*'                      # allow anything between path and filename
                                + ecco_file.ECCOMDSFilestr(
                                    prefix=variable,
                                    averaging_period=file_freq_pat).re_filestr)
                            variable_files.extend(
                                [os.path.join(
                                    urllib.parse.urlunparse(
                                        (s3_parts.scheme,s3_parts.netloc,'','','','')),
                                    f.key)
                                    for f in files_in_bucket if re.match(s3_key_pat,f.key)])
                        else:
                            file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                prefix=variable,
                                averaging_period=file_freq_pat).re_filestr)
                            for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                if cfg['ecco_version'] in dirpath:
                                    variable_files.extend(
                                        [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                    else:
                        # explicit list of time steps; one match per item:
                        time_steps_as_int_list = ast.literal_eval(job.time_steps)
                        for time in time_steps_as_int_list:
                            s3_key_pat = re.compile(
                                s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                + '.*'                      # allow anything between path and filename
                                + ecco_file.ECCOMDSFilestr(
                                    prefix=variable,
                                    averaging_period=file_freq_pat,
                                    time=time).re_filestr)
                            if aws.ecco_aws.is_s3_uri(ecco_source_root):
                                variable_files.append(
                                    [os.path.join(
                                        urllib.parse.urlunparse(
                                            (s3_parts.scheme,s3_parts.netloc,'','','','')),
                                        f.key)
                                        for f in files_in_bucket if re.match(s3_key_pat,f.key)])
                            else:
                                file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                    prefix=variable,averaging_period=file_freq_pat,time=time).re_filestr)
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_files.append(
                                            [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])

                    # group .data/.meta pairs for all specified/retrieved time
                    # steps:

                    variable_files.sort()
                    variable_files_as_list_of_lists = []
                    for f in variable_files:
                        if ecco_file.ECCOMDSFilestr(os.path.basename(f)).ext == 'data':
                            tmplist = [f]
                        else:   # '.meta'
                            tmplist.append(f)
                            variable_files_as_list_of_lists.append([tmplist])
                            # note that above appends as list of single list for
                            # "symmetry" with component input (i.e., not
                            # one_to_one) above

                    variable_files = variable_files_as_list_of_lists

                # save list of variable file lists in time-keyed dictionaries
                # for gather operations in Step 2:

                variable_files_as_time_keyed_dict = {}
                for file_list in variable_files:
                    #dbg:
                    #print(' ')
                    #print(f'file_list: {file_list}')
                    #print(f'file_list[0][0]: {file_list[0][0]}')
                    #print(ecco_file.ECCOMDSFilestr(os.path.basename(file_list[0][0])).time)
                    #end.
                    variable_files_as_time_keyed_dict[ecco_file.ECCOMDSFilestr(
                        os.path.basename(file_list[0][0])).time] = file_list
                        #os.path.basename(file_list[0])).time] = file_list

                variable_inputs[variable] = variable_files_as_time_keyed_dict

            # finally, does the job metadata specify any variable renames?:

            if 'variable_rename' in job_metadata.keys():
                old_varname, new_varname = job_metadata['variable_rename'].split(':')
                variable_inputs[new_varname] = variable_inputs.pop(old_varname)

#           json.dump(variable_inputs,sys.stdout,indent=4)

            #
            # Step 2: walk through collection of variable inputs and organize
            # into list of directories describing individual tasks. Note that
            # approach allows for output even if not all variables exist for a
            # given time.
            #

            # get list of times for which any of the variables exist:
            all_times = set()
            for k,v in variable_inputs.items():
                all_times = all_times | set(v.keys())
            all_times = list(all_times)
            all_times.sort()

            for time in all_times:
                # TODO: when finalized, replace 'task={}' with 'task =
                # ECCOTask()'; subsequent operations using class functions.
                task = {}

                tb,center_time = ecco_time.make_time_bounds_metadata(
                    granule_time=time,
                    model_start_time=cfg['model_start_time'],
                    model_end_time=cfg['model_end_time'],
                    model_timestep=cfg['model_timestep'],
                    model_timestep_units=cfg['model_timestep_units'],
                    averaging_period=job.frequency)

                if file_freq_pat == 'mon_mean':
                    # in the case of monthly means, ensure file date stamp is
                    # correct (tb[1] sometimes places end date at start of
                    # subsequent month, e.g., tb = [1992-01,1992-02] for a
                    # 1992-01 monthly average)
                    file_date_stamp = center_time
                else:
                    file_date_stamp = tb[1]

                output_filename = ecco_file.ECCOGranuleFilestr(
                    prefix=job_metadata['filename'],
                    averaging_period=file_freq_pat,
                    date=pd.Timestamp(file_date_stamp).strftime("%Y-%m-%dT%H:%M:%S"),
                    #date=pd.Timestamp(tb[1]).strftime("%Y-%m-%dT%H:%M:%S"),
                    version=cfg['ecco_version'],
                    grid_type=job.product_type,
                    grid_label=cfg['ecco_production_filestr_grid_label'][job.product_type],
                ).filestr

                task['granule'] = os.path.join(ecco_destination_root,output_filename)
                task_variables = {}
                for variable_name,variable_file_list in variable_inputs.items():
                    task_variables[variable_name] = variable_file_list[time]
                task['variables'] = task_variables
                task['ecco_grid_loc'] = ecco_grid_loc
                task['ecco_mapping_factors_loc'] = ecco_mapping_factors_loc
                task['ecco_metadata_loc'] = ecco_metadata_loc
                # dynamic metadata:
                task['metadata'] = {
                    'name':job_metadata['name'],
                    'dimension':job_metadata['dimension'],
                    'time_coverage_start': pd.Timestamp(tb[0]).strftime("%Y-%m-%dT%H:%M:%S"),
                    'time_coverage_end': pd.Timestamp(tb[1]).strftime("%Y-%m-%dT%H:%M:%S"),
                    'time_coverage_center': pd.Timestamp(center_time).strftime("%Y-%m-%dT%H:%M:%S")
                    #TODO:
                    #'time_coverage_duration'
                    #'time_coverage_resolution'
                }
                task_list.append(task)

    return task_list


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    task_list = create_job_task_list(
        jobfile=args.jobfile,
        ecco_source_root=args.ecco_source_root,
        ecco_destination_root=args.ecco_destination_root,
        ecco_grid_loc=args.ecco_grid_loc,
        ecco_mapping_factors_loc=args.ecco_mapping_factors_loc,
        ecco_metadata_loc=args.ecco_metadata_loc,
        cfgfile=args.cfgfile,
        keygen=args.keygen, profile=args.profile,
        log_level=args.log_level)

    if args.outfile:
        fp = open(args.outfile,'w')
    else:
        fp = sys.stdout
    json.dump(task_list,fp,indent=4)
    fp.close()

