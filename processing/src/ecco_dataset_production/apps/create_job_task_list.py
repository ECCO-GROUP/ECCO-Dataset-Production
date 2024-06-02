#!/usr/bin/env python3

import argparse
import ast
import boto3
import collections
import glob
import importlib.resources
import json
import logging
import os
import re
import subprocess
import sys
import urllib

from .. import configuration
from .. import ecco_file
from .. import metadata


logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')


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

    # parser.add_argument('--

    parser.add_argument('--cfgfile', default='./product_generation_config.yaml',
        help="""(Path and) filename of ECCO Dataset Production configuration
        file (default: '%(default)s')""")
    parser.add_argument('--outfile', help="""
        Resulting job task output file (json format) (default: stdout)""")
    parser.add_argument('--keygen', help="""
        If ecco_source_root references an S3 bucket and if running in JPL
        domain, federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--profile_name', help="""
        If ecco_source_root references an S3 bucket and if running in JPL
        domain, AWS credential profile name (e.g., 'saml-pub', 'default',
        etc.)""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")

    return parser


#
# common utilities, at some point:
# 

def is_s3_uri(path_or_uri_str):
    """Determines whether or not input string is an AWS S3Uri.

    Args:
        path_or_uri_str (str): Input string.

    Returns:
        True if string matches 's3://', False otherwise.
    """ 
    if re.search( r's3:\/\/', path_or_uri_str, re.IGNORECASE):
        return True
    else:
        return False


#def time_str(granule_filename):
#    """Get ten digit time string from ECCO granule filename.
#
#    Args:
#        granule_filename (str): Name of ECCO granule file (e.g., either
#            SSH_mon_mean.0000000732.data or SSH_mon_mean.0000000732.meta)
#
#    Returns:
#        Ten digit time string (e.g., '0000000732')
#    """
#    m = re.search('\d{10}',granule_filename)
#    if m:
#        return m.group()
#    else:
#        return None


def create_job_task_list(
    jobfile=None, ecco_source_root=None, ecco_destination_root=None,
    cfgfile=None, keygen=None, profile_name=None, log_level=None):
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
            bucket or folder (s3://...)
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        keygen (str): If ecco_source_root If ecco_source_root references an S3
            bucket and if running in JPL domain, federated login key generation
            script (e.g., /usr/local/bin/aws-login-pub.darwin.amd64).
            (default: None)
        profile_name (str): If ecco_source_root references an AWS S3 bucket and
            if running in JPL domain, AWS credential profile name (e.g.,
            'saml-pub', 'default', etc.)
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        List of resulting job tasks, each as a dictionary with 'input',
        'output', and 'metadata' keys.

    Raises:
        RuntimeError: If ecco_source_root or ecco_destination_root are not
            provided.
        ValueError: If ecco_source_root type cannot be determined or if jobfile
            contains format errors.

    Notes:
        . ECCO version string (e.g., "V4r5") is assumed to be contained in
        either directory path or in AWS S3 object names.

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

    if is_s3_uri(ecco_source_root):
        # update login credentials:
        log.info('updating credentials...')
        try:
            subprocess.run(keygen,check=True)
        except subprocess.CalledProcessError as e:
            log.error(e)
            sys.exit(1)
        log.info('...done')
        # set session defaults:
        boto3.setup_default_session(profile_name=profile_name)
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

    log.info('collecting %s package resource metadata...', cfg['ecco_version'])
    dataset_groupings = {}
    traversible = importlib.resources.files(metadata)
    with importlib.resources.as_file(traversible/cfg['ecco_version']) as files:
        for file in files.glob('*groupings*'):
            if re.search(r'_1D_',file.name,re.IGNORECASE):
                log.debug('parsing 1D groupings metadata file %s', file)
                with open(file) as f:
                    dataset_groupings['1D'] = json.load(f)
            elif re.search(r'_latlon_',file.name,re.IGNORECASE):
                log.debug('parsing latlon groupings metadata file %s', file)
                with open(file) as f:
                    dataset_groupings['latlon'] = json.load(f)
            elif re.search(r'_native_',file.name,re.IGNORECASE):
                log.debug('parsing native groupings metadata file %s', file)
                with open(file) as f:
                    dataset_groupings['native'] = json.load(f)
    log.debug('dataset grouping metadata:')
    for key,list_of_dicts in dataset_groupings.items():
        log.debug('%s:', key)
        for i,dict_i in enumerate(list_of_dicts):
            log.debug(' %d:', i)
            for k,v in dict_i.items():
                log.debug('  %s: %s', k, v)
    log.info('...done collecting %s resource metadata.', cfg['ecco_version'])

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
                raise ValueError('job frequency must be one of AVG_DAY, AVG_MON, or SNAP')

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
                            file_pat = re.compile( r'.*' + ecco_file.ECCOFilestr(
                                    varname=variable_input_component_key,averaging_period=file_freq_pat).filestr)
                            #time_pat = '.*'
                            #file_pat = re.compile(
                            #    '.*' + variable_input_component_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                            if is_s3_uri(ecco_source_root):
                                variable_input_component_files.extend(
                                    [os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                        for f in files_in_bucket if re.match(file_pat,f.key)])
                            else:
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_input_component_files.extend(
                                            [os.path.join(dirpath,f)
                                                for f in filenames if re.match(file_pat,f)])
                        else:
                            # explicit list of time steps; one match per item:
                            time_steps_as_int_list = ast.literal_eval(job.time_steps)
                            for time in time_steps_as_int_list:
                                file_pat = re.compile( r'.*' + ecco_file.ECCOFilestr(
                                    varname=variable_input_component_key,averaging_period=file_freq_pat,time=time).filestr)
                                #time_pat = "{0:0>10d}".format(int(time))
                                #file_pat = re.compile(
                                #    '.*' + variable_input_component_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                                if is_s3_uri(ecco_source_root):
                                    variable_input_component_files.append(
                                        [os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                            for f in files_in_bucket if re.match(file_pat,f.key)])
                                else:
                                    for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                        if cfg['ecco_version'] in dirpath:
                                            variable_input_component_files.append(
                                                [os.path.join(dirpath,f)
                                                    for f in filenames if re.match(file_pat,f)])
                        variable_input_component_files.sort()
                        all_variable_input_component_files[variable_input_component_key] = variable_input_component_files

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
                            times = {ecco_file.ECCOFilestr(os.path.basename(filename)).time for filename in v}
                            #times = {time_str(filename) for filename in v}
                        else:
                            # compare:
                            times = times & {ecco_file.ECCOFilestr(os.path.basename(filename)).time for filename in v}
                            #times = times & {time_str(filename) for filename in v}
                    times = list(times)
                    times.sort()
                    # reduce, and group by time:
                    for time in times:
                        tmp = []
                        for _,v in all_variable_input_component_files.items():
                            tmp.append(next(file for file in v if
                                time==ecco_file.ECCOFilestr(os.path.basename(file)).time))
                            #tmp.append(next(file for file in v if time==time_str(file)))
                        variable_files.append(tmp)

                else:

                    # variable depends on a single input, of same type. for data
                    # organization purposes, arrange as list of lists (a
                    # variable's single input is contained in a list, and a list
                    # of such lists comprises all selected times).

                    #variable_pat = variable
                    variable_files = []
                    if 'all' == job.time_steps.lower():
                        # get all possible time matches:
                        file_pat = re.compile( r'.*' + ecco_file.ECCOFilestr(
                            varname=variable, averaging_period=file_freq_pat).filestr)
                        #time_pat = '.*'
                        #file_pat = re.compile('.*' + variable_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                        if is_s3_uri(ecco_source_root):
                            variable_files.extend(
                                [os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                    for f in files_in_bucket if re.match(file_pat,f.key)])
                        else:
                            for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                if cfg['ecco_version'] in dirpath:
                                    variable_files.extend(
                                        [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])
                        variable_files.sort()
                        # arrange as list of single element lists:
                        variable_files = [[f] for f in variable_files]
                    else:
                        # explicit list of time steps; one match per item:
                        time_steps_as_int_list = ast.literal_eval(job.time_steps)
                        for time in time_steps_as_int_list:
                            file_pat = re.compile( r'.*' + ecco_file.ECCOFilestr(
                                varname=variable,averaging_period=file_freq_pat,time=time).filestr)
                            #time_pat = "{0:0>10d}".format(int(time))
                            #file_pat = re.compile(
                            #    '.*' + variable_pat + '_' + file_freq_pat + '\.' + time_pat + '\.data')
                            if is_s3_uri(ecco_source_root):
                                variable_files.append(
                                    [os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                        for f in files_in_bucket if re.match(file_pat,f.key)])
                            else:
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_files.append(
                                            [os.path.join(dirpath,f) for f in filenames if re.match(file_pat,f)])

                # save list of variable file lists in time-keyed dictionaries
                # for gather operations in Step 2:

                variable_files_as_time_keyed_dict = {}
                for file_list in variable_files:
                    variable_files_as_time_keyed_dict[ecco_file.ECCOFilestr(
                        os.path.basename(file_list[0])).time] = file_list
                    #variable_files_as_time_keyed_dict[time_str(file_list[0])] = file_list

                variable_inputs[variable] = variable_files_as_time_keyed_dict

            # finally, does the metadata specify any variable renames?:

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
                task = {}
                # common util perhaps at some point:
                output_filename =                   \
                    job_metadata['filename']        \
                    + '_' + file_freq_pat           \
                    + '_' + time                    \
                    + '_' + 'ECCO'                  \
                    + '_' + cfg['ecco_version']     \
                    + '_' + job_metadata['product'] \
                    + '_' + cfg['filename_tail_'+job_metadata['product']]
                task['output'] = os.path.join(ecco_destination_root,output_filename)
                task_inputs = {}
                for variable_name,variable_file_list in variable_inputs.items():
                    task_inputs[variable_name] = variable_file_list[time]
                task['input'] = task_inputs
                # metadata that hasn't been used here, and will be of later use:
                task['metadata'] = {
                    'name':job_metadata['name'],
                    'dimension':job_metadata['dimension']}
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
        cfgfile=args.cfgfile,
        keygen=args.keygen, profile_name=args.profile_name,
        log_level=args.log_level)

    if args.outfile:
        fp = open(args.outfile,'w')
    else:
        fp = sys.stdout
    json.dump(task_list,fp,indent=4)
    fp.close()


if __name__=='__main__':
    main()

