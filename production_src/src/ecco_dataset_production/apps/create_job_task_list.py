"""
"""
import argparse
import ast
import boto3
import collections
import importlib.resources
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import subprocess
import sys
import urllib

from .. import aws
from .. import configuration
from .. import ecco_file
from .. import ecco_metadata
from .. import ecco_time


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
        (Path and) filename of ECCO Dataset Production jobs simple text
        file.""")
    parser.add_argument('--ecco_source_root', help="""
        ECCO results unique root location, either directory path (e.g.,
        /ecco_nfs_1/shared/ECCOV4r5) or AWS S3 bucket
        (s3://ecco-model-granules/V4r4). 'Unique' implies that multiple versions
        of the same source files cannot be found from the root.""")
    parser.add_argument('--ecco_destination_root', help="""
        ECCO Dataset Production output root location, either directory path
        (e.g., ECCOV4r5_datasets) or AWS S3 bucket (s3://bucket_name).""")
    parser.add_argument('--ecco_grid_loc', help="""
        Directory containing ECCO grid files (XC.*, YC.*, *latlon*.nc,
        *native*.nc, etc., as well as the file available_diagnostics.log), or
        ECCO grid zipfile, or similar remote location given by AWS S3
        bucket/prefix.""")
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
    parser.add_argument('--ecco_cfg_loc', help="""
        (Path and) filename of ECCO Dataset Production configuration file (yaml
        format), or similar remote location given by AWS S3
        bucket/prefix/filename.""")
    parser.add_argument('--outfile', help="""
        Resulting job task output file (json format) (default: stdout).""")
    parser.add_argument('--keygen', help="""
        If ecco_source_root references an S3 bucket and if running in an
        institutionally-managed AWS IAM Identity Center (SSO) environment
        domain, federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64).""")
    parser.add_argument('--profile', help="""
        If ecco_source_root references an S3 bucket and if running in an SSO
        environment, AWS credential profile name (e.g., 'saml-pub', 'default',
        etc.).""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s).""")
    return parser


def s3_list_files( s3_client, bucket, prefix):
    """A paginated approach to retrieving AWS S3 file lists.

    """
    file_list = []

    paginator = s3_client.get_paginator('list_objects_v2')

    # make sure prefix doesn't start with a '/' (api call will return 'OK' but
    # with null 'Contents', no messages issued), page through results and field
    # 'Key' values from objects in 'Contents':

    for page in paginator.paginate(Bucket=bucket,Prefix=prefix.lstrip('/')):
        for obj in page.get('Contents',[]):
            file_list.append(obj['Key'])
    return file_list


def create_job_task_list(
    jobfile=None, ecco_source_root=None, ecco_destination_root=None,
    ecco_grid_loc=None, ecco_mapping_factors_loc=None,
    ecco_metadata_loc=None, ecco_cfg_loc=None,
    keygen=None, profile=None, log_level=None):
    """Create a list of task inputs and outputs from an ECCO Dataset Production
    job file.

    Args:
        jobfile (str): (Path and) filename of ECCO Dataset Production jobs
            simple text file, each line containing a Python case insensitive
            list-style specifier of the form
            "[<metadata_groupings_id>,<product_type>,<frequency>,<time_steps>]"
            where metadata_groupings_id is an integer from 0 through N,
            product_type is one of '1D', 'latlon', or 'native', frequency is one
            of 'SNAP', 'AVG_MON', or 'AVG_DAY', and time_steps is either a list
            of integer time steps or 'all'.
        ecco_source_root (str): ECCO results root location, either directory
            path (e.g., /ecco_nfs_1/shared/ECCOV4r5) or AWS S3 bucket
            (s3://...).
        ecco_destination_root (str): ECCO Dataset Production output root
            location, either directory path (e.g., ECCOV4r5_datasets) or AWS S3
            bucket or folder (s3://...).
        ecco_grid_loc (str): Directory containing ECCO grid files (XC.*, YC.*,
            *latlon*.nc, *native*.nc, etc., as well as the file
            available_diagnostics.log), or ECCO grid zipfile, or similar remote
            location given by AWS S3 bucket/prefix.
        ecco_mapping_factors_loc (str): Directory containing ECCO mapping
            factors (3D, land_mask, latlon_grid, and sparse subdirectories), or
            similar remote location given by AWS S3 bucket/prefix.
        ecco_metadata_loc (str): Directory containing ECCO metadata *.json
            source files (*_groupings_for_{1D,latlon,native}_datasets.json,
            *_global_metadata_for_{all,native,latlon}_datasets.json, etc.), or
            similar remote location given by AWS S3 bucket/prefix.
        ecco_cfg_loc (str): (Path and) filename of ECCO Dataset Production
            configuration file (yaml format), or similar remote location given
            by AWS S3 bucket/prefix/filename.
        keygen (str): If ecco_source_root references an S3 bucket and if running
            in JPL domain, federated login key generation script (e.g.,
            /usr/local/bin/aws-login-pub.darwin.amd64).  (default: None)
        profile (str): If ecco_source_root references an AWS S3 bucket and
            if running in JPL domain, AWS credential profile name (e.g.,
            'saml-pub', 'default', etc.)
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        List of resulting job tasks, each as a dictionary with 'granule',
        'variables', 'ecco_cfg_loc', 'ecco_grid_loc',
        'ecco_mapping_factors_loc', 'ecco_metadata_loc' and 'dynamic_metadata'
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
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    # configuration initialization:

    log.info('initializing configuration parameters...')
    cfg = configuration.ECCODatasetProductionConfig(cfgfile=ecco_cfg_loc)

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
        if keygen:
            # running in SSO environment; update login credentials:
            log.info('updating credentials...')
            try:
                subprocess.run(keygen,check=True)
            except subprocess.CalledProcessError as e:
                log.error(e)
                sys.exit(1)
            log.info('...done')
        # set session defaults:
        boto3.setup_default_session(profile_name=profile)
        # much of the following is now performed on a job entry-by-entry basis, below
        ## get bucket object list:
        #s3r = boto3.resource('s3')
        s3_parts = urllib.parse.urlparse(ecco_source_root)
        #bucket_name = s3_parts.netloc
        #log.info("getting contents of bucket '%s'...", bucket_name)
        #bucket = s3r.Bucket(bucket_name)
        #files_in_bucket = list(bucket.objects.all())
        #log.info('...done')
        # and, instead, create an s3 client for later use instead:
        s3c = boto3.client('s3')
    elif not os.path.exists(ecco_source_root):
        raise ValueError(
            f"Nonexistent ecco_source_root directory location, '{ecco_source_root}'")

    # collect job groupings-related package metadata and organize into a
    # dictionary with primary keys, '1D', 'latlon', and 'native':

    dataset_groupings = ecco_metadata.ECCOMetadata(
        ecco_metadata_loc=ecco_metadata_loc,
        keygen=keygen, profile=profile).dataset_groupings

    #
    # list of job descriptions to be built in two steps: first, gather list of
    # all available variable (*) input granules, second, step through the
    # collection and organize into list of output directories with 'granule',
    # 'variables', etc. keys.
    #
    # (*) - "variable" is meant to imply a NetCDF file variable, i.e., an ECCO
    # dataset production output file component
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

            #
            # a few preliminaries ...
            #

            try:
                # assume job file entry is a Python list expression:
                job = Job(*ast.literal_eval(line))
            except:
                # failure to parse may have been intentional (e.g., leading
                # comment ('#'), blank lines, etc.), so just issue a
                # low-priority log:
                log.info('skipping jobfile entry "%s"', line.rstrip())
                continue

            job_metadata = dataset_groupings[job.product_type][job.metadata_groupings_id]
            log.debug('job metadata for %s, %d: %s',
                job.product_type, job.metadata_groupings_id, job_metadata)

            if job.frequency.lower() == 'avg_day':
                path_freq_pat = 'diags_daily'
                file_freq_pat = 'day_mean'
                time_long_name = 'center time of averaging period'
                time_coverage_duration = time_coverage_resolution = 'P1D'
                dataset_description_head = 'This dataset contains daily-averaged '
            elif job.frequency.lower() == 'avg_mon':
                path_freq_pat = 'diags_monthly'
                file_freq_pat = 'mon_mean'
                time_long_name = 'center time of averaging period'
                time_coverage_duration = time_coverage_resolution = 'P1M'
                dataset_description_head = 'This dataset contains monthly-averaged '
            elif job.frequency.lower() == 'snap':
                path_freq_pat = 'diags_inst'
                file_freq_pat = 'day_snap'
                time_long_name = 'snapshot time'
                time_coverage_duration = time_coverage_resolution = 'PT0S'
                dataset_description_head = 'This dataset contains instantaneous '
            else:
                raise ValueError("job frequency must be one of 'avg_day', 'avg_mon', or 'snap'")

            if job.product_type.lower() == '1d':
                dataset_description_tail = cfg['dataset_description_tail_1D']
            elif job.product_type.lower() == 'latlon':
                dataset_description_tail = cfg['dataset_description_tail_latlon']
            elif job.product_type.lower() == 'native':
                dataset_description_tail = cfg['dataset_description_tail_native']
            else:
                raise ValueError("job product type must be one of '1d', 'latlon', or 'native'")

            #
            # find all source files referenced by this job/job_metadata combination:
            #

            variable_inputs = {}

            for variable in job_metadata['fields'].replace(' ','').split(','):  # remove spaces, 'fields'
                                                                                # string as iterable
                # collect list of available input files for the output variable.
                # accommodate two basic schemas: direct (one-to-one), and vector
                # component based (output based on many input components):

                variable_files = []

                if 'field_components' in job_metadata.keys() and variable in job_metadata['field_components'].keys():

                    # variable depends on multiple component inputs; determine
                    # availability of input files and gather accordingly:

                    all_variable_input_component_files = {}

                    for variable_input_component in \
                        job_metadata['field_components'][variable].values(): # i.e., the "UVEL","VVEL", not "x" and "y"

                        variable_input_component_files = []

                        if aws.ecco_aws.is_s3_uri(ecco_source_root):
                            all_var_files_in_bucket = s3_list_files(
                                s3_client=s3c,
                                bucket=s3_parts.netloc,
                                prefix=os.path.join(
                                    s3_parts.path,
                                    path_freq_pat,
                                    '_'.join([variable_input_component,file_freq_pat])))

                        if isinstance(job.time_steps,str) and 'all'==job.time_steps.lower():
                            # get all possible time matches:
                            if aws.ecco_aws.is_s3_uri(ecco_source_root):

                                s3_variable_input_component_pat = re.compile(
                                    s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                    + '.*'                      # allow anything between path and filename
                                    + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component,
                                        averaging_period=file_freq_pat).re_filestr)

                                variable_input_component_files.extend(
                                    [os.path.join(
                                        urllib.parse.urlunparse(
                                            (s3_parts.scheme,s3_parts.netloc,'','','','')),f)
                                        for f in all_var_files_in_bucket if re.match(s3_variable_input_component_pat,f)])
                            else:
                                variable_input_component_file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                    prefix=variable_input_component,
                                    averaging_period=file_freq_pat).re_filestr)
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_input_component_files.extend(
                                            [os.path.join(dirpath,f)
                                                for f in filenames if re.match(variable_input_component_file_pat,f)])
                        else:
                            # assume explicit list of integer time steps; one match per item:
                            #time_steps_as_int_list = ast.literal_eval(job.time_steps)
                            for time in job.time_steps:
                            #for time in time_steps_as_int_list:
                                s3_variable_input_component_pat = re.compile(
                                    s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                    + '.*'                      # allow anything between path and filename
                                    + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component,
                                        averaging_period=file_freq_pat,
                                        time=time).re_filestr)
                                if aws.ecco_aws.is_s3_uri(ecco_source_root):
                                    variable_input_component_files.extend(
                                        [os.path.join(
                                            urllib.parse.urlunparse(
                                                (s3_parts.scheme,s3_parts.netloc,'','','','')),f)
                                            for f in all_var_files_in_bucket if re.match(s3_variable_input_component_pat,f)])
                                        #[os.path.join(urllib.parse.urlunparse(s3_parts),f.key)
                                        #    for f in files_in_bucket if re.match(file_pat,f.key)])
                                else:
                                    variable_input_component_file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                        prefix=variable_input_component,
                                        averaging_period=file_freq_pat,
                                        time=time).re_filestr)
                                    for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                        if cfg['ecco_version'] in dirpath:
                                            variable_input_component_files.extend(
                                                [os.path.join(dirpath,f)
                                                    for f in filenames if re.match(variable_input_component_file_pat,f)])

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

                        all_variable_input_component_files[variable_input_component] = \
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

                else:

                    # variable depends on a single MDS input pair (.data/.meta),
                    # of same type. for data organization purposes, arrange as
                    # list of lists (a variable's single input MDS pair is
                    # contained in a list, and a list of such lists comprises
                    # all selected/retrieved times).

                    variable_files = []

                    if aws.ecco_aws.is_s3_uri(ecco_source_root):
                        prefix=os.path.join(
                                s3_parts.path,
                                path_freq_pat,
                                '_'.join([variable,file_freq_pat]))
                        all_var_files_in_bucket = s3_list_files(
                            s3_client=s3c,
                            bucket=s3_parts.netloc,
                            prefix=prefix)


                    if isinstance(job.time_steps,str) and 'all'==job.time_steps.lower():
                    #if 'all' == job.time_steps.lower():
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
                                        (s3_parts.scheme,s3_parts.netloc,'','','','')),f)
                                    for f in all_var_files_in_bucket if re.match(s3_key_pat,f)])
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
                        #time_steps_as_int_list = ast.literal_eval(job.time_steps)
                        for time in job.time_steps:
                        #for time in time_steps_as_int_list:
                            s3_key_pat = re.compile(
                                s3_parts.path.strip('/')    # remove leading '/' from urlpath
                                + '.*'                      # allow anything between path and filename
                                + ecco_file.ECCOMDSFilestr(
                                    prefix=variable,
                                    averaging_period=file_freq_pat,
                                    time=time).re_filestr)
                            if aws.ecco_aws.is_s3_uri(ecco_source_root):
                                variable_files.extend(
                                    [os.path.join(
                                        urllib.parse.urlunparse(
                                            (s3_parts.scheme,s3_parts.netloc,'','','','')),f)
                                        for f in all_var_files_in_bucket if re.match(s3_key_pat,f)])
                            else:
                                file_pat = re.compile( r'.*' + ecco_file.ECCOMDSFilestr(
                                    prefix=variable,averaging_period=file_freq_pat,time=time).re_filestr)
                                for dirpath,dirnames,filenames in os.walk(ecco_source_root):
                                    if cfg['ecco_version'] in dirpath:
                                        variable_files.extend(
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
                            # "symmetry" with field_components schema above

                    variable_files = variable_files_as_list_of_lists

                # save list of variable file lists in time-keyed dictionaries
                # for gather operations in Step 2:

                variable_files_as_time_keyed_dict = {}
                for file_list in variable_files:
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

            if not all_times:
                log.warning('No existing input variables/times for %s; skipping.',
                    job_metadata['filename'])
                # nothing to write to task list; continue with next jobfile entry
                continue

            for time in all_times:
                # TODO: when finalized, replace 'task={}' with 'task =
                # ECCOTask()'; subsequent operations using class functions.
                task = {}

                model_start_time=cfg['model_start_time']
                model_end_time=cfg['model_end_time']
                model_timestep=cfg['model_timestep']
                model_timestep_units=cfg['model_timestep_units']

                if 'snap' in job.frequency.lower():
                    mst = np.datetime64(model_start_time)
                    td64 = np.timedelta64(int(time)*model_timestep ,model_timestep_units)
                    center_time = \
                        mst + \
                        td64
                    tb = []
                    tb.append(center_time)
                    tb.append(center_time)

                else:
                    tb,center_time = ecco_time.make_time_bounds_metadata(
                        granule_time=time,
                        model_start_time=model_start_time,
                        model_end_time=model_end_time,
                        model_timestep=model_timestep,
                        model_timestep_units=model_timestep_units,
                        averaging_period=job.frequency)

                if file_freq_pat == 'mon_mean':
                    # in the case of monthly means, ensure file date stamp is
                    # correct (tb[1] sometimes places end date at start of
                    # subsequent month, e.g., tb = [1992-01,1992-02] for a
                    # 1992-01 monthly average)
                    file_date_stamp = center_time
                else:
                    file_date_stamp = tb[1]

                # in the future, the input file frequency pattern will just be
                # 'snap' instead of 'day_snap'. In the meantime, make sure
                # output files adhere to the future standard:
                if re.match('.*snap',file_freq_pat):
                    _file_freq_pat = 'snap'
                else:
                    _file_freq_pat = file_freq_pat

                output_filename = ecco_file.ECCOGranuleFilestr(
                    prefix=job_metadata['filename'],
                    averaging_period=_file_freq_pat,    # see above test
                    date=pd.Timestamp(file_date_stamp).strftime("%Y-%m-%dT%H:%M:%S"),
                    #date=pd.Timestamp(tb[1]).strftime("%Y-%m-%dT%H:%M:%S"),
                    version=cfg['ecco_version'],
                    grid_type=job.product_type,
                    grid_label=cfg['ecco_production_filestr_grid_label'][job.product_type],
                ).filestr

                task['granule'] = os.path.join(
                    ecco_destination_root,
                    cfg['ecco_version'],
                    job.product_type.lower(),
                    _file_freq_pat,                     # see above test/assignment
                    job_metadata['filename'],
                    output_filename)
                #task['granule'] = os.path.join(ecco_destination_root,output_filename)
                task_variables = {}
                for variable_name,variable_file_list in variable_inputs.items():
                    try:
                        task_variables[variable_name] = variable_file_list[time]
                    except:
                        log.warning("Granule %s: missing input detected for variable '%s' at time step '%s'.",
                            task['granule'], variable_name, time)
                task['variables'] = task_variables
                task['ecco_cfg_loc'] = ecco_cfg_loc
                task['ecco_grid_loc'] = ecco_grid_loc
                task['ecco_mapping_factors_loc'] = ecco_mapping_factors_loc
                task['ecco_metadata_loc'] = ecco_metadata_loc
                task['dynamic_metadata'] = {
                    'name':job_metadata['name'],
                    'dimension':job_metadata['dimension'],
                    'time_coverage_start': pd.Timestamp(tb[0]).strftime("%Y-%m-%dT%H:%M:%S"),
                    'time_coverage_end': pd.Timestamp(tb[1]).strftime("%Y-%m-%dT%H:%M:%S"),
                    'time_coverage_center': pd.Timestamp(center_time).strftime("%Y-%m-%dT%H:%M:%S")
                }
                task['dynamic_metadata']['time_long_name']          = time_long_name
                task['dynamic_metadata']['time_coverage_duration']  = time_coverage_duration
                task['dynamic_metadata']['time_coverage_resolution']= time_coverage_resolution

                # optional metadata that may or may not exist:
                try:
                    task['dynamic_metadata']['comment'] = job_metadata['comment']
                except:
                    pass
                try:
                    task['dynamic_metadata']['field_components'] = job_metadata['field_components']
                except:
                    pass
                try:
                    task['dynamic_metadata']['field_orientations'] = job_metadata['field_orientations']
                except:
                    pass

                #if 'mean' in file_freq_pat:
                #    task['dynamic_metadata']['time_long_name'] = 'center time of averaging period'
                #    if 'day' in file_freq_pat:
                #        task['dynamic_metadata']['time_coverage_duration']  = 'P1D'
                #        task['dynamic_metadata']['time_coverage_resolution']= 'P1D'
                #    elif 'mon' in file_freq_pat:
                #        task['dynamic_metadata']['time_coverage_duration']  = 'P1M'
                #        task['dynamic_metadata']['time_coverage_resolution']= 'P1M'
                #else:
                #    task['dynamic_metadata']['time_long_name'] = 'snapshot time'
                #    task['dynamic_metadata']['time_coverage_duration']  = 'P0S'
                #    task['dynamic_metadata']['time_coverage_resolution']= 'P0S'

                task['dynamic_metadata']['summary'] = ' '.join(
                    [dataset_description_head, job_metadata['name'], dataset_description_tail])
                # remove (possible) redundant whitespace chars:
                task['dynamic_metadata']['summary'] = ' '.join(
                    task['dynamic_metadata']['summary'].split())

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
        ecco_cfg_loc=args.ecco_cfg_loc,
        keygen=args.keygen, profile=args.profile,
        log_level=args.log_level)

    if args.outfile:
        fp = open(args.outfile,'w')
    else:
        fp = sys.stdout
    json.dump(task_list,fp,indent=4)
    fp.close()

