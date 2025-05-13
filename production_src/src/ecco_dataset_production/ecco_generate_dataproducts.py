
from collections import defaultdict
import datetime
import fnmatch
import glob
import itertools
import json
import logging
import netCDF4
import numpy as np
import os
import tempfile
import pandas as pd
import uuid
import xarray as xr
import yaml

import ecco_v4_py

from . import aws
from . import ecco_dataset
from . import configuration
from . import ecco_file
from . import ecco_grid
from . import ecco_mapping_factors
from . import ecco_metadata
from . import ecco_podaac_metadata
from . import ecco_task


def ecco_make_granule( task, cfg,
    grid=None, mapping_factors=None, metadata=None, log_level=None, **kwargs):
    """Create PO.DAAC-ready ECCO granule per instructions provided in input task
    descriptor.

    Args:
        task (dict): Single task from parsed ECCO dataset production
            json-formatted task list.
        cfg (dict): Parsed ECCO dataset production yaml file.
        grid (obj): Instance of ECCOGrid class for current granule task.
        mapping_factors (obj): Instance of ECCOMappingFactors for current
            granule task.
        metadata (obj): Optional instance of ECCOMetadata for current granule task.
        log_level (str): Optional local logging level for the ecco_make_granule
            task ('DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL').  If called
            by a top-level application, the default will be that of the parent
            logger ('edp').
        **kwargs: Depending on run context:
            keygen (str): If tasklist descriptors reference AWS S3 endpoints and
                if running in an institutionally-managed AWS IAM Identity Center
                (SSO) environment, path and) name of federated login key
                generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64)
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        Indirectly, NetCDF4-formatted ECCO granule, named according to, and
        written to location specified by, input task 'granule' keyword.

    Raises:
        RuntimeError if indeterminate output granule type (i.e., not native or
        latlon).

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    # ECCOTask object to answer some basic questions:
    this_task = ecco_task.ECCOTask(task)

    variable_datasets = []

    with tempfile.TemporaryDirectory() as build_tmpdir: # (*)

        # (*) The reason for this particular construct, i.e., build_tmpdir at
        # the highest level, is that, although build_tmpdir is not explicitly
        # referenced after the calls to ecco_dataset.ECCOMDSDataset, the
        # resulting xarray Dataset is, however, memory-resident until written
        # (to_netcdf()). All operations prior, then, assume the persistence of
        # build_tmpdir, which can only go out of scope after write and
        # (possible) S3 upload are complete.

        if this_task.is_latlon:
            log.info('generating %s ...', os.path.basename(this_task['granule']))
            for variable in this_task.variable_names:
                log.debug('... adding %s using:', variable)
                for infile in itertools.chain.from_iterable(this_task.variable_inputs(variable)):
                    log.debug('    %s', infile)
                emdsds = ecco_dataset.ECCOMDSDataset(
                    task=this_task, variable=variable, grid=grid,
                    mapping_factors=mapping_factors, cfg=cfg, tmpdir=build_tmpdir,
                    **kwargs)
                emdsds.drop_all_variables_except(variable)
                variable_datasets.append(emdsds.as_latlon(variable))    # as_latlon returns xarray DataArray
            merged_variable_dataset = xr.merge(variable_datasets)

        elif this_task.is_native:
            log.info('generating %s ...', os.path.basename(this_task['granule']))
            for variable in this_task.variable_names:
                log.debug('... adding %s using:', variable)
                for infile in itertools.chain.from_iterable(this_task.variable_inputs(variable)):
                    log.debug('    %s', infile)
                emdsds = ecco_dataset.ECCOMDSDataset(
                    task=this_task, variable=variable, grid=grid,
                    mapping_factors=mapping_factors, cfg=cfg, tmpdir=build_tmpdir,
                    **kwargs)
                emdsds.drop_all_variables_except(variable)
                emdsds.apply_land_mask_to_native_variable(variable)
                variable_datasets.append(emdsds)
            merged_variable_dataset = xr.merge([ds.ds for ds in variable_datasets])

        else:
            raise RuntimeError('Could not determine output granule type (latlon or native)')

        # set miscellaneous granule attributes and properties:
        merged_variable_dataset_with_ancillary_data = set_granule_ancillary_data(
            dataset=merged_variable_dataset, task=this_task,
            grid=grid, mapping_factors=mapping_factors, cfg=cfg)

        # append metadata:
        merged_variable_dataset_with_all_metadata, encoding = set_granule_metadata(
            dataset=merged_variable_dataset_with_ancillary_data,
            task=this_task,
            ecco_metadata=metadata,
            cfg=cfg)

        # write:
        if this_task.is_granule_local:
            if os.path.dirname(this_task['granule']) and not os.path.exists(os.path.dirname(this_task['granule'])):
                os.makedirs(os.path.dirname(this_task['granule']))
            merged_variable_dataset_with_all_metadata.to_netcdf(
                this_task['granule'], encoding=encoding)
        else:
            with tempfile.TemporaryDirectory() as upload_tmpdir:
                # temporary directory will self-destruct at end of with block
                _src = os.path.basename(this_task['granule'])
                _dest = this_task['granule']
                merged_variable_dataset_with_all_metadata.to_netcdf(
                    os.path.join(upload_tmpdir,_src), encoding=encoding)
                log.info('uploading %s to %s', os.path.join(upload_tmpdir,_src), _dest)
                aws.ecco_aws_s3_cp.aws_s3_cp( src=os.path.join(upload_tmpdir,_src), dest=_dest, **kwargs)

    log.info('... done')


def set_granule_ancillary_data(
    dataset=None, task=None, grid=None, mapping_factors=None, cfg=None):
    """Collect, and set, global and ancillary data such as array precision, fill values,
    variable-level min/max values, time bounds, and additional coordinate data
    not already established during the process of Dataset granule creation.

    Args:
        dataset (xarray.Dataset): ECCO results data container to which
            additional ancillary data, and data conventions, are to be applied.
        task (obj): ECCOTask granule task descriptor providing
            'dynamic_metadata'.
        grid (obj): ECCOGrid instance providing, in the case of ECCO native
            granule generation, XC, YC, and Z bounds.
        mapping_factors (obj): ECCOMappingFactors instance providing, in the
            case of ECCO latlon granules, lat/lon/depth bounds.
        cfg (dict): Parsed ECCO dataset production yaml file.

    Returns:
        Input xarray.Datatset, with global ancillary data applied.

    """
    # ensure consistent variable (array) representation:
    prec = cfg['array_precision'] if 'array_precision' in cfg else 'float64'
    ncfill = netCDF4.default_fillvals['f4'] if prec=='float32' else netCDF4.default_fillvals['f8']
    for var in dataset.data_vars:
        dataset[var].values = dataset[var].astype(eval('np.'+prec))
        dataset[var].attrs['valid_min'] = np.nanmin(dataset[var].values)
        dataset[var].attrs['valid_max'] = np.nanmax(dataset[var].values)
        dataset[var].values = np.where(np.isnan(dataset[var].values),ncfill,dataset[var].values)

    # time coordinate bounds:
    if all( [k in task['dynamic_metadata'] for k in
        ('time_coverage_start','time_coverage_end','time_coverage_center')]):
        # original ported code that doesn't work (raises
        # "IndexError: index 0 is out of bounds for axis 0 with size 0"):
        #dataset['time_bnds'] = []
        #dataset.time_bnds.values[0][0] = np.datetime64(task['dynamic_metadata']['time_coverage_start'])
        #dataset.time_bnds.values[0][1] = np.datetime64(task['dynamic_metadata']['time_coverage_end'])
        #dataset['time'] = []
        #dataset.time.values[0] = np.datetime64(task['dynamic_metadata']['time_coverage_center'])
        # possible solution:
        dataset.coords['time_bnds'] = (
        #dataset['time_bnds'] = (
            ('time','nv'),
            [[pd.Timestamp(task['dynamic_metadata']['time_coverage_start']),
              pd.Timestamp(task['dynamic_metadata']['time_coverage_end'])]])
            #[[np.datetime64(task['dynamic_metadata']['time_coverage_start']),
            #  np.datetime64(task['dynamic_metadata']['time_coverage_end'])]])
        dataset['time'] = (
            ('time'),
            [pd.Timestamp(task['dynamic_metadata']['time_coverage_center'])])
            #[np.datetime64(task['dynamic_metadata']['time_coverage_center'])])

    # per PO.DAAC request, if present, remove 'timestep' non-dimension
    # coordinate (value would have come from time string portion of input
    # filename(s), e.g., '732', '1428', etc.):
    try:
        dataset = dataset.drop_vars('timestep')
    except:
        pass

    # spatial coordinate bounds:
    if task.is_latlon:
        # assign lat/lon/depth bounds using data from mapping factors:
        dataset = dataset.assign_coords(
            {'latitude_bnds':(('latitude','nv'), mapping_factors.latitude_bounds)})
        dataset = dataset.assign_coords(
            {'longitude_bnds':(('longitude','nv'), mapping_factors.longitude_bounds)})
        if task.is_3d:
            dataset = dataset.assign_coords(
                {'Z_bnds':(('Z','nv'),mapping_factors.depth_bounds)})
    else: # task.is_native
        XC_bnds = grid.native_grid['XC_bnds']
        YC_bnds = grid.native_grid['YC_bnds']
        if XC_bnds.chunks is not None:
            XC_bnds.load()
        if YC_bnds.chunks is not None:
            YC_bnds.load()
        dataset = dataset.assign_coords(
            {"XC_bnds": (("tile","j","i","nb"), XC_bnds.data)})
            #{"XC_bnds": (("tile","j","i","nb"), grid.native_grid['XC_bnds'].data)})
        dataset = dataset.assign_coords(
            {"YC_bnds": (("tile","j","i","nb"), YC_bnds.data)})
            #{"YC_bnds": (("tile","j","i","nb"), grid.native_grid['YC_bnds'].data)})
        if task.is_3d:
            Z_bnds = grid.native_grid['Z_bnds']
            if Z_bnds.chunks is not None:
                Z_bnds.load()
            dataset = dataset.assign_coords(
                {"Z_bnds": (('k','nv'), Z_bnds.data)})
                #{'Z_bnds':(('k','nv'),mapping_factors.depth_bounds)})
    return dataset


def set_granule_metadata( dataset=None, task=None, ecco_metadata=None, cfg=None, **kwargs):
    """Primary routine for aggregrating and setting mtadata collected from all
    sources, e.g., ECCO configuration data, task list references, etc.  Note
    that set_granule_metadata operations depend in large part on functionality
    provided by ecco_v4_py.ecco_utils.

    Args:
        dataset (xarray.Dataset): ECCO results data container to which metadata
            are to be applied.
        task (obj): ECCOTask granule task descriptor providing
            'dynamic_metadata', 'ecco_metadata_loc' definitions.
        ecco_metadata (obj): Optional ECCOMetadata class instance. If not
            provided, instance will be locally instantiated using
            task['ecco_metadata_loc'] descriptor.
        cfg (dict): Parsed ECCO dataset production configuration file.
        **kwargs: Depending on run context:
            keygen (str): If ecco_metadata is not provided and task object key
                'ecco_metadata_loc' references an AWS S3 endpoint and if running
                in an institutionally-managed AWS IAM Identity Center (SSO)
                environment, (path and) name of federated login key generation
                script (e.g., /usr/local/bin/aws-login-pub.darwin.amd64)
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        Input xarray.Datatset, with all metadata applied.

    """
    log = logging.getLogger('edp.'+__name__)

    # point to ecco metadata, using whatever form provided:
    if not ecco_metadata:
        ecco_metadata_source = ecco_metadata.ECCOMetadata( task, **kwargs)
    else:
        ecco_metadata_source = ecco_metadata

    expected_metadata_source_identifiers = {
        'coord_1D':         ['coordinate_metadata_for_1D_datasets'],
        'coord_latlon':     ['coordinate_metadata_for_latlon_datasets'],
        'coord_native':     ['coordinate_metadata_for_native_datasets'],
        'coord_time':       ['time_coordinate_metadata'],
        'geometry_latlon':  ['geometry_metadata_for_latlon_datasets'],
        'geometry_native':  ['geometry_metadata_for_native_datasets'],
        'global_all':       ['global_metadata_for_all_datasets'],
        'global_latlon':    ['global_metadata_for_latlon_datasets'],
        'global_native':    ['global_metadata_for_native_datasets'],
        'groupings_1D':     ['groupings_for_1D_datasets'],
        'groupings_latlon': ['groupings_for_latlon_datasets'],
        'groupings_native': ['groupings_for_native_datasets'],
        'var_latlon':       ['variable_metadata_for_latlon_datasets'],
        'var_native':       ['variable_metadata','geometry_metadata_for_native_datasets']}

    all_metadata = defaultdict(list)
    for file in glob.glob(os.path.join(ecco_metadata_source.metadata_dir,'*.json')):
        for key,identifiers in expected_metadata_source_identifiers.items():
            if  any([fnmatch.fnmatch(file,match_string)
                    for match_string in ['*'+id+'.*' for id in identifiers]]):
                all_metadata[key].extend(json.load(open(file)))

    # variable-specific metadata:
    dataset, grouping_gcmd_keywords = ecco_v4_py.ecco_utils.add_variable_metadata(
        all_metadata['var_native'], dataset)
    if task.is_latlon:
        dataset, grouping_gcmd_keywords = ecco_v4_py.ecco_utils.add_variable_metadata(
            all_metadata['var_latlon'], dataset)

    # coordinate metadata:
    if task.is_latlon:
        dataset = ecco_v4_py.ecco_utils.add_coordinate_metadata(
            all_metadata['coord_latlon'],dataset)
    elif task.is_native:
        dataset = ecco_v4_py.ecco_utils.add_coordinate_metadata(
            all_metadata['coord_native'],dataset)

    # global metadata:
    dataset = ecco_v4_py.ecco_utils.add_global_metadata(
        all_metadata['global_all'], dataset, task['dynamic_metadata']['dimension'])
    if task.is_latlon:
        dataset = ecco_v4_py.ecco_utils.add_global_metadata(
            all_metadata['global_latlon'], dataset, task['dynamic_metadata']['dimension'])
        pass # tmp!!
    elif task.is_native:
        dataset = ecco_v4_py.ecco_utils.add_global_metadata(
            all_metadata['global_native'], dataset, task['dynamic_metadata']['dimension'])

    # time metadata:
    if 'time' in dataset.coords:
        dataset = ecco_v4_py.ecco_utils.add_coordinate_metadata(
            all_metadata['coord_time'], dataset)

    # global time and date-associated metadata:
    if 'mean' in task.averaging_period:
        dataset.attrs['time_coverage_start']= task['dynamic_metadata']['time_coverage_start']
        dataset.attrs['time_coverage_end']  = task['dynamic_metadata']['time_coverage_end']
    #TODO:
    #else:
    #    G.attrs['time_coverage_start'] = str(G.time.values[0])[0:19]
    #    G.attrs['time_coverage_end'] = str(G.time.values[0])[0:19]

    # production date/time ('YYYY-MM-DDThh:mm:ss', i.e., minus decimal seconds)
    current_time = datetime.datetime.now().isoformat().split('.')[0]
    dataset.attrs['date_created']           = current_time
    dataset.attrs['date_modified']          = current_time
    dataset.attrs['date_metadata_modified'] = current_time
    dataset.attrs['date_issued']            = current_time

    # some miscellaneous attributes sourced directly from cfg data:
    dataset.attrs['history']                    = cfg['history']
    dataset.attrs['geospatial_vertical_min']    = cfg['geospatial_vertical_min']
    dataset.attrs['product_time_coverage_start']= cfg['model_start_time']
    dataset.attrs['product_time_coverage_end']  = cfg['model_end_time']
    dataset.attrs['product_version']            = cfg['product_version']
    dataset.attrs['references']                 = cfg['references']
    dataset.attrs['source']                     = cfg['source']
    dataset.attrs['summary']                    = cfg['summary']    # more later...

    # remove upstream attribute assignments that aren't necessary/desirable:
    dataset.attrs.pop('original_mds_grid_dir',None) # assigned in ecco_v4_py.read_bin_llc
    dataset.attrs.pop('original_mds_var_dir',None)  # "                                 "

    # variable-specific encodings:
    var_encoding = {}
    prec = cfg['array_precision'] if 'array_precision' in cfg else 'float64'
    fill_value = netCDF4.default_fillvals['f4'] if prec=='float32' else netCDF4.default_fillvals['f8']
    for var in list(dataset.data_vars):
        var_encoding[var] = cfg['netcdf4_compression_encodings']
        var_encoding[var]['_FillValue'] = fill_value
        # per PO.DAAC request (above), overwrite default coordinates encoding
        # attribute based on key order in dataset[var].coords:
        dataset[var].encoding['coordinates'] = ' '.join(
            [c for c in list(dataset[var].coords) if c in cfg['variable_coordinates_as_encoded_attributes']])

    # specific coordinate datatype encodings:
    coord_encoding = {}
    for coord in dataset.coords:
        # default:
        coord_encoding[coord] = {'_FillValue':None, 'dtype':'float32'}
        if dataset[coord].values.dtype==np.int32 or dataset[coord].values.dtype==np.int64:
            coord_encoding[coord]['dtype'] = 'int32'
        if coord=='time' or coord=='time_bnds':
            coord_encoding[coord]['dtype'] = 'int32'
            if 'units' in dataset[coord].attrs:
                # apply units as encoding for time...:
                coord_encoding[coord]['units'] = dataset[coord].attrs['units']
                # ... and remove from attributes list:
                del dataset[coord].attrs['units']
        elif coord == 'time_step':
            coord_encoding[coord]['dtype'] = 'int32'

    # merge encodings for variables and coordinates:
    encoding = var_encoding | coord_encoding

    # merge gcmd keywords:
    common_gcmd_keywords = dataset.keywords.split(',')
    gcmd_keywords = sorted(list(set(grouping_gcmd_keywords+common_gcmd_keywords)))  # as list
    gcmd_keywords = ', '.join(gcmd_keywords)                                        # as single cs string
    dataset.attrs['keywords'] = gcmd_keywords

    # uuid:
    dataset.attrs['uuid'] = str(uuid.uuid1())

    # add/append dataset grouping-specific comments if included in task metadata:
    try:
        if 'comment' in dataset.attrs.keys():
            dataset.attrs['comment'] = \
                ' '.join([dataset.attrs['comment'],task['dynamic_metadata']['comment']])
        else:
            dataset.attrs['comment'] = task['dynamic_metadata']['comment']
    except:
        pass
    dataset.time.attrs['long_name'] = task['dynamic_metadata']['time_long_name']
    dataset.attrs['time_coverage_duration'] = task['dynamic_metadata']['time_coverage_duration']
    dataset.attrs['time_coverage_resolution'] = task['dynamic_metadata']['time_coverage_resolution']
    dataset.attrs['product_name'] = os.path.basename(task['granule'])
    dataset.attrs['summary'] = ' '.join([task['dynamic_metadata']['summary'],dataset.attrs['summary']])

    # optional PO.DAAC metadata:
    try:
        # first, locate podaac metadata source file in ecco metadata directory:
        pm = ecco_podaac_metadata.ECCOPODAACMetadata(
            metadata_src=os.path.join(task['ecco_metadata_loc'],cfg['podaac_metadata_filename']),
            **kwargs).metadata
        #pm = pd.read_csv( os.path.join(
        #    task['ecco_metadata_loc'], cfg['podaac_metadata_filename']))
        # get PO.DAAC metadata (row) corresponding to 'DATASET.FILENAME' column
        # element that matches "generic" granule file string (i.e., without date and
        # version):
        granule_filestr = ecco_file.ECCOGranuleFilestr(os.path.basename(task['granule']))
        granule_filestr.date = None
        granule_filestr.version = None
        pm_row_for_this_granule = pm[pm['DATASET.FILENAME'].str.match(granule_filestr.re_filestr)]
        if len(pm_row_for_this_granule) != 1:
            e1 = f'granule regular expression, {granule_filestr.re_filestr},'
            if not len(pm_row_for_this_granule):
                e2 = 'did not match any PO.DAAC DATASET.FILENAME column elements.'
            else:
                e2 = 'matched more that one PO.DAAC DATASET.FILENAME column element.'
            raise RuntimeError(' '.join([e1,e2]))
        dataset.attrs['id'] = \
            pm_row_for_this_granule['DATASET.PERSISTENT_ID'].iloc[0].\
            replace('PODAAC-',f"{cfg['doi_prefix']}/")
        dataset.attrs['metadata_link'] = \
            cfg['metadata_link_root'] + \
            pm_row_for_this_granule['DATASET.SHORT_NAME'].iloc[0]
        dataset.attrs['title'] = \
            pm_row_for_this_granule['DATASET.LONG_NAME'].iloc[0]
        # additional specific PO.DAAC metadata request:
        dataset.attrs['coordinates_comment'] = \
            "Note: the global 'coordinates' attribute describes auxillary coordinates."
    except:
        log.info('Skipping PO.DAAC metadata inclusion')
        pass

    dataset.attrs = dict(sorted(dataset.attrs.items(),key = lambda x : x[0].casefold()))

    return (dataset,encoding)


def generate_dataproducts( tasklist, log_level=None, **kwargs):
    """Generate PO.DAAC-ready ECCO granule(s) for all tasks in tasklist.

    Args:
        tasklist: (Path and) name, or similar AWS S3 object name of
            json-formatted file containing list of ECCO dataset generation task
            descriptions, generated by create_job_task_list. See that function
            for formats and details.
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on run context:
            keygen (str): If tasklist, or tasklist descriptors reference AWS S3
                endpoints and if running in an institutionally-managed AWS IAM
                Identity Center (SSO) environment, (path and) name of federated
                login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64)
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        PO.DAAC-ready ECCO granule(s) to location(s) defined in tasklist.

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    shared_ecco_resources = False
    shared_ecco_grid = shared_ecco_mapping_factors = shared_ecco_metadata = None

    if aws.ecco_aws.is_s3_uri(tasklist):
        with tempfile.TemporaryDirectory() as tmpdir:
            _dest = os.path.join(tmpdir,os.path.basename(tasklist))
            aws.ecco_aws_s3_cp.aws_s3_cp( src=tasklist, dest=_dest, **kwargs)
            parsed_tasklist = json.load(open(_dest))
    else:
        parsed_tasklist = json.load(open(tasklist))


    for task in parsed_tasklist:

        cfg = configuration.ECCODatasetProductionConfig(cfgfile=task['ecco_cfg_loc'])

        # Assuming all tasks share the same ECCO grid, mapping factors, and
        # metadata references then, for performance reasons, create ECCOGrid,
        # ECCOMappingFactors, and ECCOMetadata objects up-front (using the first
        # task descriptor) to be shared by all granule creation tasks:

        if not shared_ecco_resources:
            try:
                shared_ecco_grid = ecco_grid.ECCOGrid(
                    task=task, **kwargs)
                shared_ecco_mapping_factors = ecco_mapping_factors.ECCOMappingFactors(
                    task=task, **kwargs)
                shared_ecco_metadata = ecco_metadata.ECCOMetadata(
                    task=task, **kwargs)
                shared_ecco_resources = True
            except Exception as e:
                # If shared resources can't be created, all subsequent jobs
                # would most certainly fail, even if they tried to create their
                # own grid/factors/metadata instances; just take hard exit:
                errmsg = 'Could not create shared ECCO resources'
                log.error(errmsg)
                raise SystemExit(e)

        try:
            ecco_make_granule( task, cfg,
                grid=shared_ecco_grid,
                mapping_factors=shared_ecco_mapping_factors,
                metadata=shared_ecco_metadata,
                log_level=log_level, **kwargs)
        except Exception as e:
            # just log the error and continue
            log.error('Error encountered during generation of %s: %s', task['granule'], e)

