
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

from .aws import ecco_aws_s3_cp
from . import ecco_dataset
from . import ecco_file
from . import ecco_grid
from . import ecco_mapping_factors
from . import ecco_metadata_store
from . import ecco_task
#from . import metadata


def ecco_make_granule( task, cfg,
    grid=None, mapping_factors=None, log_level=None, **kwargs):
    """Create PO.DAAC-ready ECCO granule per instructions provided in input task
    descriptor.

    Args:
        task (dict):
        cfg
    """

    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    # ECCOTask object to answer some basic questions:
    this_task = ecco_task.ECCOTask(task)

    variable_datasets = []

    if this_task.is_latlon:
        log.info('generating %s ...', os.path.basename(this_task['granule']))
        for variable in this_task.variable_names:
            log.debug('... adding %s using:', variable)
            for infile in itertools.chain.from_iterable(this_task.variable_inputs(variable)):
                log.debug('    %s', infile)
            emdsds = ecco_dataset.ECCOMDSDataset(
                task=this_task, variable=variable, grid=grid,
                mapping_factors=mapping_factors, cfg=cfg, **kwargs)
            emdsds.drop_all_variables_except(variable)
            var_as_latlon_ds = emdsds.as_latlon(variable)
            variable_datasets.append(var_as_latlon_ds)
        merged_variable_dataset = xr.merge(variable_datasets)

    elif this_task.is_native:

        log.info('generating %s ...', os.path.basename(this_task['granule']))
        for variable in this_task.variable_names:

            log.debug('... adding %s using:', variable)
            for infile in itertools.chain.from_iterable(this_task.variable_inputs(variable)):
                log.debug('    %s', infile)
            emdsds = ecco_dataset.ECCOMDSDataset(
                task=this_task, variable=variable, grid=grid, cfg=cfg, **kwargs)
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
        task=this_task, cfg=cfg)

    # write:
    if this_task.is_granule_local:
        if not os.path.exists(os.path.dirname(this_task['granule'])):
            os.makedirs(os.path.dirname(this_task['granule']))
        merged_variable_dataset_with_all_metadata.to_netcdf(
            this_task['granule'], encoding=encoding)
    else:

        with tempfile.TemporaryDirectory() as tmpdir:
            log.info('temporary directory created: %s ', tmpdir)

            _src = os.path.basename(this_task['granule'])
            _dest = this_task['granule']

            merged_variable_dataset_with_all_metadata.to_netcdf(
                os.path.join(tmpdir,_src), encoding=encoding)
        
            log.info('uploading %s to %s', os.path.join(tmpdir,_src), _dest)
            ecco_aws_s3_cp.aws_s3_cp( src=os.path.join(tmpdir,_src), dest=_dest, **kwargs)
            # temporary directory will self-destruct at end of with block

    log.info('... done')


def set_granule_ancillary_data(
    dataset=None, task=None, grid=None, mapping_factors=None, cfg=None):
    """
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


def set_granule_metadata( dataset=None, task=None, cfg=None, **kwargs):
    """
    """
    log = logging.getLogger('edp.'+__name__)

    # get ecco metadata, wherever it may be:
    ecco_metadata_source = ecco_metadata_store.ECCOMetadataStore(
        metadata_loc=task['ecco_metadata_loc'], **kwargs)

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

    # add coordinate attributes to all variables:
    var_coord_attrs = {}
    coord_set = set(['XC','YC','XG','YG','Z','Zp1','Zu','Zl','time'])
    for var in list(dataset.data_vars):
        var_coord_original = set(list(dataset[var].coords))
        set_intersect = var_coord_original.intersection(coord_set)
        var_coord_attrs[var] = ' '.join(set_intersect)

    # specific variable encodings:
    prec = cfg['array_precision'] if 'array_precision' in cfg else 'float64'
    ncfill = netCDF4.default_fillvals['f4'] if prec=='float32' else netCDF4.default_fillvals['f8']
    var_encoding = {}
    for var in list(dataset.data_vars):
        var_encoding[var] = {
            'zlib':True,
            'complevel':5,
            'shuffle':True,
            '_FillValue': ncfill}
        # per PO.DAAC request, overwrite default coordinates attribute:
        dataset[var].encoding['coordinates'] = var_coord_attrs[var]

    # specific coordinate encodings:
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
        pm = pd.read_csv( os.path.join(
            task['ecco_metadata_loc'], cfg['podaac_metadata_filename']))
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


def generate_dataproducts( tasklist, cfgfile,
    log_level=None, **kwargs):
    """Generate PO.DAAC-ready ECCO granule(s) for all tasks in tasklist.

    Args:
        tasklist: (Path and) name of json-formatted file containing list of
            ECCO dataset generation task descriptions, generated by
            create_job_task_list. See that function for formats and details.
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on run context:
            keygen (str): If tasklist descriptors reference AWS S3 endpoints and
                if running in JPL domain, (path and) name of federated login key
                generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64)
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        PO.DAAC-ready ECCO granule(s) to location(s) defined in tasklist.

    """
    log = logging.getLogger('edp'+__name__)
    if log_level:
        log.setLevel(log_level)

    log.info('Parsing configuration file...')
    cfg = yaml.safe_load(open(cfgfile))
    log.debug('configuration key value pairs:')
    for k,v in cfg.items():
        log.debug('%s: %s', k, v)
    log.info('...done parsing configuration file.')

    shared_ecco_grid = shared_ecco_mapping_factors = None

    for task in json.load(open(tasklist)):

        # Assuming all tasks share the same ECCO grid and mapping factors
        # references then, for performance reasons, create ECCOGrid and
        # ECCOMappingFactors objects up-front (using the first task descriptor)
        # to be shared by all granule creation tasks:
        if not shared_ecco_grid and not shared_ecco_mapping_factors:
            shared_ecco_grid = ecco_grid.ECCOGrid(task=task)
            shared_ecco_mapping_factors = ecco_mapping_factors.ECCOMappingFactors(task=task)

        try:
            ecco_make_granule( task, cfg,
                grid=shared_ecco_grid, mapping_factors=shared_ecco_mapping_factors,
                log_level=log_level, **kwargs)
        except Exception as e:
            # just log the error and continue
            log.error('Error encountered during generation of %s: %s', task['granule'], e)

