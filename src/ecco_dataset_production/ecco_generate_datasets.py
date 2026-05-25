"""Main entry points for ECCO granule generation.

This module provides the primary functions for generating PO.DAAC/ESDIS-ready
NetCDF granules from ECCO model output. It serves as the main orchestration
layer for the dataset production pipeline.

Typical usage::

    >>> from ecco_dataset_production import ecco_generate_datasets
    >>> ecco_generate_datasets.generate_datasets('tasks.json')

Or from the command line::

    $ edp_generate_datasets --tasklist tasks.json

"""

from collections import defaultdict
from pprint import pprint
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
    """Create PO.DAAC/ESDIS-ready ECCO granule per instructions provided in
    input task descriptor.

    .. mermaid::

        %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
        flowchart TD
            A[Create ECCOTask wrapper] --> B{Grid type?}
            B -->|latlon| C[For each variable]
            C --> D[Create ECCOMDSDataset]
            D --> E[Transform to latlon grid]
            E --> F{More variables?}
            F -->|Yes| C
            F -->|No| G[Merge variables]
            B -->|native| H[For each variable]
            H --> I[Create ECCOMDSDataset]
            I --> J[Apply land mask]
            J --> K{More variables?}
            K -->|Yes| H
            K -->|No| G
            G --> L[set_granule_ancillary_data]
            L --> M[set_granule_metadata]
            M --> N{Output location?}
            N -->|Local| O[Write NetCDF locally]
            N -->|S3| P[Write to temp, upload to S3]

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
                (SSO) environment, (path and) name of federated login key
                generation script (e.g.,
                /usr/local/bin/aws-login.darwin.universal, etc.).
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
            task=this_task, ecco_metadata=metadata, cfg=cfg)

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

    .. mermaid::

        %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
        flowchart TD
            A[Get array precision from cfg] --> B[For each data variable]
            B --> C[Cast to specified precision]
            C --> D[Calculate valid_min/max]
            D --> E[Replace NaN with fill value]
            E --> F{More variables?}
            F -->|Yes| B
            F -->|No| G[Set time coordinate bounds]
            G --> H[Remove timestep coordinate]
            H --> I{Latlon or Native?}
            I -->|Latlon| J[Assign bounds from mapping factors]
            I -->|Native| K[Assign bounds from grid]
            J --> L[Return dataset]
            K --> L

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
    """Primary routine for aggregrating and setting metadata collected from all
    sources, e.g., ECCO configuration data, task list references, etc.  Note
    that set_granule_metadata operations depend in large part on functionality
    provided by ecco_v4_py.ecco_utils.

    .. mermaid::

        %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
        flowchart TD
            A[Load ECCOMetadata if not provided] --> B[Load all JSON metadata files]
            B --> C[Add variable-specific metadata]
            C --> D[Add coordinate metadata]
            D --> E[Add DOI metadata]
            E --> F[Add global metadata]
            F --> G[Add time metadata]
            G --> H[Set production timestamps]
            H --> I[Add cfg-sourced attributes]
            I --> J[Create variable encodings]
            J --> K[Set compression and fill values]
            K --> L[Create coordinate encodings]
            L --> M[Merge GCMD keywords]
            M --> N[Generate UUID]
            N --> O[Add PO.DAAC metadata]
            O --> P[Sort attributes alphabetically]
            P --> Q[Return dataset and encoding]

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
                script (e.g., /usr/local/bin/aws-login.darwin.universal, etc.).
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
    else:
        log.error('This granule is neither latlon nor native and is therefore missing coordinate metadata: %s', task['granule'])

        print()
    # doi-related metadata:
    dataset.attrs['metadata_link'] = cfg['doi_prefix']
    dataset.attrs['identifier_product_doi'] = cfg['doi_prefix']
    dataset.attrs['identifier_product_doi_authority'] = cfg['doi_authority']

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
    if not task.is_time_invariant:
        if 'time' in dataset.coords:
            dataset = ecco_v4_py.ecco_utils.add_coordinate_metadata(
                all_metadata['coord_time'], dataset)

    # global time and date-associated metadata:
    if not task.is_time_invariant:
        if 'mean' in task.averaging_period:
            dataset.attrs['time_coverage_start']= task['dynamic_metadata']['time_coverage_start']
            dataset.attrs['time_coverage_end']  = task['dynamic_metadata']['time_coverage_end']
    else:
        #if granule is time-invariant, drop time_coverage_start or end if present
        dataset.attrs.pop('time_coverage_start',None)
        dataset.attrs.pop('time_coverage_end',None)
        
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
    dataset.attrs['project_summary']            = cfg['project_summary']

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
    common_gcmd_keywords = [k.strip() for k in dataset.keywords.split(',')] if 'keywords' in dataset.attrs else []
    gcmd_keywords = sorted(list(set(grouping_gcmd_keywords+common_gcmd_keywords)))  # as list
    gcmd_keywords = ', '.join([k for k in gcmd_keywords if k])  # filter empty strings, join
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

    if not task.is_time_invariant:
        dataset.time.attrs['long_name'] = task['dynamic_metadata']['time_long_name']
        dataset.attrs['time_coverage_duration'] = task['dynamic_metadata']['time_coverage_duration']
        dataset.attrs['time_coverage_resolution'] = task['dynamic_metadata']['time_coverage_resolution']

    dataset.attrs['product_name'] = os.path.basename(task['granule'])
    dataset.attrs['summary'] = ' '.join([task['dynamic_metadata']['summary'], dataset.attrs['project_summary']])

    # optional PO.DAAC metadata:
    log.info('Attempting to add PO.DAAC metadata from CSV file')
    try:
        # first, locate podaac metadata source file in ecco metadata directory:
        podaac_csv_path = os.path.join(task['ecco_metadata_loc'], cfg['podaac_metadata_filename'])
        log.info('  Loading PO.DAAC metadata from: %s', podaac_csv_path)

        pm = ecco_podaac_metadata.ECCOPODAACMetadata(
            metadata_src=podaac_csv_path,
            **kwargs).metadata

        log.info('  Loaded %d entries from PO.DAAC CSV file', len(pm))

        # get PO.DAAC metadata (row) corresponding to 'DATASET.FILENAME' column
        # element that matches "generic" granule file string (i.e., without date and
        # version):
        granule_filestr = ecco_file.ECCOGranuleFilestr(os.path.basename(task['granule']))
        granule_filestr.date = None
        granule_filestr.version = None

        log.info('  Searching for granule pattern: %s', granule_filestr.re_filestr)

        pm_row_for_this_granule = pm[pm['DATASET.FILENAME'].str.match(granule_filestr.re_filestr)]

        if pm_row_for_this_granule.empty:
            log.warning('')
            log.warning('='*70)
            log.warning('WARNING: NO PO.DAAC METADATA ENTRY FOUND')
            log.warning('='*70)
            log.warning('Granule pattern: %s', granule_filestr.re_filestr)
            log.warning('PO.DAAC CSV file: %s', podaac_csv_path)
            log.warning('This granule will NOT have PO.DAAC-specific metadata:')
            log.warning('  - id (DATASET.SHORT_NAME)')
            log.warning('  - title (DATASET.LONG_NAME)')
            log.warning('  - metadata_link (will use config doi_prefix: %s)', cfg['doi_prefix'])
            log.warning('  - identifier_product_doi (will use config doi_prefix: %s)', cfg['doi_prefix'])
            log.warning('  - coordinates (descriptive text about auxiliary coordinates)')
            log.warning('='*70)
            log.warning('')
            e1 = f'granule regular expression, {granule_filestr.re_filestr},'
            e2 = 'did not match any PO.DAAC DATASET.FILENAME column elements.'
            raise RuntimeError(f'{e1} {e2}')
        elif pm_row_for_this_granule.shape[0] > 1:
            log.error('Found %d matching entries (expected exactly 1)', pm_row_for_this_granule.shape[0])
            log.error('Matching entries:')
            for idx, row in pm_row_for_this_granule.iterrows():
                log.error('  - %s', row['DATASET.FILENAME'])
            e1 = f'granule regular expression, {granule_filestr.re_filestr},'
            e2 = 'matched more than one PO.DAAC DATASET.FILENAME column element.'
            raise RuntimeError(f'{e1} {e2}')

        # Found exactly one match
        matched_filename = pm_row_for_this_granule['DATASET.FILENAME'].iloc[0]
        log.info('  Found matching PO.DAAC entry: %s', matched_filename)

        # Extract metadata values
        short_name = pm_row_for_this_granule['DATASET.SHORT_NAME'].iloc[0]
        long_name = pm_row_for_this_granule['DATASET.LONG_NAME'].iloc[0]
        persistent_id = pm_row_for_this_granule['DATASET.PERSISTENT_ID'].iloc[0]

        # Construct DOI URL from DATASET.PERSISTENT_ID
        doi_url = f'https://doi.org/{persistent_id}'

        log.info('  Adding PO.DAAC metadata to granule:')
        log.info('    id (SHORT_NAME): %s', short_name)
        log.info('    title (LONG_NAME): %s', long_name)
        log.info('    PERSISTENT_ID: %s', persistent_id)
        log.info('    metadata_link: %s', doi_url)
        log.info('    identifier_product_doi: %s', doi_url)
        log.info('    coordinates_note: a warning note about the global attribute "coordinates" that is added to all variables by xarray during file writing')

        # Apply metadata
        dataset.attrs['id'] = short_name
        dataset.attrs['metadata_link'] = doi_url
        dataset.attrs['identifier_product_doi'] = doi_url
        dataset.attrs['title'] = long_name
        dataset.attrs['coordinates_note'] = \
            "The 'coordinates' attribute lists a subset of auxiliary coordinates. Created by xarray.to_netcdf()."

        log.info('  Successfully added PO.DAAC metadata')

    except Exception as e:
        log.warning('Failed to add PO.DAAC metadata: %s', e)
        log.warning('Continuing without PO.DAAC-specific metadata')
        pass

    dataset.attrs = dict(sorted(dataset.attrs.items(),key = lambda x : x[0].casefold()))

    return (dataset,encoding)


def print_dataset_metadata(dataset):
    """
    Prints all metadata of an xarray.Dataset.
    """
    print("==================================================")
    print("                Dataset Metadata                  ")
    print("==================================================")

    print("\n>>> Global Attributes:")
    for attr, value in dataset.attrs.items():
        print(f"    {attr}: {value}")

    print("\n>>> Dimensions:")
    for dim, size in dataset.dims.items():
        print(f"    {dim}: {size}")

    print("\n>>> Coordinates:")
    for coord_name, coord_var in dataset.coords.items():
        print(f"    - {coord_name}:")
        print(f"        dims: {coord_var.dims}")
        print(f"        shape: {coord_var.shape}")
        print(f"        dtype: {coord_var.dtype}")
        if coord_var.attrs:
            print("        Attributes:")
            for attr, value in coord_var.attrs.items():
                print(f"            {attr}: {value}")

    print("\n>>> Data Variables:")
    for var_name, data_var in dataset.data_vars.items():
        print(f"    - {var_name}:")
        print(f"        dims: {data_var.dims}")
        print(f"        shape: {data_var.shape}")
        print(f"        dtype: {data_var.dtype}")
        if data_var.attrs:
            print("        Attributes:")
            for attr, value in data_var.attrs.items():
                print(f"            {attr}: {value}")
    print("==================================================")


def process_time_invariant_granule(task, cfg, grid=None, mapping_factors=None, metadata=None, log_level=None, **kwargs):
    """
    Process a time-invariant granule NetCDF file to add ancillary data and metadata.
    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    netcdf_file = task['input_netcdf']

    # Load the NetCDF file into an xarray Dataset
    merged_variable_dataset = xr.open_dataset(netcdf_file)
    try:
        #print('\n\npre metadata stripping:')
        #print_dataset_metadata(merged_variable_dataset)

        if task.get('strip_attributes', False):
            # Remove all global attributes
            merged_variable_dataset.attrs = {}
            # Remove all variable and coordinate attributes
            for var in merged_variable_dataset.variables:
                merged_variable_dataset[var].attrs = {}

        #print('\n\npost metadata stripping:')
        #print_dataset_metadata(merged_variable_dataset)

        # set miscellaneous granule attributes and properties:
        merged_variable_dataset_with_ancillary_data = set_granule_ancillary_data(
            dataset=merged_variable_dataset, task=task,
            grid=grid, mapping_factors=mapping_factors, cfg=cfg)

        # append metadata:
        merged_variable_dataset_with_all_metadata, encoding = set_granule_metadata(
            dataset=merged_variable_dataset_with_ancillary_data,
            task=task, ecco_metadata=metadata, cfg=cfg)

        # write:
        if task.is_granule_local:
            if os.path.dirname(task['granule']) and not os.path.exists(os.path.dirname(task['granule'])):
                os.makedirs(os.path.dirname(task['granule']))
            merged_variable_dataset_with_all_metadata.to_netcdf(
                task['granule'], encoding=encoding)
        else:
            with tempfile.TemporaryDirectory() as upload_tmpdir:
                # temporary directory will self-destruct at end of with block
                _src = os.path.basename(task['granule'])
                _dest = task['granule']
                merged_variable_dataset_with_all_metadata.to_netcdf(
                    os.path.join(upload_tmpdir,_src), encoding=encoding)
                log.info('uploading %s to %s', os.path.join(upload_tmpdir,_src), _dest)
                aws.ecco_aws_s3_cp.aws_s3_cp( src=os.path.join(upload_tmpdir,_src), dest=_dest, **kwargs)
    finally:
        merged_variable_dataset.close()

    log.info('... completely finished processing time-invariant granule %s', os.path.basename(task['granule']))


def apply_metadata_to_netcdf(
    input_netcdf, output_netcdf, ecco_metadata_loc, cfg,
    grid_type='native', is_2d=None, strip_attributes=False,
    log_level=None, **kwargs):
    """Apply ECCO metadata to an existing NetCDF file without loading external grid files or mapping factors.

    This function is designed for adding metadata to "bare" NetCDF files (like grid geometry files)
    that already contain their coordinate data. Unlike process_time_invariant_granule, this does NOT
    require or load external grid files or mapping factors, making it suitable for bootstrapping
    grid geometry files.

    Args:
        input_netcdf (str): Path to input NetCDF file to which metadata will be added.
        output_netcdf (str): Path where the metadata-enriched NetCDF file will be written.
        ecco_metadata_loc (str): Path to directory containing ECCO metadata JSON files,
            or AWS S3 bucket/prefix.
        cfg (dict or str): Either a parsed configuration dictionary or path to YAML config file.
        grid_type (str): Grid type - 'native' or 'latlon' (default: 'native').
        is_2d (bool or None): Whether dataset is 2D. If None (default), auto-detects from file
            by checking for vertical dimensions (Z, k, k_l, k_u, k_p1). If any of these
            dimensions exist, dataset is 3D. Set explicitly to True or False to override.
        strip_attributes (bool): If True, strip all existing attributes before applying new ones
            (default: False).
        log_level (str): Optional logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        **kwargs: If ecco_metadata_loc references AWS S3, may include:
            keygen (str): AWS SSO key generation script path.
            profile (str): AWS profile name.

    Returns:
        None. Output is written to output_netcdf path.

    Example:
        >>> from ecco_dataset_production import ecco_generate_datasets
        >>> # Auto-detect dimension
        >>> ecco_generate_datasets.apply_metadata_to_netcdf(
        ...     input_netcdf='bare_grid.nc',
        ...     output_netcdf='GRID_GEOMETRY_ECCO_V4r6_native_llc0090.nc',
        ...     ecco_metadata_loc='/path/to/metadata',
        ...     cfg='configs/config_V4r6.yaml',
        ...     grid_type='native')
    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    log.info('Applying metadata to %s', input_netcdf)

    # Load config if it's a file path
    if isinstance(cfg, str):
        cfg = configuration.ECCODatasetProductionConfig(cfg, **kwargs)

    # Load metadata
    metadata = ecco_metadata.ECCOMetadata(ecco_metadata_loc=ecco_metadata_loc, **kwargs)

    # Load the input NetCDF file
    dataset = xr.open_dataset(input_netcdf)

    # Auto-detect 2D vs 3D if not specified
    if is_2d is None:
        # Check for vertical dimensions common in ECCO datasets
        vertical_dims = {'Z', 'k', 'k_l', 'k_u', 'k_p1'}
        found_dim = None
        for dim in vertical_dims:
            if dim in dataset.dims:
                found_dim = dim
                break

        if found_dim:
            is_2d = False
            log.info('Auto-detected 3D dataset (found vertical dimension "%s")', found_dim)
        else:
            is_2d = True
            log.info('Auto-detected 2D dataset (no vertical dimensions found)')
    else:
        log.info('Using specified dimension: %s', '2D' if is_2d else '3D')

    # Determine dataset-specific summary
    # For GRID_GEOMETRY files, use special descriptions from JSON
    granule_basename = os.path.basename(output_netcdf)
    if 'GRID_GEOMETRY' in granule_basename:
        log.info('Detected GRID_GEOMETRY file, loading geometry-specific description')
        try:
            # Load grid geometry descriptions
            geom_desc_file = os.path.join(ecco_metadata_loc, 'grid_geometry_dataset_descriptions.json')
            if aws.utils.is_s3_uri(geom_desc_file):
                # Handle S3 location
                with tempfile.TemporaryDirectory() as tmpdir:
                    local_file = os.path.join(tmpdir, 'grid_geometry_dataset_descriptions.json')
                    aws.ecco_aws_s3_cp.aws_s3_cp(src=geom_desc_file, dest=local_file, **kwargs)
                    with open(local_file, 'r') as f:
                        geom_descriptions = json.load(f)
            else:
                # Local file
                with open(geom_desc_file, 'r') as f:
                    geom_descriptions = json.load(f)

            # Get the template for this grid type
            template = geom_descriptions[grid_type]['description_template']

            # Format template with config values
            dataset_summary = template.format(
                llc_grid_size=cfg.get('llc_grid_size', ''),
                llc_code=cfg.get('llc_code', ''),
                product_version=cfg.get('product_version', ''),
                ecco_version=cfg.get('ecco_version', ''),
                latlon_grid_resolution=cfg.get('latlon_grid_resolution', '')
            )
            log.info('Using geometry-specific description for %s grid', grid_type)
        except Exception as e:
            log.warning('Failed to load grid geometry description: %s', e)
            log.warning('Using default summary')
            dataset_summary = 'ECCO dataset'
    else:
        dataset_summary = 'ECCO dataset'

    # Create a minimal task-like dict with required metadata fields
    # This provides the structure that set_granule_metadata expects
    task_dict = {
        'ecco_metadata_loc': ecco_metadata_loc,
        'granule': output_netcdf,
        'dynamic_metadata': {
            'dimension': '2D' if is_2d else '3D',
            'summary': dataset_summary,
            'comment': ''
        }
    }

    # Create ECCOTask-like object with required properties
    class MinimalTask:
        def __init__(self, task_dict, grid_type):
            self.data = task_dict
            self._grid_type = grid_type

        def __getitem__(self, key):
            return self.data[key]

        def get(self, key, default=None):
            return self.data.get(key, default)

        @property
        def is_latlon(self):
            return self._grid_type == 'latlon'

        @property
        def is_native(self):
            return self._grid_type == 'native'

        @property
        def is_2d(self):
            return self.data['dynamic_metadata']['dimension'] == '2D'

        @property
        def is_3d(self):
            return self.data['dynamic_metadata']['dimension'] == '3D'

        @property
        def is_time_invariant(self):
            return True

        @property
        def is_granule_local(self):
            return not aws.utils.is_s3_uri(self.data['granule'])

    task = MinimalTask(task_dict, grid_type)

    try:
        # Optionally strip existing attributes
        if strip_attributes:
            log.info('Stripping existing attributes')
            dataset.attrs = {}
            for var in dataset.variables:
                dataset[var].attrs = {}

        # Apply basic data type conversions and fill values
        log.info('Applying precision and fill value settings')
        prec = cfg['array_precision'] if 'array_precision' in cfg else 'float32'
        ncfill = netCDF4.default_fillvals['f4'] if prec=='float32' else netCDF4.default_fillvals['f8']

        for var in dataset.data_vars:
            dataset[var].values = dataset[var].astype(eval('np.'+prec))
            # Only calculate valid_min/max if there are non-NaN values
            if not np.all(np.isnan(dataset[var].values)):
                dataset[var].attrs['valid_min'] = np.nanmin(dataset[var].values)
                dataset[var].attrs['valid_max'] = np.nanmax(dataset[var].values)
            dataset[var].values = np.where(np.isnan(dataset[var].values), ncfill, dataset[var].values)

        # Apply metadata (this is the key function that adds all attributes)
        log.info('Applying ECCO metadata')
        dataset_with_metadata, encoding = set_granule_metadata(
            dataset=dataset,
            task=task,
            ecco_metadata=metadata,
            cfg=cfg,
            **kwargs)

        # Write output
        log.info('Writing output to %s', output_netcdf)
        if task.is_granule_local:
            if os.path.dirname(output_netcdf) and not os.path.exists(os.path.dirname(output_netcdf)):
                os.makedirs(os.path.dirname(output_netcdf))
            dataset_with_metadata.to_netcdf(output_netcdf, encoding=encoding)
        else:
            # Handle S3 output
            with tempfile.TemporaryDirectory() as upload_tmpdir:
                _src = os.path.basename(output_netcdf)
                _dest = output_netcdf
                dataset_with_metadata.to_netcdf(os.path.join(upload_tmpdir, _src), encoding=encoding)
                log.info('Uploading %s to %s', os.path.join(upload_tmpdir, _src), _dest)
                aws.ecco_aws_s3_cp.aws_s3_cp(src=os.path.join(upload_tmpdir, _src), dest=_dest, **kwargs)

    finally:
        dataset.close()

    log.info('Successfully applied metadata to %s', output_netcdf)


def generate_datasets( tasklist, log_level=None, **kwargs):
    """Generate PO.DAAC/ESDIS-ready ECCO granule(s) for all tasks in tasklist.

    .. mermaid::

        %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
        flowchart TD
            A[Load tasklist JSON] --> B{Is S3 URI?}
            B -->|Yes| C[Download from S3]
            B -->|No| D[Load local file]
            C --> E[Parse tasklist]
            D --> E
            E --> F[Initialize shared resources]
            F --> G[ECCOGrid]
            G --> H[ECCOMappingFactors]
            H --> I[ECCOMetadata]
            I --> J[For each task in tasklist]
            J --> K[Load ECCODatasetProductionConfig]
            K --> L[ecco_make_granule]
            L --> M{More tasks?}
            M -->|Yes| J
            M -->|No| N[Done]

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
                /usr/local/bin/aws-login.darwin.universal, etc.).
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        PO.DAAC/ESDIS-ready ECCO granule(s) to location(s) defined in tasklist.

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    shared_ecco_resources = False
    shared_ecco_grid = shared_ecco_mapping_factors = shared_ecco_metadata = None

    if aws.utils.is_s3_uri(tasklist):
        with tempfile.TemporaryDirectory() as tmpdir:
            _dest = os.path.join(tmpdir,os.path.basename(tasklist))
            aws.ecco_aws_s3_cp.aws_s3_cp( src=tasklist, dest=_dest, **kwargs)
            parsed_tasklist = json.load(open(_dest))
    else:
        parsed_tasklist = json.load(open(tasklist))


    for task in parsed_tasklist:
        print('\n=================================')
        print('NEW TASK!')
        pprint(task)
        try:
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
                    log.exception(e)
                    raise SystemExit(e)
                
            # this_task object needed to check for time invariance:
            this_task = ecco_task.ECCOTask(task)

            if this_task.is_time_invariant:
                # if time-invariant, then assume that the task's 'input_netcdf' key
                # points to a pre-existing NetCDF file that just needs ancillary
                # data and metadata added; process accordingly:
                process_time_invariant_granule(
                    task=this_task, cfg=cfg,
                    grid=shared_ecco_grid, mapping_factors=shared_ecco_mapping_factors,
                    metadata=shared_ecco_metadata, log_level=log_level, **kwargs)
            else:
                ecco_make_granule( this_task, cfg,
                    grid=shared_ecco_grid,
                    mapping_factors=shared_ecco_mapping_factors,
                    metadata=shared_ecco_metadata,
                    log_level=log_level, **kwargs)

        except Exception as e:
            # just log the error and continue
            try:
                log.error('Error encountered during generation of %s: %s', task['granule'], e)
            except:
                log.error('Error encountered during generation of a granule: %s', e)
            log.exception(e)
