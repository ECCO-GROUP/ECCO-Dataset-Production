
import itertools
import json
import logging
import os
import xarray as xr
import yaml

from . import ecco_dataset
from . import ecco_task


def ecco_make_granule( task, cfg, log_level=None, **kwargs):
    """

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
        # TODO...
        pass

    elif this_task.is_native:
        log.info('generating %s ...', os.path.basename(this_task['granule']))
        for variable in this_task.variable_names:
            log.debug('... adding %s using:', variable)
            for infile in itertools.chain.from_iterable(this_task.variable_inputs(variable)):
                log.debug('    %s', infile)
            emdsds = ecco_dataset.ECCOMDSDataset(
                task=this_task, variable=variable, cfg=cfg, **kwargs)
            emdsds.drop_all_variables_except(variable)
            emdsds.apply_land_mask_to_native_variable(variable)
            variable_datasets.append(emdsds)
    else:
        raise RuntimeError('Could not determine output granule type (latlon or native)')

    # create a merged xarray dataset using the datasets contained in the list of
    # EMDSDataset objects:
    merged_variable_dataset = xr.merge([ds.ds for ds in variable_datasets])

    # append metadata:
    # TODO

    if this_task.is_granule_local:
        merged_variable_dataset.to_netcdf(this_task['granule'])
    else:
        # TODO: write to temp location, upload...
        pass

    log.info('... done')


def generate_dataproducts( tasklist, cfgfile, workingdir='.',
    log_level=None, **kwargs):
    """Generate PO.DAAC-ready ECCO granule(s) for all tasks in tasklist.

    Args:
        tasklist: (Path and) name of json-formatted file containing list of
            ECCO dataset generation task descriptions, generated by
            create_job_task_list. See that function for formats and details.
        cfgfile (str): (Path and) filename of ECCO Dataset Production
            configuration file.
        workingdir (str): Working directory path definition default if explicit
            path definitions are otherwise unassigned in cfgfile (default='.').
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on run context:
            keygen (str): If tasklist descriptors reference AWS S3 endpoints and
                if running in JPL domain, (path and) name of federated login key
                generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64)
            profile_name (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)

    Returns:
        PO.DAAC-ready ECCO granule(s) to location(s) defined in tasklist.

    """
    log = logging.getLogger('edp'+__name__)
    if log_level:
        log.setLevel(log_level)

    log.info('Initializing configuration parameters...')
    cfg = yaml.safe_load(open(cfgfile))
    log.debug('Configuration key value pairs:')
    for k,v in cfg.items():
        log.debug('%s: %s', k, v)
    log.info('...done initializing configuration parameters.')

    # TODO: peek at tasklist to see if ecco grid is non-local (e.g., aws s3); if
    # so, create an ECCOGrid object here so that it may be shared by all tasks.

    for task in json.load(open(tasklist)):

        ecco_make_granule( task, cfg, log_level, **kwargs)
