"""
"""

import glob
import json
import logging
import os
import re
import tempfile

from . import aws
from . import ecco_task

log = logging.getLogger('edp.'+__name__)

# TODO: This class definition is really only just a starting point for other
# useful functionality. Consider implementing wider code cleanup by moving other
# ecco general metadata operations here.


class ECCOMetadata(object):
    """Class to manage ECCO metadata *json file access, cleanup.

    Args:
        task (str, dict, or taskobj): Optional (path and) name of json-formatted
            file defining a single task, single task description dictionary, or
            ECCOTask object. Either task or metadata_loc may be provided, but
            not both.
        ecco_metadata_loc (str): Optional pathname of either ECCO metadata directory,
            or similar remote location given by AWS S3 bucket/prefix. Either
            ecco_metadata_loc or task may be provided but not both.
        **kwargs: If either task or ecco_metadata_loc references an AWS S3
            endpoint and if running within an institutionally-managed AWS IAM
            Identity Center (SSO) environment, additional arguments that may be
            necessary include:
            keygen (str): Login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        task (ECCOTask): If provided, local object store of input task
            descriptor.
        metadata_dir (str): Resulting local ECCO metadata directory name (see
            tmpdir), ecco_metadata_loc otherwise.
        tmpdir (tempfile.TemporaryDirectory object): If task or ecco_metadata_loc
            references an AWS S3 endpoint, temporary directory object whose
            'name' attribute is assigned to metadata_dir. In the case of a zipped
            archive (see "Notes"), tmpdir's 'name' may be extended accordingly
            prior to assigning to grid_dir.

    """
    def __init__( self, task=None, ecco_metadata_loc=None, **kwargs):
        """Create instance of ECCOMetadata class.

        """
        self.metadata_dir = None
        self.task = None
        self.tmpdir = None

        if task and metadata_loc:
            raise RuntimeError('Either task or metadata_loc may be provided, but not both')

        if task:
            if not isinstance(task,ecco_task.ECCOTask):
            # instantiate from file or dict:
                self.task = ecco_task.ECCOTask(task)
            else:
                # just use directly:
                self.task = task
            ecco_metadata_loc = self.task['ecco_metadata_loc']

        if aws.ecco_aws.is_s3_uri(ecco_metadata_loc):
            # retrieve ecco metadata to temporary local storage:
            self.tmpdir = tempfile.TemporaryDirectory()
            self.metadata_dir = self.tmpdir.name
            aws.ecco_aws_s3_sync.aws_s3_sync( src=ecco_metadata_loc, dest=self.metadata_dir, **kwargs)
            if not os.listdir(self.metadata_dir):
                raise RuntimeError(
                    f'Remote ECCO metadata fetch failed. Ensure ecco_metadata_loc ({ecco_metadata_loc}) refers to a valid s3 bucket and prefix (only).')
        else:
            # just point to local directory:
            self.tmpdir = None
            self.metadata_dir = ecco_metadata_loc


    @property
    def dataset_groupings(self):
        """Get 'groupings' related metadata, returned as dictionary with '1D',
        'latlon', and 'native' keys.

        """
        log.info("collecting 'groupings' metadata sourced from %s...", self.metadata_loc)
        dataset_groupings = {}
        for file in glob.glob(os.path.join(self.metadata_dir,'*groupings*')):
            if re.search(r'_1D_',os.path.basename(file),re.IGNORECASE):
                log.debug('parsing 1D groupings metadata file %s ... ', file)
                with open(file) as f:
                    dataset_groupings['1D'] = json.load(f)
            elif re.search(r'_latlon_',os.path.basename(file),re.IGNORECASE):
                log.debug('parsing latlon groupings metadata file %s ... ', file)
                with open(file) as f:
                    dataset_groupings['latlon'] = json.load(f)
            elif re.search(r'_native_',os.path.basename(file),re.IGNORECASE):
                log.debug('parsing native groupings metadata file %s ... ', file)
                with open(file) as f:
                    dataset_groupings['native'] = json.load(f)
        log.debug('dataset grouping metadata:')
        for key,list_of_dicts in dataset_groupings.items():
            log.debug('%s:', key)
            for i,dict_i in enumerate(list_of_dicts):
                log.debug(' %d:', i)
                for k,v in dict_i.items():
                    log.debug('  %s: %s', k, v)
        log.info("...done collecting 'groupings' metadata sourced from %s", self.metadata_loc)
        return dataset_groupings


    def __del__(self):
        """Remove temporary metadata storage when ECCOMetadata instance goes out
        of scope or is explicitly deleted.

        """
        try:
            self.tmpdir.cleanup()
        except:
            pass

