"""
"""

import glob
import json
import logging
import os
import re
import tempfile

from . import aws


log = logging.getLogger('edp.'+__name__)


class ECCOMetadataStore(object):
    """Class to manage ECCO metadata *json file access, cleanup.

    """
    def __init__( self, metadata_loc=None, **kwargs):
        """Create instance of ECCOMetadataStore class.

        """
        self.metadata_loc = metadata_loc
        self.tmpdir = self.metadata_dir = None

        if self.metadata_loc:
            if aws.ecco_aws.is_s3_uri(self.metadata_loc):
                # retrieve remote metadata to temporary local storage:
                self.tmpdir = tempfile.TemporaryDirectory()
                self.metadata_dir = self.tmpdir.name
                aws.ecco_aws_s3_sync.aws_s3_sync( src=self.metadata_loc, dest=self.metadata_dir, **kwargs)
            else:
                # just point to local metadata directory:
                self.metadata_dir = self.metadata_loc


    @property
    def dataset_groupings(self):
        """Get 'groupings' related metadata, returned as dictionary with '1D',
        'latlon', and 'native' keys.

        """
        log.info("collecting 'groupings' metadata sourced from %s...", self.metadata_loc)
        dataset_groupings = {}
        for file in glob.glob(os.path.join(self.metadata_loc,'*groupings*')):
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
        """Remove temporary metadata directory when ECCOMetadataStore instance
        goes out of scope or is explicitly deleted.

        """
        try:
            self.tmpdir.cleanup()
        except:
            pass
