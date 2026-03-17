"""PO.DAAC-specific metadata handling.

This module provides the :class:`ECCOPODAACMetadata` class for loading and
accessing PO.DAAC (Physical Oceanography Distributed Active Archive Center)
specific metadata mappings.

PO.DAAC metadata includes:

- Dataset short names and long names for archive cataloging
- Collection identifiers
- Filename-to-dataset mappings

This metadata is applied during granule production to ensure compatibility
with PO.DAAC's data distribution requirements.

Example:
    >>> from ecco_dataset_production import ecco_podaac_metadata
    >>> pm = ecco_podaac_metadata.ECCOPODAACMetadata(metadata_src='podaac.csv')
    >>> df = pm.metadata  # pandas DataFrame

"""

import logging
import os
import pandas as pd
import tempfile

from . import aws

log = logging.getLogger('edp.'+__name__)

# TODO: This class definition is really only just a starting point for other
# useful functionality. Consider implementing wider code cleanup by moving other
# PO.DAAC-related metadata here.


class ECCOPODAACMetadata(object):
    """Class to manage access to ECCO-related PO.DAAC metadata, cleanup.

    """
    def __init__( self, metadata_src=None, **kwargs):
        """Create instance of ECCOPODAACMetadata class.

        Args:
            metadata_src (str): (Path and) filename of ECCO-related PO.DAAC
                metadata, or similar AWS S3 bucket/prefix/name.
            \*\*kwargs: If metadata_src references an AWS S3 endpoint and if
                running within an institutionally-managed AWS IAM Identity
                Center (SSO) environment, additional arguments that may be
                necessary include:
                keygen (str): Login key generation script (e.g.,
                    /usr/local/bin/aws-login.darwin.universal, etc.).
                profile (str): Optional profile to be used in combination with
                    keygen (e.g., 'default', 'saml-pub', etc.)

        """
        self.metadata_src = None
        self.tmpdir = None

        if metadata_src:
            if aws.utils.is_s3_uri(metadata_src):
                # retrieve remote metadata to temporary local storage:
                self.tmpdir = tempfile.TemporaryDirectory()
                aws.ecco_aws_s3_cp.aws_s3_cp( src=metadata_src, dest=self.tmpdir.name, **kwargs)
                self.metadata_src = os.path.join(self.tmpdir.name,os.path.basename(metadata_src))
            else:
                # just point to local metadata source:
                self.metadata_src = metadata_src


    @property
    def metadata( self):
        """Parse metadata source csv and return as pandas DataFrame.

        """
        return pd.read_csv( self.metadata_src)


    def __del__(self):
        """Remove temporary metadata storage when ECCOPODAACMetadata instance
        goes out of scope or is explicitly deleted.

        """
        try:
            self.tmpdir.cleanup()
        except:
            pass

