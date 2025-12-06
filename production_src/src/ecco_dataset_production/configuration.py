"""
"""

import logging
import os
import tempfile
import yaml

from . import aws

log = logging.getLogger('edp.'+__name__)


class ECCODatasetProductionConfig(dict):
    """Wrapper class for storage of, and basic operations on, ECCO Dataset
    Production configuration data.

    Args:
        cfgfile (str): (Path and) filename of configuration file (yaml format),
            or similar remote location given by AWS S3 bucket/prefix/filename.
        **kwargs: If cfgfile references an AWS S3 endpoint and if running within
            an institutionally-managed AWS IAM Identity Center (SSO)
            environment, additional arguments that may be necessary include:
            keygen (str): Federated login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64).
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'default', 'saml-pub', etc.)

    Attributes:
        cfgfile (str): Local store of cfgfile input string.

    """
    def __init__( self, cfgfile=None, **kwargs):
        super().__init__()
        if cfgfile:
            self.cfgfile = cfgfile
            if aws.ecco_aws.is_s3_uri(self.cfgfile):
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir_and_fname = os.path.join(tmpdir,os.path.basename(self.cfgfile))
                    log.debug('Fetching %s to %s', self.cfgfile, tmpdir_and_fname)
                    aws.ecco_aws_s3_cp.aws_s3_cp(src=self.cfgfile,dest=tmpdir,**kwargs)
                    self.update(yaml.safe_load(open(tmpdir_and_fname)))
            else:
                self.update(yaml.safe_load(open(self.cfgfile)))

            for k,v in self.items():
                log.debug(' %s: %s', k, v)

