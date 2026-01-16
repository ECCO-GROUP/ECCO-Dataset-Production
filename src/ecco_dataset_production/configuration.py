"""
"""

from collections import UserDict
import logging
import os
import tempfile
import yaml

from . import aws

log = logging.getLogger('edp.'+__name__)


class ECCODatasetProductionConfig(UserDict):
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

            log.debug('Using configuration data per "%s":', cfgfile)
            for k,v in self.items():
                log.debug(' %s: %s', k, v)


    def __getitem__(self,key):
        """In case of an undefined key, log a WARNING and return an empty string
        ('') instead of raising a key error.

        """
        # Since standard dict behaviour in the case of an undefined key is to
        # raise an exception with the (uninformative) message f"{key}", simply
        # log a WARNING and return an empty string instead, since omitted keys
        # may be intentional (such as is the case with intentionally undefined
        # metadata).

        try:
            return self.data[key]
        except:
            log.warning(f'Undefined configuration parameter reference, "%s".', key)
            return ''

