"""AWS S3 integration utilities.

This subpackage provides utilities for interacting with AWS S3 storage,
enabling the pipeline to read input data from and write output granules
to S3 buckets.

Supports institutionally-managed AWS IAM Identity Center (SSO) environments
through optional ``keygen`` and ``profile`` parameters.

Example:
    >>> from ecco_dataset_production.aws import ecco_aws
    >>> ecco_aws.is_s3_uri('s3://bucket/key')
    True
    >>> ecco_aws.is_s3_uri('/local/path')
    False

"""
#from . import ecco_aws
from . import ecco_aws_s3_cp
from . import ecco_aws_s3_sync
from . import utils
