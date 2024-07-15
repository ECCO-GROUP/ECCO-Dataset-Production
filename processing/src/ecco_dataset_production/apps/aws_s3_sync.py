#!/usr/bin/env python

"""A Python command-line wrapper for AWS CLI S3 sync operations.  Supports all
sync modes: local-remote, remote-local, and remote-remote. Mulitprocessing is
supported for local-remote operations.

"""

import argparse
import logging
import os
import re
import subprocess
import sys
import time

from .. import ecco_aws_s3_sync

SLEEP_SECONDS = 5

logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')


def create_parser():
    """Set up list of command-line arguments to aws_s3_sync.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""A Python wrapper for AWS CLI S3 sync operations.
        Supports all sync modes: local-remote, remote-local, and remote-remote.
        Mulitprocessing is supported for local-remote operations.""",
        epilog="""Note: The values provided by the --src and --dest arguments
        must exist, as they will not be created by %(prog)s.  In other words, if
        either is a local directory, it must exist and, if it is an AWS S3 URI,
        that bucket/prefix must have been created by, e.g. 'aws s3 mb'.""")
    parser.add_argument('--src', default='.', help="""
        Source location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--dest', default='.', help="""
        Destination location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--nproc', type=int, default=1, help="""
        Maximum number of local-remote sync processes (default: %(default)s)""")
    parser.add_argument('--keygen', help="""
        If running in JPL domain, federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--profile', help="""
        If running in JPL domain, AWS credential profile name (e.g., 'saml-pub',
        'default', etc.)""")
    parser.add_argument('--dryrun', action='store_true', help="""
        Set AWS S3 CLI argument '--dryrun'""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")
    return parser


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    ecco_aws_s3_sync.aws_s3_sync( src=args.src, dest=args.dest,
        nproc=args.nproc, dryrun=args.dryrun, log_level=args.log_level,
        keygen=args.keygen, profile=args.profile)

