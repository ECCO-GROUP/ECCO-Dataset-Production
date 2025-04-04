#!/usr/bin/env python3

import argparse
import logging

from .. import ecco_generate_dataproducts


# initialize root logger:
logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')


def create_parser():
    """Set up list of command-line arguments to generate_dataproducts.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--tasklist', help="""
        (Path and) name of json-formatted file containing list of ECCO dataset
        generation task descriptions.""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")
    parser.add_argument('--keygen', help="""
        If tasklist descriptors reference AWS S3 endpoints and if running in an
        institutionally-managed AWS IAM Identity Center (SSO) environment, (path
        and) name of federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--profile', help="""
        Optional profile name to be used in combination with keygen (e.g.,
        'saml-pub', 'default', etc.)""")

    return parser


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    # application-level logger:
    log = logging.getLogger('edp')
    log.setLevel(args.log_level)

    ecco_generate_dataproducts.generate_dataproducts(
        tasklist=args.tasklist,
        #log_level=args.log_level,  # logger hierarchy makes this redundant
        keygen=args.keygen, profile=args.profile)

