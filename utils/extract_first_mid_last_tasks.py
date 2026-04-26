#!/usr/bin/env python3

"""Create abbreviated task lists.

For various purposes such as incremental testing, extract first, midpoint, and
last tasks from a tasklist(s).

"""

import argparse
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

import ecco_dataset_production as edp


logging.basicConfig(
    format = '%(levelname)-10s %(asctime)s %(message)s')
log = logging.getLogger('edp')


def create_parser():
    """Set up list of command-line arguments to extract_first_mid_last_tasks.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""Create abbreviated tasklist(s) by extracting first,
            midpoint, and last task descriptors from a single tasklist, or the
            same, from a collection of tasklists.""",
        epilog="""Although valid for any tasklist or collection of tasklists,
            this routine is most useful in an incremental testing context, with
            each tasklist containing only a single granule/dataset type.""")
    parser.add_argument('--tasklist', nargs='+',  help="""
        (Path and) filename of ECCO Dataset Production tasklist(s) in json file
        format. If any tasklist is a path only (either local directory or AWS S3
        bucket/prefix), all tasklists at the destination will be processed
        (*.json files assumed).""")
    parser.add_argument('--dest', default='./', help="""
        Abbreviated task list(s) output destination, either local directory or
        AWS S3 bucket/prefix (default: %(default)s).""")
    parser.add_argument('--postfix', default='_first_mid_last', help="""
        Identifying string to be appended to the task list file stem to form
        output tasklist name(s) (default: %(default)s; for example,
        tasklist_abc.json -> tasklist_abc%(default)s.json).""")
    parser.add_argument('--keygen', help="""
        If tasklist or dest reference an S3 bucket and if running in an
        institutionally-managed AWS IAM Identity Center (SSO) environment
        domain, name of federated login key generation script (e.g.,
        /usr/local/bin/aws-login.darwin.universal).""")
    parser.add_argument('--profile', help="""
        If taskist or dest reference an S3 bucket and if running in an SSO
        environment, AWS credential profile name (e.g., 'saml-pub', 'default',
        etc.).""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s).""")

    return parser


def extract_first_mid_last_tasks(
    tasklist=None, dest="./", postfix="_first_mid_last",
    keygen=None, profile=None, log_level=None):
    """Create abbreviated tasklist(s) by extracting first, midpoint, and last
    task descriptors from a single tasklist, or collection of tasklists.

    Args:
        tasklist (list): List of (path and) filename of ECCO Dataset Production
            tasklist(s) in json file format. If tasklist is a path only (either
            local directory or AWS S3 bucket/prefix, the latter ending in a
            '/'), all tasklists at the destination will be processed.
        dest (str): Abbreviated task list(s) output destination, either local
            directory or AWS S3 bucket/prefix, the latter ending in a '/'.
        postfix (str): Identifying string to be appended to the task list file
            stem to form output tasklist name(s) (i.e., for the default postfix,
            tasklist_abc.json -> tasklist_abc_first_mid_last.json).
        keygen (str): If tasklist or dest reference an S3 bucket and if running
            in an institutionally-managed AWS IAM Identity Center (SSO)
            environment, name of federated login key generation script (e.g.,
            /usr/local/bin/aws-login-pub.darwin.amd64).
        profile (str): If taskist or dest reference an S3 bucket and if running
            in an SSO environment, AWS credential profile name (e.g.,
            'saml-pub', 'default', etc.)
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    Returns:
        Abbreviated tasklist(s) containing first, midpoint, and last tasks, in
        json file format, written to dest.

    Raises:
        RuntimError if AWS S3-hosted tasklist string does not end in '/' (bucket
        and prefix designator) or '.json' (single task list).

    """
    # implementation notes:
    # - tasklist arg is a list that might contain multiple tasklists or tasklist
    #   directories
    # - if an item in the tasklist arg is a file, it is assumed to be a
    #   json-formatted file with a '.json' file extension; all other file types
    #   will be silently ignored (allows shell wildcard-generated input)
    # - dest is taken to be a single destination directory

    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    if any([edp.aws.utils.is_s3_uri(tl) for tl in tasklist]) or edp.aws.utils.is_s3_uri(dest):
        edp.aws.utils.update_login_credentials( keygen=keygen, profile=profile, log=log)

    with (
        tempfile.TemporaryDirectory() as tmpdir_in,
        tempfile.TemporaryDirectory() as tmpdir_out):

        # gather all tasklist input locally:

        for tl in tasklist:
            if edp.aws.utils.is_s3_uri(tl):
                if re.search(r'/$',tl):
                    edp.aws.ecco_aws_s3_sync.aws_s3_sync(
                        tl, tmpdir_in, profile=profile) # no keygen -> don't update credentials
                elif re.search(r'.json$',tl):
                    edp.aws.ecco_aws_s3_cp.aws_s3_cp(
                        tl, tmpdir_in, profile=profile) # no keygen -> don't update credentials
                else:
                    errmsg = "tasklist(s) is(/are) hosted on AWS S3, but string does not end in a '/' or '.json'; no data copied"
                    log.error(errmsg)
                    raise RuntimeError(errmsg)
            else:
                # for simplicity, just copy local tasklist(s) to tmpdir_in:
                if Path(tl).is_file() and Path(tl).suffix=='.json':
                    shutil.copy(Path(tl).resolve(),tmpdir_in)
                elif Path(tl).is_dir():
                    for f in Path(tl).iterdir():
                        if f.suffix == '.json':
                            shutil.copy(f.resolve(),tmpdir_in)

        # first/mid/last tasklists:

        for tl in Path(tmpdir_in).iterdir():
            if tl.suffix=='.json':  # AWS-copied or synced objects might be other than '.json'
                log.debug('Extracting first/mid/last from %s ...',tl.name)
                _tl = json.load(open(tl.resolve()))
                if not isinstance(_tl,list) or len(_tl)==0:
                    log.warning('Tasklist %s is not a non-empty list; skipping',tl.name)
                    continue

                if len(_tl)==1 or len(_tl)==2:
                    _tl_short = _tl
                else:
                    _tl_short = [_tl[0], _tl[len(_tl)//2], _tl[-1]]

                json.dump(_tl_short,open(Path(tmpdir_out).joinpath(tl.stem+postfix+tl.suffix),'w'),indent=4)

        #for tl in Path(tmpdir_out).iterdir():
        #    print(f'{tl}:')
        #    print(json.dumps(json.load(open(tl.resolve())),indent=4))

        # copy abbreviated tasklist(s) to destination:

        if edp.aws.utils.is_s3_uri(dest):
            edp.aws.ecco_aws_s3_sync.aws_s3_sync(
                tmpdir_out, dest, profile=profile) # no keygen -> don't update credentials
        else:
            os.makedirs(dest,exist_ok=True)
            for f in Path(tmpdir_out).iterdir():
                shutil.copy(f.resolve(),dest)


def main():
    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])
    extract_first_mid_last_tasks(
        args.tasklist, args.dest, args.postfix,
        args.keygen, args.profile, args.log_level)


if __name__=="__main__":
    main()

