#!/usr/bin/env python

"""A Python multiprocess wrapper for AWS CLI S3 sync operations.  Supports all
sync modes: local-remote, remote-local, and remote-remote."""

import argparse
import logging
import os
import re
import subprocess
import sys
import time

SLEEP_SECONDS = 5


def create_parser():
    """Set up list of command-line arguments to aws_s3_sync.

    Returns:
        argparser.ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(
        description="""A Python multiprocess wrapper for AWS CLI S3 sync operations""")
    parser.add_argument('--src', default='.', help="""
        Source location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--dest', default='.', help="""
        Destination location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--nproc', type=int, default=1, help="""
        Maximum number of sync processes (default: %(default)s)""")
    parser.add_argument('--keygen', help="""
        JPL federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--dryrun', action='store_true', help="""
        Set AWS S3 CLI argument '--dryrun'""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='INFO', help="""
        Set logging level (default: %(default)s)""")
    return parser


def is_s3_uri(path_or_uri_str):
    """Determines whether or not input string is an AWS S3Uri.

    Args:
        path_or_uri_str (str): Input string.

    Returns:
        True if string matches 's3://', False otherwise.
    """
    if re.search( 's3:\/\/', path_or_uri_str, re.IGNORECASE):
        return True
    else:
        return False


def sync_local_to_remote( src, dest, nproc, keygen, dryrun):
    """Functional wrapper for 'aws s3 sync <local> <s3uri>'

    """
    log = logging.getLogger('ecco_dataset_production')

    # update login credentials:
    log.info('updating credentials...')
    subprocess.run(keygen)
    log.info('...done')

    # list of submitted processes:
    proclist = []

    # parallelize at subdirectory level(s), working our way up the tree (note
    # that is incurs some overhead as AWS S3 checks sync status of previously-
    # synced subdirectores/folders, but the assumption is that this overhead is
    # more than compensated for by enabling nproc simultaneous syncs):

    for (dirpath,dirnames,_) in os.walk(src,topdown=False):

        this_proc_is_running = False

        # s3uri destination for this directory:
        dest_folder = os.path.relpath(dirpath,src).strip('./')
        dest_s3uri = os.path.join(dest,dest_folder)

        # s3uri destinations for this directory's subdirectories:
        dest_sub_s3uri = []
        for dirname in dirnames:
            dest_sub_folder = os.path.relpath(
                os.path.join(dirpath,dirname),src).strip('./')
            dest_sub_s3uri.append(os.path.join(dest,dest_sub_folder))

        log.debug('destination s3 uri: %s', dest_s3uri)
        log.debug('destination sub s3 uri(s): %s', dest_sub_s3uri)

        while not this_proc_is_running:

            # if none of this directory's (possible) subdirectories are
            # still syncing (blocking_procs>0) and if a process slot is
            # available (running_procs<nproc), submit, otherwise wait:

            blocking_procs  = 0
            running_procs   = 0
            completed_procs = 0
            for p in proclist:
                p.poll()
                if p.returncode is not None:
                    completed_procs += 1
                else:
                    running_procs += 1
                    # are any of these blocking?
                    for s3uri in dest_sub_s3uri:
                        if s3uri in p.args:
                            blocking_procs += 1

            log.debug('running_procs: %d, completed_procs: %d, blocking_procs: %d',
                running_procs, completed_procs, blocking_procs)

            if running_procs<nproc and not blocking_procs>0:

                # there's room in the queue, and no subdirectory syncs are running; submit:

                cmd = [ 'aws', 's3', 'sync',
                    dirpath,                # "source"
                    dest_s3uri,             # "destination"
                    '--profile','saml-pub']
                if dryrun:
                    cmd.append('--dryrun')
                log.info('invoking subprocess: %s', cmd)
                proclist.append(subprocess.Popen(
                    cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE))
                this_proc_is_running = True

            else:
                # the queue is either full, or a subdirectory sync is blocking,
                # wait for a bit:
                time.sleep(SLEEP_SECONDS)

            if this_proc_is_running:
                # try for the next:
                break

    for p in proclist:
        print(p.returncode)
        print(p.args)
        stdout,stderr = p.communicate()
        print(stdout)
        print(stderr)


def aws_s3_sync(
    src=None, dest=None, nproc=None, keygen=None, dryrun=None, log_level=None):
    """Top-level functional wrapper for 'aws s3 sync' operations.

    """
    log = logging.getLogger('ecco_dataset_production')
    log.info('aws_s3_sync called with the following arguments:')
    log.info('src: %s', src)
    log.info('dest: %s', dest)
    log.info('nproc: %s', nproc)
    log.info('log_level: %s', log_level)

    if not keygen:
        errstr = f'{sys._getframe().f_code.co_name} requires key generation script input'
        log.error(errstr)
        sys.exit(errstr)

    if not is_s3_uri(src) and is_s3_uri(dest):
        # AWS S3 upload:
        ## locally-sourced data implies AWS S3 upload:
        sync_local_to_remote( src, dest, nproc, keygen, dryrun)

    # TODO: remote-local, remote-remote options.


def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    logging.basicConfig(
        format = '%(levelname)-10s %(asctime)s %(message)s',
        level=args.log_level)

    aws_s3_sync(
        args.src, args.dest, args.nproc, args.keygen, args.dryrun, args.log_level)


if __name__=='__main__':
    main()

