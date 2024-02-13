#!/usr/bin/env python

"""A Python wrapper for AWS CLI S3 sync operations.  Supports all sync modes:
local-remote, remote-local, and remote-remote. Mulitprocessing is supported for
local-remote operations."""

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
        description="""A Python wrapper for AWS CLI S3 sync operations.
        Supports all sync modes: local-remote, remote-local, and remote-remote.
        Mulitprocessing is supported for local-remote operations.""",
        epilog="""Note: The values provided by the --src and --dest arguments
        must exist, as they will not be created by %(prog)s.  In other words, if
        either is a local directory, it must exist and, if it is an AWS S3 URI,
        that bucket must have been created by, e.g. 'aws s3 mb'.""")
    parser.add_argument('--src', default='.', help="""
        Source location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--dest', default='.', help="""
        Destination location (local path or AWS S3 URI) (default: "%(default)s")""")
    parser.add_argument('--nproc', type=int, default=1, help="""
        Maximum number of local-remote sync processes (default: %(default)s)""")
    parser.add_argument('--keygen', help="""
        JPL federated login key generation script (e.g.,
        /usr/local/bin/aws-login-pub.darwin.amd64)""")
    parser.add_argument('--dryrun', action='store_true', help="""
        Set AWS S3 CLI argument '--dryrun'""")
    parser.add_argument('-l','--log', dest='log_level',
        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
        default='WARNING', help="""
        Set logging level (default: %(default)s)""")
    return parser


def is_s3_uri(path_or_uri_str):
    """Determines whether or not input string is an AWS S3Uri.

    Args:
        path_or_uri_str (str): Input string.

    Returns:
        True if string matches 's3://', False otherwise.
    """
    if re.search( r's3:\/\/', path_or_uri_str, re.IGNORECASE):
        return True
    else:
        return False


def sync_local_to_remote( src, dest, nproc, keygen, dryrun):
    """Functional wrapper for multiprocess 'aws s3 sync <local> <s3uri>'

    """
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

                # there's room in the queue, and no subdirectory syncs are
                # running; submit:

                cmd = [ 'aws', 's3', 'sync',
                    dirpath,                # "source"
                    dest_s3uri,             # "destination"
                    '--profile','saml-pub']
                if dryrun:
                    cmd.append('--dryrun')
                log.info('invoking subprocess: %s', cmd)
                proclist.append(subprocess.Popen(
                    cmd
                    #stdout=subprocess.PIPE,    <- buffer overflows and hangs
                    #stderr=subprocess.PIPE     <- for "real" ecco syncs
                    ))
                this_proc_is_running = True

            else:

                # the queue is either full, or a subdirectory sync is blocking,
                # wait for a bit:
                time.sleep(SLEEP_SECONDS)

            if this_proc_is_running:

                # try for the next:
                break

    # wait for all sync processes to complete and produce final diagnostics:

    for p in proclist:
        p.wait()
        # as per above comment, pipe buffers overflow for large sync operations;
        # just rely on regular sys output.
        #stdout,stderr = p.communicate()     # read to eof and wait for returncode
        log.info('sync process %s:', p.args)
        log.info('   rtn:    %d', p.returncode)
        #log.info('   stdout: %s', bytes.decode(stdout))
        #log.info('   stderr: %s', bytes.decode(stderr))


def sync_remote_to_remote_or_local(  src, dest, keygen, dryrun):
    """Functional wrapper for either 'aws s3 sync <s3uri> <s3uri>' or
    'aws s3 sync <s3uri> <local>'

    """
    # update login credentials:
    log.info('updating credentials...')
    subprocess.run(keygen)
    log.info('...done')

    cmd = [ 'aws', 's3', 'sync', src, dest,
        '--profile','saml-pub']
    if dryrun:
        cmd.append('--dryrun')

    log.info('invoking subprocess: %s', cmd)
    p = subprocess.Popen( cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    rtn = p.wait()
    log.info('subprocess returned %d', rtn)

    if not rtn:
        # normal termination:
        log.info('%s', bytes.decode(p.stdout.read()))
    else:
        # error return:
        log.info('%s', bytes.decode(p.stderr.read()))


def aws_s3_sync(
    src=None, dest=None, nproc=None, keygen=None, dryrun=None, log_level=None):
    """Top-level functional wrapper for 'aws s3 sync' operations.

    Args:
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').

    """
    logging.basicConfig(
        format = '%(levelname)-10s %(asctime)s %(message)s',
        level=log_level)
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
        # upload:
        sync_local_to_remote( src, dest, nproc, keygen, dryrun)

    elif is_s3_uri(src) and is_s3_uri(dest):
        # remote sync:
        sync_remote_to_remote_or_local( src, dest, keygen, dryrun)

    elif is_s3_uri(src) and not is_s3_uri(dest):
        # download:
        sync_remote_to_remote_or_local( src, dest, keygen, dryrun)

    else:
        errstr = f'cannot sync {src} to {dest}'
        log.error(errstr)
        sys.exit(errstr)
 

def main():
    """Command-line entry point.

    """
    parser = create_parser()
    args = parser.parse_args()

    aws_s3_sync(
        args.src, args.dest, args.nproc, args.keygen, args.dryrun, args.log_level)


if __name__=='__main__':
    main()

