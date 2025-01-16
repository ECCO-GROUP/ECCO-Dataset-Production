#!/usr/bin/env python
"""Python wrappers for CLI-driven AWS S3 SYNC operations.

"""
import logging
import os
import subprocess
import sys
import time

from . import ecco_aws

SLEEP_SECONDS = 5


def sync_local_to_remote( src=None, dest=None, nproc=1, dryrun=False,
    log_level=None, **kwargs):
    """Functional wrapper for multiprocess 'aws s3 sync <local> <s3uri>'

    Args:
        src (str): Source location (local path).
        dest (str): Destination location (AWS S3 URI).
        nproc (int):  Maximum number of local-remote sync processes.
        dryrun (bool): Set AWS S3 CLI argument '--dryrun'.
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on the invocation context, additional arguments that
            may be necessary include:
            keygen (str): If aws_s3_sync is invoked within an SSO environment,
                keygen can be used to provide the name of a requried federated
                login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64). Note that keygen is
                not necessary if aws_s3_sync is invoked within an AWS
                IAM-managed application.
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'saml-pub', 'default', etc.)

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    if kwargs.get('keygen',None):
        # update login credentials:
        cmd = [kwargs['keygen']]
        print(f'cmd: {cmd}')
        if kwargs.get('profile',None):
            cmd.extend(['--profile', kwargs['profile']])
            print(f'cmd: {cmd}')
        log.info("updating credentials using '%s' ...", cmd)
        try:
            subprocess.run(cmd,check=True)
            #subprocess.run([keygen,'-c'],check=True)
        except subprocess.CalledProcessError as e:
            log.error(e)
            sys.exit(1)
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
                    dest_s3uri]             # "destination"
                if kwargs.get('profile',None):
                    cmd.extend(['--profile',kwargs['profile']])
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


def sync_remote_to_remote_or_local( src=None, dest=None,
    dryrun=False, log_level=None, **kwargs):
    """Functional wrapper for either 'aws s3 sync <s3uri> <s3uri>' or
    'aws s3 sync <s3uri> <local>'

    Args:
        src (str): Source location (AWS S3 URI).
        dest (str): Destination location (local path or AWS S3 URI).
        nproc (int):  Maximum number of local-remote sync processes.
        dryrun (bool): Set AWS S3 CLI argument '--dryrun'.
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on the invocation context, additional arguments that
            may be necessary include:
            keygen (str): If aws_s3_sync is invoked within an SSO environment,
                keygen can be used to provide the name of a requried federated
                login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64). Note that keygen is
                not necessary if aws_s3_sync is invoked within an AWS
                IAM-managed application.
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'saml-pub', 'default', etc.)

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    if kwargs.get('keygen',None):
        # update login credentials:
        cmd = [kwargs['keygen']]
        if kwargs.get('profile',None):
            cmd.extend(['--profile', kwargs['profile']])
        log.info("updating credentials using '%s' ...", cmd)
        try:
            subprocess.run(cmd,check=True)
            #subprocess.run([keygen,'-c'],check=True)
        except subprocess.CalledProcessError as e:
            log.error(e)
            sys.exit(1)
        log.info('...done')

    cmd = [ 'aws', 's3', 'sync', src, dest]
    if kwargs.get('profile',None):
        cmd.extend(['--profile',kwargs['profile']])
    if dryrun:
        cmd.append('--dryrun')

    log.info('invoking subprocess: %s', cmd)
    p = subprocess.Popen( cmd)
    p = subprocess.Popen( cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO: although the following works fine for a sync remote->remote, for
    # some reason, p.wait() never returns from a sync remote->local
    rtn = p.wait()
    if not rtn:
        # normal termination:
        log.info('%s', bytes.decode(p.stdout.read()))
    else:
        # error return:
        log.info('%s', bytes.decode(p.stderr.read()))
    log.info('subprocess returned %d', rtn)


def aws_s3_sync(
    src=None, dest=None, nproc=1, dryrun=False, log_level=None, **kwargs):
    """Top-level functional wrapper for 'aws s3 sync' operations.

    Args:
        src (str): Source location (local path or AWS S3 URI).
        dest (str): Destination location (local path or AWS S3 URI).
        nproc (int):  Maximum number of local-remote sync processes.
        dryrun (bool): Set AWS S3 CLI argument '--dryrun'.
        log_level (str): Optional local logging level ('DEBUG', 'INFO',
            'WARNING', 'ERROR' or 'CRITICAL').  If called by a top-level
            application, the default will be that of the parent logger ('edp'),
            or 'WARNING' if called in standalone mode.
        **kwargs: Depending on the invocation context, additional arguments that
            may be necessary include:
            keygen (str): If aws_s3_sync is invoked within an SSO environment,
                keygen can be used to provide the name of a requried federated
                login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64). Note that keygen is
                not necessary if aws_s3_sync is invoked within an AWS
                IAM-managed application.
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'saml-pub', 'default', etc.)

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    log.info('aws_s3_sync called with the following arguments:')
    log.info('src: %s', src)
    log.info('dest: %s', dest)
    log.info('nproc: %s', nproc)
    log.info('dryrun: %s', dryrun)
    log.info('log_level: %s', log_level)
    if kwargs.get('keygen',None):
        log.info('keygen: %s', kwargs['keygen'])
    if kwargs.get('profile',None):
        log.info('profile: %s', kwargs['profile'])

    if not ecco_aws.is_s3_uri(src) and ecco_aws.is_s3_uri(dest):
        # upload:
        sync_local_to_remote( src, dest, nproc, dryrun, log_level, **kwargs)

    elif ecco_aws.is_s3_uri(src) and ecco_aws.is_s3_uri(dest):
        # remote sync:
        sync_remote_to_remote_or_local( src, dest, dryrun, log_level, **kwargs)

    elif ecco_aws.is_s3_uri(src) and not ecco_aws.is_s3_uri(dest):
        # download:
        sync_remote_to_remote_or_local( src, dest, dryrun, **kwargs)

    else:
        errstr = f'cannot sync {src} to {dest}'
        log.error(errstr)
        sys.exit(errstr)
 
