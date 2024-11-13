#!/usr/bin/env python
"""Python wrapper for CLI-driven AWS S3 CP operations.

"""
import logging
import subprocess
import sys


def aws_s3_cp(
    src=None, dest=None, dryrun=False, log_level=None, **kwargs):
    """Top-level functional wrapper for 'aws s3 cp' operations.

    Args:
        src (str): Source location (local path or AWS S3 URI).
        dest (str): Destination location (local path or AWS S3 URI).
        dryrun (bool): Set AWS S3 CLI argument '--dryrun'.
        log_level (str): log_level choices per Python logging module
            ('DEBUG','INFO','WARNING','ERROR' or 'CRITICAL'; default='WARNING').
        **kwargs: Depending on the invocation context, additional arguments that
            may be necessary include:
            keygen (str): If aws_s3_cp is invoked within an SSO environment,
                keygen can be used to provide the name of a requried federated
                login key generation script (e.g.,
                /usr/local/bin/aws-login-pub.darwin.amd64). Note that keygen is
                not necessary if aws_s3_cp is invoked within an AWS IAM-managed
                application.
            profile (str): Optional profile to be used in combination with
                keygen (e.g., 'saml-pub', 'default', etc.)

    """
    log = logging.getLogger('edp.'+__name__)
    if log_level:
        log.setLevel(log_level)

    log.debug('aws_s3_cp called with the following arguments:')
    log.debug('src: %s', src)
    log.debug('dest: %s', dest)
    log.debug('dryrun: %s', dryrun)
    log.debug('log_level: %s', log_level)
    if kwargs.get('keygen',None):
        log.debug('keygen: %s', kwargs['keygen'])
    if kwargs.get('profile',None):
        log.debug('profile: %s', kwargs['profile'])

    if kwargs.get('keygen',None):
        # update login credentials:
        cmd = [kwargs['keygen']]
        if kwargs.get('profile',None):
            cmd.extend(['--profile', kwargs['profile']])
        log.debug("updating credentials using '%s' ...", cmd)
        try:
            subprocess.run(cmd,check=True)
        except subprocess.CalledProcessError as e:
            log.error(e)
            sys.exit(1)
        log.debug('...done')

    cmd = [ 'aws', 's3', 'cp', src, dest]
    if kwargs.get('profile',None):
        cmd.extend(['--profile',kwargs['profile']])
    if dryrun:
        cmd.append('--dryrun')

    log.debug('invoking subprocess: %s', cmd)
    p = subprocess.Popen( cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    rtn = p.wait()
    log.debug('subprocess returned %d', rtn)

    if not rtn:
        # normal termination:
        log.debug('%s', bytes.decode(p.stdout.read()))
    else:
        # error return:
        log.error('%s', bytes.decode(p.stderr.read()))

