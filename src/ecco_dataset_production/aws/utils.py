"""ECCO Dataset Production AWS utility layer

"""

import re
import subprocess


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


def update_login_credentials( **kwargs):
    """If AWS IAM Identity Center (successor to AWS Single Sign-On) is being
    used to manage AWS access, check/update login credentials via a call to an
    institutionally-provided keygen routine.

    Args:
        **kwargs: Depending on run context:
            keygen (str):
                (Path and) name of federated login key generation script (e.g.,
                /usr/local/bin/aws-login.darwin.universal)
            profile (str): Optional profile name to be used in combination
                with keygen (e.g., 'saml-pub', 'default', etc.)
            log (logging.RootLogger): logging.getLogger() instance.

    """
    keygen  = kwargs.get('keygen')
    profile = kwargs.get('profile')
    log     = kwargs.get('log')

    if keygen:
        # running in SSO environment; check/update login credentials:
        cmd = [kwargs['keygen']]
        if profile:
            cmd.extend(['--profile', kwargs['profile']])
        if log:
            log.debug("updating credentials using '%s' ...", cmd)
        try:
            subprocess.run(cmd,check=True)
        except subprocess.CalledProcessError as e:
            if log:
                log.error(e)
            sys.exit(1)
        if log:
            log.debug('...done')
    else:
        log.warning("no keygen provided; AWS credentials cannot be checked/updated")

