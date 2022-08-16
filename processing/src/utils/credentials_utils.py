"""
ECCO Dataset Production AWS credentials utilities

Author: Duncan Bark

Contains functions necessary for getting the user's AWS credentials

"""

import sys
import subprocess
from pathlib import Path


# ==========================================================================================================================
# GET CREDENTIALS
# ==========================================================================================================================
def get_aws_credentials(credential_method=None):
    """
    Get AWS credentials from the user via the specified method. If nothing is passed,
    return the credentials already present.

    Args:
        credential_method (optional, dict): Contains information for getting the users credentials including:
            'bash_filepath': Path to '/aws/src/utils/aws_login/update_AWS_cred_ecco_production.sh'
            'type': 'binary' or 'python', 'binary' will call the aws-login... binary file, 'python' will call the
                aws-login.py python file.
            'aws_login_file': File name of the login script to call (i.e. 'aws-login.py', 'aws-login.darwin.amd64', etc.)
            'region': AWS region

    Returns:
        credentials (dict): Dictionary containing contents of ~/.aws/credentials file
    """
    # if nothing is passsed, just return the credentials
    if credential_method == None:
        credentials = __get_credentials_helper()
    else:
        # credential method is a dict. with the 'region' and 'type'
        # type is one of binary or python:
        # both types call the "update_AWS_cred_ecco_production.sh" shell script,
        # but that script will call the aws-login.py python file or the "aws-log.{darwin/linux}.amd64"
        # binary file, depending on the type provided 
        bash_filepath = credential_method['bash_filepath']
        cred_type = credential_method['type']
        aws_login_file = credential_method['aws_login_file']
        aws_region = credential_method['region']
        login_file_dir = str(Path(bash_filepath).parent)
        try:
            subprocess.run([bash_filepath, cred_type, aws_login_file, aws_region, login_file_dir], check=True)

            credentials = __get_credentials_helper()
        except:
            print(f'Unable to run script to get credentials ("{bash_filepath}"). Exiting')
            sys.exit()

    return credentials


def __get_credentials_helper():
    """
    Get the AWS credentials from the ~/.aws/credentials file

    Args:
        None

    Returns:
        credentials (dict): Dictionary containing contents of ~/.aws/credentials file
    """
    # Parse the ~/.aws/credentials file present after running the get AWS credentials method(s)
    cred_path = Path.home() / '.aws/credentials'
    credentials = {}
    if cred_path.exists():
        with open(cred_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line == '':
                    break
                elif line[0] == '#':
                    credentials['expiration_date'] = line.split(' = ')[-1]
                elif line[0] == '[':
                    credentials['profile_name'] = line[1:-1]
                else:
                    name, value = line.split(' = ')
                    credentials[name] = value
    return credentials
