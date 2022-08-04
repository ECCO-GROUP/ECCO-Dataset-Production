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
            'region': AWS region
            'type': 'binary' or 'bash', 'binary' will call the aws-login... binary file, 'bash' will call the
                update_AWS_cred... shell script.
            'aws_credential_path': Path to '/aws/src/utils/aws_login/{file to call}'

    Returns:
        credentials (dict): Dictionary containing contents of ~/.aws/credentials file
    """
    # if nothing is passsed, just return the credentials
    if credential_method == None:
        credentials = __get_credentials_helper()
    else:
        # credential method is a dict. with the 'region' and 'type'
        # type is one of binary or bash 
        # if binary then aws_credential_path needs to point to the binary file
        # if bash then aws_credential_path needs to point to the bash script
        aws_region = credential_method['region'] 
        aws_credential_path = credential_method['aws_credential_path']
        try:
            if credential_method['type'] == 'binary':
                subprocess.run([aws_credential_path, '-r', f'{aws_region}'], check=True)
            elif credential_method['type'] == 'bash':
                subprocess.run([aws_credential_path], check=True)

            credentials = __get_credentials_helper()
        except:
            print(f'Unable to run script to get credentials ("{aws_credential_path}"). Exiting')
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
