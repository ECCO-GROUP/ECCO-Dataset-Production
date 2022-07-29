import sys
import subprocess
from pathlib import Path


# ==========================================================================================================================
# CREDENTIALS
# ==========================================================================================================================
def get_credentials_helper():
    # Get credentials for AWS from "~/.aws/credentials" file
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


def get_aws_credentials(credential_method): 
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

        credentials = get_credentials_helper()
    except:
        print(f'Unable to run script to get credentials ("{aws_credential_path}"). Exiting')
        sys.exit()

    return credentials
