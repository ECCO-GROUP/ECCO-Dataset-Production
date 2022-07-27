import sys
import subprocess
from pathlib import Path
from collections import defaultdict


# ==========================================================================================================================
# CALCULATE JOBS
# ==========================================================================================================================
def calculate_all_jobs(latlon_groupings, native_groupings):
    all_groupings = {'latlon':latlon_groupings, 'native':native_groupings}
    jobs = defaultdict(list)
    for product_type, groupings in all_groupings.items():
        for i, grouping in enumerate(groupings):
            freqs = grouping['frequency'].split(', ')
            for freq in freqs:
                if grouping['dimension'] == '2D':
                    jobs[f'2D_{product_type}'].append([i, product_type, freq, 'all'])
                if grouping['dimension'] == '3D':
                    jobs[f'3D_{product_type}'].append([i, product_type, freq, 'all'])

    all_jobs = []
    all_jobs.extend(jobs['3D_native'])
    all_jobs.extend(jobs['3D_latlon'])
    all_jobs.extend(jobs['2D_native'])
    all_jobs.extend(jobs['2D_latlon'])

    return all_jobs


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
