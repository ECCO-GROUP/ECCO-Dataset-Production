"""
ECCO Dataset Production upload local files to S3

Author: Duncan Bark

Contains functions for large scale file uploading from a "local" directory to an AWS S3 bucket.
This method uploads one file at a time and can be very slow. See the "aws_sync_directories_to_S3.sh"
script for a quicker option.

OUTDATED -- DO NOT USE -- WORKS BUT HAS SPECIFIC PATHS AND FILE STRUCTURE REQUIREMENTS NOT GENERALIZED

"""

import sys
import glob
import time
import boto3
import pprint
import argparse
import platform
from pathlib import Path

main_path = Path(__file__).parent.parent.resolve()
sys.path.append(f'{main_path / "aws" / "src" / "utils"}')
import credentials_utils as credentials_utils

def create_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--upload', default=False, action='store_true',
                        help='Upload photos form source_dir to bucket. Not including does a "dry-run".')

    parser.add_argument('--bucket', default=None, required=True,
                        help='Bucket name to upload files to')

    parser.add_argument('--source_dir', default=None, required=True,
                        help='Source directory to download photos from (i.e. "/home/data/ECCO-DATA/V4r4")')

    parser.add_argument('--number_of_files', default=0, type=int,
                        help='Number of files to upload')

    parser.add_argument('--by_field', default=False, action='store_true',
                        help='Upload "number_of_files" files from EACH field in provided directory')

    parser.add_argument('--force_reconfigure', default=False, action='store_true',
                        help='Force code to re-run code to get AWS credentials')

    parser.add_argument('--credential_method_type', default='', required=True,
                        help='either binary or python')

    parser.add_argument('--bash_filepath', default='', required=False, 
                        help='full path to executable bash script that renews the aws creds')

    parser.add_argument('--region', default='us-west-2',
                        help='AWS Region')
    return parser


# ==========================================================================================================================
# COLLECT FILES FROM S3
# ==========================================================================================================================
def get_files_on_s3(s3, 
                    bucket, 
                    prefix, 
                    check_list=True):
    # Collect files currently on the S3 bucket
    # If, when uploading, the name exists in this list, skip it.
    files_on_s3 = []
    StartAfter = ''
    if check_list:
        while True:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix,
                     MaxKeys=20000, StartAfter=StartAfter)

            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                print(f'Unable to collect objects in bucket {bucket}')
                return -1
            else:
               if 'Contents' not in response:
                  break
               else:
                  files_on_s3.extend([k['Key'] for k in response['Contents']])
                  StartAfter = files_on_s3[-1] 
       
    return files_on_s3


# ==========================================================================================================================
# UPLOAD FILES TO S3
# ==========================================================================================================================
def upload_S3(source_path, 
              bucket, 
              number_of_files, 
              by_field, 
              credential_method, 
              check_list=True, 
              upload=False):
    # Upload provided file to the provided bucket via the provided credentials.

    #parse the ~/.aws/credentials file and puts values into a
    # dictionary -- DOES NOT MAKE NEW CREDENTIALS
    credentials = credentials_utils.get_aws_credentials()

    # Setup S3 bucket client via boto3
    boto3.setup_default_session(profile_name=credentials['profile_name'])
    s3 = boto3.client('s3')

    # Collect list of files within source_path
    # data_files = glob.glob(f'{source_path}/**/*.data', recursive=True)
    # num_files = len(data_files)
    fields = sorted(glob.glob(f'{source_path}/*/*'))
    if by_field:
        num_files = len(fields) * number_of_files
    else:
        num_files = number_of_files

 
    ticking_time_bomb = time.time() 
    # Upload photos from source_path to S3 bucket
    upload_resp = input(f'\nAbout to upload {num_files} files, from {source_path}, to bucket {bucket}. Continue? y/n \n')
    if upload_resp.strip().lower() == 'y':
        print(f'\n' + '='*55)
        for field_path in fields:
            prefix = 'diags_all' + field_path.split('diags_all')[-1]
            print(f'>>> I think my S3 path is : {prefix}')
           
            # get the files on S3 in the provided bucket that match this file's prefix
            files_on_s3 = get_files_on_s3(s3, bucket, prefix, check_list=True)
            if len(files_on_s3) > 0:
               print(f'files on S3:\n{files_on_s3[0]}\n{files_on_s3[-1]}')

            print(f'\nUploading files for field "{field_path.split("/")[-1]}"')
    #       matches [m][e]ta or [d][a]ta
            data_files = sorted(glob.glob(f'{field_path}/*.[dm][ae]ta'))
            for i, data_file in enumerate(data_files):
                if i == number_of_files:
                    break
                # print(f'\t{i+1:7} / {number_of_files}', end='\r')
                name = f'diags_all/{data_file.split("/diags_all/")[-1]}'
                if i == 0: 
                    print(f'to S3 <<{bucket}>>  {Path(name).parent}')

                print(f'\tUploading {data_file.split("/")[-1]}', end='')
                if name in files_on_s3:
                    print(' -- Present')
                    continue
                try:
                    if upload:
                        response = s3.upload_file(data_file, bucket, name)
                    # print(f'Uploaded {data_file} to bucket {bucket}')
                    print(' -- Successful')
                except:
                    print(' -- Failed')
                    # print(f'Unable to upload file {data_file} to bucket {bucket}')

                tick_tick_tick = time.time() - ticking_time_bomb
                # once per hour, refresh the credentials
                if tick_tick_tick > 3600.0:
                   print(f'... time elapsed {tick_tick_tick}s')
                   print(f'... getting new credentials!') 
                   credentials = credentials_utils.get_aws_credentials(credential_method)
                   ticking_time_bomb = time.time()
        print('\n' + '='*55)
    else:
        return 0

    return 1


# ==========================================================================================================================
# "MAIN" FUNCTION
# ==========================================================================================================================
if __name__ == "__main__":
    # Parse command line arguments
    parser = create_parser()
    args = parser.parse_args()
    dict_key_args = {key: value for key, value in args._get_kwargs()} 

    bucket = dict_key_args['bucket']
    source_dir = dict_key_args['source_dir']
    number_of_files = dict_key_args['number_of_files']
    by_field = dict_key_args['by_field']
    force_reconfigure = dict_key_args['force_reconfigure']
    region = dict_key_args['region']
    upload = dict_key_args['upload']
    credential_method_type = dict_key_args['credential_method_type']
    bash_filepath = dict_key_args['bash_filepath']

    if bash_filepath == '':
        bash_filepath = str(main_path / "aws" / "src" / "utils" / "aws_login" / "update_AWS_cred_ecco_production.sh")

    if not upload:
        print('\nDoing dry-run of code. Photos will NOT be uploaded')

    if credential_method_type == 'binary':
        if 'linux' in platform.platform().lower():
            aws_login_file = 'aws-login.linux.amd64'
        else:
            aws_login_file = 'aws-login.darwin.amd64'
    else:
        aws_login_file = 'aws-login.py'

    credential_method = dict()
    credential_method['region'] = region
    credential_method['type'] = credential_method_type
    credential_method['aws_login_file'] = aws_login_file 
    credential_method['bash_filepath'] = bash_filepath

    pprint.pprint(credential_method)
    # Verify credentials
    credentials = credentials_utils.get_aws_credentials()
    pprint.pprint(credentials)
    try:
        if force_reconfigure:
            # Getting new credentials
            credentials = credentials_utils.get_aws_credentials(credential_method)
        elif credentials != {}:
            boto3.setup_default_session(profile_name=credentials['profile_name'])
            try:
                boto3.client('s3').list_buckets()
            except:
                # Present credentials are invalid, try to get new ones
                credentials = credentials_utils.get_aws_credentials(credential_method)
        else:
            # No credentials present, try to get new ones
            credentials = credentials_utils.get_aws_credentials(credential_method)
    except Exception as e:
        print(f'Unable to login to AWS. Exiting')
        print(e)
        sys.exit()

    status = upload_S3(source_dir, bucket, number_of_files, by_field, credential_method, upload=upload)

    if status == 1:
        print(f'\nSuccessfully uploaded photos to {bucket} bucket')
    elif status == 0:
        print(f'\nDid NOT upload photos')
    else:
        print(f'\nUploading photos failed')
