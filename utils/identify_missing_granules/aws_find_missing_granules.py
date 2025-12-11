#!/usr/bin/env python
# coding: utf-8

# In[27]:


import numpy as np
import json
import boto3
import botocore  
from pathlib import Path
from pprint import pprint

import json
import boto3
import botocore
from boto3.session import Session

from collections import Counter
import datetime

import argparse

# In[28]:


def get_unique_task_names(task_dir):
    all_json_task_files = list(Path(task_dir).glob('*json'))
   
    unique_task_names = []
    
    for tf in all_json_task_files:
        tmp = str(tf).split('/')[-1].split('_task')[0]
        unique_task_names.append(tmp)
        
    unique_task_names= set(unique_task_names)
    
    return np.sort(list(unique_task_names))


# In[29]:


def get_all_tasks_for_task_name(task_dir, task_name):
    
    print('getting all tasks for ', task_name)
    all_json_task_files = list(Path(task_dir).glob('*' + task_name + '*json'))
    return all_json_task_files
    


# In[30]:


def combine_tasks(json_task_files): 

    print('... combining tasks from n json task files n=', len(json_task_files))
    combined = []
    
    for file in json_task_files:
        with open(file, 'r') as f:
            try:
                data = json.load(f)
                combined = combined + data
            except json.JSONDecodeError as e:
                print(f"Skipping {file}: {e}")

    return combined


# In[31]:


def find_duplicate_granules(granule_list):
    counts = Counter(granule_list)

    # Extract duplicates
    duplicates = [item for item, count in counts.items() if count > 1]

    print("!Duplicates:", duplicates)
    print('\n')


# In[32]:


def get_s3_file_info(s3_url):
    if not s3_url.startswith("s3://"):
        return (False, 0)
        
    bucket, key = s3_url[5:].split('/', 1)
    
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        size = response['ContentLength']
        return (True, size)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return (False, 0)
        else:
            raise  # Unexpected error


# In[33]:


def s3_exists(s3_url):
    
    if not s3_url.startswith("s3://"):
        return False
    parts = s3_url[5:].split('/', 1)
    bucket = parts[0]
    key = parts[1]

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise  # Raise unexpected errors


# In[34]:


def count_files_in_s3_path(s3, bucket, prefix, extension='nc'):
    
    # bucket name
    # prefix (the directory equivalent of s3 buckets)
    # extension = ".nc"            # File extension to filter by
    # s3 = boto3.client("s3")

    print(bucket, prefix)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    count = 0
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(extension):
                count += 1
    
    print(f"{count} {extension} files found in s3://{bucket}/{prefix}")
    return count


# In[35]:


def get_all_filenames_in_bucket(s3, bucket, prefix):
        
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    all_keys = []
    
    for page in page_iterator:
        for obj in page.get("Contents", []):
            all_keys.append(obj["Key"])

    return all_keys


# In[36]:


def save_new_task_list(json_output_path, task_name, task_list):

    print('saving # tasks ', len(task_list))
    
    output_filename = json_output_path / str(task_name + '_redo.json')
    print(output_filename)
        
    with open(output_filename, "w") as f:
        json.dump(task_list, f, indent=2)
    


# In[37]:


def create_redo_task_list(task_dir, task_name, new_task_dir, s3):


    print ('\n\n---------------------------------------------------------------------')           
    print (' PROCESSING ', task_name)
    print ('---------------------------------------------------------------------\n')           
        
    # how many parts is this task type divided into?
    # the number of task files of this task type

    # get the list of json files associated with this take type
    # (dataset - grid - averaging period)
    json_task_files = np.sort(list(get_all_tasks_for_task_name(task_dir, task_name)))
    
    print('# of task files for this task type ', len(json_task_files))
    print('\n')
    pprint(json_task_files[:5])
        
    # combine all separate tasks into a single giant list
    # one item per task
    
    # combined tasks (cts)
    cts = combine_tasks(json_task_files)
    
    # extract the granule name from the combined tasks
    granule_filepaths = np.sort([c['granule'] for c in cts])
    print('# of granule filepaths in the task json files ', len(granule_filepaths))
    
    # report duplicate granules 
    find_duplicate_granules(granule_filepaths)
    
    # make a dictionary with the granule name as key
    # and the associated task json text block as the entry
    granule_dict = dict()
    
    # go through all tasks
    for ct in cts:
        # add the task to the dictionary
        # use the final granule filepath as the key
        granule_dict[ct['granule']] = ct
    
    granule_dict_keys = np.sort(list(granule_dict.keys()))
    
    num_granules_in_task = len(granule_dict_keys)
    print('# unique granules in tasks ', num_granules_in_task)
    
    # get the s3 bucket name and prefix from the first file in the granules list
    first_granule_path = Path(granule_filepaths[0])
    
    # ... the s3 bucket name and prefix
    bucket = str(first_granule_path.parent).split('/')[1]; 
    prefix = str(first_granule_path.parent).split(bucket)[1][1:]
    
    # get a list of filenames in the destination bucket
    files_in_bucket = get_all_filenames_in_bucket(s3, bucket, prefix)
    files_in_bucket_full_path = ['s3://' + bucket + '/' + f for f in files_in_bucket]
    
    num_files_in_bucket = len(files_in_bucket_full_path)
    print('found # files in bucket ', num_files_in_bucket)
    
    # Determine which granules are missing and which are present in the destination bucket
    found_granules = []
    missing_granules = []
    
    # loop through all of the granule filepaths extracted from the json task lists
    for g in granule_dict_keys:
        # is the granule filepath present in the bucket
        if g in files_in_bucket_full_path:
            found_granules.append(g) # found
        else:
            missing_granules.append(g) # not found
    
    print('found_granules   ', len(found_granules))
    print('missing_granules ' , len(missing_granules))
    
    # construct a list of tasks the need to be resubmitted
    tasks_to_redo = []
    
    if len(missing_granules) > 0:
        print('\nsome granules missing, adding missing tasks')
            
        for granule_filename in missing_granules:
            tasks_to_redo.append(granule_dict[granule_filename])
    
        save_new_task_list(new_task_dir, task_name, tasks_to_redo)
    
    else:
        print('\nall granules present, nothing to redo')
    
        
#####################################

def main():
    parser = argparse.ArgumentParser(description="Process task JSONs and find missing granules.")

    parser.add_argument("--task_dir", required=True, type=str, help="Path to directory with task JSON files")
    parser.add_argument("--new_task_dir", required=True, type=str, help="Path for new task output directory")
    parser.add_argument("--aws_profile", default=None, help="Optional AWS CLI profile to use")


    args = parser.parse_args()

    # Start boto3 session
    if args.aws_profile:
        session = Session(profile_name=args.aws_profile)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")

    # Resolve input and output paths
    task_dir = Path(args.task_dir).resolve()
    new_task_dir = Path(args.new_task_dir) 

    print("Creating directory for new task JSON files:", new_task_dir)
    try:
        new_task_dir.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        print(f"Directory already exists: {new_task_dir}")

    # Get unique task names
    print('looking in ', task_dir)
    unique_task_names = get_unique_task_names(task_dir)
    print("Unique task names:")
    pprint(unique_task_names)

    # Find and write missing granules for each task
    for task_name in unique_task_names:
        create_redo_task_list(task_dir, task_name, new_task_dir, s3)


if __name__ == "__main__":
    main()

