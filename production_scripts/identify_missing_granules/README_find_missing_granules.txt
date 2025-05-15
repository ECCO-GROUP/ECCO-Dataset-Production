Some granules may be missing on s3 after processing json task files To make a new set of task json files for only those missing files do the following

STEP 1. download all of the original tasks json files to a directory (task_dir)

STEP 2. run "aws_find_missing_granules.py" 

This routine will make a complete list of all of the granules we 
wanted to create and save to a target s3 bucket/prefix.
it then checks which files are present in the s3 bucket/prefix.
The task blocks for all missing granules are saved in a new json file
in 'new_task_dir' with a filename that includes the corresponding 
task type [dataset name - grid type - averaging period]

if an AWS profile is needed, it can be provided as an optional argument

---------------------------------------------
$ python aws_find_missing_granules.py -h
usage: aws_find_missing_granules.py [-h] --task_dir TASK_DIR --new_task_dir NEW_TASK_DIR [--aws_profile AWS_PROFILE]

Process task JSONs and find missing granules.

options:
  -h, --help            show this help message and exit
  --task_dir TASK_DIR   Path to directory with task JSON files
  --new_task_dir NEW_TASK_DIR
                        Path for new task output directory
  --aws_profile AWS_PROFILE
                        Optional AWS CLI profile to use
---------------------------------------------

STEP 3. upload the contents of 'new_task_dir' to s3

$ aws s3 sync new_task_dir s3://bucket/prefix/new_task_dir

and re-run the AWS batch on those files. 
Maybe don't use spot instances this second time around,
or else be prepared to run through the process again and again.
but beware if some files are not created because of some 
other error, then simply resubmitted task lists over
and over again will be tragic.


STEP 4 (OPTIONAL). 

You can make n json files that have a random
shuffling of the json tasks. The 'load_and_scatter_json_tasks.py'
program makes up to N_CHUNKS files with generic filenames like
 'chunk_n.json', one for each of the N_CHUNKS you 
specify. Simply specify an input directory that contains a set
of presumably nicely ordered jsons, and an output_dir
which will contain the randomized shuffled jsons output_n.json.

note: even though you ask for N_CHUNKS, you may get fewer
depending on how the number of tasks are distributed. 
For example, if you have 100 missing granules and 
ask for 11 chunks, you will get 10 chunk files each with  
 10 granules instead of 11 chunk files since 100 goes into 10,
not 11. deal with it.


----------------------------------------------
$ python ./load_and_scatter_json_tasks.py -h
usage: load_and_scatter_json_tasks.py [-h] --input_dir INPUT_DIR --output_dir OUTPUT_DIR --n_chunks N_CHUNKS

Combine, shuffle, and split JSON entries.

options:
  -h, --help            show this help message and exit
  --input_dir INPUT_DIR
                        Directory containing input JSON files
  --output_dir OUTPUT_DIR
                        Directory to write output chunks
  --n_chunks N_CHUNKS   Number of output chunks to create
---------------------------------------------
