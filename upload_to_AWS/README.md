# **/upload_to_AWS/**
Contains scripts that allow the user to easily upload files from a local directory to an AWS S3 bucket.

- **aws_sync_directoryies_to_S3.sh**
  - Bash script to mass upload files to S3. Utilizes the AWS CLI *sync* command to upload large number of files quickly to S3. Takes in the following arguments:
    - *credentials type*: Either 'python' or 'binary'. This dictates which aws-login file is called in order to login to AWS (python calls aws-login.py, and binary calls the .amd64 binary file for for system)
    - *credentials file name*: Name of aws-login file to call ('aws-login.py', 'aws-login.linux.amd64', or 'aws-log.darwin.amd64'), which should match the passed *credentials type*
    - *region*: AWS region
    - *login file directory*: Path to the 'aws_login/' directory (or where the python login file is located)
    - *S3 directory*: Path to location in S3 to save files to (i.e. ecco-model-granules/V4r4)
    - *local file directory*: Path to local directory that contains the files to upload (i.e. data/V4r4/mon_mean). Must point to a frequency directory
    - *update_AWS_cred_file*: Path to the file to run for getting AWS credentials (i.e. /processing/src/utils/aws_login/update_AWS_cred_ecco_production.sh)
    - *dryrun*: Boolean, controls whether or not AWS sync is run with the '--dryrun' argument
- **upload_S3.py**
  - Outdated and non-generalized script that uploads files one at a time to an AWS S3 bucket. Run "upload_S3.py --h" to get a description of arguments