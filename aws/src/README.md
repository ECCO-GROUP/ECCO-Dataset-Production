# **/src/**
Contains all the code files required for processing.
- **ecco_cloud_utils**
  - A collection of files and functions from ECCO-ACCESS
- **utils/**
    - **aws_login/**
    - Directory containing scripts needed for logging into AWS and getting the necessary AWS credentials. See https://github.jpl.nasa.gov/cloud/Access-Key-Generation for a description of the files used.
    - **aws_misc_utils.py**
      - Misc utils used for AWS related processes
        - *calculate_all_jobs()*
        - *get_credentials_helper()*
        - *get_aws_credentials()*
    - **create_factors_utils.py**
      - Functions related to creating and getting the mapping factors for processing
        - *get_mapping_factors()*
        - *create_mapping_factors()*
        - *create_land_mask()*
        - *create_all_factors()*
    - **gen_netcdf_utils.py**
      - General functions used while processing and creating the dataset netcdfs
        - *download_all_files()*
        - *delete_files()*
        - *get_land_mask()*
        - *transform_latlon()*
        - *transform_native()*
        - *global_ds_changes()*
        - *sort_attrs()*
        - *find_podaac_metadata()*
        - *apply_podaac_metadata()*
        - *set_metadata()*
    - **lambda_utils.py**
      - Functions for creating and updating lambda functions, and invoking them
        - *update_lambda_functon()*
        - *create_lambda_function()*
        - *invoke_lambda()*
    - **logging_utils.py**
      - Functions for getting AWS CloudWatch logs for the lambda jobs, and then parsing them and saving a log file locally
        - *get_logs()*
        - *save_logs()*
        - *lambda_logging()*
    - **s3_utils.py**
      - Function for getting time steps and filenames for files stored in the specified AWS S3 bucket
        - *get_files_time_steps()*
- **ecco_gen_for_podaac_cloud.py**
  - Main code file for processing. This file takes a payload specifing timesteps and files to process (among other information), and processes said files to produce output datasets.
    - *logging_info()*
    - *generate_netcdfs()*
- **master_script.py**
  - Primary code that the user interacts with. Is the jumping off point for all processing related tasks and processes.
    - *create_parser()*
    - *main*